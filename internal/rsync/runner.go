package rsync

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var (
	progress2Regex   = regexp.MustCompile(`([\d,]+)\s+(\d+)%\s+([0-9.]+[A-Za-z/]+s?)\s+([0-9:]+)`)
	itemizedRegex    = regexp.MustCompile(`^\[([*><.cdLhD][^\]]*)\]\s+(\d+)\s+(.+)$`)
	statsFilesRegex  = regexp.MustCompile(`^Number of files:\s+([\d,]+)`)
	statsXferRegex   = regexp.MustCompile(`^Number of regular files transferred:\s+([\d,]+)`)
	statsSizeRegex   = regexp.MustCompile(`^Total file size:\s+([\d,]+)\s+bytes`)
	statsSentRegex   = regexp.MustCompile(`^Total bytes sent:\s+([\d,]+)`)
	statsTotalXfer   = regexp.MustCompile(`^Total transferred file size:\s+([\d,]+)\s+bytes`)
	summarySentRegex = regexp.MustCompile(`^sent\s+([\d,]+)\s+bytes`)
	summarySizeRegex = regexp.MustCompile(`^total size is\s+([\d,]+)`)
)

type ProgressUpdate struct {
	BytesTransferred int64
	Percent          int
	Speed            string
	ETA              string
	RawLine          string
}

type ChangedFile struct {
	Path   string `json:"path"`
	Action string `json:"action"` // "created", "modified", "deleted"
	Size   int64  `json:"size"`
}

type TaskResult struct {
	FilesScanned     int           `json:"files_scanned"`
	FilesChanged     int           `json:"files_changed"`
	TotalSize        int64         `json:"total_size"`
	BytesTransferred int64         `json:"bytes_transferred"`
	TotalBytesSent   int64         `json:"total_bytes_sent"`
	ChangedFiles     []ChangedFile `json:"changed_files"`
}

type SyncTask struct {
	ID             string
	Name           string
	Source         string
	Destination    string
	NetworkTimeout int
	SSHPort        int
	SSHUser        string
	SSHPassword    string
	ExtraArgs      []string
	Compress       bool
	BackupEnabled  bool
	BackupDir      string
	BackupBaseDir  string
}

// BuildRsyncArgs constructs optimal rsync arguments for delta sync, stateful resumption, and strict timeouts
func BuildRsyncArgs(task SyncTask) []string {
	flags := "-av"
	if task.Compress {
		flags = "-avz"
	}
	args := []string{
		flags,
		"-k",
		"--delete",
		"--partial-dir=.synchop-partial",
		"--info=progress2",
		"--out-format=[%i] %l %n",
		"--stats",
	}

	if task.BackupEnabled && task.BackupDir != "" {
		args = append(args, "--backup", fmt.Sprintf("--backup-dir=%s", task.BackupDir))
		base := task.BackupBaseDir
		if base == "" {
			base = ".synchop-backups"
		}
		args = append(args, fmt.Sprintf("--exclude=%s", base))
	}

	netTimeout := task.NetworkTimeout
	if netTimeout <= 0 {
		netTimeout = 15
	}
	args = append(args, fmt.Sprintf("--timeout=%d", netTimeout))

	isRemote := strings.Contains(task.Source, ":") || strings.Contains(task.Destination, ":")

	if isRemote {
		var sshParts []string
		if task.SSHPort > 0 {
			sshParts = append(sshParts, fmt.Sprintf("-p %d", task.SSHPort))
		}
		sshParts = append(sshParts,
			"-o ConnectTimeout="+strconv.Itoa(netTimeout),
			"-o ServerAliveInterval=10",
			"-o ServerAliveCountMax=2",
			"-o StrictHostKeyChecking=accept-new",
			"-o NumberOfPasswordPrompts=1",
		)
		args = append(args, "-e", "ssh "+strings.Join(sshParts, " "))
	}

	if len(task.ExtraArgs) > 0 {
		args = append(args, task.ExtraArgs...)
	}

	args = append(args, task.Source, task.Destination)
	return args
}

func parseCleanInt(s string) (int, error) {
	clean := strings.ReplaceAll(s, ",", "")
	return strconv.Atoi(clean)
}

func parseCleanInt64(s string) (int64, error) {
	clean := strings.ReplaceAll(s, ",", "")
	return strconv.ParseInt(clean, 10, 64)
}

// RunSync executes an rsync process inside its own process group with strict timeouts and streaming updates
func RunSync(ctx context.Context, task SyncTask, onProgress func(ProgressUpdate), onLog func(string)) (*TaskResult, error) {
	// Ensure destination directory parent exists if local
	if !strings.Contains(task.Destination, ":") {
		if err := os.MkdirAll(task.Destination, 0755); err != nil {
			return nil, fmt.Errorf("failed to create destination dir: %w", err)
		}
	}

	args := BuildRsyncArgs(task)
	cmd := exec.CommandContext(ctx, "rsync", args...)

	// Handle SSH password if provided via native SSH_ASKPASS
	if task.SSHPassword != "" {
		exe, err := os.Executable()
		if err == nil {
			cmd.Env = append(os.Environ(),
				"SSH_ASKPASS="+exe,
				"SSH_ASKPASS_REQUIRE=force",
				"SYNCHOP_IS_ASKPASS=1",
				"DISPLAY=:0",
				"SYNCHOP_SSH_PASS="+task.SSHPassword,
			)
		}
	}

	// Create a distinct process group so we can cleanly terminate rsync and any child ssh processes
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe failed: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe failed: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start rsync: %w", err)
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		pgid = cmd.Process.Pid
	}

	result := &TaskResult{
		ChangedFiles: make([]ChangedFile, 0),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Stream stdout (captures progress2, itemized changes, and stats)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		scanner.Split(scanLinesOrCarriageReturns)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			// Progress2 line
			if matches := progress2Regex.FindStringSubmatch(line); len(matches) == 5 {
				bytesVal, _ := parseCleanInt64(matches[1])
				percentVal, _ := strconv.Atoi(matches[2])
				if onProgress != nil {
					onProgress(ProgressUpdate{
						BytesTransferred: bytesVal,
						Percent:          percentVal,
						Speed:            matches[3],
						ETA:              matches[4],
						RawLine:          line,
					})
				}
				continue
			}

			// Itemized changed files line: "[>f+++++++++] 12 file.txt"
			if matches := itemizedRegex.FindStringSubmatch(line); len(matches) == 4 {
				flags := matches[1]
				size, _ := parseCleanInt64(matches[2])
				filePath := matches[3]

				var action string
				if strings.HasPrefix(flags, "*deleting") {
					action = "deleted"
				} else if len(flags) > 1 && flags[1] == 'd' {
					// Directory itemized line (skip directories from file changes)
				} else if strings.HasPrefix(flags, ">f+++++++++") || strings.HasPrefix(flags, "cL+++++++++") {
					action = "created"
				} else if strings.HasPrefix(flags, ">f") || strings.HasPrefix(flags, ".f") || strings.HasPrefix(flags, "cL") || strings.HasPrefix(flags, ">") {
					action = "modified"
				}

				if action != "" {
					result.ChangedFiles = append(result.ChangedFiles, ChangedFile{
						Path:   filePath,
						Action: action,
						Size:   size,
					})
				}
				continue
			}

			// Stats parsing
			if m := statsFilesRegex.FindStringSubmatch(line); len(m) == 2 {
				result.FilesScanned, _ = parseCleanInt(m[1])
			} else if m := statsXferRegex.FindStringSubmatch(line); len(m) == 2 {
				result.FilesChanged, _ = parseCleanInt(m[1])
			} else if m := statsSizeRegex.FindStringSubmatch(line); len(m) == 2 {
				result.TotalSize, _ = parseCleanInt64(m[1])
			} else if m := statsSentRegex.FindStringSubmatch(line); len(m) == 2 {
				result.TotalBytesSent, _ = parseCleanInt64(m[1])
			} else if m := statsTotalXfer.FindStringSubmatch(line); len(m) == 2 {
				result.BytesTransferred, _ = parseCleanInt64(m[1])
			} else if m := summarySentRegex.FindStringSubmatch(line); len(m) == 2 {
				if result.TotalBytesSent == 0 {
					result.TotalBytesSent, _ = parseCleanInt64(m[1])
				}
			} else if m := summarySizeRegex.FindStringSubmatch(line); len(m) == 2 {
				if result.TotalSize == 0 {
					result.TotalSize, _ = parseCleanInt64(m[1])
				}
			}
		}
	}()

	var lastErrMsg string
	var errMu sync.Mutex

	// Stream stderr
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			text := strings.TrimSpace(scanner.Text())
			if text != "" {
				errMu.Lock()
				lastErrMsg = text
				errMu.Unlock()
				if onLog != nil {
					onLog(fmt.Sprintf("[%s ERROR] %s", task.Name, text))
				}
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		wg.Wait()
		done <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		// Kill the entire process group cleanly
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		<-done
		return nil, ctx.Err()
	case err := <-done:
		if result.FilesChanged == 0 && len(result.ChangedFiles) > 0 {
			result.FilesChanged = len(result.ChangedFiles)
		}
		if result.BytesTransferred == 0 {
			result.BytesTransferred = result.TotalBytesSent
		}

		if err != nil {
			errMu.Lock()
			msg := lastErrMsg
			errMu.Unlock()
			if msg != "" {
				return result, fmt.Errorf("%s (code: %w)", msg, err)
			}
			return result, fmt.Errorf("rsync exited with error: %w", err)
		}
		return result, nil
	}
}

// scanLinesOrCarriageReturns splits on both \n and \r to catch in-place terminal progress updates
func scanLinesOrCarriageReturns(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	for i, b := range data {
		if b == '\n' || b == '\r' {
			return i + 1, data[0:i], nil
		}
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}
