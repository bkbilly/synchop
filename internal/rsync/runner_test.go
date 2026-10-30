package rsync

import (
	"strings"
	"testing"
)

func TestItemizedParsing(t *testing.T) {
	lines := []string{
		"[>f+++++++++] 1024 docs/file1.pdf",
		"[>f.st......] 512 docs/file2.txt",
		"[*deleting  ] 0 old.log",
		"[cd+++++++++] 4096 docs",
		"[.d..t......] 80 ./",
	}

	var changed []ChangedFile
	for _, line := range lines {
		if matches := itemizedRegex.FindStringSubmatch(line); len(matches) == 4 {
			flags := matches[1]
			size, _ := parseCleanInt64(matches[2])
			filePath := matches[3]

			var action string
			if strings.HasPrefix(flags, "*deleting") {
				action = "deleted"
			} else if len(flags) > 1 && flags[1] == 'd' {
				// skip
			} else if strings.HasPrefix(flags, ">f+++++++++") || strings.HasPrefix(flags, "cL+++++++++") {
				action = "created"
			} else if strings.HasPrefix(flags, ">f") || strings.HasPrefix(flags, ".f") || strings.HasPrefix(flags, "cL") || strings.HasPrefix(flags, ">") {
				action = "modified"
			}

			if action != "" {
				changed = append(changed, ChangedFile{
					Path:   filePath,
					Action: action,
					Size:   size,
				})
			}
		}
	}

	if len(changed) != 3 {
		t.Fatalf("expected 3 changed files, got %d", len(changed))
	}
	if changed[0].Path != "docs/file1.pdf" || changed[0].Action != "created" || changed[0].Size != 1024 {
		t.Errorf("unexpected file 0: %+v", changed[0])
	}
	if changed[1].Path != "docs/file2.txt" || changed[1].Action != "modified" || changed[1].Size != 512 {
		t.Errorf("unexpected file 1: %+v", changed[1])
	}
	if changed[2].Path != "old.log" || changed[2].Action != "deleted" || changed[2].Size != 0 {
		t.Errorf("unexpected file 2: %+v", changed[2])
	}
}

func TestStatsParsing(t *testing.T) {
	lines := []string{
		"Number of files: 2,450 (reg: 2,000, dir: 450)",
		"Number of created files: 12 (reg: 12)",
		"Number of regular files transferred: 34",
		"Total file size: 1,048,576,000 bytes",
		"Total transferred file size: 10,485,760 bytes",
		"Total bytes sent: 11,000,000",
		"Total bytes received: 50,000",
	}

	var res TaskResult
	for _, line := range lines {
		if m := statsFilesRegex.FindStringSubmatch(line); len(m) == 2 {
			res.FilesScanned, _ = parseCleanInt(m[1])
		} else if m := statsXferRegex.FindStringSubmatch(line); len(m) == 2 {
			res.FilesChanged, _ = parseCleanInt(m[1])
		} else if m := statsSizeRegex.FindStringSubmatch(line); len(m) == 2 {
			res.TotalSize, _ = parseCleanInt64(m[1])
		} else if m := statsSentRegex.FindStringSubmatch(line); len(m) == 2 {
			res.TotalBytesSent, _ = parseCleanInt64(m[1])
		} else if m := statsTotalXfer.FindStringSubmatch(line); len(m) == 2 {
			res.BytesTransferred, _ = parseCleanInt64(m[1])
		}
	}

	if res.FilesScanned != 2450 {
		t.Errorf("expected 2450 scanned, got %d", res.FilesScanned)
	}
	if res.FilesChanged != 34 {
		t.Errorf("expected 34 changed, got %d", res.FilesChanged)
	}
	if res.TotalSize != 1048576000 {
		t.Errorf("expected 1048576000 total size, got %d", res.TotalSize)
	}
	if res.BytesTransferred != 10485760 {
		t.Errorf("expected 10485760 transferred, got %d", res.BytesTransferred)
	}
	if res.TotalBytesSent != 11000000 {
		t.Errorf("expected 11000000 sent, got %d", res.TotalBytesSent)
	}
}

func TestBuildRsyncArgsWithBackup(t *testing.T) {
	task := SyncTask{
		ID:             "dest-1",
		Source:         "/staging/",
		Destination:    "user@host:/backup/dest",
		BackupEnabled:  true,
		BackupDir:      ".synchop-backups/2026-10-30_12-00-00",
		BackupBaseDir:  ".synchop-backups",
		NetworkTimeout: 20,
	}

	args := BuildRsyncArgs(task)
	argStr := strings.Join(args, " ")

	if !strings.Contains(argStr, "--backup") {
		t.Errorf("expected --backup in args: %s", argStr)
	}
	if !strings.Contains(argStr, "--backup-dir=.synchop-backups/2026-10-30_12-00-00") {
		t.Errorf("expected --backup-dir in args: %s", argStr)
	}
	if !strings.Contains(argStr, "--exclude=.synchop-backups") {
		t.Errorf("expected --exclude in args: %s", argStr)
	}
}

func TestBuildRsyncArgsCompression(t *testing.T) {
	taskNoComp := SyncTask{
		ID:          "dest-1",
		Source:      "/src/",
		Destination: "/dst/",
		Compress:    false,
	}
	argsNoComp := BuildRsyncArgs(taskNoComp)
	if argsNoComp[0] != "-av" {
		t.Errorf("expected -av when Compress is false, got %s", argsNoComp[0])
	}

	taskComp := SyncTask{
		ID:          "dest-2",
		Source:      "/src/",
		Destination: "/dst/",
		Compress:    true,
	}
	argsComp := BuildRsyncArgs(taskComp)
	if argsComp[0] != "-avz" {
		t.Errorf("expected -avz when Compress is true, got %s", argsComp[0])
	}
}
