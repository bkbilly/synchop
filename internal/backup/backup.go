package backup

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"synchop/internal/config"
	"synchop/internal/smb"
	"synchop/internal/webdav"
)

// GenerateBackupSubDir creates a timestamped subfolder path inside baseDirName, e.g. ".synchop-backups/2026-10-30_00-15-00"
func GenerateBackupSubDir(baseDirName string) string {
	cleanBase := strings.Trim(baseDirName, `/\`)
	if cleanBase == "" {
		cleanBase = ".synchop-backups"
	}
	ts := time.Now().Format("2006-01-02_15-04-05")
	return fmt.Sprintf("%s/%s", cleanBase, ts)
}

// PruneBackups prunes historical backup directories exceeding retentionCount
func PruneBackups(ctx context.Context, dest config.EndpointSpec, baseDirName string, retentionCount int, timeoutSec int) ([]string, error) {
	if retentionCount <= 0 {
		return nil, nil
	}

	cleanBase := strings.Trim(baseDirName, `/\`)
	if cleanBase == "" {
		cleanBase = ".synchop-backups"
	}

	switch dest.Type {
	case config.TypeLocal:
		return PruneLocalBackups(dest.RemotePath, cleanBase, retentionCount)
	case config.TypeSSH:
		return PruneSSHBackups(ctx, dest, cleanBase, retentionCount, timeoutSec)
	case config.TypeSMB:
		return PruneSMBBackups(ctx, dest, cleanBase, retentionCount, timeoutSec)
	case config.TypeWebDAV:
		return PruneWebDAVBackups(ctx, dest, cleanBase, retentionCount, timeoutSec)
	default:
		return nil, nil
	}
}

// PruneLocalBackups removes oldest backup directories from local destination
func PruneLocalBackups(destDir, baseDirName string, retentionCount int) ([]string, error) {
	if retentionCount <= 0 {
		return nil, nil
	}
	backupRoot := filepath.Join(destDir, baseDirName)
	entries, err := os.ReadDir(backupRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}

	sort.Strings(dirs)
	if len(dirs) <= retentionCount {
		return nil, nil
	}

	toRemove := dirs[:len(dirs)-retentionCount]
	var removed []string
	for _, d := range toRemove {
		fullPath := filepath.Join(backupRoot, d)
		if err := os.RemoveAll(fullPath); err == nil {
			removed = append(removed, d)
		}
	}
	return removed, nil
}

// PruneSSHBackups removes oldest backup directories over SSH
func PruneSSHBackups(ctx context.Context, dest config.EndpointSpec, baseDirName string, retentionCount int, timeoutSec int) ([]string, error) {
	if retentionCount <= 0 {
		return nil, nil
	}

	if timeoutSec <= 0 {
		timeoutSec = 15
	}

	port := dest.Port
	if port <= 0 {
		port = 22
	}

	var authMethods []ssh.AuthMethod
	if dest.Password != "" {
		authMethods = append(authMethods, ssh.Password(dest.Password))
	}

	sshConfig := &ssh.ClientConfig{
		User:            dest.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(timeoutSec) * time.Second,
	}

	addr := net.JoinHostPort(dest.Host, strconv.Itoa(port))
	client, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("ssh dial failed: %w", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("ssh session failed: %w", err)
	}
	defer session.Close()

	// POSIX-compliant pruning script
	destPath := strings.TrimRight(dest.RemotePath, "/")
	backupRoot := destPath + "/" + baseDirName

	cmd := fmt.Sprintf(`cd "%s" 2>/dev/null || exit 0
DIRS=$(ls -1d 20* 2>/dev/null | sort)
COUNT=$(echo "$DIRS" | grep -v '^$' | wc -l)
EXCESS=$((COUNT - %d))
if [ "$EXCESS" -gt 0 ]; then
  echo "$DIRS" | head -n "$EXCESS" | while read -r d; do
    [ -n "$d" ] && rm -rf "$d" && echo "$d"
  done
fi`, backupRoot, retentionCount)

	out, err := session.CombinedOutput(cmd)
	if err != nil {
		return nil, fmt.Errorf("prune ssh command failed: %w: %s", err, string(out))
	}

	var removed []string
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed != "" {
			removed = append(removed, trimmed)
		}
	}
	return removed, nil
}

// PruneSMBBackups removes oldest backup directories on SMB share
func PruneSMBBackups(ctx context.Context, dest config.EndpointSpec, baseDirName string, retentionCount int, timeoutSec int) ([]string, error) {
	if retentionCount <= 0 {
		return nil, nil
	}

	if timeoutSec <= 0 {
		timeoutSec = 15
	}

	session, conn, err := smb.DialSMB(dest.Host, dest.Port, dest.User, dest.Password, dest.Domain, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	defer session.Logoff()

	cleanShare := strings.Trim(dest.Share, `/\`)
	fs, err := session.Mount(cleanShare)
	if err != nil {
		return nil, err
	}
	defer fs.Umount()

	cleanRemote := strings.TrimPrefix(filepath.ToSlash(dest.RemotePath), "/")
	backupRoot := filepath.ToSlash(filepath.Join(cleanRemote, baseDirName))

	entries, err := fs.ReadDir(backupRoot)
	if err != nil {
		return nil, nil // directory may not exist yet
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}

	sort.Strings(dirs)
	if len(dirs) <= retentionCount {
		return nil, nil
	}

	toRemove := dirs[:len(dirs)-retentionCount]
	var removed []string
	for _, d := range toRemove {
		fullPath := filepath.ToSlash(filepath.Join(backupRoot, d))
		if err := fs.RemoveAll(fullPath); err == nil {
			removed = append(removed, d)
		}
	}
	return removed, nil
}

// PruneWebDAVBackups removes oldest backup directories on WebDAV endpoint
func PruneWebDAVBackups(ctx context.Context, dest config.EndpointSpec, baseDirName string, retentionCount int, timeoutSec int) ([]string, error) {
	if retentionCount <= 0 {
		return nil, nil
	}

	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	client, err := webdav.NewClient(dest.URL, dest.User, dest.Password, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		return nil, err
	}

	cleanRemote := strings.TrimSpace(dest.RemotePath)
	if cleanRemote == "" {
		cleanRemote = "/"
	}
	backupRoot := filepath.ToSlash(filepath.Join(cleanRemote, baseDirName))

	entries, err := client.ReadDir(backupRoot)
	if err != nil {
		return nil, nil
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}

	sort.Strings(dirs)
	if len(dirs) <= retentionCount {
		return nil, nil
	}

	toRemove := dirs[:len(dirs)-retentionCount]
	var removed []string
	for _, d := range toRemove {
		fullPath := filepath.ToSlash(filepath.Join(backupRoot, d))
		if err := client.RemoveAll(fullPath); err == nil {
			removed = append(removed, d)
		}
	}
	return removed, nil
}
