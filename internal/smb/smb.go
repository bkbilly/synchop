package smb

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hirochachacha/go-smb2"
	"synchop/internal/config"
	"synchop/internal/filter"
	"synchop/internal/history"
)

type SyncResult struct {
	FilesScanned     int
	FilesChanged     int
	TotalSize        int64
	BytesTransferred int64
	ChangedFiles     []history.FileChange
}

func DialSMB(host string, port int, user, pass, domain string, timeout time.Duration) (*smb2.Session, net.Conn, error) {
	if port <= 0 {
		port = 445
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot connect to SMB server at %s: %w", addr, err)
	}

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     user,
			Password: pass,
			Domain:   domain,
		},
	}

	session, err := d.Dial(conn)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("SMB authentication failed: %w", err)
	}

	return session, conn, nil
}

// TestConnection dials SMB, authenticates, and attempts to mount the share
func TestConnection(host string, port int, share, user, pass, domain string) error {
	if host == "" {
		return fmt.Errorf("host/IP is required")
	}
	session, conn, err := DialSMB(host, port, user, pass, domain, 4*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer session.Logoff()

	if share != "" {
		cleanShare := strings.Trim(share, `/\`)
		fs, err := session.Mount(cleanShare)
		if err != nil {
			return fmt.Errorf("authentication succeeded, but could not access share '%s': %w", cleanShare, err)
		}
		_ = fs.Umount()
		return nil
	}

	return nil
}

// SyncFromSMB pulls files from a remote SMB share into a local destination directory
func SyncFromSMB(ctx context.Context, spec config.EndpointSpec, localDest string, onProgress func(bytes int64, currentFile string)) (*SyncResult, error) {
	timeout := time.Duration(15) * time.Second
	session, conn, err := DialSMB(spec.Host, spec.Port, spec.User, spec.Password, spec.Domain, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	defer session.Logoff()

	cleanShare := strings.Trim(spec.Share, `/\`)
	if cleanShare == "" {
		return nil, fmt.Errorf("SMB share name is required")
	}

	fs, err := session.Mount(cleanShare)
	if err != nil {
		return nil, fmt.Errorf("failed to mount share '%s': %w", cleanShare, err)
	}
	defer fs.Umount()

	if err := os.MkdirAll(localDest, 0755); err != nil {
		return nil, fmt.Errorf("failed to create local destination: %w", err)
	}

	remoteBase := strings.TrimPrefix(filepath.ToSlash(spec.RemotePath), "/")
	if remoteBase == "" {
		remoteBase = "."
	}

	f := filter.NewFilter(spec.FilterMode, spec.FilterPatterns)
	var totalTransferred int64

	res := &SyncResult{
		ChangedFiles: make([]history.FileChange, 0),
	}

	var syncDir func(relDir string) error
	syncDir = func(relDir string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		currentRemoteDir := remoteBase
		if relDir != "" {
			currentRemoteDir = filepath.ToSlash(filepath.Join(remoteBase, relDir))
		}

		entries, err := fs.ReadDir(currentRemoteDir)
		if err != nil {
			return fmt.Errorf("cannot read remote SMB dir '%s': %w", currentRemoteDir, err)
		}

		localTargetDir := localDest
		if relDir != "" {
			localTargetDir = filepath.Join(localDest, relDir)
		}
		if err := os.MkdirAll(localTargetDir, 0755); err != nil {
			return err
		}

		for _, entry := range entries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			entryRelPath := filepath.Join(relDir, entry.Name())
			if !f.ShouldKeep(entryRelPath, entry.IsDir()) {
				continue
			}

			if entry.IsDir() {
				if err := syncDir(entryRelPath); err != nil {
					return err
				}
				continue
			}

			res.FilesScanned++
			res.TotalSize += entry.Size()

			// File comparison: check if local file exists and matches size and modtime
			localFilePath := filepath.Join(localDest, entryRelPath)
			localFi, statErr := os.Stat(localFilePath)
			if statErr == nil && localFi.Size() == entry.Size() && !entry.ModTime().After(localFi.ModTime().Add(time.Second)) {
				// File is identical, skip download
				continue
			}

			action := "created"
			if statErr == nil {
				action = "modified"
			}

			// Transfer file
			remoteFilePath := filepath.ToSlash(filepath.Join(currentRemoteDir, entry.Name()))
			if err := downloadSMBFile(ctx, fs, remoteFilePath, localFilePath, func(n int64) {
				totalTransferred += n
				if onProgress != nil {
					onProgress(totalTransferred, entry.Name())
				}
			}); err != nil {
				return fmt.Errorf("failed to download %s: %w", remoteFilePath, err)
			}

			res.ChangedFiles = append(res.ChangedFiles, history.FileChange{
				Path:   entryRelPath,
				Action: action,
				Size:   entry.Size(),
			})
			res.FilesChanged++
		}

		return nil
	}

	err = syncDir("")
	res.BytesTransferred = totalTransferred
	if err != nil {
		return res, err
	}
	return res, nil
}

func downloadSMBFile(ctx context.Context, fs *smb2.RemoteFileSystem, remotePath, localPath string, onChunk func(int64)) error {
	remoteFile, err := fs.Open(remotePath)
	if err != nil {
		return err
	}
	defer remoteFile.Close()

	fi, err := remoteFile.Stat()
	if err != nil {
		return err
	}

	partPath := localPath + ".synchop-partial"
	localFile, err := os.Create(partPath)
	if err != nil {
		return err
	}

	buf := make([]byte, 128*1024)
	for {
		select {
		case <-ctx.Done():
			_ = localFile.Close()
			_ = os.Remove(partPath)
			return ctx.Err()
		default:
		}

		n, rErr := remoteFile.Read(buf)
		if n > 0 {
			if _, wErr := localFile.Write(buf[:n]); wErr != nil {
				_ = localFile.Close()
				return wErr
			}
			onChunk(int64(n))
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			_ = localFile.Close()
			return rErr
		}
	}

	_ = localFile.Close()
	_ = os.Chtimes(partPath, fi.ModTime(), fi.ModTime())

	return os.Rename(partPath, localPath)
}

// SyncToSMB pushes files from local staging to remote SMB share
func SyncToSMB(ctx context.Context, localSrc string, spec config.EndpointSpec, onProgress func(bytes int64, currentFile string), backupDir ...string) (*SyncResult, error) {
	timeout := time.Duration(15) * time.Second
	session, conn, err := DialSMB(spec.Host, spec.Port, spec.User, spec.Password, spec.Domain, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	defer session.Logoff()

	cleanShare := strings.Trim(spec.Share, `/\`)
	if cleanShare == "" {
		return nil, fmt.Errorf("SMB share name is required")
	}

	fs, err := session.Mount(cleanShare)
	if err != nil {
		return nil, fmt.Errorf("failed to mount share '%s': %w", cleanShare, err)
	}
	defer fs.Umount()

	remoteBase := strings.TrimPrefix(filepath.ToSlash(spec.RemotePath), "/")
	if remoteBase == "" {
		remoteBase = "."
	}
	_ = fs.MkdirAll(remoteBase, 0755)

	f := filter.NewFilter(spec.FilterMode, spec.FilterPatterns)
	var totalTransferred int64

	res := &SyncResult{
		ChangedFiles: make([]history.FileChange, 0),
	}

	err = filter.WalkDirWithSymlinks(localSrc, func(rel string, info os.FileInfo, fullPath string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !f.ShouldKeep(rel, info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		remotePath := filepath.ToSlash(filepath.Join(remoteBase, rel))

		if info.IsDir() {
			return fs.MkdirAll(remotePath, 0755)
		}

		res.FilesScanned++
		res.TotalSize += info.Size()

		// Check if remote file exists and is identical
		remoteFi, statErr := fs.Stat(remotePath)
		if statErr == nil && remoteFi.Size() == info.Size() && !info.ModTime().After(remoteFi.ModTime().Add(time.Second)) {
			return nil
		}

		action := "created"
		if statErr == nil {
			action = "modified"
			if len(backupDir) > 0 && backupDir[0] != "" {
				backupFilePath := filepath.ToSlash(filepath.Join(backupDir[0], rel))
				_ = fs.MkdirAll(filepath.ToSlash(filepath.Dir(backupFilePath)), 0755)
				_ = fs.Rename(remotePath, backupFilePath)
			}
		}

		// Upload file
		localFile, err := os.Open(fullPath)
		if err != nil {
			return err
		}
		defer localFile.Close()

		remoteFile, err := fs.Create(remotePath)
		if err != nil {
			return err
		}

		buf := make([]byte, 128*1024)
		for {
			select {
			case <-ctx.Done():
				_ = remoteFile.Close()
				return ctx.Err()
			default:
			}

			n, rErr := localFile.Read(buf)
			if n > 0 {
				if _, wErr := remoteFile.Write(buf[:n]); wErr != nil {
					_ = remoteFile.Close()
					return wErr
				}
				totalTransferred += int64(n)
				if onProgress != nil {
					onProgress(totalTransferred, info.Name())
				}
			}
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				_ = remoteFile.Close()
				return rErr
			}
		}
		_ = remoteFile.Close()

		res.ChangedFiles = append(res.ChangedFiles, history.FileChange{
			Path:   rel,
			Action: action,
			Size:   info.Size(),
		})
		res.FilesChanged++

		return nil
	})

	res.BytesTransferred = totalTransferred
	if err != nil {
		return res, err
	}
	return res, nil
}
