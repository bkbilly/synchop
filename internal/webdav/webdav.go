package webdav

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/studio-b12/gowebdav"
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

func NewClient(rawURL, user, pass string, timeout time.Duration) (*gowebdav.Client, error) {
	u := strings.TrimSpace(rawURL)
	if u == "" {
		return nil, fmt.Errorf("WebDAV Server URL is required")
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "https://" + u
	}

	c := gowebdav.NewClient(u, user, pass)
	c.SetTimeout(timeout)
	return c, nil
}

// TestConnection verifies connectivity and authentication with the WebDAV endpoint
func TestConnection(rawURL, user, pass, remotePath string) error {
	client, err := NewClient(rawURL, user, pass, 5*time.Second)
	if err != nil {
		return err
	}

	if err := client.Connect(); err != nil {
		return fmt.Errorf("WebDAV connection failed: %w", err)
	}

	path := strings.TrimSpace(remotePath)
	if path != "" && path != "/" {
		if _, err := client.Stat(path); err != nil {
			return fmt.Errorf("connected, but cannot access folder '%s': %w", path, err)
		}
	}

	return nil
}

// SyncFromWebDAV downloads changed files from WebDAV to local staging
func SyncFromWebDAV(ctx context.Context, spec config.EndpointSpec, localDest string, onProgress func(bytes int64, curFile string)) (*SyncResult, error) {
	client, err := NewClient(spec.URL, spec.User, spec.Password, 30*time.Second)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(localDest, 0755); err != nil {
		return nil, fmt.Errorf("cannot create destination dir: %w", err)
	}

	remoteBase := strings.TrimSpace(spec.RemotePath)
	if remoteBase == "" {
		remoteBase = "/"
	}
	if !strings.HasPrefix(remoteBase, "/") {
		remoteBase = "/" + remoteBase
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

		entries, err := client.ReadDir(currentRemoteDir)
		if err != nil {
			return fmt.Errorf("cannot read WebDAV directory '%s': %w", currentRemoteDir, err)
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

			// Check if local file exists and matches
			localFilePath := filepath.Join(localDest, entryRelPath)
			localFi, statErr := os.Stat(localFilePath)
			if statErr == nil && localFi.Size() == entry.Size() && !entry.ModTime().After(localFi.ModTime().Add(time.Second)) {
				continue
			}

			action := "created"
			if statErr == nil {
				action = "modified"
			}

			// Download file with partial resumption support
			remoteFilePath := filepath.ToSlash(filepath.Join(currentRemoteDir, entry.Name()))
			if err := downloadWebDAVFile(ctx, client, remoteFilePath, localFilePath, entry.Size(), entry.ModTime(), func(n int64) {
				totalTransferred += n
				if onProgress != nil {
					onProgress(totalTransferred, entry.Name())
				}
			}); err != nil {
				return fmt.Errorf("failed to download WebDAV file %s: %w", remoteFilePath, err)
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

func downloadWebDAVFile(ctx context.Context, client *gowebdav.Client, remotePath, localPath string, size int64, modTime time.Time, onChunk func(int64)) error {
	partPath := localPath + ".synchop-partial"
	var startOffset int64

	fi, err := os.Stat(partPath)
	if err == nil && fi.Size() < size {
		startOffset = fi.Size()
	} else if err == nil && fi.Size() >= size {
		_ = os.Remove(partPath)
		startOffset = 0
	}

	var stream io.ReadCloser

	if startOffset > 0 {
		stream, err = client.ReadStreamRange(remotePath, startOffset, size-startOffset)
		if err != nil {
			// Fallback to full download if server doesn't support ranges
			stream, err = client.ReadStream(remotePath)
			startOffset = 0
		}
	} else {
		stream, err = client.ReadStream(remotePath)
	}

	if err != nil {
		return err
	}
	defer stream.Close()

	var localFile *os.File
	if startOffset > 0 {
		localFile, err = os.OpenFile(partPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		localFile, err = os.Create(partPath)
	}
	if err != nil {
		return err
	}

	buf := make([]byte, 128*1024)
	for {
		select {
		case <-ctx.Done():
			_ = localFile.Close()
			return ctx.Err()
		default:
		}

		n, rErr := stream.Read(buf)
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
	_ = os.Chtimes(partPath, modTime, modTime)

	return os.Rename(partPath, localPath)
}

// SyncToWebDAV uploads files from local staging to remote WebDAV endpoint
func SyncToWebDAV(ctx context.Context, localSrc string, spec config.EndpointSpec, onProgress func(bytes int64, curFile string), backupDir ...string) (*SyncResult, error) {
	client, err := NewClient(spec.URL, spec.User, spec.Password, 30*time.Second)
	if err != nil {
		return nil, err
	}

	remoteBase := strings.TrimSpace(spec.RemotePath)
	if remoteBase == "" {
		remoteBase = "/"
	}
	if !strings.HasPrefix(remoteBase, "/") {
		remoteBase = "/" + remoteBase
	}
	_ = client.MkdirAll(remoteBase, 0755)

	f := filter.NewFilter(spec.FilterMode, spec.FilterPatterns)
	var totalTransferred int64

	res := &SyncResult{
		ChangedFiles: make([]history.FileChange, 0),
	}

	err = filepath.Walk(localSrc, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(localSrc, path)
		if err != nil || rel == "." {
			return nil
		}

		if !f.ShouldKeep(rel, info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		remotePath := filepath.ToSlash(filepath.Join(remoteBase, rel))

		if info.IsDir() {
			return client.MkdirAll(remotePath, 0755)
		}

		res.FilesScanned++
		res.TotalSize += info.Size()

		// Check if remote file exists and is identical
		remoteFi, statErr := client.Stat(remotePath)
		if statErr == nil && remoteFi.Size() == info.Size() && !info.ModTime().After(remoteFi.ModTime().Add(time.Second)) {
			return nil
		}

		action := "created"
		if statErr == nil {
			action = "modified"
			if len(backupDir) > 0 && backupDir[0] != "" {
				backupFilePath := filepath.ToSlash(filepath.Join(backupDir[0], rel))
				_ = client.MkdirAll(filepath.ToSlash(filepath.Dir(backupFilePath)), 0755)
				_ = client.Rename(remotePath, backupFilePath, true)
			}
		}

		// Upload file
		localFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer localFile.Close()

		countingReader := &progressReader{
			r: localFile,
			onBytes: func(n int64) {
				totalTransferred += n
				if onProgress != nil {
					onProgress(totalTransferred, info.Name())
				}
			},
		}

		if err := client.WriteStreamWithLength(remotePath, countingReader, info.Size(), 0644); err != nil {
			return err
		}

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

type progressReader struct {
	r       io.Reader
	onBytes func(int64)
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if n > 0 && pr.onBytes != nil {
		pr.onBytes(int64(n))
	}
	return n, err
}
