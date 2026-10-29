package browse

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"synchop/internal/smb"
)

func BrowseSMB(host string, port int, share, user, pass, domain, targetPath string) (*BrowseResult, error) {
	if strings.TrimSpace(host) == "" {
		return nil, fmt.Errorf("NAS / Server Host is required to browse")
	}

	cleanShare := strings.Trim(strings.TrimSpace(share), `/\`)
	if cleanShare == "" {
		return nil, fmt.Errorf("Share Name is required to browse SMB")
	}

	session, conn, err := smb.DialSMB(host, port, user, pass, domain, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	defer session.Logoff()

	fs, err := session.Mount(cleanShare)
	if err != nil {
		return nil, fmt.Errorf("failed to mount share '%s': %w", cleanShare, err)
	}
	defer fs.Umount()

	cleanPath := strings.Trim(filepath.ToSlash(targetPath), "/")
	if cleanPath == "" {
		cleanPath = "."
	}

	entries, err := fs.ReadDir(cleanPath)
	if err != nil {
		// If subdirectory doesn't exist, try root
		if cleanPath != "." {
			cleanPath = "."
			entries, err = fs.ReadDir(".")
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read folder inside share '%s': %w", cleanShare, err)
		}
	}

	var items []FileItem
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		itemPath := entry.Name()
		if cleanPath != "." && cleanPath != "" {
			itemPath = filepath.ToSlash(filepath.Join(cleanPath, entry.Name()))
		}
		if !strings.HasPrefix(itemPath, "/") {
			itemPath = "/" + itemPath
		}

		items = append(items, FileItem{
			Name:  entry.Name(),
			Path:  itemPath,
			IsDir: entry.IsDir(),
			Size:  entry.Size(),
		})
	}

	// Sort directories first, then alphabetical
	sort.Slice(items, func(i, j int) bool {
		if items[i].IsDir != items[j].IsDir {
			return items[i].IsDir
		}
		return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
	})

	parentPath := ""
	currentDisplay := "/"
	if cleanPath != "." && cleanPath != "" {
		currentDisplay = "/" + cleanPath
		parentPath = filepath.ToSlash(filepath.Dir(cleanPath))
		if parentPath == "." {
			parentPath = "/"
		} else if !strings.HasPrefix(parentPath, "/") {
			parentPath = "/" + parentPath
		}
	}

	return &BrowseResult{
		CurrentPath: currentDisplay,
		ParentPath:  parentPath,
		Items:       items,
	}, nil
}
