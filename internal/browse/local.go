package browse

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func BrowseLocal(targetPath string) (*BrowseResult, error) {
	cleanPath := strings.TrimSpace(targetPath)

	if cleanPath == "" || cleanPath == "~" || strings.HasPrefix(cleanPath, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			if cleanPath == "" || cleanPath == "~" {
				cleanPath = home
			} else {
				cleanPath = filepath.Join(home, cleanPath[2:])
			}
		} else if cleanPath == "" {
			cleanPath = "/"
		}
	}

	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}

	fi, err := os.Stat(absPath)
	if err != nil {
		// If path doesn't exist, try its parent directory
		parent := filepath.Dir(absPath)
		if pFi, pErr := os.Stat(parent); pErr == nil && pFi.IsDir() {
			absPath = parent
		} else {
			return nil, fmt.Errorf("path does not exist: %w", err)
		}
	} else if !fi.IsDir() {
		absPath = filepath.Dir(absPath)
	}

	entries, err := os.ReadDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read directory '%s': %w", absPath, err)
	}

	var items []FileItem
	for _, entry := range entries {
		// Ignore hidden files / directories
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		isDir := entry.IsDir()
		var size int64
		if !isDir {
			if info, err := entry.Info(); err == nil {
				size = info.Size()
			}
		}

		itemPath := filepath.Join(absPath, entry.Name())
		items = append(items, FileItem{
			Name:  entry.Name(),
			Path:  itemPath,
			IsDir: isDir,
			Size:  size,
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
	if absPath != "/" {
		parentPath = filepath.Dir(absPath)
	}

	return &BrowseResult{
		CurrentPath: absPath,
		ParentPath:  parentPath,
		Items:       items,
	}, nil
}
