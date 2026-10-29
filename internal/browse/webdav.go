package browse

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"synchop/internal/webdav"
)

func BrowseWebDAV(urlStr, user, pass, targetPath string) (*BrowseResult, error) {
	if strings.TrimSpace(urlStr) == "" {
		return nil, fmt.Errorf("WebDAV Server URL is required to browse")
	}

	client, err := webdav.NewClient(urlStr, user, pass, 10*time.Second)
	if err != nil {
		return nil, err
	}

	cleanPath := strings.TrimSpace(targetPath)
	if cleanPath == "" {
		cleanPath = "/"
	}
	if !strings.HasPrefix(cleanPath, "/") {
		cleanPath = "/" + cleanPath
	}

	entries, err := client.ReadDir(cleanPath)
	if err != nil {
		// If custom path fails, try root
		if cleanPath != "/" {
			cleanPath = "/"
			entries, err = client.ReadDir("/")
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read WebDAV folder '%s': %w", cleanPath, err)
		}
	}

	var items []FileItem
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		itemPath := path.Join(cleanPath, entry.Name())
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
	if cleanPath != "/" {
		parentPath = path.Dir(strings.TrimSuffix(cleanPath, "/"))
		if parentPath == "." {
			parentPath = "/"
		}
	}

	return &BrowseResult{
		CurrentPath: cleanPath,
		ParentPath:  parentPath,
		Items:       items,
	}, nil
}
