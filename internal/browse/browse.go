package browse

import (
	"fmt"
	"strings"
)

type FileItem struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	IsDir   bool   `json:"is_dir"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time,omitempty"`
}

type BrowseResult struct {
	CurrentPath string     `json:"current_path"`
	ParentPath  string     `json:"parent_path"`
	Items       []FileItem `json:"items"`
}

type BrowseRequest struct {
	Type     string `json:"type"` // "local", "ssh", "smb", "webdav"
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Share    string `json:"share"`
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
	Domain   string `json:"domain"`
	Path     string `json:"path"`
}

func Browse(req BrowseRequest) (*BrowseResult, error) {
	switch strings.ToLower(strings.TrimSpace(req.Type)) {
	case "local":
		return BrowseLocal(req.Path)
	case "ssh":
		return BrowseSSH(req.Host, req.Port, req.User, req.Password, req.Path)
	case "smb":
		return BrowseSMB(req.Host, req.Port, req.Share, req.User, req.Password, req.Domain, req.Path)
	case "webdav":
		return BrowseWebDAV(req.URL, req.User, req.Password, req.Path)
	default:
		return nil, fmt.Errorf("unsupported browse type: %s", req.Type)
	}
}
