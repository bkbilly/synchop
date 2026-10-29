package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"synchop/internal/browse"
	"synchop/internal/config"
	"synchop/internal/engine"
	"synchop/internal/smb"
	"synchop/internal/webdav"
)

//go:embed static/*
var staticFS embed.FS

type Server struct {
	store  *config.Store
	engine *engine.Engine
	hub    *Hub
	mux    *http.ServeMux
}

func NewServer(store *config.Store, eng *engine.Engine, hub *Hub) *Server {
	s := &Server{
		store:  store,
		engine: eng,
		hub:    hub,
		mux:    http.NewServeMux(),
	}

	s.routes()
	return s
}

func (s *Server) routes() {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))

	// SSE stream
	s.mux.Handle("/api/events", s.hub)

	// Single-Sync API Endpoints
	s.mux.HandleFunc("/api/sync/state", s.handleSyncState)
	s.mux.HandleFunc("/api/sync/start", s.handleSyncStart)
	s.mux.HandleFunc("/api/sync/stop", s.handleSyncStop)
	s.mux.HandleFunc("/api/config", s.handleConfig)
	s.mux.HandleFunc("/api/test-connection", s.handleTestConnection)
	s.mux.HandleFunc("/api/history", s.handleHistory)
	s.mux.HandleFunc("/api/browse", s.handleBrowse)

	// Static UI assets
	s.mux.Handle("/", fileServer)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleSyncState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.engine.GetState())
}

func (s *Server) handleSyncStart(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.engine.TriggerSync(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (s *Server) handleSyncStop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.engine.AbortSync(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(s.store.Config.Sync)
	case http.MethodPost:
		var syncCfg config.SyncConfig
		if err := json.NewDecoder(r.Body).Decode(&syncCfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.store.UpdateSync(syncCfg); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.engine.StartScheduler()
		json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Type     string `json:"type"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Share    string `json:"share"`
		URL      string `json:"url"`
		User     string `json:"user"`
		Password string `json:"password"`
		Domain   string `json:"domain"`
		Path     string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if req.Type == "ssh" {
		if req.Host == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": "Server Host / IP is required"})
			return
		}
		port := req.Port
		if port == 0 {
			port = 22
		}
		addr := net.JoinHostPort(req.Host, strconv.Itoa(port))
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": fmt.Sprintf("Cannot reach %s (Host is offline or SSH port closed)", addr)})
			return
		}
		_ = conn.Close()
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": fmt.Sprintf("Server reachable on %s:%d", req.Host, port)})
		return
	}

	if req.Type == "smb" {
		if req.Host == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": "NAS / Server Host is required"})
			return
		}
		err := smb.TestConnection(req.Host, req.Port, req.Share, req.User, req.Password, req.Domain)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": err.Error()})
			return
		}
		shareMsg := ""
		if req.Share != "" {
			shareMsg = fmt.Sprintf(" and share '%s' accessible", req.Share)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": fmt.Sprintf("Successfully connected to SMB%s", shareMsg)})
		return
	}

	if req.Type == "webdav" {
		if req.URL == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": "WebDAV Server URL is required"})
			return
		}
		err := webdav.TestConnection(req.URL, req.User, req.Password, req.Path)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": "Successfully connected and verified WebDAV endpoint"})
		return
	}

	if req.Type == "local" {
		if req.Path == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": "Directory path is required"})
			return
		}
		fi, err := os.Stat(req.Path)
		if err != nil {
			if os.IsNotExist(err) {
				json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": "Directory will be created automatically upon sync"})
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": fmt.Sprintf("Path error: %v", err)})
			return
		}
		if !fi.IsDir() {
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "message": "Specified path is a file, not a directory"})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": "Local folder is accessible"})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": "Path format valid"})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	hStore := s.engine.GetHistoryStore()
	if hStore == nil {
		http.Error(w, "History store unavailable", http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodGet:
		id := r.URL.Query().Get("id")
		if id != "" {
			run := hStore.GetRun(id)
			if run == nil {
				http.Error(w, "Run not found", http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(run)
			return
		}

		limit := 50
		if lStr := r.URL.Query().Get("limit"); lStr != "" {
			if lVal, err := strconv.Atoi(lStr); err == nil && lVal > 0 {
				limit = lVal
			}
		}

		offset := 0
		if oStr := r.URL.Query().Get("offset"); oStr != "" {
			if oVal, err := strconv.Atoi(oStr); err == nil && oVal >= 0 {
				offset = oVal
			}
		}

		runs := hStore.GetRuns(limit, offset)
		summary := hStore.GetSummary()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"runs":    runs,
			"summary": summary,
		})

	case http.MethodDelete:
		if err := hStore.Clear(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleBrowse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req browse.BrowseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := browse.Browse(req)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":      false,
			"message": err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":           true,
		"current_path": res.CurrentPath,
		"parent_path":  res.ParentPath,
		"items":        res.Items,
	})
}
