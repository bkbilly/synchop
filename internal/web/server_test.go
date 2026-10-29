package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synchop/internal/browse"
	"synchop/internal/config"
	"synchop/internal/engine"
	"synchop/internal/history"
)

func TestHistoryAPI(t *testing.T) {
	tmpDir := t.TempDir()

	cfgPath := filepath.Join(tmpDir, "config.json")
	store, err := config.LoadStore(cfgPath, tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	histPath := filepath.Join(tmpDir, "history.json")
	histStore, err := history.NewStore(histPath)
	if err != nil {
		t.Fatal(err)
	}

	// Add test runs
	now := time.Now()
	_ = histStore.AddRun(history.SyncRunRecord{
		ID:           "test-run-1",
		StartTime:    now.Add(-2 * time.Minute),
		EndTime:      now.Add(-1 * time.Minute),
		DurationSec:  60,
		Status:       "completed",
		Trigger:      "manual",
		FilesChanged: 4,
		TotalSize:    50000,
		SendSize:     1200,
		Files: []history.FileChange{
			{Path: "doc.txt", Action: "created", Size: 500},
		},
	})

	hub := NewHub()
	eng := engine.NewEngine(store, histStore, hub)
	server := NewServer(store, eng, hub)

	// Test GET /api/history
	req := httptest.NewRequest(http.MethodGet, "/api/history", nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	var res struct {
		Runs    []history.SyncRunRecord `json:"runs"`
		Summary history.HistorySummary  `json:"summary"`
	}
	if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(res.Runs) != 1 || res.Summary.TotalRuns != 1 {
		t.Fatalf("expected 1 run, got %d runs and %d total in summary", len(res.Runs), res.Summary.TotalRuns)
	}
	if res.Runs[0].ID != "test-run-1" || res.Runs[0].FilesChanged != 4 {
		t.Fatalf("unexpected run data: %+v", res.Runs[0])
	}

	// Test GET /api/history?id=test-run-1
	reqID := httptest.NewRequest(http.MethodGet, "/api/history?id=test-run-1", nil)
	wID := httptest.NewRecorder()
	server.ServeHTTP(wID, reqID)

	if wID.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for run by ID, got %d", wID.Code)
	}

	var runByID history.SyncRunRecord
	if err := json.NewDecoder(wID.Body).Decode(&runByID); err != nil {
		t.Fatalf("failed to decode run by ID: %v", err)
	}
	if runByID.ID != "test-run-1" || len(runByID.Files) != 1 {
		t.Fatalf("unexpected run by ID: %+v", runByID)
	}

	// Test DELETE /api/history
	reqDel := httptest.NewRequest(http.MethodDelete, "/api/history", nil)
	wDel := httptest.NewRecorder()
	server.ServeHTTP(wDel, reqDel)

	if wDel.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for delete, got %d", wDel.Code)
	}

	// Verify empty after delete
	reqEmpty := httptest.NewRequest(http.MethodGet, "/api/history", nil)
	wEmpty := httptest.NewRecorder()
	server.ServeHTTP(wEmpty, reqEmpty)

	var resEmpty struct {
		Runs    []history.SyncRunRecord `json:"runs"`
		Summary history.HistorySummary  `json:"summary"`
	}
	_ = json.NewDecoder(wEmpty.Body).Decode(&resEmpty)
	if len(resEmpty.Runs) != 0 || resEmpty.Summary.TotalRuns != 0 {
		t.Fatalf("expected empty runs after clear, got %d", len(resEmpty.Runs))
	}
}

func TestBrowseAPI(t *testing.T) {
	tmpDir := t.TempDir()

	cfgPath := filepath.Join(tmpDir, "config.json")
	store, _ := config.LoadStore(cfgPath, tmpDir)
	histPath := filepath.Join(tmpDir, "history.json")
	histStore, _ := history.NewStore(histPath)
	hub := NewHub()
	eng := engine.NewEngine(store, histStore, hub)
	server := NewServer(store, eng, hub)

	// Create test folder & file
	sub := filepath.Join(tmpDir, "docs")
	_ = os.MkdirAll(sub, 0755)

	reqBody := `{"type":"local", "path":"` + tmpDir + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/browse", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	var res struct {
		Ok          bool               `json:"ok"`
		CurrentPath string             `json:"current_path"`
		ParentPath  string             `json:"parent_path"`
		Items       []browse.FileItem  `json:"items"`
	}
	if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !res.Ok {
		t.Fatalf("expected ok=true, got false")
	}
	if len(res.Items) == 0 {
		t.Fatalf("expected at least 1 item in browse result, got 0")
	}
	if res.Items[0].Name != "docs" || !res.Items[0].IsDir {
		t.Fatalf("expected docs dir, got %+v", res.Items[0])
	}
}
