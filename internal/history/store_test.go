package history

import (
	"path/filepath"
	"testing"
	"time"
)

func TestHistoryStore(t *testing.T) {
	tmpDir := t.TempDir()
	historyFile := filepath.Join(tmpDir, "history.json")

	store, err := NewStore(historyFile)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	if len(store.GetRuns(10, 0)) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(store.GetRuns(10, 0)))
	}

	now := time.Now()
	run1 := SyncRunRecord{
		ID:           "run-1",
		StartTime:    now.Add(-10 * time.Second),
		EndTime:      now,
		DurationSec:  10,
		Status:       "completed",
		Trigger:      "manual",
		FilesChanged: 3,
		TotalSize:    1024 * 1024,
		SendSize:     512,
		Files: []FileChange{
			{Path: "file1.txt", Action: "created", Size: 256},
			{Path: "file2.txt", Action: "modified", Size: 256},
			{Path: "file3.txt", Action: "deleted", Size: 0},
		},
	}

	if err := store.AddRun(run1); err != nil {
		t.Fatalf("AddRun failed: %v", err)
	}

	runs := store.GetRuns(10, 0)
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	if runs[0].ID != "run-1" || runs[0].FilesChanged != 3 {
		t.Fatalf("unexpected run data: %+v", runs[0])
	}

	// Test reload from disk
	store2, err := NewStore(historyFile)
	if err != nil {
		t.Fatalf("NewStore reload failed: %v", err)
	}
	runs2 := store2.GetRuns(10, 0)
	if len(runs2) != 1 || runs2[0].ID != "run-1" {
		t.Fatalf("reloaded store mismatch: %+v", runs2)
	}

	summary := store2.GetSummary()
	if summary.TotalRuns != 1 || summary.SuccessfulRuns != 1 || summary.TotalBytesSent != 512 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	// Test GetRun
	found := store2.GetRun("run-1")
	if found == nil || len(found.Files) != 3 {
		t.Fatalf("GetRun failed, got: %+v", found)
	}

	// Test Clear
	if err := store2.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}
	if len(store2.GetRuns(10, 0)) != 0 {
		t.Fatalf("expected 0 runs after clear")
	}
}
