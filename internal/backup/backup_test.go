package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateBackupSubDir(t *testing.T) {
	sub := GenerateBackupSubDir(".synchop-backups")
	if !strings.HasPrefix(sub, ".synchop-backups/20") {
		t.Fatalf("unexpected backup sub dir format: %s", sub)
	}

	subEmpty := GenerateBackupSubDir("")
	if !strings.HasPrefix(subEmpty, ".synchop-backups/20") {
		t.Fatalf("expected default fallback for empty base: %s", subEmpty)
	}
}

func TestPruneLocalBackups(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_prune_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	base := ".synchop-backups"
	root := filepath.Join(tmpDir, base)
	_ = os.MkdirAll(root, 0755)

	// Create 5 dated directories
	dirs := []string{
		"2026-10-25_10-00-00",
		"2026-10-26_10-00-00",
		"2026-10-27_10-00-00",
		"2026-10-28_10-00-00",
		"2026-10-29_10-00-00",
	}
	for _, d := range dirs {
		_ = os.MkdirAll(filepath.Join(root, d), 0755)
		_ = os.WriteFile(filepath.Join(root, d, "file.txt"), []byte("test"), 0644)
	}

	// Retention = 3 -> Should remove the 2 oldest (2026-10-25 and 2026-10-26)
	removed, err := PruneLocalBackups(tmpDir, base, 3)
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}

	if len(removed) != 2 {
		t.Fatalf("expected 2 removed, got %d: %v", len(removed), removed)
	}
	if removed[0] != "2026-10-25_10-00-00" || removed[1] != "2026-10-26_10-00-00" {
		t.Fatalf("unexpected removed order: %v", removed)
	}

	// Verify remaining directories
	entries, _ := os.ReadDir(root)
	if len(entries) != 3 {
		t.Fatalf("expected 3 remaining, got %d", len(entries))
	}

	// Running again with Retention = 3 -> Nothing should be removed
	removed2, err := PruneLocalBackups(tmpDir, base, 3)
	if err != nil {
		t.Fatalf("second prune failed: %v", err)
	}
	if len(removed2) != 0 {
		t.Fatalf("expected 0 removed, got %v", removed2)
	}
}
