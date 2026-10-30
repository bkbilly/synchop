package filter

import "testing"

func TestIncludeOnly(t *testing.T) {
	f := NewFilter("include_only", "*.txt, *.pdf")

	if !f.ShouldKeep("notes.txt", false) {
		t.Errorf("expected notes.txt to be kept")
	}
	if !f.ShouldKeep("docs/report.pdf", false) {
		t.Errorf("expected docs/report.pdf to be kept")
	}
	if f.ShouldKeep("photo.jpg", false) {
		t.Errorf("expected photo.jpg to be dropped")
	}
	if f.ShouldKeep("video.mp4", false) {
		t.Errorf("expected video.mp4 to be dropped")
	}
	if !f.ShouldKeep("docs", true) {
		t.Errorf("expected directory docs to be kept for traversal")
	}

	rsyncArgs := ToRsyncArgs("include_only", "*.txt")
	expected := []string{"--include=*/", "--include=*.txt", "--exclude=*", "--prune-empty-dirs", "--delete-excluded"}
	if len(rsyncArgs) != len(expected) {
		t.Fatalf("expected %d args, got %d", len(expected), len(rsyncArgs))
	}
}

func TestExclude(t *testing.T) {
	f := NewFilter("exclude", "*.tmp\nnode_modules/")

	if f.ShouldKeep("temp.tmp", false) {
		t.Errorf("expected temp.tmp to be dropped")
	}
	if f.ShouldKeep("node_modules", true) {
		t.Errorf("expected node_modules dir to be dropped")
	}
	if f.ShouldKeep("src/node_modules/index.js", false) {
		t.Errorf("expected file in node_modules to be dropped")
	}
	if !f.ShouldKeep("document.txt", false) {
		t.Errorf("expected document.txt to be kept")
	}

	rsyncArgs := ToRsyncArgs("exclude", "*.tmp, *.bak")
	if len(rsyncArgs) != 3 || rsyncArgs[0] != "--exclude=*.tmp" || rsyncArgs[1] != "--exclude=*.bak" || rsyncArgs[2] != "--delete-excluded" {
		t.Errorf("unexpected rsync exclude args: %v", rsyncArgs)
	}
}

func TestFilterWithSize(t *testing.T) {
	f := NewFilter("exclude", "*.tmp, >100MB")
	if f.MaxFileSize != 100*1024*1024 {
		t.Errorf("expected 104857600 bytes, got %d", f.MaxFileSize)
	}
	rsyncArgs := ToRsyncArgs("exclude", "*.tmp, >100MB")
	hasMaxSize := false
	for _, a := range rsyncArgs {
		if a == "--max-size=100mb" {
			hasMaxSize = true
			break
		}
	}
	if !hasMaxSize {
		t.Errorf("expected --max-size=100mb in args: %v", rsyncArgs)
	}
}

func TestToRsyncArgsForSource(t *testing.T) {
	args := ToRsyncArgsForSource("my_photos", "exclude", "*.iso, cache/")
	hasFilter := false
	for _, a := range args {
		if a == "--filter=- /my_photos/cache/" {
			hasFilter = true
			break
		}
	}
	if !hasFilter {
		t.Errorf("expected scoped filter for cache/ in args: %v", args)
	}
}
