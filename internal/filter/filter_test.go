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
