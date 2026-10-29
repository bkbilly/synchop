package browse

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBrowseLocal(t *testing.T) {
	tmpDir := t.TempDir()

	subDirA := filepath.Join(tmpDir, "folder_a")
	subDirB := filepath.Join(tmpDir, "folder_b")
	_ = os.MkdirAll(subDirA, 0755)
	_ = os.MkdirAll(subDirB, 0755)

	file1 := filepath.Join(tmpDir, "file1.txt")
	file2 := filepath.Join(tmpDir, "file2.pdf")
	_ = os.WriteFile(file1, []byte("test content"), 0644)
	_ = os.WriteFile(file2, []byte("pdf data 12345"), 0644)

	res, err := BrowseLocal(tmpDir)
	if err != nil {
		t.Fatalf("BrowseLocal failed: %v", err)
	}

	if res.CurrentPath != tmpDir {
		t.Errorf("expected current path %s, got %s", tmpDir, res.CurrentPath)
	}

	if len(res.Items) != 4 {
		t.Fatalf("expected 4 items (2 dirs, 2 files), got %d", len(res.Items))
	}

	// Directories must come first
	if !res.Items[0].IsDir || !res.Items[1].IsDir {
		t.Errorf("expected first 2 items to be directories, got %+v, %+v", res.Items[0], res.Items[1])
	}
	if res.Items[0].Name != "folder_a" || res.Items[1].Name != "folder_b" {
		t.Errorf("expected folder_a and folder_b, got %s and %s", res.Items[0].Name, res.Items[1].Name)
	}

	// Files must follow
	if res.Items[2].IsDir || res.Items[3].IsDir {
		t.Errorf("expected items 2 and 3 to be files, got %+v, %+v", res.Items[2], res.Items[3])
	}
}

func TestParseShellLSOutput(t *testing.T) {
	raw := `/var/www/html
./
../
css/
images/
index.html
app.js
.hidden_file
`
	res := parseShellLSOutput(raw)
	if res.CurrentPath != "/var/www/html" {
		t.Errorf("expected /var/www/html, got %s", res.CurrentPath)
	}
	if res.ParentPath != "/var/www" {
		t.Errorf("expected parent /var/www, got %s", res.ParentPath)
	}

	if len(res.Items) != 4 {
		t.Fatalf("expected 4 visible items, got %d", len(res.Items))
	}

	// Dirs first
	if !res.Items[0].IsDir || res.Items[0].Name != "css" {
		t.Errorf("expected css/ dir first, got %+v", res.Items[0])
	}
	if !res.Items[1].IsDir || res.Items[1].Name != "images" {
		t.Errorf("expected images/ dir second, got %+v", res.Items[1])
	}
	// Files next
	if res.Items[2].IsDir || res.Items[2].Name != "app.js" {
		t.Errorf("expected app.js, got %+v", res.Items[2])
	}
	if res.Items[3].IsDir || res.Items[3].Name != "index.html" {
		t.Errorf("expected index.html, got %+v", res.Items[3])
	}
}
