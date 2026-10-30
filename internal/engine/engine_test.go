package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"synchop/internal/config"
	"synchop/internal/filter"
	"synchop/internal/history"
)

type dummyBroadcaster struct{}

func (d *dummyBroadcaster) BroadcastState(_ SyncState) {}

func TestEngineHistoryIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	srcDir := filepath.Join(tmpDir, "source1")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello world sync test!"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "data.bin"), make([]byte, 2048), 0644); err != nil {
		t.Fatal(err)
	}

	destDir := filepath.Join(tmpDir, "destination")
	stagingDir := filepath.Join(tmpDir, "staging")

	configPath := filepath.Join(tmpDir, "config.json")
	store, err := config.LoadStore(configPath, tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	syncCfg := store.Config.Sync
	syncCfg.LocalStagingDir = stagingDir
	syncCfg.Sources = []config.EndpointSpec{
		{
			ID:         "src-1",
			Name:       "Test Local Source",
			Type:       config.TypeLocal,
			RemotePath: srcDir,
		},
	}
	syncCfg.Destination = config.EndpointSpec{
		ID:         "dest-1",
		Name:       "Test Local Destination",
		Type:       config.TypeLocal,
		RemotePath: destDir,
	}

	if err := store.UpdateSync(syncCfg); err != nil {
		t.Fatal(err)
	}

	historyPath := filepath.Join(tmpDir, "history.json")
	histStore, err := history.NewStore(historyPath)
	if err != nil {
		t.Fatal(err)
	}

	eng := NewEngine(store, histStore, &dummyBroadcaster{})

	// 1st Run: Should sync 2 files
	ctx := context.Background()
	if err := eng.RunSyncBlocking(ctx, "cli"); err != nil {
		t.Fatalf("RunSyncBlocking failed: %v", err)
	}

	runs := histStore.GetRuns(10, 0)
	if len(runs) != 1 {
		t.Fatalf("expected 1 history run, got %d", len(runs))
	}

	run1 := runs[0]
	if run1.Status != "completed" {
		t.Errorf("expected completed status, got %s", run1.Status)
	}
	if run1.Trigger != "cli" {
		t.Errorf("expected cli trigger, got %s", run1.Trigger)
	}
	if run1.FilesChanged == 0 {
		t.Errorf("expected files changed > 0, got %d", run1.FilesChanged)
	}
	if run1.TotalSize == 0 {
		t.Errorf("expected total size > 0, got %d", run1.TotalSize)
	}
	if run1.SendSize == 0 {
		t.Errorf("expected send size > 0, got %d", run1.SendSize)
	}
	if len(run1.Files) == 0 {
		t.Errorf("expected changed files list not to be empty")
	}

	t.Logf("Run 1 success: FilesChanged=%d, TotalSize=%d, SendSize=%d, ChangedFiles=%+v",
		run1.FilesChanged, run1.TotalSize, run1.SendSize, run1.Files)

	// 2nd Run: No changes made, should report 0 files changed
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("2nd RunSyncBlocking failed: %v", err)
	}

	runs2 := histStore.GetRuns(10, 0)
	if len(runs2) != 2 {
		t.Fatalf("expected 2 history runs, got %d", len(runs2))
	}

	run2 := runs2[0] // newest is first
	if run2.Status != "completed" {
		t.Errorf("expected completed status on run2, got %s", run2.Status)
	}
	if run2.FilesChanged != 0 {
		t.Errorf("expected 0 files changed on run2, got %d (files: %+v)", run2.FilesChanged, run2.Files)
	}
	if run2.TotalSize == 0 {
		t.Errorf("expected total size > 0 on run2, got %d", run2.TotalSize)
	}

	t.Logf("Run 2 success: FilesChanged=%d, TotalSize=%d, SendSize=%d",
		run2.FilesChanged, run2.TotalSize, run2.SendSize)

	// 3rd Run: Modify 1 file and add 1 file
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("modified hello world sync test!"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "new_file.txt"), []byte("brand new file"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := eng.RunSyncBlocking(ctx, "scheduled"); err != nil {
		t.Fatalf("3rd RunSyncBlocking failed: %v", err)
	}

	runs3 := histStore.GetRuns(10, 0)
	if len(runs3) != 3 {
		t.Fatalf("expected 3 history runs, got %d", len(runs3))
	}
	run3 := runs3[0]
	if run3.Status != "completed" {
		t.Errorf("expected completed status on run3, got %s", run3.Status)
	}
	if run3.Trigger != "scheduled" {
		t.Errorf("expected trigger 'scheduled', got %s", run3.Trigger)
	}
	if run3.FilesChanged == 0 {
		t.Errorf("expected files changed > 0 on run3, got %d", run3.FilesChanged)
	}
	t.Logf("Run 3 success: FilesChanged=%d, TotalSize=%d, SendSize=%d, ChangedFiles=%+v",
		run3.FilesChanged, run3.TotalSize, run3.SendSize, run3.Files)
}

func TestEngineBackupRetentionIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "synchop_backup_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	srcDir := filepath.Join(tmpDir, "source")
	destDir := filepath.Join(tmpDir, "destination")
	stagingDir := filepath.Join(tmpDir, "staging")
	cfgPath := filepath.Join(tmpDir, "config.json")
	histPath := filepath.Join(tmpDir, "history.json")

	_ = os.MkdirAll(srcDir, 0755)
	_ = os.MkdirAll(destDir, 0755)
	_ = os.MkdirAll(stagingDir, 0755)

	store, err := config.LoadStore(cfgPath, tmpDir)
	if err != nil {
		t.Fatalf("failed to create config store: %v", err)
	}

	histStore, err := history.NewStore(histPath)
	if err != nil {
		t.Fatalf("failed to create history store: %v", err)
	}

	eng := NewEngine(store, histStore, nil)

	store.Config.Sync.LocalStagingDir = stagingDir
	store.Config.Sync.BackupEnabled = true
	store.Config.Sync.BackupRetentionCount = 2
	store.Config.Sync.BackupDirName = ".synchop-backups"
	store.Config.Sync.Sources = []config.EndpointSpec{
		{
			ID:         "src-1",
			Name:       "test_src",
			Type:       config.TypeLocal,
			RemotePath: srcDir,
		},
	}
	store.Config.Sync.Destination = config.EndpointSpec{
		ID:         "dest-1",
		Name:       "test_dest",
		Type:       config.TypeLocal,
		RemotePath: destDir,
	}

	ctx := context.Background()

	// 1. Create file in source and do initial sync
	_ = os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("version 1"), 0644)
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("Run 1 failed: %v", err)
	}

	// 2. Modify file in source and do second sync (version 1 should be backed up)
	time.Sleep(1100 * time.Millisecond) // ensure distinct timestamp
	_ = os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("version 2"), 0644)
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("Run 2 failed: %v", err)
	}

	backupRoot := filepath.Join(destDir, ".synchop-backups")
	entries, err := os.ReadDir(backupRoot)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected 1 backup dir, got %d (err: %v)", len(entries), err)
	}

	// 3. Modify again and do third sync (version 2 should be backed up -> 2 backup dirs total)
	time.Sleep(1100 * time.Millisecond)
	_ = os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("version 3"), 0644)
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("Run 3 failed: %v", err)
	}

	entries2, err := os.ReadDir(backupRoot)
	if err != nil || len(entries2) != 2 {
		t.Fatalf("expected 2 backup dirs, got %d", len(entries2))
	}

	// 4. Modify again and do fourth sync (retention count is 2 -> oldest should be pruned, still 2 backup dirs)
	time.Sleep(1100 * time.Millisecond)
	_ = os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("version 4"), 0644)
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("Run 4 failed: %v", err)
	}

	entries3, err := os.ReadDir(backupRoot)
	if err != nil || len(entries3) != 2 {
		t.Fatalf("expected 2 backup dirs after retention pruning, got %d", len(entries3))
	}

	t.Logf("Backup retention verified! Current backup dirs: %s, %s", entries3[0].Name(), entries3[1].Name())
}

func TestEngineLocalSourceNoCopyIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	srcDir := filepath.Join(tmpDir, "local_photos")
	destDir := filepath.Join(tmpDir, "dest_backup")
	stagingDir := filepath.Join(tmpDir, "temp_staging")
	cfgPath := filepath.Join(tmpDir, "config.json")
	histPath := filepath.Join(tmpDir, "history.json")

	_ = os.MkdirAll(srcDir, 0755)
	_ = os.MkdirAll(destDir, 0755)
	_ = os.MkdirAll(stagingDir, 0755)

	// Create test files in local source
	_ = os.WriteFile(filepath.Join(srcDir, "image1.jpg"), []byte("photo 1 binary data"), 0644)
	_ = os.WriteFile(filepath.Join(srcDir, "image2.jpg"), []byte("photo 2 binary data"), 0644)

	store, err := config.LoadStore(cfgPath, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	histStore, err := history.NewStore(histPath)
	if err != nil {
		t.Fatal(err)
	}

	eng := NewEngine(store, histStore, nil)
	store.Config.Sync.LocalStagingDir = stagingDir
	store.Config.Sync.Sources = []config.EndpointSpec{
		{
			ID:         "src-local",
			Name:       "photos",
			Type:       config.TypeLocal,
			RemotePath: srcDir,
		},
	}
	store.Config.Sync.Destination = config.EndpointSpec{
		ID:         "dest-local",
		Name:       "backup",
		Type:       config.TypeLocal,
		RemotePath: destDir,
	}

	ctx := context.Background()
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// 1. Verify that stagingDir/photos is a symlink, NOT a copied directory
	stagingEntry := filepath.Join(stagingDir, "photos")
	lfi, err := os.Lstat(stagingEntry)
	if err != nil {
		t.Fatalf("staging entry not found: %v", err)
	}
	if lfi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected staging entry to be a symlink (direct reference), but it is a real directory (copied)!")
	}
	target, _ := os.Readlink(stagingEntry)
	if target != srcDir {
		t.Fatalf("expected symlink target %s, got %s", srcDir, target)
	}

	// 2. Verify destination has the actual files
	destFile1 := filepath.Join(destDir, "photos", "image1.jpg")
	data1, err := os.ReadFile(destFile1)
	if err != nil || string(data1) != "photo 1 binary data" {
		t.Fatalf("destination file1 missing or mismatch: %v", err)
	}

	destFile2 := filepath.Join(destDir, "photos", "image2.jpg")
	data2, err := os.ReadFile(destFile2)
	if err != nil || string(data2) != "photo 2 binary data" {
		t.Fatalf("destination file2 missing or mismatch: %v", err)
	}

	// 3. Modify local file directly, sync again
	_ = os.WriteFile(filepath.Join(srcDir, "image1.jpg"), []byte("photo 1 updated"), 0644)
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("second sync failed: %v", err)
	}

	data1Updated, _ := os.ReadFile(destFile1)
	if string(data1Updated) != "photo 1 updated" {
		t.Fatalf("expected updated file on destination, got %s", string(data1Updated))
	}

	t.Log("Verified: local directories are accessed directly via zero-copy symlinks without copying to temporary staging!")
}

func TestScanLocalDirectoryProgressive(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a nested directory tree with known files
	sub1 := filepath.Join(tmpDir, "sub1")
	sub2 := filepath.Join(tmpDir, "sub2")
	sub3 := filepath.Join(sub1, "nested")
	_ = os.MkdirAll(sub1, 0755)
	_ = os.MkdirAll(sub2, 0755)
	_ = os.MkdirAll(sub3, 0755)

	_ = os.WriteFile(filepath.Join(sub1, "a.txt"), []byte("12345"), 0644)
	_ = os.WriteFile(filepath.Join(sub2, "b.txt"), []byte("1234567890"), 0644)
	_ = os.WriteFile(filepath.Join(sub3, "c.txt"), []byte("123456789012345"), 0644)

	eng := &Engine{}
	var callbackCalls int
	var lastPercent int
	var lastEta string

	totalBytes, fileCount, err := eng.scanLocalDirectoryProgressive(context.Background(), tmpDir, nil, func(files int, bytes int64, pct int, speed, eta string) {
		callbackCalls++
		lastPercent = pct
		lastEta = eta
	})

	if err != nil {
		t.Fatalf("scanLocalDirectoryProgressive failed: %v", err)
	}
	if fileCount != 3 {
		t.Errorf("expected 3 files, got %d", fileCount)
	}
	if totalBytes != 30 {
		t.Errorf("expected 30 bytes, got %d", totalBytes)
	}
	t.Logf("Scan completed: files=%d, bytes=%d, callbackCalls=%d, lastPercent=%d, lastEta=%s", fileCount, totalBytes, callbackCalls, lastPercent, lastEta)
}

func TestScanLocalDirectoryProgressiveWithFilter(t *testing.T) {
	tmpDir := t.TempDir()

	sub := filepath.Join(tmpDir, "sub")
	bigDir := filepath.Join(tmpDir, "big_folder")
	_ = os.MkdirAll(sub, 0755)
	_ = os.MkdirAll(bigDir, 0755)

	_ = os.WriteFile(filepath.Join(sub, "keep.txt"), []byte("keep me"), 0644)
	_ = os.WriteFile(filepath.Join(sub, "large.iso"), []byte("large file content"), 0644)
	_ = os.WriteFile(filepath.Join(bigDir, "inside_big.txt"), []byte("inside big"), 0644)

	eng := &Engine{}
	filt := filter.NewFilter("exclude", "*.iso, big_folder")

	totalBytes, fileCount, err := eng.scanLocalDirectoryProgressive(context.Background(), tmpDir, filt, nil)
	if err != nil {
		t.Fatalf("scan with filter failed: %v", err)
	}

	if fileCount != 1 {
		t.Errorf("expected only 1 file (keep.txt) to be scanned, got %d", fileCount)
	}
	if totalBytes != int64(len("keep me")) {
		t.Errorf("expected %d bytes, got %d", len("keep me"), totalBytes)
	}
}

func TestEngineLocalSourceFilterIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	srcDir := filepath.Join(tmpDir, "local_src")
	destDir := filepath.Join(tmpDir, "dest")
	stagingDir := filepath.Join(tmpDir, "staging")
	cfgPath := filepath.Join(tmpDir, "config.json")
	histPath := filepath.Join(tmpDir, "history.json")

	_ = os.MkdirAll(srcDir, 0755)
	_ = os.MkdirAll(destDir, 0755)
	_ = os.MkdirAll(stagingDir, 0755)

	_ = os.WriteFile(filepath.Join(srcDir, "doc.txt"), []byte("important document"), 0644)
	_ = os.WriteFile(filepath.Join(srcDir, "large.iso"), []byte("gigantic iso file"), 0644)

	store, err := config.LoadStore(cfgPath, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	histStore, err := history.NewStore(histPath)
	if err != nil {
		t.Fatal(err)
	}

	eng := NewEngine(store, histStore, nil)
	store.Config.Sync.LocalStagingDir = stagingDir
	store.Config.Sync.Sources = []config.EndpointSpec{
		{
			ID:             "src-local",
			Name:           "my_local",
			Type:           config.TypeLocal,
			RemotePath:     srcDir,
			FilterMode:     "exclude",
			FilterPatterns: "*.iso",
		},
	}
	store.Config.Sync.Destination = config.EndpointSpec{
		ID:         "dest-local",
		Name:       "backup_dest",
		Type:       config.TypeLocal,
		RemotePath: destDir,
	}

	ctx := context.Background()
	if err := eng.RunSyncBlocking(ctx, "manual"); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify doc.txt is synced
	if _, err := os.Stat(filepath.Join(destDir, "my_local", "doc.txt")); err != nil {
		t.Fatalf("expected doc.txt on destination, got err: %v", err)
	}

	// Verify large.iso is EXCLUDED from destination
	if _, err := os.Stat(filepath.Join(destDir, "my_local", "large.iso")); !os.IsNotExist(err) {
		t.Fatalf("expected large.iso to be excluded from destination, but it was copied!")
	}
}

