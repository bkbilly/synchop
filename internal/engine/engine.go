package engine

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"synchop/internal/backup"
	"synchop/internal/config"
	"synchop/internal/filter"
	"synchop/internal/history"
	"synchop/internal/rsync"
	"synchop/internal/smb"
	"synchop/internal/webdav"
)

type Broadcaster interface {
	BroadcastState(state SyncState)
}

type DestSyncResult struct {
	FilesScanned     int
	FilesChanged     int
	TotalSize        int64
	BytesTransferred int64
	BackupDir        string
	ChangedFiles     []history.FileChange
}

type Engine struct {
	store        *config.Store
	historyStore *history.Store
	broadcaster  Broadcaster

	mu         sync.RWMutex
	active     bool
	cancelFunc context.CancelFunc
	state      SyncState

	schedMu   sync.Mutex
	schedStop chan struct{}
}

func NewEngine(store *config.Store, historyStore *history.Store, broadcaster Broadcaster) *Engine {
	return &Engine{
		store:        store,
		historyStore: historyStore,
		broadcaster:  broadcaster,
		state: SyncState{
			Status:       StatusIdle,
			CurrentPhase: "Ready",
			Tasks:        make(map[string]TaskProgress),
		},
	}
}

func (e *Engine) GetHistoryStore() *history.Store {
	return e.historyStore
}

func (e *Engine) StartScheduler() {
	e.schedMu.Lock()
	defer e.schedMu.Unlock()

	if e.schedStop != nil {
		close(e.schedStop)
		e.schedStop = nil
	}

	cfg := e.store.Config.Sync
	if !cfg.Enabled || cfg.IntervalMinutes <= 0 {
		return
	}

	e.schedStop = make(chan struct{})
	go func(intervalMin int, stop chan struct{}) {
		ticker := time.NewTicker(time.Duration(intervalMin) * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_ = e.TriggerSync("scheduled")
			}
		}
	}(cfg.IntervalMinutes, e.schedStop)
}

func (e *Engine) GetState() SyncState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

func (e *Engine) updateState(fn func(s *SyncState)) {
	e.mu.Lock()
	fn(&e.state)
	st := e.state
	e.mu.Unlock()

	if e.broadcaster != nil {
		e.broadcaster.BroadcastState(st)
	}
}

func (e *Engine) AbortSync() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.active || e.cancelFunc == nil {
		return fmt.Errorf("no sync operation is currently active")
	}

	e.cancelFunc()
	return nil
}

func (e *Engine) TriggerSync(trigger ...string) error {
	t := "manual"
	if len(trigger) > 0 && trigger[0] != "" {
		t = trigger[0]
	}

	e.mu.Lock()
	if e.active {
		e.mu.Unlock()
		return fmt.Errorf("a sync operation is already in progress")
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.active = true
	e.cancelFunc = cancel
	cfg := e.store.Config.Sync
	e.mu.Unlock()

	go e.runSyncPipeline(ctx, cfg, t)
	return nil
}

// RunSyncBlocking executes the sync pipeline synchronously, blocking until completion or cancellation
func (e *Engine) RunSyncBlocking(ctx context.Context, trigger ...string) error {
	t := "cli"
	if len(trigger) > 0 && trigger[0] != "" {
		t = trigger[0]
	}

	e.mu.Lock()
	if e.active {
		e.mu.Unlock()
		return fmt.Errorf("a sync operation is already in progress")
	}

	subCtx, cancel := context.WithCancel(ctx)
	e.active = true
	e.cancelFunc = cancel
	cfg := e.store.Config.Sync
	e.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.runSyncPipeline(subCtx, cfg, t)
	}()

	select {
	case <-ctx.Done():
		_ = e.AbortSync()
		<-done
		return ctx.Err()
	case <-done:
		st := e.GetState()
		if st.Status == StatusFailed {
			return fmt.Errorf("%s", st.LastError)
		}
		if st.Status == StatusAborted {
			return fmt.Errorf("sync cancelled")
		}
		return nil
	}
}

func (e *Engine) runSyncPipeline(ctx context.Context, cfg config.SyncConfig, trigger string) {
	defer func() {
		e.mu.Lock()
		e.active = false
		e.cancelFunc = nil
		e.mu.Unlock()
	}()

	startTime := time.Now()
	runID := fmt.Sprintf("run-%s", startTime.Format("20060102-150405"))

	e.updateState(func(s *SyncState) {
		s.Status = StatusGathering
		s.CurrentPhase = "Step 1 of 2: Gathering files from sources"
		s.StartTime = startTime
		s.EndTime = time.Time{}
		s.ElapsedSec = 0
		s.TotalBytesTransferred = 0
		s.OverallPercent = 0
		s.CurrentSpeed = "0 B/s"
		s.Tasks = make(map[string]TaskProgress)
		s.LastError = ""
	})

	stagingDir := expandPath(cfg.LocalStagingDir)
	if stagingDir == "" {
		stagingDir = "/tmp/synchop_staging"
	}
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		failErr := fmt.Errorf("cannot create local staging directory: %w", err)
		e.recordFailedRun(runID, startTime, trigger, cfg, failErr, nil, nil)
		e.failSync(failErr)
		return
	}

	// --------------------------------------------------------------------
	// PHASE 1: GATHER SOURCES TO LOCAL STAGING (Parallel)
	// --------------------------------------------------------------------
	var ingestCtx context.Context
	var cancelIngest context.CancelFunc
	if cfg.IngestTimeoutSec > 0 {
		ingestCtx, cancelIngest = context.WithTimeout(ctx, time.Duration(cfg.IngestTimeoutSec)*time.Second)
	} else {
		ingestCtx, cancelIngest = context.WithCancel(ctx)
	}
	defer cancelIngest()

	gatherStats, err := e.executeSourcesGather(ingestCtx, cfg, stagingDir)
	if err != nil {
		var finalErr error
		if cfg.IngestTimeoutSec > 0 && ingestCtx.Err() == context.DeadlineExceeded {
			finalErr = fmt.Errorf("Step 1 (Gathering) timed out after %d seconds", cfg.IngestTimeoutSec)
			e.recordFailedRun(runID, startTime, trigger, cfg, finalErr, gatherStats, nil)
			e.failSync(finalErr)
		} else if ctx.Err() == context.Canceled {
			e.recordAbortedRun(runID, startTime, trigger, cfg, gatherStats, nil)
			e.abortSyncState()
		} else {
			finalErr = fmt.Errorf("Step 1 (Gathering) failed: %w", err)
			e.recordFailedRun(runID, startTime, trigger, cfg, finalErr, gatherStats, nil)
			e.failSync(finalErr)
		}
		return
	}

	// --------------------------------------------------------------------
	// PHASE 2: SEND GATHERED DATA TO DESTINATION
	// --------------------------------------------------------------------
	e.updateState(func(s *SyncState) {
		s.Status = StatusSending
		s.CurrentPhase = "Step 2 of 2: Sending files to destination"
		s.OverallPercent = 50
	})

	var egressCtx context.Context
	var cancelEgress context.CancelFunc
	if cfg.EgressTimeoutSec > 0 {
		egressCtx, cancelEgress = context.WithTimeout(ctx, time.Duration(cfg.EgressTimeoutSec)*time.Second)
	} else {
		egressCtx, cancelEgress = context.WithCancel(ctx)
	}
	defer cancelEgress()

	sendStat, destResult, err := e.executeDestinationSend(egressCtx, cfg, stagingDir)
	if err != nil {
		var finalErr error
		if cfg.EgressTimeoutSec > 0 && egressCtx.Err() == context.DeadlineExceeded {
			finalErr = fmt.Errorf("Step 2 (Sending) timed out after %d seconds", cfg.EgressTimeoutSec)
			e.recordFailedRun(runID, startTime, trigger, cfg, finalErr, gatherStats, sendStat)
			e.failSync(finalErr)
		} else if ctx.Err() == context.Canceled {
			e.recordAbortedRun(runID, startTime, trigger, cfg, gatherStats, sendStat)
			e.abortSyncState()
		} else {
			finalErr = fmt.Errorf("Step 2 (Sending) failed: %w", err)
			e.recordFailedRun(runID, startTime, trigger, cfg, finalErr, gatherStats, sendStat)
			e.failSync(finalErr)
		}
		return
	}

	// --------------------------------------------------------------------
	// COMPLETED SUCCESSFULLY
	// --------------------------------------------------------------------
	endTime := time.Now()
	elapsed := int(endTime.Sub(startTime).Seconds())
	now := time.Now()

	totalSize := destResult.TotalSize
	if totalSize == 0 {
		stagingSize, _ := calcDirStats(stagingDir)
		totalSize = stagingSize
	}

	var allTasks []history.TaskStats
	allTasks = append(allTasks, gatherStats...)
	if sendStat != nil {
		allTasks = append(allTasks, *sendStat)
	}

	rec := history.SyncRunRecord{
		ID:              runID,
		StartTime:       startTime,
		EndTime:         endTime,
		DurationSec:     elapsed,
		Status:          "completed",
		Trigger:         trigger,
		FilesChanged:    destResult.FilesChanged,
		TotalSize:       totalSize,
		SendSize:        destResult.BytesTransferred,
		SourcesGathered: len(gatherStats),
		DestinationName: cfg.Destination.Name,
		DestinationType: string(cfg.Destination.Type),
		BackupDir:       destResult.BackupDir,
		Files:           destResult.ChangedFiles,
		Tasks:           allTasks,
	}

	if e.historyStore != nil {
		_ = e.historyStore.AddRun(rec)
	}

	e.updateState(func(s *SyncState) {
		s.Status = StatusCompleted
		s.CurrentPhase = "Sync completed successfully"
		s.EndTime = endTime
		s.ElapsedSec = elapsed
		s.OverallPercent = 100
		s.TotalBytesTransferred = destResult.BytesTransferred
		s.LastSuccessTime = &now
		s.LastRunID = runID
	})
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(sec int) string {
	if sec <= 0 {
		return "0s"
	}
	if sec < 60 {
		return fmt.Sprintf("%ds", sec)
	}
	m := sec / 60
	s := sec % 60
	if m < 60 {
		return fmt.Sprintf("%dm %02ds", m, s)
	}
	h := m / 60
	m = m % 60
	return fmt.Sprintf("%dh %02dm %02ds", h, m, s)
}

func calcDirStats(dir string) (int64, int) {
	var totalSize int64
	var fileCount int
	_ = filter.WalkDirWithSymlinks(dir, func(rel string, info os.FileInfo, p string) error {
		if !info.IsDir() && !strings.Contains(p, ".synchop-partial") {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})
	return totalSize, fileCount
}

// scanLocalDirectoryProgressive walks localPath while applying filters and streaming live progress, speed, ETA, and percent
func (e *Engine) scanLocalDirectoryProgressive(ctx context.Context, localPath string, filt *filter.Filter, onProgress func(scannedFiles int, scannedBytes int64, percent int, speed, eta string)) (int64, int, error) {
	fi, err := os.Stat(localPath)
	if err != nil {
		return 0, 0, err
	}
	if !fi.IsDir() {
		if onProgress != nil {
			onProgress(1, fi.Size(), 100, "Direct Local", "0s")
		}
		return fi.Size(), 1, nil
	}

	topEntries, err := os.ReadDir(localPath)
	if err != nil {
		return 0, 0, err
	}
	if len(topEntries) == 0 {
		if onProgress != nil {
			onProgress(0, 0, 100, "Direct Local", "0s")
		}
		return 0, 0, nil
	}

	// Filter top-level entries against filter
	var validTop []os.DirEntry
	for _, entry := range topEntries {
		name := entry.Name()
		if strings.HasPrefix(name, ".synchop-partial") || strings.HasPrefix(name, ".synchop-backups") {
			continue
		}
		if filt != nil && !filt.ShouldKeep(name, entry.IsDir()) {
			continue
		}
		validTop = append(validTop, entry)
	}

	if len(validTop) == 0 {
		if onProgress != nil {
			onProgress(0, 0, 100, "Direct Local", "0s")
		}
		return 0, 0, nil
	}

	// If there is only one top-level directory, drill down to find representative branches
	branches := validTop
	branchBase := localPath
	for len(branches) == 1 && branches[0].IsDir() {
		nextDir := filepath.Join(branchBase, branches[0].Name())
		sub, err := os.ReadDir(nextDir)
		if err == nil && len(sub) > 0 {
			var validSub []os.DirEntry
			for _, se := range sub {
				name := se.Name()
				if strings.HasPrefix(name, ".synchop-partial") || strings.HasPrefix(name, ".synchop-backups") {
					continue
				}
				rel, _ := filepath.Rel(localPath, filepath.Join(nextDir, name))
				if filt != nil && !filt.ShouldKeep(rel, se.IsDir()) {
					continue
				}
				validSub = append(validSub, se)
			}
			if len(validSub) > 0 {
				branches = validSub
				branchBase = nextDir
				continue
			}
		}
		break
	}

	var totalFiles int
	var totalBytes int64
	var lastPct int
	var mu sync.Mutex

	startTime := time.Now()
	lastUpdate := time.Now()

	notify := func(pct int, force bool) {
		mu.Lock()
		defer mu.Unlock()
		if pct < lastPct {
			pct = lastPct
		} else {
			lastPct = pct
		}
		now := time.Now()
		if !force && now.Sub(lastUpdate) < 100*time.Millisecond {
			return
		}
		elapsed := now.Sub(startTime).Seconds()
		if elapsed > 0 {
			speedBytes := float64(totalBytes) / elapsed
			speedFiles := float64(totalFiles) / elapsed
			speedStr := fmt.Sprintf("%s (%d files/s)", formatBytes(int64(speedBytes))+"/s", int(speedFiles))

			etaStr := "-"
			if pct > 0 {
				estimatedTotal := elapsed / (float64(pct) / 100.0)
				remaining := estimatedTotal - elapsed
				if remaining < 0 {
					remaining = 0
				}
				etaStr = formatDuration(int(remaining))
			}

			if onProgress != nil {
				onProgress(totalFiles, totalBytes, pct, speedStr, etaStr)
			}
		}
		lastUpdate = now
	}

	numBranches := len(branches)
	branchProgress := make([]float64, numBranches)

	var walkBranch func(dirPath string, currentRel string, depth int, bIdx int, minFrac, maxFrac float64) error
	walkBranch = func(dirPath string, currentRel string, depth int, bIdx int, minFrac, maxFrac float64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return nil
		}

		numEnt := len(entries)
		for idx, entry := range entries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			name := entry.Name()
			if strings.HasPrefix(name, ".synchop-partial") || strings.HasPrefix(name, ".synchop-backups") {
				continue
			}

			subRel := filepath.Join(currentRel, name)
			isDir := entry.IsDir()

			// Fast filter check BEFORE any stat syscalls!
			if filt != nil && !filt.ShouldKeep(subRel, isDir) {
				continue
			}

			entPath := filepath.Join(dirPath, name)

			// Handle symlinks if entry itself is a symlink
			isSymlink := entry.Type()&os.ModeSymlink != 0
			if isSymlink {
				if realFi, err := os.Stat(entPath); err == nil {
					isDir = realFi.IsDir()
					if filt != nil && !filt.ShouldKeep(subRel, isDir) {
						continue
					}
				}
			}

			entMinFrac := minFrac
			entMaxFrac := maxFrac
			if numEnt > 0 && depth <= 3 {
				slice := (maxFrac - minFrac) / float64(numEnt)
				entMinFrac = minFrac + float64(idx)*slice
				entMaxFrac = entMinFrac + slice
			}

			if isDir {
				if err := walkBranch(entPath, subRel, depth+1, bIdx, entMinFrac, entMaxFrac); err != nil {
					return err
				}
			} else {
				eInfo, eErr := entry.Info()
				if eErr != nil {
					continue
				}
				if isSymlink {
					if realFi, err := os.Stat(entPath); err == nil {
						eInfo = realFi
					}
				}

				if filt != nil && filt.MaxFileSize > 0 && eInfo.Size() > filt.MaxFileSize {
					continue
				}

				mu.Lock()
				totalFiles++
				totalBytes += eInfo.Size()
				branchProgress[bIdx] = entMaxFrac
				var sumFrac float64
				for _, fr := range branchProgress {
					sumFrac += fr
				}
				pct := int((sumFrac / float64(numBranches)) * 100.0)
				if pct > 99 {
					pct = 99
				}
				mu.Unlock()

				notify(pct, false)
			}
		}
		return nil
	}

	var wg sync.WaitGroup
	concurrency := 4
	if numBranches < concurrency {
		concurrency = numBranches
	}
	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, numBranches)

	for i, b := range branches {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		default:
		}

		bRel := b.Name()
		bIsDir := b.IsDir()
		bPath := filepath.Join(branchBase, b.Name())
		bIndex := i

		wg.Add(1)
		go func(path string, rel string, isDirectory bool, idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if isDirectory {
				if err := walkBranch(path, rel, 1, idx, 0.0, 1.0); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
				mu.Lock()
				branchProgress[idx] = 1.0
				mu.Unlock()
			} else {
				fi, err := os.Stat(path)
				if err == nil {
					if filt != nil && filt.MaxFileSize > 0 && fi.Size() > filt.MaxFileSize {
						return
					}
					mu.Lock()
					totalFiles++
					totalBytes += fi.Size()
					branchProgress[idx] = 1.0
					var sumFrac float64
					for _, fr := range branchProgress {
						sumFrac += fr
					}
					pct := int((sumFrac / float64(numBranches)) * 100.0)
					if pct > 99 {
						pct = 99
					}
					mu.Unlock()
					notify(pct, false)
				}
			}
		}(bPath, bRel, bIsDir, bIndex)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		if err != nil {
			return 0, 0, err
		}
	default:
	}

	notify(100, true)
	return totalBytes, totalFiles, nil
}

func (e *Engine) recordFailedRun(runID string, startTime time.Time, trigger string, cfg config.SyncConfig, err error, gathers []history.TaskStats, send *history.TaskStats) {
	if e.historyStore == nil {
		return
	}
	endTime := time.Now()
	elapsed := int(endTime.Sub(startTime).Seconds())

	var allTasks []history.TaskStats
	allTasks = append(allTasks, gathers...)
	if send != nil {
		allTasks = append(allTasks, *send)
	}

	rec := history.SyncRunRecord{
		ID:              runID,
		StartTime:       startTime,
		EndTime:         endTime,
		DurationSec:     elapsed,
		Status:          "failed",
		Trigger:         trigger,
		DestinationName: cfg.Destination.Name,
		DestinationType: string(cfg.Destination.Type),
		SourcesGathered: len(gathers),
		Tasks:           allTasks,
		ErrorMessage:    err.Error(),
	}
	_ = e.historyStore.AddRun(rec)
}

func (e *Engine) recordAbortedRun(runID string, startTime time.Time, trigger string, cfg config.SyncConfig, gathers []history.TaskStats, send *history.TaskStats) {
	if e.historyStore == nil {
		return
	}
	endTime := time.Now()
	elapsed := int(endTime.Sub(startTime).Seconds())

	var allTasks []history.TaskStats
	allTasks = append(allTasks, gathers...)
	if send != nil {
		allTasks = append(allTasks, *send)
	}

	rec := history.SyncRunRecord{
		ID:              runID,
		StartTime:       startTime,
		EndTime:         endTime,
		DurationSec:     elapsed,
		Status:          "aborted",
		Trigger:         trigger,
		DestinationName: cfg.Destination.Name,
		DestinationType: string(cfg.Destination.Type),
		SourcesGathered: len(gathers),
		Tasks:           allTasks,
		ErrorMessage:    "Cancelled by user",
	}
	_ = e.historyStore.AddRun(rec)
}

func expandPath(p string) string {
	if strings.HasPrefix(p, "~/") || p == "~" {
		home, err := os.UserHomeDir()
		if err == nil {
			if p == "~" {
				return home
			}
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

func sanitizeFolderName(name string) string {
	var out []rune
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			out = append(out, r)
		} else if r == ' ' {
			out = append(out, '_')
		}
	}
	res := strings.Trim(string(out), "_-")
	if res == "" {
		return "source"
	}
	return res
}

func toHistoryChanges(cf []rsync.ChangedFile) []history.FileChange {
	out := make([]history.FileChange, len(cf))
	for i, f := range cf {
		out[i] = history.FileChange{
			Path:   f.Path,
			Action: f.Action,
			Size:   f.Size,
		}
	}
	return out
}

func (e *Engine) executeSourcesGather(ctx context.Context, cfg config.SyncConfig, stagingDir string) ([]history.TaskStats, error) {
	// Clean up stale source folders in staging that are no longer configured
	activeFolders := make(map[string]bool)
	for _, s := range cfg.Sources {
		activeFolders[sanitizeFolderName(s.Name)] = true
	}
	if entries, err := os.ReadDir(stagingDir); err == nil {
		for _, entry := range entries {
			if !activeFolders[entry.Name()] && !strings.HasPrefix(entry.Name(), ".") {
				_ = os.RemoveAll(filepath.Join(stagingDir, entry.Name()))
			}
		}
	}

	workers := cfg.ParallelWorkers
	if workers <= 0 {
		workers = 4
	}
	sem := make(chan struct{}, workers)

	var wg sync.WaitGroup
	errCh := make(chan error, len(cfg.Sources))

	var statsMu sync.Mutex
	allStats := make([]history.TaskStats, 0, len(cfg.Sources))

	for _, src := range cfg.Sources {
		src := src
		sem <- struct{}{}
		wg.Add(1)

		destPath := filepath.Join(stagingDir, sanitizeFolderName(src.Name))

		e.updateState(func(s *SyncState) {
			s.Tasks[src.ID] = TaskProgress{
				ID:        src.ID,
				Name:      src.Name,
				Phase:     "Gather",
				Status:    "running",
				UpdatedAt: time.Now(),
			}
		})

		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()

			if src.Type == config.TypeSMB {
				onProgress := func(bytes int64, curFile string) {
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.BytesTransferred = bytes
						task.Speed = "Streaming"
						task.UpdatedAt = time.Now()
						s.Tasks[src.ID] = task
						s.TotalBytesTransferred = bytes
					})
				}

				res, err := smb.SyncFromSMB(ctx, src, destPath, onProgress)
				tStat := history.TaskStats{
					ID:           src.ID,
					Name:         src.Name,
					Phase:        "gather",
					EndpointType: string(src.Type),
				}
				if res != nil {
					tStat.FilesScanned = res.FilesScanned
					tStat.FilesChanged = res.FilesChanged
					tStat.TotalSize = res.TotalSize
					tStat.BytesTransferred = res.BytesTransferred
					tStat.Files = res.ChangedFiles
				}

				if err != nil {
					tStat.Status = "failed"
					tStat.ErrorMessage = err.Error()
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.Status = "failed"
						task.ErrorMessage = err.Error()
						s.Tasks[src.ID] = task
					})
					errCh <- fmt.Errorf("[%s] %w", src.Name, err)
				} else {
					tStat.Status = "completed"
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.Status = "completed"
						task.Percent = 100
						s.Tasks[src.ID] = task
					})
				}

				statsMu.Lock()
				allStats = append(allStats, tStat)
				statsMu.Unlock()
				return
			}

			if src.Type == config.TypeWebDAV {
				onProgress := func(bytes int64, curFile string) {
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.BytesTransferred = bytes
						task.Speed = "Streaming"
						task.UpdatedAt = time.Now()
						s.Tasks[src.ID] = task
						s.TotalBytesTransferred = bytes
					})
				}

				res, err := webdav.SyncFromWebDAV(ctx, src, destPath, onProgress)
				tStat := history.TaskStats{
					ID:           src.ID,
					Name:         src.Name,
					Phase:        "gather",
					EndpointType: string(src.Type),
				}
				if res != nil {
					tStat.FilesScanned = res.FilesScanned
					tStat.FilesChanged = res.FilesChanged
					tStat.TotalSize = res.TotalSize
					tStat.BytesTransferred = res.BytesTransferred
					tStat.Files = res.ChangedFiles
				}

				if err != nil {
					tStat.Status = "failed"
					tStat.ErrorMessage = err.Error()
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.Status = "failed"
						task.ErrorMessage = err.Error()
						s.Tasks[src.ID] = task
					})
					errCh <- fmt.Errorf("[%s] %w", src.Name, err)
				} else {
					tStat.Status = "completed"
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.Status = "completed"
						task.Percent = 100
						s.Tasks[src.ID] = task
					})
				}

				statsMu.Lock()
				allStats = append(allStats, tStat)
				statsMu.Unlock()
				return
			}

			if src.Type == config.TypeLocal {
				localPath := expandPath(src.RemotePath)
				fi, statErr := os.Stat(localPath)
				tStat := history.TaskStats{
					ID:           src.ID,
					Name:         src.Name,
					Phase:        "gather",
					EndpointType: string(src.Type),
				}

				if statErr != nil {
					tStat.Status = "failed"
					tStat.ErrorMessage = statErr.Error()
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.Status = "failed"
						task.ErrorMessage = statErr.Error()
						s.Tasks[src.ID] = task
					})
					errCh <- fmt.Errorf("[%s] local path error: %w", src.Name, statErr)
					statsMu.Lock()
					allStats = append(allStats, tStat)
					statsMu.Unlock()
					return
				}

				if !fi.IsDir() {
					errNotDir := fmt.Errorf("local source path '%s' is not a directory", localPath)
					tStat.Status = "failed"
					tStat.ErrorMessage = errNotDir.Error()
					e.updateState(func(s *SyncState) {
						task := s.Tasks[src.ID]
						task.Status = "failed"
						task.ErrorMessage = errNotDir.Error()
						s.Tasks[src.ID] = task
					})
					errCh <- fmt.Errorf("[%s] %w", src.Name, errNotDir)
					statsMu.Lock()
					allStats = append(allStats, tStat)
					statsMu.Unlock()
					return
				}

				// If destPath exists and is a real directory (e.g. from previous runs that copied), remove it
				if lfi, lerr := os.Lstat(destPath); lerr == nil {
					if lfi.Mode()&os.ModeSymlink == 0 {
						_ = os.RemoveAll(destPath)
					} else {
						target, _ := os.Readlink(destPath)
						if target != localPath {
							_ = os.Remove(destPath)
						}
					}
				}

				// Create direct symlink so destination sync accesses local directory directly without copying
				if _, lerr := os.Lstat(destPath); os.IsNotExist(lerr) {
					if linkErr := os.Symlink(localPath, destPath); linkErr != nil {
						tStat.Status = "failed"
						tStat.ErrorMessage = linkErr.Error()
						e.updateState(func(s *SyncState) {
							task := s.Tasks[src.ID]
							task.Status = "failed"
							task.ErrorMessage = linkErr.Error()
							s.Tasks[src.ID] = task
						})
						errCh <- fmt.Errorf("[%s] failed to link local directory: %w", src.Name, linkErr)
						statsMu.Lock()
						allStats = append(allStats, tStat)
						statsMu.Unlock()
						return
					}
				}

				// Direct symlink established; gathering is instantaneous zero-copy
				tStat.FilesScanned = 1
				tStat.TotalSize = 0
				tStat.BytesTransferred = 0
				tStat.Status = "completed"

				e.updateState(func(s *SyncState) {
					task := s.Tasks[src.ID]
					task.Status = "completed"
					task.Percent = 100
					task.Speed = "Direct Local"
					task.ETA = "0s"
					task.BytesTransferred = 0
					task.UpdatedAt = time.Now()
					s.Tasks[src.ID] = task

					if len(s.Tasks) > 0 {
						var sumPercent int
						for _, t := range s.Tasks {
							sumPercent += t.Percent
						}
						avg := float64(sumPercent) / float64(len(s.Tasks))
						s.OverallPercent = int(avg * 0.5)
					}
				})

				statsMu.Lock()
				allStats = append(allStats, tStat)
				statsMu.Unlock()
				return
			}

			// Remote SSH via rsync
			userHost := src.Host
			if src.User != "" {
				userHost = src.User + "@" + src.Host
			}
			srcEndpoint := fmt.Sprintf("%s:%s", userHost, src.RemotePath)

			if !strings.HasSuffix(srcEndpoint, "/") {
				srcEndpoint += "/"
			}

			var extraArgs []string
			if rsyncFilters := filter.ToRsyncArgs(src.FilterMode, src.FilterPatterns); len(rsyncFilters) > 0 {
				extraArgs = append(extraArgs, rsyncFilters...)
			}

			task := rsync.SyncTask{
				ID:             src.ID,
				Name:           src.Name,
				Source:         srcEndpoint,
				Destination:    destPath,
				NetworkTimeout: cfg.NetworkTimeoutSec,
				SSHPort:        src.Port,
				SSHUser:        src.User,
				SSHPassword:    src.Password,
				ExtraArgs:      extraArgs,
				Compress:       cfg.RsyncCompress,
			}

			onProgress := func(p rsync.ProgressUpdate) {
				e.updateState(func(s *SyncState) {
					t := s.Tasks[task.ID]
					t.BytesTransferred = p.BytesTransferred
					t.Percent = p.Percent
					t.Speed = p.Speed
					t.ETA = p.ETA
					t.UpdatedAt = time.Now()
					s.Tasks[task.ID] = t

					s.CurrentSpeed = p.Speed
					s.TotalBytesTransferred = p.BytesTransferred

					if len(s.Tasks) > 0 {
						var sumPercent int
						for _, tk := range s.Tasks {
							sumPercent += tk.Percent
						}
						avg := float64(sumPercent) / float64(len(s.Tasks))
						s.OverallPercent = int(avg * 0.5)
					}
				})
			}

			res, err := rsync.RunSync(ctx, task, onProgress, nil)
			tStat := history.TaskStats{
				ID:           src.ID,
				Name:         src.Name,
				Phase:        "gather",
				EndpointType: string(src.Type),
			}
			if res != nil {
				tStat.FilesScanned = res.FilesScanned
				tStat.FilesChanged = res.FilesChanged
				tStat.TotalSize = res.TotalSize
				tStat.BytesTransferred = res.BytesTransferred
				tStat.Files = toHistoryChanges(res.ChangedFiles)
			}

			if err != nil {
				tStat.Status = "failed"
				tStat.ErrorMessage = err.Error()
				e.updateState(func(s *SyncState) {
					t := s.Tasks[task.ID]
					t.Status = "failed"
					t.ErrorMessage = err.Error()
					s.Tasks[task.ID] = t
				})
				errCh <- fmt.Errorf("[%s] %w", task.Name, err)
			} else {
				tStat.Status = "completed"
				e.updateState(func(s *SyncState) {
					t := s.Tasks[task.ID]
					t.Status = "completed"
					t.Percent = 100
					s.Tasks[task.ID] = t
				})
			}

			statsMu.Lock()
			allStats = append(allStats, tStat)
			statsMu.Unlock()
		}()
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		return allStats, <-errCh
	}
	return allStats, nil
}

func (e *Engine) executeDestinationSend(ctx context.Context, cfg config.SyncConfig, stagingDir string) (*history.TaskStats, *DestSyncResult, error) {
	dest := cfg.Destination

	var backupSubDir string
	if cfg.BackupEnabled {
		backupSubDir = backup.GenerateBackupSubDir(cfg.BackupDirName)
	}

	e.updateState(func(s *SyncState) {
		s.Tasks[dest.ID] = TaskProgress{
			ID:        dest.ID,
			Name:      dest.Name,
			Phase:     "Send",
			Status:    "running",
			UpdatedAt: time.Now(),
		}
	})

	tStat := &history.TaskStats{
		ID:           dest.ID,
		Name:         dest.Name,
		Phase:        "send",
		EndpointType: string(dest.Type),
	}

	if dest.Type == config.TypeSMB {
		onProgress := func(bytes int64, curFile string) {
			e.updateState(func(s *SyncState) {
				t := s.Tasks[dest.ID]
				t.BytesTransferred = bytes
				t.Speed = "Streaming"
				t.UpdatedAt = time.Now()
				s.Tasks[dest.ID] = t
				s.TotalBytesTransferred = bytes
			})
		}

		res, err := smb.SyncToSMB(ctx, stagingDir, dest, onProgress, backupSubDir)
		destRes := &DestSyncResult{
			BackupDir: backupSubDir,
		}
		if res != nil {
			destRes.FilesScanned = res.FilesScanned
			destRes.FilesChanged = res.FilesChanged
			destRes.TotalSize = res.TotalSize
			destRes.BytesTransferred = res.BytesTransferred
			destRes.ChangedFiles = res.ChangedFiles

			tStat.FilesScanned = res.FilesScanned
			tStat.FilesChanged = res.FilesChanged
			tStat.TotalSize = res.TotalSize
			tStat.BytesTransferred = res.BytesTransferred
			tStat.Files = res.ChangedFiles
		}

		if err != nil {
			tStat.Status = "failed"
			tStat.ErrorMessage = err.Error()
			e.updateState(func(s *SyncState) {
				t := s.Tasks[dest.ID]
				t.Status = "failed"
				t.ErrorMessage = err.Error()
				s.Tasks[dest.ID] = t
			})
			return tStat, destRes, fmt.Errorf("[%s] %w", dest.Name, err)
		}

		if cfg.BackupEnabled && cfg.BackupRetentionCount > 0 {
			_, _ = backup.PruneBackups(ctx, dest, cfg.BackupDirName, cfg.BackupRetentionCount, cfg.NetworkTimeoutSec)
		}

		tStat.Status = "completed"
		e.updateState(func(s *SyncState) {
			t := s.Tasks[dest.ID]
			t.Status = "completed"
			t.Percent = 100
			s.Tasks[dest.ID] = t
		})
		return tStat, destRes, nil
	}

	if dest.Type == config.TypeWebDAV {
		onProgress := func(bytes int64, curFile string) {
			e.updateState(func(s *SyncState) {
				t := s.Tasks[dest.ID]
				t.BytesTransferred = bytes
				t.Speed = "Streaming"
				t.UpdatedAt = time.Now()
				s.Tasks[dest.ID] = t
				s.TotalBytesTransferred = bytes
			})
		}

		res, err := webdav.SyncToWebDAV(ctx, stagingDir, dest, onProgress, backupSubDir)
		destRes := &DestSyncResult{
			BackupDir: backupSubDir,
		}
		if res != nil {
			destRes.FilesScanned = res.FilesScanned
			destRes.FilesChanged = res.FilesChanged
			destRes.TotalSize = res.TotalSize
			destRes.BytesTransferred = res.BytesTransferred
			destRes.ChangedFiles = res.ChangedFiles

			tStat.FilesScanned = res.FilesScanned
			tStat.FilesChanged = res.FilesChanged
			tStat.TotalSize = res.TotalSize
			tStat.BytesTransferred = res.BytesTransferred
			tStat.Files = res.ChangedFiles
		}

		if err != nil {
			tStat.Status = "failed"
			tStat.ErrorMessage = err.Error()
			e.updateState(func(s *SyncState) {
				t := s.Tasks[dest.ID]
				t.Status = "failed"
				t.ErrorMessage = err.Error()
				s.Tasks[dest.ID] = t
			})
			return tStat, destRes, fmt.Errorf("[%s] %w", dest.Name, err)
		}

		if cfg.BackupEnabled && cfg.BackupRetentionCount > 0 {
			_, _ = backup.PruneBackups(ctx, dest, cfg.BackupDirName, cfg.BackupRetentionCount, cfg.NetworkTimeoutSec)
		}

		tStat.Status = "completed"
		e.updateState(func(s *SyncState) {
			t := s.Tasks[dest.ID]
			t.Status = "completed"
			t.Percent = 100
			s.Tasks[dest.ID] = t
		})
		return tStat, destRes, nil
	}

	// Preflight connectivity check if SSH
	if dest.PreflightCheck && dest.Type == config.TypeSSH && dest.Host != "" {
		port := dest.Port
		if port == 0 {
			port = 22
		}
		addr := net.JoinHostPort(dest.Host, strconv.Itoa(port))
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			errWrap := fmt.Errorf("remote destination %s (%s) is offline or unreachable: %w", dest.Name, addr, err)
			tStat.Status = "failed"
			tStat.ErrorMessage = errWrap.Error()
			return tStat, &DestSyncResult{}, errWrap
		}
		_ = conn.Close()
	}

	destEndpoint := dest.RemotePath
	if dest.Type == config.TypeSSH {
		userHost := dest.Host
		if dest.User != "" {
			userHost = dest.User + "@" + dest.Host
		}
		destEndpoint = fmt.Sprintf("%s:%s", userHost, dest.RemotePath)
	} else if dest.Type == config.TypeLocal {
		destEndpoint = expandPath(dest.RemotePath)
		if err := os.MkdirAll(destEndpoint, 0755); err != nil {
			return tStat, &DestSyncResult{}, fmt.Errorf("failed to create destination directory: %w", err)
		}
	}

	srcStaging := stagingDir
	if !strings.HasSuffix(srcStaging, "/") {
		srcStaging += "/"
	}

	var destExtraArgs []string
	if rsyncFilters := filter.ToRsyncArgs(dest.FilterMode, dest.FilterPatterns); len(rsyncFilters) > 0 {
		destExtraArgs = append(destExtraArgs, rsyncFilters...)
	}

	for _, src := range cfg.Sources {
		if src.Type == config.TypeLocal && (src.FilterMode != string(filter.ModeAll) || src.FilterPatterns != "") {
			if srcFilters := filter.ToRsyncArgsForSource(src.Name, src.FilterMode, src.FilterPatterns); len(srcFilters) > 0 {
				destExtraArgs = append(destExtraArgs, srcFilters...)
			}
		}
	}

	task := rsync.SyncTask{
		ID:             dest.ID,
		Name:           dest.Name,
		Source:         srcStaging,
		Destination:    destEndpoint,
		NetworkTimeout: cfg.NetworkTimeoutSec,
		SSHPort:        dest.Port,
		SSHUser:        dest.User,
		SSHPassword:    dest.Password,
		ExtraArgs:      destExtraArgs,
		Compress:       cfg.RsyncCompress,
		BackupEnabled:  cfg.BackupEnabled,
		BackupDir:      backupSubDir,
		BackupBaseDir:  cfg.BackupDirName,
	}

	onProgress := func(p rsync.ProgressUpdate) {
		e.updateState(func(s *SyncState) {
			t := s.Tasks[dest.ID]
			t.BytesTransferred = p.BytesTransferred
			t.Percent = p.Percent
			t.Speed = p.Speed
			t.ETA = p.ETA
			t.UpdatedAt = time.Now()
			s.Tasks[dest.ID] = t

			s.CurrentSpeed = p.Speed
			s.TotalBytesTransferred = p.BytesTransferred
			s.OverallPercent = 50 + int(float64(p.Percent)*0.5)
		})
	}

	res, err := rsync.RunSync(ctx, task, onProgress, nil)
	destRes := &DestSyncResult{
		BackupDir: backupSubDir,
	}
	if res != nil {
		destRes.FilesScanned = res.FilesScanned
		destRes.FilesChanged = res.FilesChanged
		destRes.TotalSize = res.TotalSize
		destRes.BytesTransferred = res.BytesTransferred
		destRes.ChangedFiles = toHistoryChanges(res.ChangedFiles)

		tStat.FilesScanned = res.FilesScanned
		tStat.FilesChanged = res.FilesChanged
		tStat.TotalSize = res.TotalSize
		tStat.BytesTransferred = res.BytesTransferred
		tStat.Files = destRes.ChangedFiles
	}

	if err != nil {
		tStat.Status = "failed"
		tStat.ErrorMessage = err.Error()
		e.updateState(func(s *SyncState) {
			t := s.Tasks[dest.ID]
			t.Status = "failed"
			t.ErrorMessage = err.Error()
			s.Tasks[dest.ID] = t
		})
		return tStat, destRes, fmt.Errorf("[%s] %w", dest.Name, err)
	}

	if cfg.BackupEnabled && cfg.BackupRetentionCount > 0 {
		_, _ = backup.PruneBackups(ctx, dest, cfg.BackupDirName, cfg.BackupRetentionCount, cfg.NetworkTimeoutSec)
	}

	tStat.Status = "completed"
	e.updateState(func(s *SyncState) {
		t := s.Tasks[dest.ID]
		t.Status = "completed"
		t.Percent = 100
		s.Tasks[dest.ID] = t
	})

	return tStat, destRes, nil
}

func (e *Engine) failSync(err error) {
	endTime := time.Now()
	e.updateState(func(s *SyncState) {
		s.Status = StatusFailed
		s.CurrentPhase = "Sync failed"
		s.EndTime = endTime
		s.LastError = err.Error()
	})
}

func (e *Engine) abortSyncState() {
	endTime := time.Now()
	e.updateState(func(s *SyncState) {
		s.Status = StatusAborted
		s.CurrentPhase = "Sync cancelled by user"
		s.EndTime = endTime
		s.LastError = "Cancelled by user"
	})
}
