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
	ingestTimeout := time.Duration(cfg.IngestTimeoutSec) * time.Second
	if ingestTimeout <= 0 {
		ingestTimeout = 10 * time.Minute
	}
	ingestCtx, cancelIngest := context.WithTimeout(ctx, ingestTimeout)
	defer cancelIngest()

	gatherStats, err := e.executeSourcesGather(ingestCtx, cfg, stagingDir)
	if err != nil {
		var finalErr error
		if ingestCtx.Err() == context.DeadlineExceeded {
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

	egressTimeout := time.Duration(cfg.EgressTimeoutSec) * time.Second
	if egressTimeout <= 0 {
		egressTimeout = 10 * time.Minute
	}
	egressCtx, cancelEgress := context.WithTimeout(ctx, egressTimeout)
	defer cancelEgress()

	sendStat, destResult, err := e.executeDestinationSend(egressCtx, cfg, stagingDir)
	if err != nil {
		var finalErr error
		if egressCtx.Err() == context.DeadlineExceeded {
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

func calcDirStats(dir string) (int64, int) {
	var totalSize int64
	var fileCount int
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && !strings.Contains(p, ".synchop-partial") {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})
	return totalSize, fileCount
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
			if entry.IsDir() && !activeFolders[entry.Name()] && !strings.HasPrefix(entry.Name(), ".") {
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

			// SSH or Local via rsync
			srcEndpoint := src.RemotePath
			if src.Type == config.TypeSSH {
				userHost := src.Host
				if src.User != "" {
					userHost = src.User + "@" + src.Host
				}
				srcEndpoint = fmt.Sprintf("%s:%s", userHost, src.RemotePath)
			} else if src.Type == config.TypeLocal {
				srcEndpoint = expandPath(src.RemotePath)
			}

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
