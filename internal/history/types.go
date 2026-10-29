package history

import "time"

type FileChange struct {
	Path   string `json:"path"`
	Action string `json:"action"` // "created", "modified", "deleted"
	Size   int64  `json:"size"`
}

type TaskStats struct {
	ID               string       `json:"id"`
	Name             string       `json:"name"`
	Phase            string       `json:"phase"`         // "gather" or "send"
	EndpointType     string       `json:"endpoint_type"` // "ssh", "smb", "webdav", "local"
	Status           string       `json:"status"`        // "completed", "failed", "aborted"
	FilesScanned     int          `json:"files_scanned"`
	FilesChanged     int          `json:"files_changed"`
	TotalSize        int64        `json:"total_size"`
	BytesTransferred int64        `json:"bytes_transferred"`
	Files            []FileChange `json:"files,omitempty"`
	ErrorMessage     string       `json:"error_message,omitempty"`
}

type SyncRunRecord struct {
	ID              string       `json:"id"`
	StartTime       time.Time    `json:"start_time"`
	EndTime         time.Time    `json:"end_time"`
	DurationSec     int          `json:"duration_sec"`
	Status          string       `json:"status"`  // "completed", "failed", "aborted"
	Trigger         string       `json:"trigger"` // "manual", "scheduled", "cli"
	FilesChanged    int          `json:"files_changed"`
	TotalSize       int64        `json:"total_size"`
	SendSize        int64        `json:"send_size"`
	SourcesGathered int          `json:"sources_gathered"`
	DestinationName string       `json:"destination_name"`
	DestinationType string       `json:"destination_type"`
	BackupDir       string       `json:"backup_dir,omitempty"`
	Files           []FileChange `json:"files,omitempty"`
	Tasks           []TaskStats  `json:"tasks,omitempty"`
	ErrorMessage    string       `json:"error_message,omitempty"`
}

type HistorySummary struct {
	TotalRuns      int            `json:"total_runs"`
	SuccessfulRuns int            `json:"successful_runs"`
	FailedRuns     int            `json:"failed_runs"`
	TotalBytesSent int64          `json:"total_bytes_sent"`
	LastRun        *SyncRunRecord `json:"last_run,omitempty"`
}
