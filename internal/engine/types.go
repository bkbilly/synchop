package engine

import "time"

type SyncStatus string

const (
	StatusIdle      SyncStatus = "idle"
	StatusGathering SyncStatus = "gathering" // Phase 1: Ingest
	StatusSending   SyncStatus = "sending"   // Phase 2: Egress
	StatusCompleted SyncStatus = "completed"
	StatusFailed    SyncStatus = "failed"
	StatusAborted   SyncStatus = "aborted"
)

type TaskProgress struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Phase            string    `json:"phase"`
	Status           string    `json:"status"`
	BytesTransferred int64     `json:"bytes_transferred"`
	Percent          int       `json:"percent"`
	Speed            string    `json:"speed"`
	ETA              string    `json:"eta"`
	ErrorMessage     string    `json:"error_message,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type SyncState struct {
	Status                SyncStatus              `json:"status"`
	CurrentPhase          string                  `json:"current_phase"`
	StartTime             time.Time               `json:"start_time,omitempty"`
	EndTime               time.Time               `json:"end_time,omitempty"`
	ElapsedSec            int                     `json:"elapsed_sec"`
	TotalBytesTransferred int64                   `json:"total_bytes_transferred"`
	OverallPercent        int                     `json:"overall_percent"`
	CurrentSpeed          string                  `json:"current_speed"`
	Tasks                 map[string]TaskProgress `json:"tasks"`
	LastError             string                  `json:"last_error,omitempty"`
	LastSuccessTime       *time.Time              `json:"last_success_time,omitempty"`
	LastRunID             string                  `json:"last_run_id,omitempty"`
}
