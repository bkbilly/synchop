package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type EndpointType string

const (
	TypeLocal  EndpointType = "local"
	TypeSSH    EndpointType = "ssh"
	TypeSMB    EndpointType = "smb"
	TypeWebDAV EndpointType = "webdav"
)

type EndpointSpec struct {
	ID             string       `json:"id"`
	Name           string       `json:"name"`
	Type           EndpointType `json:"type"`
	Host           string       `json:"host,omitempty"`
	Port           int          `json:"port,omitempty"`
	Share          string       `json:"share,omitempty"`
	URL            string       `json:"url,omitempty"`
	User           string       `json:"user,omitempty"`
	Password       string       `json:"password,omitempty"`
	Domain         string       `json:"domain,omitempty"`
	RemotePath     string       `json:"remote_path"`
	FilterMode     string       `json:"filter_mode,omitempty"`     // "all", "include_only", "exclude"
	FilterPatterns string       `json:"filter_patterns,omitempty"` // e.g. "*.txt, *.pdf"
	SubPath        string       `json:"sub_path,omitempty"`
	PreflightCheck bool         `json:"preflight_check,omitempty"`
}

type SyncConfig struct {
	Name              string         `json:"name"`
	Enabled           bool           `json:"enabled"`
	SchedulePreset    string         `json:"schedule_preset"`
	IntervalMinutes   int            `json:"interval_minutes"`
	ParallelWorkers   int            `json:"parallel_workers"`
	IngestTimeoutSec  int            `json:"ingest_timeout_sec"`
	EgressTimeoutSec  int            `json:"egress_timeout_sec"`
	NetworkTimeoutSec int            `json:"network_timeout_sec"`
	RsyncCompress     bool           `json:"rsync_compress"`
	LocalStagingDir      string         `json:"local_staging_dir"`
	BackupEnabled        bool           `json:"backup_enabled"`
	BackupRetentionCount int            `json:"backup_retention_count"`
	BackupDirName        string         `json:"backup_dir_name"`
	Sources              []EndpointSpec `json:"sources"`
	Destination          EndpointSpec   `json:"destination"`
}

type AppConfig struct {
	HTTPPort int        `json:"http_port"`
	Sync     SyncConfig `json:"sync"`
}

type Store struct {
	filePath string
	mu       sync.RWMutex
	Config   AppConfig
}

func DefaultConfig(baseDir string) AppConfig {
	staging := filepath.Join(baseDir, "staging")
	return AppConfig{
		HTTPPort: 8080,
		Sync: SyncConfig{
			Name:              "Main Scheduled Sync",
			Enabled:           true,
			SchedulePreset:    "manual",
			IntervalMinutes:   0,
			ParallelWorkers:   4,
			IngestTimeoutSec:  0, // 0 = unlimited / no timeout
			EgressTimeoutSec:  0, // 0 = unlimited / no timeout
			NetworkTimeoutSec: 15,
			RsyncCompress:     false,
			LocalStagingDir:      staging,
			BackupEnabled:        false,
			BackupRetentionCount: 5,
			BackupDirName:        ".synchop-backups",
			Sources:              []EndpointSpec{},
			Destination: EndpointSpec{
				ID:             "dest-1",
				Name:           "",
				Type:           TypeSSH,
				Port:           22,
				PreflightCheck: true,
			},
		},
	}
}

func LoadStore(filePath string, defaultBaseDir string) (*Store, error) {
	s := &Store{filePath: filePath}
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.Config = DefaultConfig(defaultBaseDir)
			if err := s.Save(); err != nil {
				return nil, err
			}
			return s, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(data, &s.Config); err != nil || s.Config.Sync.Name == "" {
		s.Config = DefaultConfig(defaultBaseDir)
		_ = s.Save()
		return s, nil
	}

	if s.Config.HTTPPort == 0 {
		s.Config.HTTPPort = 8080
	}
	if s.Config.Sync.ParallelWorkers == 0 {
		s.Config.Sync.ParallelWorkers = 4
	}
	if s.Config.Sync.NetworkTimeoutSec == 0 {
		s.Config.Sync.NetworkTimeoutSec = 15
	}
	if s.Config.Sync.LocalStagingDir == "" {
		s.Config.Sync.LocalStagingDir = filepath.Join(defaultBaseDir, "staging")
	}

	return s, nil
}

func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(s.Config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.filePath, data, 0644)
}

func (s *Store) UpdateSync(syncCfg SyncConfig) error {
	s.mu.Lock()
	s.Config.Sync = syncCfg
	s.mu.Unlock()
	return s.Save()
}
