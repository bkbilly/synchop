package history

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	filePath string
	mu       sync.RWMutex
	records  []SyncRunRecord
	maxRuns  int
}

func NewStore(filePath string) (*Store, error) {
	s := &Store{
		filePath: filePath,
		maxRuns:  200,
		records:  make([]SyncRunRecord, 0),
	}

	if err := s.load(); err != nil && !os.IsNotExist(err) {
		s.records = make([]SyncRunRecord, 0)
	}

	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	var recs []SyncRunRecord
	if err := json.Unmarshal(data, &recs); err != nil {
		return err
	}

	s.records = recs
	return nil
}

func (s *Store) saveLocked() error {
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(s.records, "", "  ")
	if err != nil {
		return err
	}

	// Atomic write via temp file
	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, s.filePath)
}

func (s *Store) AddRun(record SyncRunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Limit changed files list per record to 1,000 files to avoid JSON bloating
	const maxFilesPerRecord = 1000
	if len(record.Files) > maxFilesPerRecord {
		record.Files = record.Files[:maxFilesPerRecord]
	}
	for i := range record.Tasks {
		if len(record.Tasks[i].Files) > maxFilesPerRecord {
			record.Tasks[i].Files = record.Tasks[i].Files[:maxFilesPerRecord]
		}
	}

	// Prepend newest run
	s.records = append([]SyncRunRecord{record}, s.records...)

	// Trim old runs if exceeding limit
	if s.maxRuns > 0 && len(s.records) > s.maxRuns {
		s.records = s.records[:s.maxRuns]
	}

	return s.saveLocked()
}

func (s *Store) GetRuns(limit, offset int) []SyncRunRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := len(s.records)
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []SyncRunRecord{}
	}

	end := total
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	result := make([]SyncRunRecord, end-offset)
	copy(result, s.records[offset:end])
	return result
}

func (s *Store) GetRun(id string) *SyncRunRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, r := range s.records {
		if r.ID == id {
			res := r
			return &res
		}
	}
	return nil
}

func (s *Store) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = make([]SyncRunRecord, 0)
	return s.saveLocked()
}

func (s *Store) GetSummary() HistorySummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	summary := HistorySummary{
		TotalRuns: len(s.records),
	}

	for i := range s.records {
		r := &s.records[i]
		if r.Status == "completed" {
			summary.SuccessfulRuns++
			summary.TotalBytesSent += r.SendSize
		} else if r.Status == "failed" {
			summary.FailedRuns++
		}
	}

	if len(s.records) > 0 {
		last := s.records[0]
		summary.LastRun = &last
	}

	return summary
}
