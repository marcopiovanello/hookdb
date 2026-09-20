package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type State string

const (
	StatePending   State = "PENDING"
	StateCommitted State = "COMMITTED"
)

type Record struct {
	ID        string    `json:"id"`
	Table     string    `json:"table"`
	OldFiles  []string  `json:"old_files"`
	NewFile   string    `json:"new_file"`
	NewLevel  int       `json:"new_level"`
	State     State     `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func Open(dataDir string) (*WAL, error) {
	walPath := filepath.Join(dataDir, "catalog.wal")
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{
		path: walPath,
		file: f,
	}, nil
}

func (w *WAL) LogPending(id, table string, oldFiles []string, newFile string, newLevel int) (*Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := Record{
		ID:        id,
		Table:     table,
		OldFiles:  oldFiles,
		NewFile:   newFile,
		NewLevel:  newLevel,
		State:     StatePending,
		Timestamp: time.Now(),
	}

	if err := w.appendRecord(rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (w *WAL) LogCommitted(id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := Record{
		ID:        id,
		State:     StateCommitted,
		Timestamp: time.Now(),
	}
	return w.appendRecord(rec)
}

func (w *WAL) appendRecord(rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Recover() ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := os.ReadFile(w.path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	records := make(map[string]Record)
	lines := splitLines(data)

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}

		if rec.State == StateCommitted {
			delete(records, rec.ID)
		} else {
			records[rec.ID] = rec
		}
	}

	var uncleanedFiles []string
	for _, rec := range records {
		uncleanedFiles = append(uncleanedFiles, rec.OldFiles...)
	}

	return uncleanedFiles, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
