// Package wal implements a Write-Ahead-Log (WAL) to handle changes and deletions to the Parquet files
// in a transaction fashion.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type State string

const (
	StatePending   State = "PENDING"
	StateCommitted State = "COMMITTED"
)

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

func (w *WAL) LogPending(id, table string, oldFiles []string, newFile string, newLevel int) (*LogRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := LogRecord{
		Id:            id,
		Table:         table,
		OldFiles:      oldFiles,
		CompactedFile: newFile,
		NewLevel:      uint32(newLevel),
		State:         string(StatePending),
		Timestamp:     time.Now().UnixNano(),
	}

	if err := w.appendRecord(&rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (w *WAL) LogCommitted(id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &LogRecord{
		Id:        id,
		State:     string(StateCommitted),
		Timestamp: time.Now().UnixNano(),
	}

	return w.appendRecord(rec)
}

func (w *WAL) appendRecord(rec *LogRecord) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(data)

	var (
		length = uint32(len(data))
		header = make([]byte, 8)
	)

	binary.BigEndian.PutUint32(header[0:4], length)
	binary.BigEndian.PutUint32(header[4:8], checksum)

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}

	return w.file.Sync()
}

func ReadNext(r io.Reader) (*LogRecord, error) {
	header := make([]byte, 8)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	expectedChecksum := binary.BigEndian.Uint32(header[4:8])

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("error while reading wal record: %w", err)
	}

	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("found corrupted data! checksum does not match!")
	}

	record := &LogRecord{}
	if err := proto.Unmarshal(payload, record); err != nil {
		return nil, err
	}

	return record, nil
}

func (w *WAL) Recover() (iter.Seq[string], error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	uncommitted := make(map[string][]string)

	for {
		record, err := ReadNext(w.file)

		if err == io.EOF {
			// succesful recover
			break
		}

		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// found truncated last record. maybe due to a forced kill...
				break
			}

			return nil, fmt.Errorf("stopped WAL recover. found corrupted data: %w", err)
		}

		if State(record.State) != StateCommitted {
			uncommitted[record.Id] = record.OldFiles
		}
	}

	seq := func(yield func(string) bool) {
		for _, files := range uncommitted {
			for _, f := range files {
				if !yield(f) {
					return
				}
			}
		}
	}

	return seq, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
