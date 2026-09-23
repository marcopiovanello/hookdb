// Package wal implements a Write-Ahead-Log (WAL) to handle changes and deletion
// of the Parquet files in a transactional fashion.
//
// A record of the WAL follows the following structure:
//
// |        header (8 bytes)      	  |   payload (sizeof(psize))   |
// |----------------------------------|-----------------------------|
// | psize(4 bytes) + CRC32 (4 bytes) |   log (protocol buffers)    |
//
// The payload is serialized with Protocol Buffers.
// The WAL file ends terminates with an EOF, no other special terminators.

package wal

import (
	"context"
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

	HeaderLen int = 8
)

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

type AsyncWAL struct {
	mu   sync.Mutex
	path string
	file *os.File

	headerBuf [HeaderLen]byte
}

func Open(ctx context.Context, dataDir string) (WAL, error) {
	walPath := filepath.Join(dataDir, "db.wal")

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	w := &AsyncWAL{
		path: walPath,
		file: f,
	}

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.mu.Lock()
				w.Sync()
				w.mu.Unlock()
			case <-ctx.Done():
				w.Close()
				return
			}
		}
	}()

	return w, nil
}

func (w *AsyncWAL) Sync() error {
	return w.file.Sync()
}

func (w *AsyncWAL) LogPending(id string, log *CompactionLog) (*LogRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := LogRecord{
		Id:            id,
		Table:         log.Table,
		OldFiles:      log.OldFiles,
		CompactedFile: log.CompactedFile,
		NewLevel:      uint32(log.NewLevel),
		State:         string(StatePending),
		Timestamp:     time.Now().UnixNano(),
	}

	if err := w.appendRecord(&rec); err != nil {
		return nil, err
	}

	return &rec, nil
}

func (w *AsyncWAL) LogCommitted(id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &LogRecord{
		Id:        id,
		State:     string(StateCommitted),
		Timestamp: time.Now().UnixNano(),
	}

	return w.appendRecord(rec)
}

// Add a log to the WAL.
func (w *AsyncWAL) appendRecord(rec *LogRecord) error {
	pBuf := bufferPool.Get().(*[]byte)
	buf := (*pBuf)[:0]

	options := proto.MarshalOptions{}
	data, err := options.MarshalAppend(buf, rec)
	if err != nil {
		bufferPool.Put(pBuf)
		return err
	}

	checksum := crc32.ChecksumIEEE(data)

	var (
		length = uint32(len(data))
		header = make([]byte, HeaderLen)
	)

	binary.BigEndian.PutUint32(header[0:4], length)
	binary.BigEndian.PutUint32(header[4:8], checksum)

	if _, err := w.file.Write(header); err != nil {
		bufferPool.Put(pBuf)
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		bufferPool.Put(pBuf)
		return err
	}

	*pBuf = data
	bufferPool.Put(pBuf)

	return nil
}

func readNext(r io.Reader) (*LogRecord, error) {
	header := make([]byte, HeaderLen)

	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[0:4])
	expectedChecksum := binary.BigEndian.Uint32(header[4:HeaderLen])

	compressedPayload := make([]byte, length)
	if _, err := io.ReadFull(r, compressedPayload); err != nil {
		return nil, fmt.Errorf("error while reading wal record: %w", err)
	}

	payload := make([]byte, 0)

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

func (w *AsyncWAL) Recover() (iter.Seq[string], error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	uncommitted := make(map[string][]string)

	for {
		record, err := readNext(w.file)

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

func (w *AsyncWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
