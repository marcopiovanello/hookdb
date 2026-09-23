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
	"bytes"
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

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

type State string

const (
	StatePending   State = "PENDING"
	StateCommitted State = "COMMITTED"

	HeaderLen int = 8
)

type WALImpl struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func Open(dataDir string) (WAL, error) {
	walPath := filepath.Join(dataDir, "db.wal")

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	return &WALImpl{
		path: walPath,
		file: f,
	}, nil
}

func (w *WALImpl) LogPending(id string, log *CompactionLog) (*LogRecord, error) {
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

func (w *WALImpl) LogCommitted(id string) error {
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
func (w *WALImpl) appendRecord(rec *LogRecord) error {
	data, err := proto.Marshal(rec)
	if err != nil {
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
		return err
	}

	walBuf := &bytes.Buffer{}

	if _, err := walBuf.Write(data); err != nil {
		return err
	}

	enc, err := zstd.NewWriter(w.file)
	if err != nil {
		return err
	}

	if _, err := io.Copy(enc, walBuf); err != nil {
		return err
	}

	return w.file.Sync()
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

	dec, err := zstd.NewReader(bytes.NewReader(compressedPayload))
	if err != nil {
		return nil, err
	}

	payload := &bytes.Buffer{}

	dec.WriteTo(payload)

	actualChecksum := crc32.ChecksumIEEE(payload.Bytes())
	if actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("found corrupted data! checksum does not match!")
	}

	record := &LogRecord{}
	if err := proto.Unmarshal(payload.Bytes(), record); err != nil {
		return nil, err
	}

	return record, nil
}

func (w *WALImpl) Recover() (iter.Seq[string], error) {
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

func (w *WALImpl) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
