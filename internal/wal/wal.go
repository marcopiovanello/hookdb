package wal

import (
	"iter"
)

type CompactionLog struct {
	Table         string
	CompactedFile string
	OldFiles      []string
	NewLevel      int
}

type WAL interface {
	// Starts a transaction.
	// Logs first the request of modifying the underlying Parquet files.
	LogPending(id string, log *CompactionLog) (*LogRecord, error)

	// Commit a transaction in the WAL
	LogCommitted(id string) error

	// Reads the entire WAL and returns an iterator containg the Parquet files that
	// are not commited.
	Recover() (iter.Seq[string], error)

	// Safely closes the WAL
	Close() error
}
