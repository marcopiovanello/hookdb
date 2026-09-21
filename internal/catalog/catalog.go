// package catalog keeps track the list of parquet file linked to a "table".
// A table is represented by a subdirectory in the data dir.
// The parquet files are syncronized in a custom view created with DuckDB.
//
// DuckDB server as a fast in-memory query planner and engine for the parquet file
// enabling efficient SQL queried against the columnar data.
//
// Every file belongs to a different level (ranging 0-n) used by the compactor to compact
// the parquet files.
// Having levels enables a size-tiering based compaction logic: raw data (L0) are merged with
// larger files on the deeper levels (L0 -> L1).
// Every file has the prefix "L<N>-" for rebuild the catalog without a separate metadata store.
package catalog

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcopiovanello/hookdb/internal/wal"
	"github.com/marcopiovanello/hookdb/pkg/sqlutil"
)

var levelPrefixRe = regexp.MustCompile(`^L(\d+)-`)

// FileMeta describes an individual file inside the catalog
type FileMeta struct {
	Path  string
	Level int
}

// Table represents the state of a table in the catalog
type Table struct {
	Name  string
	Files []FileMeta
}

type Catalog struct {
	mu      sync.RWMutex
	db      *sql.DB
	dataDir string
	tables  map[string]*Table

	deleteGrace time.Duration
	logger      *slog.Logger

	wal *wal.WAL
}

func New(db *sql.DB, dataDir string, deleteGrace time.Duration, logger *slog.Logger) *Catalog {
	if logger == nil {
		logger = slog.Default()
	}

	walLog, err := wal.Open(dataDir)
	if err != nil {
		panic(fmt.Sprintf("cannot initialize wal: %s", err.Error()))
	}

	cat := &Catalog{
		db:          db,
		dataDir:     dataDir,
		tables:      make(map[string]*Table),
		deleteGrace: deleteGrace,
		logger:      logger,
		wal:         walLog,
	}

	if err := cat.recoverPendingDeletions(); err != nil {
		logger.Error("error while recovering from WAL", "err", err)
	}

	return cat
}

func (c *Catalog) recoverPendingDeletions() error {
	oldFilesSeq, err := c.wal.Recover()
	if err != nil {
		return err
	}

	for f := range oldFilesSeq {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			c.logger.Warn("cannot remove file", "file", f, "err", err)
		}
	}
	return nil
}

// LevelOf extract the level from the filename.
// External writers typically write files without knowing which level they belogs to, so external data is
// always treated a L0
func LevelOf(path string) int {
	name := filepath.Base(path)
	m := levelPrefixRe.FindStringSubmatch(name)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}

// Scan the data dir and refreshes the views inside DuckDB. Must be called periodically.
func (c *Catalog) Scan() error {
	entries, err := os.ReadDir(c.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read data dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		tableName := e.Name()
		tableDir := filepath.Join(c.dataDir, tableName)

		files, err := listParquetFiles(tableDir)
		if err != nil {
			c.logger.Warn("unreadable table directory", "table", tableName, "err", err)
			continue
		}

		if len(files) == 0 {
			continue
		}

		c.mu.Lock()

		t, exists := c.tables[tableName]
		if !exists {
			t = &Table{Name: tableName}
			c.tables[tableName] = t
		}

		changed := !sameFileSet(t.Files, files)
		if changed {
			t.Files = files
		}

		c.mu.Unlock()

		if changed {
			if err := c.refreshView(tableName, files); err != nil {
				return fmt.Errorf("refresh view %s: %w", tableName, err)
			}
			c.logger.Info("table updated", "table", tableName, "n_file", len(files))
		}
	}
	return nil
}

// Returns a snapshot of currently known tables
func (c *Catalog) Tables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for n := range c.tables {
		names = append(names, n)
	}

	sort.Strings(names)
	return names
}

// Return the paths of the parquet files of a table of a specific level
func (c *Catalog) FilesAtLevel(table string, level int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, ok := c.tables[table]
	if !ok {
		return nil
	}

	var out []string
	for _, f := range t.Files {
		if f.Level == level {
			out = append(out, filepath.Clean(f.Path))
		}
	}

	return out
}

// ReplaceFiles atomically swaps (from the DuckDB POV) a set of Parquet files with a compacted one.
// The compacted file is typically recorded on the next level available.
// ReplaceFiles uses a WAL to delete older files (that are already compacted) safely.
//
// TLDR: ReplaceFiles is transacional
func (c *Catalog) ReplaceFiles(table string, oldFiles []string, newFile string, newLevel int) error {
	// Normalizza i path per evitare difformità nelle stringhe
	cleanOldFiles := make([]string, len(oldFiles))
	for i, f := range oldFiles {
		cleanOldFiles[i] = filepath.Clean(f)
	}
	cleanNewFile := filepath.Clean(newFile)

	// WAL transaction ID
	txID := fmt.Sprintf("%s-%d", table, time.Now().UnixNano())

	// start a transaction
	// write ahead phase - record the deletion request before thouching the storage and the db
	_, err := c.wal.LogPending(txID, table, cleanOldFiles, cleanNewFile, newLevel)
	if err != nil {
		return fmt.Errorf("wal log pending: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	t, ok := c.tables[table]
	if !ok {
		return fmt.Errorf("unknown table: %s", table)
	}

	// compute the new state. aka the level drilling.
	remaining := diffMeta(t.Files, cleanOldFiles)
	newSet := append(remaining, FileMeta{Path: cleanNewFile, Level: newLevel})

	// atomic update of duckdb
	if err := c.refreshViewLocked(table, newSet); err != nil {
		return fmt.Errorf("refresh view during compaction: %w", err)
	}

	t.Files = newSet

	// schedule a cancellation of the old files which will commit the pending WAL transaction
	go c.deleteAfterGrace(txID, cleanOldFiles)

	return nil
}

func (c *Catalog) deleteAfterGrace(txID string, files []string) {
	if c.deleteGrace > 0 {
		time.Sleep(c.deleteGrace)
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			c.logger.Warn("cannot remove compacted file", "file", f, "err", err)
		}
	}

	// commit the WAL transaction
	if err := c.wal.LogCommitted(txID); err != nil {
		c.logger.Error("cannot write commit in the WAL", "txID", txID, "err", err)
	}
}

// excecutes the swap of ReplaceFiles. it must be wrapped by a mutex lock
func (c *Catalog) refreshViewLocked(table string, files []FileMeta) error {
	if len(files) == 0 {
		return nil
	}
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + strings.ReplaceAll(f.Path, "'", "''") + "'"
	}

	stmt := fmt.Sprintf(
		"CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet([%s], union_by_name=true)",
		table,
		strings.Join(quoted, ", "),
	)
	_, err := c.db.Exec(stmt)
	return err
}

func (c *Catalog) refreshView(table string, files []FileMeta) error {
	if len(files) == 0 {
		return nil
	}

	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + strings.ReplaceAll(f.Path, "'", "''") + "'"
	}

	stmt := fmt.Sprintf(
		"CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet([%s], union_by_name=true)",
		sqlutil.ValidateIdentifier(table),
		strings.Join(quoted, ", "),
	)

	_, err := c.db.Exec(stmt)
	return err
}

func listParquetFiles(dir string) ([]FileMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var out []FileMeta

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".parquet") {
			continue
		}

		// ignores files that external writers has yet to batch
		if strings.HasSuffix(name, ".tmp") {
			continue
		}

		p := filepath.Join(dir, name)
		out = append(out, FileMeta{Path: p, Level: LevelOf(p)})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}
