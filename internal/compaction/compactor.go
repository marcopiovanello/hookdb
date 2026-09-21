package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/marcopiovanello/hookdb/internal/catalog"
	"github.com/marcopiovanello/hookdb/internal/config"
	"github.com/marcopiovanello/hookdb/pkg/sqlutil"
)

type Compactor struct {
	cat        *catalog.Catalog
	db         *sql.DB
	interval   time.Duration
	levels     []config.LevelConfig
	timeColumn string
	logger     *slog.Logger
}

type CompactorArgs struct {
	Catalog    *catalog.Catalog
	Database   *sql.DB
	Interval   time.Duration
	Levels     []config.LevelConfig
	TimeColumn string
	Logger     *slog.Logger
}

func New(args *CompactorArgs) *Compactor {
	if args.Logger == nil {
		args.Logger = slog.Default()
	}

	sorted := append([]config.LevelConfig{}, args.Levels...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Level < sorted[j].Level })

	return &Compactor{
		cat:        args.Catalog,
		db:         args.Database,
		interval:   args.Interval,
		levels:     sorted,
		timeColumn: args.TimeColumn,
		logger:     args.Logger,
	}
}

func (c *Compactor) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runOnce()
		}
	}
}

func (c *Compactor) runOnce() {
	tables := c.cat.Tables()

	for _, lvlCfg := range c.levels {
		for _, table := range tables {
			if err := c.compactLevel(table, lvlCfg); err != nil {
				c.logger.Error("compaction failed", "table", table, "level", lvlCfg.Level, "err", err)
			}
		}
	}
}

func (c *Compactor) compactLevel(table string, cfg config.LevelConfig) error {
	files := c.cat.FilesAtLevel(table, cfg.Level)

	candidates := c.filterStable(files, cfg.MinFileAge)
	if len(candidates) < cfg.MinFiles {
		return nil
	}

	var (
		ts        = time.Now().UnixNano()
		tableDir  = filepath.Dir(candidates[0])
		nextLevel = cfg.Level + 1

		tmpOut   = filepath.Join(tableDir, fmt.Sprintf(".compact-L%d-%d.parquet.tmp", nextLevel, ts))
		finalOut = filepath.Join(tableDir, fmt.Sprintf("L%d-%d.parquet", nextLevel, ts))
	)

	if err := c.mergeFiles(candidates, tmpOut); err != nil {
		_ = os.Remove(tmpOut)
		return fmt.Errorf("merge: %w", err)
	}

	if err := os.Rename(tmpOut, finalOut); err != nil {
		_ = os.Remove(tmpOut)
		return fmt.Errorf("rename: %w", err)
	}

	if err := c.cat.ReplaceFiles(table, candidates, finalOut, nextLevel); err != nil {
		return fmt.Errorf("failed updating catalog: %w", err)
	}

	c.logger.Info("compaction completed",
		"table", table,
		"from_level", cfg.Level,
		"to_level", nextLevel,
		"file_uniti", len(candidates),
		"output", finalOut,
	)

	return nil
}

func (c *Compactor) filterStable(files []string, minAge time.Duration) []string {
	if minAge <= 0 {
		return files
	}

	cutoff := time.Now().Add(-minAge)

	var stable []string

	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			stable = append(stable, f)
		}
	}

	return stable
}

func (c *Compactor) mergeFiles(files []string, outPath string) error {
	quotedFiles := sqlutil.QuoteStringSlice(files)
	quotedOut, err := sqlutil.QuoteLiteral(outPath)
	if err != nil {
		return err
	}

	orderedStmt := fmt.Sprintf(
		"COPY (SELECT * FROM read_parquet([%s], union_by_name=true) ORDER BY %s) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)",
		quotedFiles,
		sqlutil.ValidateIdentifier(c.timeColumn),
		quotedOut,
	)

	if _, err := c.db.Exec(orderedStmt); err == nil {
		return nil
	}

	fallbackStmt := fmt.Sprintf(
		"COPY (SELECT * FROM read_parquet([%s], union_by_name=true)) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)",
		quotedFiles,
		quotedOut,
	)

	_, err = c.db.Exec(fallbackStmt)
	return err
}
