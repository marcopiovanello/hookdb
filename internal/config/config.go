package config

import (
	"os"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	// DataDir is the entrypoint for the parquet files.
	// Each subdirectory will be mapped as a table in the SQL engine.
	DataDir string `yaml:"data_dir"`

	// Arrow Flight SQL gRCP server listen addr. The gRPC server is server over HTTP/2
	ListenAddr string `yaml:"listen_addr"`

	// TimeColumn is the column that is used by the ORDER BY of the time-series values
	// Not setting a timecolumn drastically lower the performance of the column store.
	TimeColumn string //XXX: deprecated time column should be always time

	// How often the parquet files are read for changes
	ScanInterval time.Duration `yaml:"scan_interval"`

	// How often the multi-level compaction job runs
	CompactionInterval time.Duration `yaml:"compaction_interval"`

	// Define the compaction level strategy
	Levels []LevelConfig `yaml:"compaction_levels"`

	// After the deletion are recorded in the WAL the values are batched for deletion.
	// This value sets when to defer the deletion job.
	DeleteGracePeriod time.Duration `yaml:"delete_grace_period"`
}

type LevelConfig struct {
	Level      int           `yaml:"level"`
	MinFiles   int           `yaml:"min_files"`
	MinFileAge time.Duration `yaml:"min_file_age"`
}

func Default() Config {
	return Config{
		DataDir:            "./data",
		ListenAddr:         "0.0.0.0:32010",
		TimeColumn:         "time",
		ScanInterval:       5 * time.Second,
		CompactionInterval: 20 * time.Second,
		DeleteGracePeriod:  30 * time.Second,

		Levels: []LevelConfig{
			{Level: 0, MinFiles: 8, MinFileAge: 45 * time.Second},
			{Level: 1, MinFiles: 6, MinFileAge: 0},
		},
	}
}

func LoadOrDefaults(path string) *Config {
	cfg := Default()

	fd, err := os.Open(path)
	if err != nil {
		return &cfg
	}

	if err := yaml.NewDecoder(fd).Decode(&cfg); err != nil {
		return &cfg
	}

	return &cfg
}
