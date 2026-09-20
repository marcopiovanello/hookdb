// Package config centralizza i parametri di avvio del motore.
package config

import (
	"os"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	DataDir            string        `yaml:"data_dir"`
	ListenAddr         string        `yaml:"listen_addr"`
	TimeColumn         string        `yaml:"time_column"`
	ScanInterval       time.Duration `yaml:"scan_interval"`
	CompactionInterval time.Duration `yaml:"compaction_interval"`
	Levels             []LevelConfig `yaml:"compaction_levels"`
	DeleteGracePeriod  time.Duration `yaml:"delete_grace_period"`
}

type LevelConfig struct {
	Level      int           `yaml:"level"`
	MinFiles   int           `yaml:"min_files"`
	MinFileAge time.Duration `yaml:"min_file_age"`
}

func Default() Config {
	return Config{
		DataDir:            "/tmp/datadir",
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
