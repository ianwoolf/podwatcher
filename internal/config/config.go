package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// LogConfig controls klog log output.
type LogConfig struct {
	File    string `yaml:"file"`    // log file path
	MaxSize int    `yaml:"maxSize"` // max size of each log file in MB
	MaxNum  int    `yaml:"maxNum"`  // number of old log files to keep
}

// PodsConfig controls the pod informer and the application/pod record stores.
type PodsConfig struct {
	ResumeFile          string `yaml:"resumeFile"`             // file to persist the pod watch resourceVersion
	MaxApplications     int    `yaml:"maxApplications"`        // max application records kept (oldest deleted evicted)
	StateFile           string `yaml:"stateFile"`              // file to persist applications and driver/executor pod coordinates
	MaxPodRecords       int    `yaml:"maxPodRecords"`          // max pod records kept (oldest deleted evicted)
	CheckpointURL       string `yaml:"checkpointUrl"`          // downstream checkpoint API base URL; when set, resourceVersion is checkpointed over HTTP instead of resumeFile
	CheckpointFlushSecs int    `yaml:"checkpointFlushSeconds"` // resourceVersion checkpoint flush interval in seconds
}

// Config is the application configuration.
type Config struct {
	Log  LogConfig  `yaml:"log"`
	Pods PodsConfig `yaml:"pods"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Log: LogConfig{
			File:    "/var/log/podwatcher/podwatcher.log",
			MaxSize: 100,
			MaxNum:  3,
		},
		Pods: PodsConfig{
			ResumeFile:          "/var/log/podwatcher/pods-resume.json",
			StateFile:           "/var/log/podwatcher/state.json",
			MaxApplications:     5000,
			MaxPodRecords:       10000,
			CheckpointFlushSecs: 10,
		},
	}
}

// Load reads configuration from file (optional) and applies environment
// variable overrides: env > config file > defaults.
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", configPath, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}

	if v := os.Getenv("PODWATCHER_LOG_FILE"); v != "" {
		cfg.Log.File = v
	}
	if v := os.Getenv("PODWATCHER_LOG_MAX_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Log.MaxSize = n
		}
	}
	if v := os.Getenv("PODWATCHER_LOG_MAX_NUM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Log.MaxNum = n
		}
	}
	if v := os.Getenv("PODWATCHER_PODS_RESUME_FILE"); v != "" {
		cfg.Pods.ResumeFile = v
	}
	if v := os.Getenv("PODWATCHER_APPLICATIONS_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Pods.MaxApplications = n
		}
	}
	if v := os.Getenv("PODWATCHER_STATE_FILE"); v != "" {
		cfg.Pods.StateFile = v
	}
	if v := os.Getenv("PODWATCHER_POD_RECORDS_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Pods.MaxPodRecords = n
		}
	}

	if v := os.Getenv("PODWATCHER_CHECKPOINT_URL"); v != "" {
		cfg.Pods.CheckpointURL = v
	}
	if v := os.Getenv("PODWATCHER_CHECKPOINT_FLUSH_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Pods.CheckpointFlushSecs = n
		}
	}

	return cfg, nil
}
