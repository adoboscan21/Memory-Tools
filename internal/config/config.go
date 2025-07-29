package config

import (
	"fmt"
	"log"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Config holds application-wide configuration.
type Config struct {
	Port             string        `json:"port"`
	ReadTimeout      time.Duration `json:"read_timeout"`
	WriteTimeout     time.Duration `json:"write_timeout"`
	IdleTimeout      time.Duration `json:"idle_timeout"`
	ShutdownTimeout  time.Duration `json:"shutdown_timeout"`
	SnapshotInterval time.Duration `json:"snapshot_interval"`  // How often to take snapshots
	EnableSnapshots  bool          `json:"enable_snapshots"`   // Whether scheduled snapshots are enabled.
	TtlCleanInterval time.Duration `json:"ttl_clean_interval"` // How often the TTL cleaner runs
}

// configJSON is an intermediate struct used for JSON unmarshalling.
type configJSON struct {
	Port             string `json:"port"`
	ReadTimeout      string `json:"read_timeout"`
	WriteTimeout     string `json:"write_timeout"`
	IdleTimeout      string `json:"idle_timeout"`
	ShutdownTimeout  string `json:"shutdown_timeout"`
	SnapshotInterval string `json:"snapshot_interval"`
	EnableSnapshots  bool   `json:"enable_snapshots"`
	TtlCleanInterval string `json:"ttl_clean_interval"`
}

// NewDefaultConfig creates a Config struct with sensible default values.
func NewDefaultConfig() Config {
	return Config{
		Port:             ":8080",
		ReadTimeout:      5 * time.Second,
		WriteTimeout:     10 * time.Second,
		IdleTimeout:      120 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		SnapshotInterval: 5 * time.Minute,
		EnableSnapshots:  true,
		TtlCleanInterval: 1 * time.Minute,
	}
}

// LoadConfig loads configuration from a specified JSON file path.
func LoadConfig(filePath string) (Config, error) {
	// Start with default configuration values.
	cfg := NewDefaultConfig()
	jsonCfg := configJSON{} // Intermediate struct for string durations

	// Read the content of the JSON file.
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Config file '%s' not found. Using default configuration.", filePath)
			return cfg, nil // Return defaults if file doesn't exist
		}
		return cfg, fmt.Errorf("failed to read config file '%s': %w", filePath, err)
	}

	// Unmarshal JSON into the intermediate struct.
	if err := json.Unmarshal(data, &jsonCfg); err != nil {
		return cfg, fmt.Errorf("failed to unmarshal config file '%s': %w", filePath, err)
	}

	// Convert string durations from jsonCfg to time.Duration in the actual Config.
	if jsonCfg.Port != "" {
		cfg.Port = jsonCfg.Port
	}
	cfg.EnableSnapshots = jsonCfg.EnableSnapshots // Direct assignment.

	var parseErr error

	if jsonCfg.ReadTimeout != "" {
		cfg.ReadTimeout, parseErr = time.ParseDuration(jsonCfg.ReadTimeout)
		if parseErr != nil {
			return cfg, fmt.Errorf("invalid 'read_timeout' format in config file: %w", parseErr)
		}
	}
	if jsonCfg.WriteTimeout != "" {
		cfg.WriteTimeout, parseErr = time.ParseDuration(jsonCfg.WriteTimeout)
		if parseErr != nil {
			return cfg, fmt.Errorf("invalid 'write_timeout' format in config file: %w", parseErr)
		}
	}
	if jsonCfg.IdleTimeout != "" {
		cfg.IdleTimeout, parseErr = time.ParseDuration(jsonCfg.IdleTimeout)
		if parseErr != nil {
			return cfg, fmt.Errorf("invalid 'idle_timeout' format in config file: %w", parseErr)
		}
	}
	if jsonCfg.ShutdownTimeout != "" {
		cfg.ShutdownTimeout, parseErr = time.ParseDuration(jsonCfg.ShutdownTimeout)
		if parseErr != nil {
			return cfg, fmt.Errorf("invalid 'shutdown_timeout' format in config file: %w", parseErr)
		}
	}
	if jsonCfg.SnapshotInterval != "" {
		cfg.SnapshotInterval, parseErr = time.ParseDuration(jsonCfg.SnapshotInterval)
		if parseErr != nil {
			return cfg, fmt.Errorf("invalid 'snapshot_interval' format in config file: %w", parseErr)
		}
	}
	if jsonCfg.TtlCleanInterval != "" {
		cfg.TtlCleanInterval, parseErr = time.ParseDuration(jsonCfg.TtlCleanInterval)
		if parseErr != nil {
			return cfg, fmt.Errorf("invalid 'ttl_clean_interval' format in config file: %w", parseErr)
		}
	}

	log.Printf("Configuration loaded from '%s'.", filePath)
	return cfg, nil
}
