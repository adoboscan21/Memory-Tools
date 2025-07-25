package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

// Config holds application-wide configuration.
type Config struct {
	Port             string
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	ShutdownTimeout  time.Duration
	SnapshotInterval time.Duration
	EnableSnapshots  bool
	TtlCleanInterval time.Duration
}

// configJSON is an intermediate struct used for JSON unmarshalling.
// All time.Duration fields are represented as strings here.
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

// NewDefaultConfig creates and returns a Config struct with sensible default values.
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
// It uses default values if the file is not found or if certain fields are missing.
func LoadConfig(filePath string) (Config, error) {
	// Start with default configuration values.
	cfg := NewDefaultConfig()
	jsonCfg := configJSON{} // Create an instance of the intermediate struct

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
	// This will populate string fields for durations.
	if err := json.Unmarshal(data, &jsonCfg); err != nil {
		return cfg, fmt.Errorf("failed to unmarshal config file '%s': %w", filePath, err)
	}

	// Now, convert the string durations from jsonCfg to time.Duration in the actual Config.
	// Use the default values if a field was not present in the JSON (json.Unmarshal keeps zero values).
	// For booleans and strings, they are directly taken from jsonCfg.

	if jsonCfg.Port != "" {
		cfg.Port = jsonCfg.Port
	}
	if jsonCfg.EnableSnapshots { // This will correctly capture true/false from JSON; if false in JSON, it stays false.
		cfg.EnableSnapshots = jsonCfg.EnableSnapshots
	}

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
