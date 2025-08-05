package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config holds application-wide configuration.
type Config struct {
	Port                 string
	ShutdownTimeout      time.Duration
	SnapshotInterval     time.Duration
	EnableSnapshots      bool
	TtlCleanInterval     time.Duration
	BackupInterval       time.Duration
	BackupRetention      time.Duration
	NumShards            int
	DefaultRootPassword  string
	DefaultAdminPassword string
}

// NewDefaultConfig creates a Config struct with sensible default values.
func NewDefaultConfig() Config {
	return Config{
		Port:                 ":5876",
		ShutdownTimeout:      10 * time.Second,
		SnapshotInterval:     5 * time.Minute,
		EnableSnapshots:      true,
		TtlCleanInterval:     1 * time.Minute,
		BackupInterval:       1 * time.Hour,
		BackupRetention:      7 * 24 * time.Hour,
		NumShards:            16,
		DefaultRootPassword:  "rootpass",
		DefaultAdminPassword: "adminpass",
	}
}

// LoadConfig loads configuration with a clear precedence: Environment > Defaults.
func LoadConfig() Config {
	// 1. Start with default configuration values.
	cfg := NewDefaultConfig()
	slog.Info("Loading configuration with default values...")

	// 2. Override defaults with environment variables, if they are set.
	applyEnvConfig(&cfg)
	slog.Info("Configuration check for environment variables complete.")

	return cfg
}

// applyEnvConfig overrides config values from environment variables.
func applyEnvConfig(cfg *Config) {
	// String values
	if portEnv := os.Getenv("MEMORYTOOLS_PORT"); portEnv != "" {
		cfg.Port = portEnv
		slog.Info("Overriding Port from environment", "value", portEnv)
	}

	// Integer values
	if numShardsEnv := os.Getenv("MEMORYTOOLS_NUM_SHARDS"); numShardsEnv != "" {
		if i, err := strconv.Atoi(numShardsEnv); err == nil && i > 0 {
			cfg.NumShards = i
			slog.Info("Overriding NumShards from environment", "value", i)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_NUM_SHARDS env var, using default", "value", numShardsEnv)
		}
	}

	// Boolean values
	if enableSnapshotsEnv := os.Getenv("MEMORYTOOLS_ENABLE_SNAPSHOTS"); enableSnapshotsEnv != "" {
		if b, err := strconv.ParseBool(enableSnapshotsEnv); err == nil {
			cfg.EnableSnapshots = b
			slog.Info("Overriding EnableSnapshots from environment", "value", b)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_ENABLE_SNAPSHOTS env var, using default", "value", enableSnapshotsEnv)
		}
	}

	if rootPassEnv := os.Getenv("MEMORYTOOLS_ROOT_PASSWORD"); rootPassEnv != "" {
		cfg.DefaultRootPassword = rootPassEnv
		slog.Info("Overriding DefaultRootPassword from environment")
	}

	if adminPassEnv := os.Getenv("MEMORYTOOLS_ADMIN_PASSWORD"); adminPassEnv != "" {
		cfg.DefaultAdminPassword = adminPassEnv
		slog.Info("Overriding DefaultAdminPassword from environment")
	}

	// Duration values
	overrideDuration("MEMORYTOOLS_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)
	overrideDuration("MEMORYTOOLS_SNAPSHOT_INTERVAL", &cfg.SnapshotInterval)
	overrideDuration("MEMORYTOOLS_TTL_CLEAN_INTERVAL", &cfg.TtlCleanInterval)
	overrideDuration("MEMORYTOOLS_BACKUP_INTERVAL", &cfg.BackupInterval)
	overrideDuration("MEMORYTOOLS_BACKUP_RETENTION", &cfg.BackupRetention)
}

// overrideDuration is a helper to avoid repetition for duration env vars.
func overrideDuration(envKey string, target *time.Duration) {
	envVal := os.Getenv(envKey)
	if envVal != "" {
		if d, err := time.ParseDuration(envVal); err == nil {
			*target = d
			slog.Info("Overriding config from environment", "key", envKey, "value", envVal)
		} else {
			slog.Warn("Invalid duration format in env var, using default", "key", envKey, "value", envVal)
		}
	}
}
