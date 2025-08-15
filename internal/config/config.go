// ./internal/config/config.go

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
	EnableWal            bool
	TtlCleanInterval     time.Duration
	BackupInterval       time.Duration
	BackupRetention      time.Duration
	NumShards            int
	DefaultRootPassword  string
	DefaultAdminPassword string
	ColdStorageMonths    int
	HotStorageCleanHours int
	WorkerPoolSize       int
}

// NewDefaultConfig creates a Config struct with sensible default values.
func NewDefaultConfig() Config {
	return Config{
		Port:                 ":5876",
		ShutdownTimeout:      10 * time.Second,
		SnapshotInterval:     5 * time.Minute,
		EnableSnapshots:      true,
		EnableWal:            false,
		TtlCleanInterval:     1 * time.Minute,
		BackupInterval:       1 * time.Hour,
		BackupRetention:      7 * 24 * time.Hour,
		NumShards:            16,
		DefaultRootPassword:  "rootpass",
		DefaultAdminPassword: "adminpass",
		ColdStorageMonths:    3,
		HotStorageCleanHours: 24,
		WorkerPoolSize:       100,
	}
}

// LoadConfig loads configuration with a clear precedence: Environment > Defaults.
func LoadConfig() Config {
	cfg := NewDefaultConfig()
	slog.Info("Loading configuration...")
	applyEnvConfig(&cfg)
	return cfg
}

// applyEnvConfig overrides config values from environment variables.
func applyEnvConfig(cfg *Config) {
	if portEnv := os.Getenv("MEMORYTOOLS_PORT"); portEnv != "" {
		cfg.Port = portEnv
		slog.Info("Overriding Port from environment", "value", portEnv)
	}

	if numShardsEnv := os.Getenv("MEMORYTOOLS_NUM_SHARDS"); numShardsEnv != "" {
		if i, err := strconv.Atoi(numShardsEnv); err == nil && i > 0 {
			cfg.NumShards = i
			slog.Info("Overriding NumShards from environment", "value", i)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_NUM_SHARDS env var, using default", "value", numShardsEnv)
		}
	}

	if coldMonthsEnv := os.Getenv("MEMORYTOOLS_COLD_STORAGE_MONTHS"); coldMonthsEnv != "" {
		if i, err := strconv.Atoi(coldMonthsEnv); err == nil && i >= 0 {
			cfg.ColdStorageMonths = i
			slog.Info("Overriding ColdStorageMonths from environment", "value", i)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_COLD_STORAGE_MONTHS env var, using default", "value", coldMonthsEnv)
		}
	}

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
	}

	if adminPassEnv := os.Getenv("MEMORYTOOLS_ADMIN_PASSWORD"); adminPassEnv != "" {
		cfg.DefaultAdminPassword = adminPassEnv
	}

	if hotHoursEnv := os.Getenv("MEMORYTOOLS_HOT_STORAGE_CLEAN_HOURS"); hotHoursEnv != "" {
		if i, err := strconv.Atoi(hotHoursEnv); err == nil && i >= 0 {
			cfg.HotStorageCleanHours = i
			slog.Info("Overriding HotStorageCleanHours from environment", "value", i)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_HOT_STORAGE_CLEAN_HOURS env var, using default", "value", hotHoursEnv)
		}
	}

	if workerPoolEnv := os.Getenv("MEMORYTOOLS_WORKER_POOL_SIZE"); workerPoolEnv != "" {
		if i, err := strconv.Atoi(workerPoolEnv); err == nil && i > 0 {
			cfg.WorkerPoolSize = i
			slog.Info("Overriding WorkerPoolSize from environment", "value", i)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_WORKER_POOL_SIZE env var, using default", "value", workerPoolEnv)
		}
	}

	if enableWalEnv := os.Getenv("MEMORYTOOLS_ENABLE_WAL"); enableWalEnv != "" {
		if b, err := strconv.ParseBool(enableWalEnv); err == nil {
			cfg.EnableWal = b
			slog.Info("Overriding EnableWal from environment", "value", b)
		} else {
			slog.Warn("Invalid MEMORYTOOLS_ENABLE_WAL env var, using default", "value", enableWalEnv)
		}
	}

	overrideDuration("MEMORYTOOLS_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)
	overrideDuration("MEMORYTOOLS_SNAPSHOT_INTERVAL", &cfg.SnapshotInterval)
	overrideDuration("MEMORYTOOLS_TTL_CLEAN_INTERVAL", &cfg.TtlCleanInterval)
	overrideDuration("MEMORYTOOLS_BACKUP_INTERVAL", &cfg.BackupInterval)
	overrideDuration("MEMORYTOOLS_BACKUP_RETENTION", &cfg.BackupRetention)
}

func overrideDuration(envKey string, target *time.Duration) {
	envVal := os.Getenv(envKey)
	if envVal != "" {
		if d, err := time.ParseDuration(envVal); err == nil {
			*target = d
			slog.Info("Overriding duration from environment", "key", envKey, "value", envVal)
		} else {
			slog.Warn("Invalid duration format in env var, using default", "key", envKey, "value", envVal)
		}
	}
}
