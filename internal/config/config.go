package config

import "time"

// Config holds application-wide configuration.
type Config struct {
	Port             string
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	ShutdownTimeout  time.Duration
	SnapshotInterval time.Duration // How often to take snapshots (e.g., 5 * time.Minute)
	EnableSnapshots  bool          // Whether scheduled snapshots are enabled.
	// New configuration for TTL cleaner
	TtlCleanInterval time.Duration // How often the TTL cleaner runs (e.g., 1 * time.Minute)
}
