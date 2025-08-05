package main

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"log/slog"
	"memory-tools/internal/config"
	"memory-tools/internal/handler"
	"memory-tools/internal/persistence"
	"memory-tools/internal/store"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// lastActivity tracks the last time a data operation occurred.
var lastActivity atomic.Value

// init sets the initial lastActivity time when the application starts.
func init() {
	lastActivity.Store(time.Now())
}

// updateActivityFunc is a helper type to implement the handler.ActivityUpdater interface.
type updateActivityFunc func()

// UpdateActivity updates the lastActivity timestamp.
func (f updateActivityFunc) UpdateActivity() {
	lastActivity.Store(time.Now())
}

func main() {
	// Attempt to load .env file. It's okay if it doesn't exist.
	if err := godotenv.Load(); err != nil {
		// This is a debug-level message because not having a .env file is normal in production.
		// We can't use slog yet as it's not configured, so we use a temporary log.
		log.Println("DEBUG: No .env file found, proceeding with existing environment")
	}

	// --- slog configuration ---
	// Create the logs directory if it doesn't exist.
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatalf("failed to create log directory: %v", err)
	}

	// Now, open the log file within that directory.
	logFile, err := os.OpenFile("logs/memory-tools.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}

	multiWriter := io.MultiWriter(os.Stdout, logFile)

	handlerLog := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	logger := slog.New(handlerLog)
	slog.SetDefault(logger)
	// --- end of slog configuration ---

	slog.Info("Logger successfully configured", "output", "console_and_file")

	// Load the application configuration.
	cfg := config.LoadConfig()

	// Initialize the main in-memory store and the collection manager.
	mainInMemStore := store.NewInMemStoreWithShards(cfg.NumShards)
	collectionPersister := &persistence.CollectionPersisterImpl{}
	collectionManager := store.NewCollectionManager(collectionPersister, cfg.NumShards)

	// Load persistent data for the main store and all collections.
	if err := persistence.LoadData(mainInMemStore); err != nil {
		slog.Error("Fatal error loading main persistent data", "error", err)
		os.Exit(1)
	}
	if err := persistence.LoadAllCollectionsIntoManager(collectionManager); err != nil {
		slog.Error("Fatal error loading persistent collections data", "error", err)
		os.Exit(1)
	}

	// Ensure the system collection and default users exist.
	systemCollection := collectionManager.GetCollection(handler.SystemCollectionName)

	// Ensure default admin user
	adminUserKey := handler.UserPrefix + "admin"
	if _, found := systemCollection.Get(adminUserKey); !found {
		slog.Info("Default admin user not found, creating...", "user", "admin")
		hashedPassword, err := handler.HashPassword("adminpass")
		if err != nil {
			slog.Error("Fatal error hashing default admin password", "error", err)
			os.Exit(1)
		}
		adminUserInfo := handler.UserInfo{
			Username:     "admin",
			PasswordHash: hashedPassword,
			IsRoot:       false,
			Permissions:  map[string]string{"*": "write", handler.SystemCollectionName: "read"},
		}
		adminUserInfoBytes, err := json.Marshal(adminUserInfo)
		if err != nil {
			slog.Error("Fatal error marshalling default admin user info", "error", err)
			os.Exit(1)
		}
		systemCollection.Set(adminUserKey, adminUserInfoBytes, 0)
		collectionManager.EnqueueSaveTask(handler.SystemCollectionName, systemCollection)
		slog.Info("Default admin user created", "user", "admin", "password", "adminpass")
	} else {
		slog.Info("Default admin user found", "user", "admin")
	}

	// Ensure default root user (localhost only)
	rootUserKey := handler.UserPrefix + "root"
	if _, found := systemCollection.Get(rootUserKey); !found {
		slog.Info("Default root user not found, creating...", "user", "root")
		hashedPassword, err := handler.HashPassword("rootpass")
		if err != nil {
			slog.Error("Fatal error hashing default root password", "error", err)
			os.Exit(1)
		}
		rootUserInfo := handler.UserInfo{
			Username:     "root",
			PasswordHash: hashedPassword,
			IsRoot:       true,
			Permissions:  map[string]string{"*": "write"},
		}
		rootUserInfoBytes, err := json.Marshal(rootUserInfo)
		if err != nil {
			slog.Error("Fatal error marshalling default root user info", "error", err)
			os.Exit(1)
		}
		systemCollection.Set(rootUserKey, rootUserInfoBytes, 0)
		collectionManager.EnqueueSaveTask(handler.SystemCollectionName, systemCollection)
		slog.Info("Default root user created", "user", "root", "password", "rootpass")
	} else {
		slog.Info("Default root user found", "user", "root")
	}

	// Load server certificate and key for TLS.
	cert, err := tls.LoadX509KeyPair("certificates/server.crt", "certificates/server.key")
	if err != nil {
		slog.Error("Failed to load server certificate or key", "error", err)
		os.Exit(1)
	}

	// Configure TLS settings, including the loaded certificate.
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Start the TLS TCP server.
	listener, err := tls.Listen("tcp", cfg.Port, tlsConfig)
	if err != nil {
		slog.Error("Fatal error starting TLS TCP server", "port", cfg.Port, "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	slog.Info("TLS TCP server listening securely", "port", cfg.Port)

	// Create and start the backup manager.
	backupManager := persistence.NewBackupManager(mainInMemStore, collectionManager, cfg.BackupInterval, cfg.BackupRetention)
	backupManager.Start()
	defer backupManager.Stop()

	// Accept connections in a goroutine.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" {
					slog.Info("Network listener closed, stopping connection acceptance.")
					return
				}
				slog.Error("Error accepting connection", "error", err)
				continue
			}

			// Handle each new connection in a separate goroutine.
			go handler.NewConnectionHandler(
				mainInMemStore,
				collectionManager,
				backupManager,
				updateActivityFunc(func() { lastActivity.Store(time.Now()) }),
				conn,
			).HandleConnection(conn)
		}
	}()

	// Initialize and start the snapshot manager.
	snapshotManager := persistence.NewSnapshotManager(mainInMemStore, cfg.SnapshotInterval, cfg.EnableSnapshots)
	go snapshotManager.Start()

	// Start the TTL cleaner goroutine.
	ttlCleanStopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cfg.TtlCleanInterval)
		defer ticker.Stop()
		slog.Info("Starting TTL cleaner", "interval", cfg.TtlCleanInterval.String())

		for {
			select {
			case <-ticker.C:
				mainInMemStore.CleanExpiredItems()
				collectionManager.CleanExpiredItemsAndSave()
			case <-ttlCleanStopChan:
				slog.Info("TTL cleaner received stop signal. Stopping.")
				return
			}
		}
	}()

	// Goroutine to monitor for inactivity and trigger memory release to the OS.
	idleMemoryCleanerStopChan := make(chan struct{})
	go func() {
		checkInterval := 2 * time.Minute
		idleThreshold := 5 * time.Minute

		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		slog.Info("Starting idle memory cleaner", "check_interval", checkInterval.String(), "idle_threshold", idleThreshold.String())

		for {
			select {
			case <-ticker.C:
				lastActive := lastActivity.Load().(time.Time)
				if time.Since(lastActive) >= idleThreshold {
					slog.Info("Inactivity detected, requesting Go runtime to release OS memory...")
					debug.FreeOSMemory()
				}
			case <-idleMemoryCleanerStopChan:
				slog.Info("Idle memory cleaner received stop signal. Stopping.")
				return
			}
		}
	}()

	// Set up a channel to listen for termination signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Termination signal received. Attempting graceful shutdown...")

	// Stop the TCP listener to prevent new connections.
	if err := listener.Close(); err != nil {
		slog.Error("Error closing TCP listener", "error", err)
	} else {
		slog.Info("TCP listener closed.")
	}

	// Stop all background tasks.
	snapshotManager.Stop()
	close(ttlCleanStopChan)
	close(idleMemoryCleanerStopChan)

	// Wait for the asynchronous collection saver to finish.
	slog.Info("Waiting for all pending collection persistence tasks to complete...")
	collectionManager.Wait()
	slog.Info("All pending collection persistence tasks completed.")

	// Context for graceful shutdown.
	_, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	// Save final data for the main store to disk.
	slog.Info("Saving final data for main store before application exit...")
	if err := persistence.SaveData(mainInMemStore); err != nil {
		slog.Error("Error saving final data for main store during shutdown", "error", err)
	} else {
		slog.Info("Final main store data saved.")
	}

	// Save final data for all collections to disk.
	slog.Info("Saving final data for all collections before application exit...")
	if err := persistence.SaveAllCollectionsFromManager(collectionManager); err != nil {
		slog.Error("Error saving final data for collections during shutdown", "error", err)
	} else {
		slog.Info("Final collection data saved. Application exiting.")
	}
}
