package main

import (
	"context"
	"crypto/tls"
	"flag"
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

	// logs configuration //
	// Create the logs directory if it doesn't exist
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatalf("failed to create log directory: %v", err)
	}

	// Now, open the log file within that directory
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
	// logs configuration //

	// Configure logging to include date, time, and file information.
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Define a command-line flag for the configuration file path.
	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	flag.Parse()

	// Load the application configuration from the specified path.
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Fatal error loading configuration: %v", err)
	}

	// Initialize the main in-memory store and the collection manager.
	mainInMemStore := store.NewInMemStoreWithShards(cfg.NumShards)
	collectionPersister := &persistence.CollectionPersisterImpl{}
	collectionManager := store.NewCollectionManager(collectionPersister, cfg.NumShards)

	// Load persistent data for the main store and all collections.
	if err := persistence.LoadData(mainInMemStore); err != nil {
		log.Fatalf("Fatal error loading main persistent data: %v", err)
	}
	if err := persistence.LoadAllCollectionsIntoManager(collectionManager); err != nil {
		log.Fatalf("Fatal error loading persistent collections data: %v", err)
	}

	// Ensure the system collection and default users exist.
	systemCollection := collectionManager.GetCollection(handler.SystemCollectionName)

	// Ensure default admin user
	adminUserKey := handler.UserPrefix + "admin"
	if _, found := systemCollection.Get(adminUserKey); !found {
		log.Println("Default admin user 'admin' not found. Creating a default admin user with password 'adminpass'.")
		hashedPassword, err := handler.HashPassword("adminpass")
		if err != nil {
			log.Fatalf("Fatal error hashing default admin password: %v", err)
		}
		adminUserInfo := handler.UserInfo{
			Username:     "admin",
			PasswordHash: hashedPassword,
			IsRoot:       false,
			// Admin can write to any collection but can only read the system collection.
			Permissions: map[string]string{"*": "write", handler.SystemCollectionName: "read"},
		}
		adminUserInfoBytes, err := json.Marshal(adminUserInfo)
		if err != nil {
			log.Fatalf("Fatal error marshalling default admin user info: %v", err)
		}
		systemCollection.Set(adminUserKey, adminUserInfoBytes, 0)
		collectionManager.EnqueueSaveTask(handler.SystemCollectionName, systemCollection)
		log.Println("Default admin user 'admin' created with password 'adminpass'.")
	} else {
		log.Println("Default admin user 'admin' found. Using existing credentials.")
	}

	// Ensure default root user (localhost only)
	rootUserKey := handler.UserPrefix + "root"
	if _, found := systemCollection.Get(rootUserKey); !found {
		log.Println("Default root user 'root' not found. Creating a default root user with password 'rootpass'.")
		hashedPassword, err := handler.HashPassword("rootpass")
		if err != nil {
			log.Fatalf("Fatal error hashing default root password: %v", err)
		}
		rootUserInfo := handler.UserInfo{
			Username:     "root",
			PasswordHash: hashedPassword,
			IsRoot:       true,
			// Root has universal write access.
			Permissions: map[string]string{"*": "write"},
		}
		rootUserInfoBytes, err := json.Marshal(rootUserInfo)
		if err != nil {
			log.Fatalf("Fatal error marshalling default root user info: %v", err)
		}
		systemCollection.Set(rootUserKey, rootUserInfoBytes, 0)
		collectionManager.EnqueueSaveTask(handler.SystemCollectionName, systemCollection)
		log.Println("Default root user 'root' created with password 'rootpass'.")
	} else {
		log.Println("Default root user 'root' found. Using existing credentials.")
	}

	// Load server certificate and key for TLS.
	cert, err := tls.LoadX509KeyPair("certificates/server.crt", "certificates/server.key")
	if err != nil {
		log.Fatalf("Failed to load server certificate or key: %v", err)
	}

	// Configure TLS settings, including the loaded certificate.
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Start the TLS TCP server.
	listener, err := tls.Listen("tcp", cfg.Port, tlsConfig)
	if err != nil {
		log.Fatalf("Fatal error starting TLS TCP server: %v", err)
	}
	defer listener.Close()
	log.Printf("TLS TCP server listening securely on %s", cfg.Port)

	// create backup manager
	backupManager := persistence.NewBackupManager(mainInMemStore, collectionManager, cfg.BackupInterval, cfg.BackupRetention)
	// start backups
	backupManager.Start()
	// sure close on exit.
	defer backupManager.Stop()

	// Accept connections in a goroutine.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" {
					log.Println("Listener de red cerrado. Deteniendo la aceptación de conexiones.")
					return
				}

				log.Printf("Error al aceptar conexión: %v", err)
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
		log.Printf("Starting TTL cleaner with interval of %s", cfg.TtlCleanInterval)

		for {
			select {
			case <-ticker.C:
				mainInMemStore.CleanExpiredItems()
				collectionManager.CleanExpiredItemsAndSave()
			case <-ttlCleanStopChan:
				log.Println("TTL cleaner received stop signal. Stopping.")
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
		log.Printf("Idle memory cleaner started. Check interval: %s, threshold: %s.", checkInterval, idleThreshold)

		for {
			select {
			case <-ticker.C:
				lastActive := lastActivity.Load().(time.Time)
				if time.Since(lastActive) >= idleThreshold {
					log.Println("Inactivity detected. Requesting Go runtime to release OS memory...")
					debug.FreeOSMemory()
				}
			case <-idleMemoryCleanerStopChan:
				log.Println("Idle memory cleaner received stop signal. Stopping.")
				return
			}
		}
	}()

	// Set up a channel to listen for termination signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// Stop the TCP listener to prevent new connections.
	if err := listener.Close(); err != nil {
		log.Printf("Error closing TCP listener: %v", err)
	} else {
		log.Println("TCP listener closed.")
	}

	// Stop all background tasks.
	snapshotManager.Stop()
	close(ttlCleanStopChan)
	close(idleMemoryCleanerStopChan)

	// Wait for the asynchronous collection saver to finish.
	log.Println("Waiting for all pending collection persistence tasks to complete...")
	collectionManager.Wait()
	log.Println("All pending collection persistence tasks completed.")

	// Context for graceful shutdown.
	_, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	// Save final data for the main store to disk.
	log.Println("Saving final data for main store before application exit...")
	if err := persistence.SaveData(mainInMemStore); err != nil {
		log.Printf("Error saving final data for main store during shutdown: %v", err)
	} else {
		log.Println("Final main store data saved.")
	}

	// Save final data for all collections to disk.
	log.Println("Saving final data for all collections before application exit...")
	if err := persistence.SaveAllCollectionsFromManager(collectionManager); err != nil {
		log.Printf("Error saving final data for collections during shutdown: %v", err)
	} else {
		log.Println("Final collection data saved. Application exiting.")
	}
}
