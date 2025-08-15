package main

import (
	"bytes"
	"crypto/tls"
	"io"
	"log/slog"
	"memory-tools/internal/config"
	"memory-tools/internal/globalconst"
	"memory-tools/internal/handler"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"memory-tools/internal/wal"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary
var lastActivity atomic.Value

func init() {
	lastActivity.Store(time.Now())
}

type updateActivityFunc func()

func (f updateActivityFunc) UpdateActivity() {
	lastActivity.Store(time.Now())
}

func main() {
	// --- Configuration and Initialization ---
	if err := godotenv.Load(); err != nil {
		slog.Info("No .env file found, proceeding with existing environment")
	}

	if err := os.MkdirAll("logs", 0755); err != nil {
		slog.Error("Failed to create log directory", "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll("json", 0755); err != nil {
		slog.Error("Failed to create json directory", "error", err)
		os.Exit(1)
	}
	logFile, err := os.OpenFile("logs/memory-tools.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		slog.Error("Failed to open log file", "error", err)
		os.Exit(1)
	}
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	slog.SetDefault(slog.New(slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	})))
	slog.Info("Logger configured successfully")

	cfg := config.LoadConfig()

	var walInstance *wal.WAL
	if cfg.EnableWal {
		if err := os.MkdirAll("data", 0755); err != nil {
			slog.Error("Fatal: failed to create data directory for WAL", "error", err)
			os.Exit(1)
		}
		walPath := filepath.Join("data", "wal.log")
		walInstance, err = wal.New(walPath)
		if err != nil {
			slog.Error("Fatal: failed to initialize WAL", "error", err)
			os.Exit(1)
		}
		defer walInstance.Close()
		slog.Info("Write-Ahead Log (WAL) is enabled.", "path", walPath)
	} else {
		slog.Info("Write-Ahead Log (WAL) is disabled.")
	}

	mainInMemStore := store.NewInMemStoreWithShards(cfg.NumShards)
	collectionPersister := &persistence.CollectionPersisterImpl{}
	collectionManager := store.NewCollectionManager(collectionPersister, cfg.NumShards)
	transactionManager := store.NewTransactionManager(collectionManager)
	transactionManager.StartGC(5*time.Minute, 10*time.Minute)

	// --- Data Loading and WAL Recovery ---
	slog.Info("Loading data from snapshots...")
	if err := persistence.LoadData(mainInMemStore); err != nil {
		slog.Error("Fatal error loading main persistent data", "error", err)
		os.Exit(1)
	}
	if err := persistence.LoadAllCollectionsIntoManager(collectionManager, cfg.ColdStorageMonths); err != nil {
		slog.Error("Fatal error loading persistent collections data", "error", err)
		os.Exit(1)
	}
	slog.Info("Finished loading data from snapshots.")

	if walInstance != nil {
		slog.Info("Starting WAL replay to recover most recent state...")
		entriesChan, err := wal.Replay(walInstance.Path())
		if err != nil {
			slog.Error("Fatal: could not start WAL replay", "error", err)
			os.Exit(1)
		}
		recoveryHandler := handler.GetConnectionHandlerFromPool(
			nil, mainInMemStore, collectionManager, nil, transactionManager,
			updateActivityFunc(func() {}), nil,
		)
		recoveryHandler.IsAuthenticated = true
		recoveryHandler.IsRoot = true
		replayedCount := 0
		for entry := range entriesChan {
			payloadReader := bytes.NewReader(entry.Payload)
			switch entry.CommandType {
			case protocol.CmdSet:
				recoveryHandler.HandleMainStoreSet(payloadReader, nil)
			case protocol.CmdCollectionCreate:
				recoveryHandler.HandleCollectionCreate(payloadReader, nil)
			case protocol.CmdCollectionDelete:
				recoveryHandler.HandleCollectionDelete(payloadReader, nil)
			case protocol.CmdCollectionIndexCreate:
				recoveryHandler.HandleCollectionIndexCreate(payloadReader, nil)
			case protocol.CmdCollectionIndexDelete:
				recoveryHandler.HandleCollectionIndexDelete(payloadReader, nil)
			case protocol.CmdCollectionItemSet:
				recoveryHandler.HandleCollectionItemSet(payloadReader, nil)
			case protocol.CmdCollectionItemSetMany:
				recoveryHandler.HandleCollectionItemSetMany(payloadReader, nil)
			case protocol.CmdCollectionItemDelete:
				recoveryHandler.HandleCollectionItemDelete(payloadReader, nil)
			case protocol.CmdCollectionItemDeleteMany:
				recoveryHandler.HandleCollectionItemDeleteMany(payloadReader, nil)
			case protocol.CmdCollectionItemUpdate:
				recoveryHandler.HandleCollectionItemUpdate(payloadReader, nil)
			case protocol.CmdCollectionItemUpdateMany:
				recoveryHandler.HandleCollectionItemUpdateMany(payloadReader, nil)
			case protocol.CmdChangeUserPassword:
				recoveryHandler.HandleChangeUserPassword(payloadReader, nil)
			case protocol.CmdUserCreate:
				recoveryHandler.HandleUserCreate(payloadReader, nil)
			case protocol.CmdUserUpdate:
				recoveryHandler.HandleUserUpdate(payloadReader, nil)
			case protocol.CmdUserDelete:
				recoveryHandler.HandleUserDelete(payloadReader, nil)
			case protocol.CmdCommit:
				recoveryHandler.HandleCommit(payloadReader, nil)
			case protocol.CmdRestore:
				recoveryHandler.HandleRestore(payloadReader, nil)
			}
			replayedCount++
		}
		handler.PutConnectionHandlerToPool(recoveryHandler)
		slog.Info("WAL replay complete.", "replayed_entries", replayedCount)
	}

	// --- Default User Creation ---
	systemCollection := collectionManager.GetCollection(globalconst.SystemCollectionName)
	if _, found := systemCollection.Get(globalconst.UserPrefix + "admin"); !found {
		slog.Info("Default admin user not found, creating...", "user", "admin")
		hashedPassword, _ := handler.HashPassword(cfg.DefaultAdminPassword)
		adminUserInfo := handler.UserInfo{
			Username:     "admin",
			PasswordHash: hashedPassword,
			IsRoot:       false,
			Permissions:  map[string]string{"*": globalconst.PermissionWrite, globalconst.SystemCollectionName: globalconst.PermissionRead},
		}
		adminUserInfoBytes, _ := json.Marshal(adminUserInfo)
		systemCollection.Set(globalconst.UserPrefix+"admin", adminUserInfoBytes, 0)
		collectionManager.EnqueueSaveTask(globalconst.SystemCollectionName, systemCollection)
	}
	if _, found := systemCollection.Get(globalconst.UserPrefix + "root"); !found {
		slog.Info("Default root user not found, creating...", "user", "root")
		hashedPassword, _ := handler.HashPassword(cfg.DefaultRootPassword)
		rootUserInfo := handler.UserInfo{
			Username:     "root",
			PasswordHash: hashedPassword,
			IsRoot:       true,
			Permissions:  map[string]string{"*": globalconst.PermissionWrite},
		}
		rootUserInfoBytes, _ := json.Marshal(rootUserInfo)
		systemCollection.Set(globalconst.UserPrefix+"root", rootUserInfoBytes, 0)
		collectionManager.EnqueueSaveTask(globalconst.SystemCollectionName, systemCollection)
	}

	// --- Server Startup and Workers ---
	cert, err := tls.LoadX509KeyPair("certificates/server.crt", "certificates/server.key")
	if err != nil {
		slog.Error("Failed to load server certificate or key", "error", err)
		os.Exit(1)
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	listener, err := tls.Listen("tcp", cfg.Port, tlsConfig)
	if err != nil {
		slog.Error("Fatal error starting TLS TCP server", "port", cfg.Port, "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	slog.Info("TLS TCP server listening securely", "port", cfg.Port)

	backupManager := persistence.NewBackupManager(mainInMemStore, collectionManager, cfg.BackupInterval, cfg.BackupRetention)
	backupManager.Start()
	defer backupManager.Stop()

	jobs := make(chan net.Conn, cfg.WorkerPoolSize)
	for w := 1; w <= cfg.WorkerPoolSize; w++ {
		go func(id int) {
			for conn := range jobs {
				h := handler.GetConnectionHandlerFromPool(
					walInstance, mainInMemStore, collectionManager, backupManager,
					transactionManager, updateActivityFunc(func() { lastActivity.Store(time.Now()) }), conn,
				)
				h.HandleConnection(conn)
				handler.PutConnectionHandlerToPool(h)
			}
		}(w)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" {
					slog.Info("Network listener closed, stopping connection acceptance.")
					close(jobs)
				} else {
					slog.Error("Error accepting connection", "error", err)
				}
				return
			}
			jobs <- conn
		}
	}()

	// --- Background Tasks ---
	shutdownChan := make(chan struct{})

	// Global Checkpoint Worker
	if cfg.EnableSnapshots {
		go func() {
			ticker := time.NewTicker(cfg.SnapshotInterval)
			defer ticker.Stop()
			slog.Info("Global Checkpoint Worker started", "interval", cfg.SnapshotInterval.String())
			for {
				select {
				case <-ticker.C:
					slog.Info("Performing global checkpoint...")
					err1 := persistence.SaveData(mainInMemStore)
					err2 := persistence.SaveAllCollectionsFromManager(collectionManager)
					if err1 != nil || err2 != nil {
						slog.Error("Error during checkpoint snapshots", "main_store_error", err1, "collections_error", err2)
					}
					if err1 == nil && err2 == nil && walInstance != nil {
						if err := walInstance.Rotate(); err != nil {
							slog.Error("CRITICAL: Failed to rotate WAL file after checkpoint", "error", err)
						}
					}
				case <-shutdownChan:
					slog.Info("Global Checkpoint Worker stopped.")
					return
				}
			}
		}()
	}

	// TTL Cleanup Worker
	go func() {
		ticker := time.NewTicker(cfg.TtlCleanInterval)
		defer ticker.Stop()
		slog.Info("Starting TTL cleaner", "interval", cfg.TtlCleanInterval.String())
		for {
			select {
			case <-ticker.C:
				mainInMemStore.CleanExpiredItems()
				collectionManager.CleanExpiredItemsAndSave()
			case <-shutdownChan:
				slog.Info("TTL cleaner stopped.")
				return
			}
		}
	}()

	if cfg.ColdStorageMonths > 0 {
		// Cold Data Eviction Worker
		go func() {
			interval := time.Duration(cfg.HotStorageCleanHours) * time.Hour
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			slog.Info("Starting Hot/Cold Eviction Worker", "interval", interval.String())
			for {
				select {
				case <-ticker.C:
					slog.Info("Eviction Worker starting run...")
					evictionThreshold := time.Now().AddDate(0, -cfg.ColdStorageMonths, 0)
					collectionManager.EvictColdData(evictionThreshold)
					slog.Info("Eviction Worker finished run.")
				case <-shutdownChan:
					slog.Info("Eviction Worker stopped.")
					return
				}
			}
		}()

		// Compaction Worker
		go func() {
			ticker := time.NewTicker(24 * time.Hour)
			defer ticker.Stop()
			slog.Info("Starting Compaction Worker", "interval", "24h")
			for {
				select {
				case <-ticker.C:
					slog.Info("Compaction Worker starting run...")
					collectionNames, err := persistence.ListCollectionFiles()
					if err != nil {
						slog.Error("Compaction worker failed to list collection files", "error", err)
						continue
					}
					for _, name := range collectionNames {
						if err := persistence.CompactCollectionFile(name); err != nil {
							slog.Error("Failed to compact collection file", "collection", name, "error", err)
						}
					}
					slog.Info("Compaction Worker finished run.")
				case <-shutdownChan:
					slog.Info("Compaction Worker stopped.")
					return
				}
			}
		}()
	}

	// Idle Memory Cleanup Worker
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
			case <-shutdownChan:
				slog.Info("Idle memory cleaner stopped.")
				return
			}
		}
	}()

	// --- Graceful Shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Termination signal received. Starting graceful shutdown...")

	if err := listener.Close(); err != nil {
		slog.Error("Error closing TCP listener", "error", err)
	} else {
		slog.Info("TCP listener closed.")
	}

	close(shutdownChan)
	transactionManager.StopGC()

	slog.Info("Saving final data before application exit...")
	if err := persistence.SaveData(mainInMemStore); err != nil {
		slog.Error("Error saving final main store data during shutdown", "error", err)
	}
	if err := persistence.SaveAllCollectionsFromManager(collectionManager); err != nil {
		slog.Error("Error saving final collections data during shutdown", "error", err)
	}

	slog.Info("Final data saved. Application exiting.")
}
