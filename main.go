package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/config"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"
)

// lastActivity tracks the last time a data operation occurred.
// Using atomic.Value to safely update it across goroutines.
var lastActivity atomic.Value

// init sets the initial lastActivity time when the application starts.
func init() {
	lastActivity.Store(time.Now())
}

// updateActivity updates the lastActivity timestamp.
func updateActivity() {
	lastActivity.Store(time.Now())
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Fatal error loading configuration: %v", err)
	}

	mainInMemStore := store.NewInMemStore()
	collectionPersister := &persistence.CollectionPersisterImpl{}
	collectionManager := store.NewCollectionManager(collectionPersister)

	// Load persistent data for main store and all collections.
	if err := persistence.LoadData(mainInMemStore); err != nil {
		log.Fatalf("Fatal error loading main persistent data: %v", err)
	}
	if err := persistence.LoadAllCollectionsIntoManager(collectionManager); err != nil {
		log.Fatalf("Fatal error loading persistent collections data: %v", err)
	}

	// Load server certificate and key.
	cert, err := tls.LoadX509KeyPair("certificates/server.crt", "certificates/server.key")
	if err != nil {
		log.Fatalf("Failed to load server certificate or key: %v", err)
	}

	// Configure TLS settings.
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12, // Recommended minimum TLS version for security.
	}

	// Start TLS TCP server.
	listener, err := tls.Listen("tcp", cfg.Port, tlsConfig)
	if err != nil {
		log.Fatalf("Fatal error starting TLS TCP server: %v", err)
	}
	defer listener.Close()
	log.Printf("TLS TCP server listening securely on %s", cfg.Port)

	// Accept connections in a goroutine.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Check if the error is due to server closing.
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" && opErr.Err.Error() == "use of closed network connection" {
					log.Println("TLS TCP listener closed, stopping accept loop.")
					return
				}
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go handleConnection(conn, mainInMemStore, collectionManager)
		}
	}()

	// Initialize and start snapshot manager.
	snapshotManager := persistence.NewSnapshotManager(mainInMemStore, cfg.SnapshotInterval, cfg.EnableSnapshots)
	go snapshotManager.Start()

	// Start TTL cleaner goroutine.
	ttlCleanStopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cfg.TtlCleanInterval)
		defer ticker.Stop()
		log.Printf("Starting TTL cleaner for main store and collections with interval of %s", cfg.TtlCleanInterval)

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
		checkInterval := 2 * time.Minute // How often to check for inactivity
		idleThreshold := 5 * time.Minute // Duration of inactivity to trigger memory release

		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		log.Printf("Idle memory cleaner started. Checking for inactivity every %s, with a threshold of %s.", checkInterval, idleThreshold)

		for {
			select {
			case <-ticker.C:
				lastActive := lastActivity.Load().(time.Time)
				if time.Since(lastActive) >= idleThreshold {
					log.Println("No activity detected for a while. Requesting Go runtime to release OS memory...")
					debug.FreeOSMemory() // Suggests the Go runtime to release unused memory to the OS
				}
			case <-idleMemoryCleanerStopChan:
				log.Println("Idle memory cleaner received stop signal. Stopping.")
				return
			}
		}
	}()

	// Set up graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// Stop TCP listener.
	if err := listener.Close(); err != nil {
		log.Printf("Error closing TCP listener: %v", err)
	} else {
		log.Println("TCP listener closed.")
	}

	// Stop snapshot manager.
	snapshotManager.Stop()

	// Stop TTL cleaner goroutine.
	close(ttlCleanStopChan)

	// Stop the idle memory cleaner goroutine.
	close(idleMemoryCleanerStopChan)

	// Context for graceful shutdown.
	_, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	// Save final data to disk for main store and collections.
	log.Println("Saving final data for main store before application exit...")
	if err := persistence.SaveData(mainInMemStore); err != nil {
		log.Printf("Error saving final data for main store during shutdown: %v", err)
	} else {
		log.Println("Final main store data saved.")
	}

	log.Println("Saving final data for all collections before application exit...")
	if err := persistence.SaveAllCollectionsFromManager(collectionManager); err != nil {
		log.Printf("Error saving final data for collections during shutdown: %v", err)
	} else {
		log.Println("Final collection data saved. Application exiting.")
	}
}

// handleConnection processes incoming commands from a TCP client.
func handleConnection(conn net.Conn, mainStore store.DataStore, collectionManager *store.CollectionManager) {
	defer conn.Close() // Ensure connection is closed when handler exits.
	log.Printf("New client connected: %s", conn.RemoteAddr())

	for {
		cmdType, err := protocol.ReadCommandType(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected: %s", conn.RemoteAddr())
			} else {
				log.Printf("Client disconnected: %s", conn.RemoteAddr())
			}
			return // End connection handling for this client.
		}

		// Update activity timestamp for idle memory cleaner.
		updateActivity()

		switch cmdType {
		// Main Store Commands.
		case protocol.CmdSet:
			key, value, ttl, err := protocol.ReadSetCommand(conn)
			if err != nil {
				log.Printf("Error reading SET command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET command format", nil)
				continue
			}
			mainStore.Set(key, value, ttl)
			if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in main store", key), nil); err != nil {
				log.Printf("Error writing SET response to %s: %v", conn.RemoteAddr(), err)
			}

		case protocol.CmdGet:
			key, err := protocol.ReadGetCommand(conn)
			if err != nil {
				log.Printf("Error reading GET command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid GET command format", nil)
				continue
			}
			value, found := mainStore.Get(key)
			if found {
				if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from main store", key), value); err != nil {
					log.Printf("Error writing GET success response to %s: %v", conn.RemoteAddr(), err)
				}
			} else {
				if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in main store", key), nil); err != nil {
					log.Printf("Error writing GET not found response to %s: %v", conn.RemoteAddr(), err)
				}
			}

		// Collection Management Commands.
		case protocol.CmdCollectionCreate:
			collectionName, err := protocol.ReadCollectionCreateCommand(conn)
			if err != nil {
				log.Printf("Error reading CREATE_COLLECTION command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION command format", nil)
				continue
			}
			if collectionName == "" {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
				continue
			}
			colStore := collectionManager.GetCollection(collectionName) // Ensures it exists in memory.
			if err := collectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
				log.Printf("Error saving new/ensured collection '%s' to disk: %v", collectionName, err)
				protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to ensure collection '%s' persistence", collectionName), nil)
			} else {
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' ensured and persisted", collectionName), nil)
			}

		case protocol.CmdCollectionDelete:
			collectionName, err := protocol.ReadCollectionDeleteCommand(conn)
			if err != nil {
				log.Printf("Error reading DELETE_COLLECTION command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION command format", nil)
				continue
			}
			if collectionName == "" {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
				continue
			}
			collectionManager.DeleteCollection(collectionName) // Delete from memory.
			if err := collectionManager.DeleteCollectionFromDisk(collectionName); err != nil {
				log.Printf("Error deleting collection file for '%s': %v", collectionName, err)
				protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to delete collection '%s' from disk", collectionName), nil)
			} else {
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted", collectionName), nil)
			}

		case protocol.CmdCollectionList:
			collectionNames := collectionManager.ListCollections()
			jsonNames, err := json.Marshal(collectionNames)
			if err != nil {
				log.Printf("Error marshalling collection names to JSON: %v", err)
				protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection names", nil)
				continue
			}
			if err := protocol.WriteResponse(conn, protocol.StatusOk, "OK: Collections listed", jsonNames); err != nil {
				log.Printf("Error writing collection list response to %s: %v", conn.RemoteAddr(), err)
			}

		// Collection Item Commands.
		case protocol.CmdCollectionItemSet:
			collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(conn)
			if err != nil {
				log.Printf("Error reading COLLECTION_ITEM_SET command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
				continue
			}
			if collectionName == "" || key == "" || len(value) == 0 {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or value cannot be empty", nil)
				continue
			}
			colStore := collectionManager.GetCollection(collectionName)
			colStore.Set(key, value, ttl)
			if err := collectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
				log.Printf("Error saving collection '%s' to disk after SET operation: %v", collectionName, err)
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence error logged)", key, collectionName), nil)
			} else {
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s'", key, collectionName), nil)
			}

		case protocol.CmdCollectionItemGet:
			collectionName, key, err := protocol.ReadCollectionItemGetCommand(conn)
			if err != nil {
				log.Printf("Error reading COLLECTION_ITEM_GET command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_GET command format", nil)
				continue
			}
			if collectionName == "" || key == "" {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
				continue
			}
			if !collectionManager.CollectionExists(collectionName) {
				protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
				continue
			}
			colStore := collectionManager.GetCollection(collectionName)
			value, found := colStore.Get(key)
			if found {
				if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s'", key, collectionName), value); err != nil {
					log.Printf("Error writing COLLECTION_ITEM_GET success response to %s: %v", conn.RemoteAddr(), err)
				}
			} else {
				if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in collection '%s'", key, collectionName), nil); err != nil {
					log.Printf("Error writing COLLECTION_ITEM_GET not found response to %s: %v", conn.RemoteAddr(), err)
				}
			}

		case protocol.CmdCollectionItemDelete:
			collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(conn)
			if err != nil {
				log.Printf("Error reading COLLECTION_ITEM_DELETE command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
				continue
			}
			if collectionName == "" || key == "" {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
				continue
			}
			if !collectionManager.CollectionExists(collectionName) {
				protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
				continue
			}
			colStore := collectionManager.GetCollection(collectionName)
			colStore.Delete(key)
			if err := collectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
				log.Printf("Error saving collection '%s' to disk after DELETE operation: %v", collectionName, err)
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s' (persistence error logged)", key, collectionName), nil)
			} else {
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s'", key, collectionName), nil)
			}

		case protocol.CmdCollectionItemList:
			collectionName, err := protocol.ReadCollectionItemListCommand(conn)
			if err != nil {
				log.Printf("Error reading COLLECTION_ITEM_LIST command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
				continue
			}
			if collectionName == "" {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
				continue
			}
			if !collectionManager.CollectionExists(collectionName) {
				protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
				continue
			}
			colStore := collectionManager.GetCollection(collectionName)
			allData := colStore.GetAll() // GetAll returns non-expired items.

			jsonResponseData, err := json.Marshal(allData)
			if err != nil {
				log.Printf("Error marshalling collection items to JSON for '%s': %v", collectionName, err)
				protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection items", nil)
				continue
			}
			if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Items from collection '%s' retrieved", collectionName), jsonResponseData); err != nil {
				log.Printf("Error writing COLLECTION_ITEM_LIST response to %s: %v", conn.RemoteAddr(), err)
			}

		default:
			log.Printf("Received unknown command type %d from %s", cmdType, conn.RemoteAddr())
			if err := protocol.WriteResponse(conn, protocol.StatusBadCommand, fmt.Sprintf("BAD COMMAND: Unknown command type %d", cmdType), nil); err != nil {
				log.Printf("Error writing unknown command response to %s: %v", conn.RemoteAddr(), err)
			}
		}
	}
}
