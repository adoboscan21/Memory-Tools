package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	// No necesitamos runtime/debug ni sync/atomic directamente aquí.
	// Solo la función para actualizar la actividad.
)

// ActivityUpdater is an interface for updating activity timestamps.
type ActivityUpdater interface {
	UpdateActivity()
}

// ConnectionHandler holds the dependencies needed to handle client connections.
type ConnectionHandler struct {
	MainStore         store.DataStore
	CollectionManager *store.CollectionManager
	ActivityUpdater   ActivityUpdater // Dependency for updating activity
}

// NewConnectionHandler creates a new instance of ConnectionHandler.
func NewConnectionHandler(mainStore store.DataStore, colManager *store.CollectionManager, updater ActivityUpdater) *ConnectionHandler {
	return &ConnectionHandler{
		MainStore:         mainStore,
		CollectionManager: colManager,
		ActivityUpdater:   updater,
	}
}

// HandleConnection processes incoming commands from a TCP client.
// It's now a method of ConnectionHandler.
func (h *ConnectionHandler) HandleConnection(conn net.Conn) {
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
		// Now calling the method on the injected ActivityUpdater.
		h.ActivityUpdater.UpdateActivity()

		switch cmdType {
		// Main Store Commands.
		case protocol.CmdSet:
			key, value, ttl, err := protocol.ReadSetCommand(conn)
			if err != nil {
				log.Printf("Error reading SET command from %s: %v", conn.RemoteAddr(), err)
				protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET command format", nil)
				continue
			}
			h.MainStore.Set(key, value, ttl)
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
			value, found := h.MainStore.Get(key)
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
			colStore := h.CollectionManager.GetCollection(collectionName) // Ensures it exists in memory.
			if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
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
			h.CollectionManager.DeleteCollection(collectionName) // Delete from memory.
			if err := h.CollectionManager.DeleteCollectionFromDisk(collectionName); err != nil {
				log.Printf("Error deleting collection file for '%s': %v", collectionName, err)
				protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to delete collection '%s' from disk", collectionName), nil)
			} else {
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted", collectionName), nil)
			}

		case protocol.CmdCollectionList:
			collectionNames := h.CollectionManager.ListCollections()
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
			colStore := h.CollectionManager.GetCollection(collectionName)
			colStore.Set(key, value, ttl)
			if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
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
			if !h.CollectionManager.CollectionExists(collectionName) {
				protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
				continue
			}
			colStore := h.CollectionManager.GetCollection(collectionName)
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
			if !h.CollectionManager.CollectionExists(collectionName) {
				protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
				continue
			}
			colStore := h.CollectionManager.GetCollection(collectionName)
			colStore.Delete(key)
			if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
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
			if !h.CollectionManager.CollectionExists(collectionName) {
				protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
				continue
			}
			colStore := h.CollectionManager.GetCollection(collectionName)
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
