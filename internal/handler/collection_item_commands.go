package handler

import (
	"fmt"
	"log"
	"memory-tools/internal/protocol"
	"net"
	"strings"

	"github.com/google/uuid"
)

// handleCollectionItemSet processes the CmdCollectionItemSet command.
func (h *ConnectionHandler) handleCollectionItemSet(conn net.Conn) {
	collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_SET command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
		return
	}
	if collectionName == "" || len(value) == 0 { // Key can be empty for UUID generation
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}
	if key == "" { // The client is expected to send a UUID here, but as a fallback.
		key = uuid.New().String()
		log.Printf("Warning: Empty key received for COLLECTION_ITEM_SET. Generated UUID '%s'. Ensure client sends UUIDs.", key)
	}

	// Specific authorization check for _system collection (even if authenticated)
	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can modify collection '%s'", SystemCollectionName), nil)
		return
	}

	updatedValue, err := ensureIDField(value, key)
	if err != nil {
		log.Printf("Error ensuring _id field for key '%s' in collection '%s': %v", key, collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to process value for _id field", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.Set(key, updatedValue, ttl)
	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
		log.Printf("Error saving collection '%s' to disk after SET operation: %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence error logged)", key, collectionName), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s'", key, collectionName), nil)
	}
}

// handleCollectionItemGet processes the CmdCollectionItemGet command.
func (h *ConnectionHandler) handleCollectionItemGet(conn net.Conn) {
	collectionName, key, err := protocol.ReadCollectionItemGetCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_GET command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_GET command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}
	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		log.Printf("Unauthorized attempt to GET item '%s' from _system collection by user '%s' from %s.", key, h.AuthenticatedUser, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can get items from collection '%s'", SystemCollectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	value, found := colStore.Get(key)
	if found {
		if collectionName == SystemCollectionName && strings.HasPrefix(key, UserPrefix) {
			var userInfo UserInfo
			if err := json.Unmarshal(value, &userInfo); err == nil {
				sanitizedInfo := map[string]string{"username": userInfo.Username, "is_root": fmt.Sprintf("%t", userInfo.IsRoot)}
				sanitizedBytes, _ := json.Marshal(sanitizedInfo)
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s' (sanitized)", key, collectionName), sanitizedBytes)
				return
			}
		}
		if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s'", key, collectionName), value); err != nil {
			log.Printf("Error writing COLLECTION_ITEM_GET success response to %s: %v", conn.RemoteAddr(), err)
		}
	} else {
		if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in collection '%s'", key, collectionName), nil); err != nil {
			log.Printf("Error writing COLLECTION_ITEM_GET not found response to %s: %v", conn.RemoteAddr(), err)
		}
	}
}

// handleCollectionItemDelete processes the CmdCollectionItemDelete command.
func (h *ConnectionHandler) handleCollectionItemDelete(conn net.Conn) {
	collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_DELETE command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}
	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		return
	}
	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can delete from collection '%s'", SystemCollectionName), nil)
		return
	}
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.Delete(key)
	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
		log.Printf("Error saving collection '%s' to disk after DELETE operation: %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s' (persistence error logged)", key, collectionName), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s'", key, collectionName), nil)
	}
}

// handleCollectionItemList processes the CmdCollectionItemList command.
func (h *ConnectionHandler) handleCollectionItemList(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionItemListCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_LIST command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}
	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
		return
	}
	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		log.Printf("Unauthorized attempt to LIST items from _system collection by user '%s' from %s.", h.AuthenticatedUser, conn.RemoteAddr())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can list items from collection '%s'", SystemCollectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	allData := colStore.GetAll()

	if collectionName == SystemCollectionName {
		sanitizedData := make(map[string]map[string]string)
		for key, val := range allData {
			if strings.HasPrefix(key, UserPrefix) {
				var userInfo UserInfo
				if err := json.Unmarshal(val, &userInfo); err == nil {
					sanitizedData[key] = map[string]string{
						"username": userInfo.Username,
						"is_root":  fmt.Sprintf("%t", userInfo.IsRoot),
					}
				} else {
					log.Printf("Warning: Failed to unmarshal user info for key '%s': %v", key, err)
					sanitizedData[key] = map[string]string{"username": "UNKNOWN", "status": "corrupted"}
				}
			} else {
				sanitizedData[key] = map[string]string{"data": "non-user system data (omitted for security)"}
			}
		}
		jsonResponseData, err := json.Marshal(sanitizedData)
		if err != nil {
			log.Printf("Error marshalling sanitized system collection items to JSON for '%s': %v", collectionName, err)
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal sanitized collection items", nil)
			return
		}
		if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Sanitized items from collection '%s' retrieved", collectionName), jsonResponseData); err != nil {
			log.Printf("Error writing COLLECTION_ITEM_LIST response to %s: %v", conn.RemoteAddr(), err)
		}
	} else {
		jsonResponseData, err := json.Marshal(allData)
		if err != nil {
			log.Printf("Error marshalling collection items to JSON for '%s': %v", collectionName, err)
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection items", nil)
			return
		}
		if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Items from collection '%s' retrieved", collectionName), jsonResponseData); err != nil {
			log.Printf("Error writing COLLECTION_ITEM_LIST response to %s: %v", conn.RemoteAddr(), err)
		}
	}
}

// handleCollectionItemSetMany processes the CmdCollectionItemSetMany command.
func (h *ConnectionHandler) handleCollectionItemSetMany(conn net.Conn) {
	collectionName, value, err := protocol.ReadCollectionItemSetManyCommand(conn)
	if err != nil {
		log.Printf("Error reading SET_COLLECTION_ITEMS_MANY command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET_COLLECTION_ITEMS_MANY command format", nil)
		return
	}

	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can modify collection '%s'", SystemCollectionName), nil)
		return
	}

	var records []map[string]any
	if err := json.Unmarshal(value, &records); err != nil {
		log.Printf("Error unmarshalling JSON array for SET_COLLECTION_ITEMS_MANY in collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	insertedCount := 0

	for _, record := range records {
		var key string
		if id, ok := record["_id"].(string); ok && id != "" {
			key = id
		} else {
			key = uuid.New().String()
		}

		record["_id"] = key

		updatedValue, err := json.Marshal(record)
		if err != nil {
			log.Printf("Error marshalling record for SET_COLLECTION_ITEMS_MANY: %v", err)
			continue
		}

		colStore.Set(key, updatedValue, 0)
		insertedCount++
	}

	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
		log.Printf("Error saving collection '%s' to disk after SET_MANY operation: %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d items set in collection '%s' (persistence error logged)", insertedCount, collectionName), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d items set in collection '%s'", insertedCount, collectionName), nil)
	}
}

// handleCollectionItemDeleteMany processes the CmdCollectionItemDeleteMany command.
func (h *ConnectionHandler) handleCollectionItemDeleteMany(conn net.Conn) {
	collectionName, keys, err := protocol.ReadCollectionItemDeleteManyCommand(conn)
	if err != nil {
		log.Printf("Error reading DELETE_COLLECTION_ITEMS_MANY command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_ITEMS_MANY command format", nil)
		return
	}

	if collectionName == "" || len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty and at least one key must be provided", nil)
		return
	}

	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can delete from collection '%s'", SystemCollectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	deletedCount := 0
	for _, key := range keys {
		colStore.Delete(key)
		deletedCount++
	}

	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
		log.Printf("Error saving collection '%s' to disk after DELETE_MANY operation: %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys deleted from collection '%s' (persistence error logged)", deletedCount, collectionName), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys deleted from collection '%s'", deletedCount, collectionName), nil)
	}
}
