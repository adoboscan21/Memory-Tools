package handler

import (
	"fmt"
	"log"
	"memory-tools/internal/protocol"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
)

// handleCollectionItemSet processes the CmdCollectionItemSet command.
// handleCollectionItemSet processes the CmdCollectionItemSet command.
func (h *ConnectionHandler) handleCollectionItemSet(conn net.Conn) {
	collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_SET command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
		return
	}
	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if key == "" {
		key = uuid.New().String()
		log.Printf("Warning: Empty key received for COLLECTION_ITEM_SET. Generated UUID '%s'.", key)
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	// --- START OF NEW TIMESTAMP LOGIC ---

	// 1. Unmarshal the incoming value into a map to modify it
	var data map[string]any
	if err := json.Unmarshal(value, &data); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid value. Must be a JSON object.", nil)
		return
	}

	// 2. Check if the item already exists to differentiate between create and update
	existingValue, found := colStore.Get(key)
	now := time.Now().UTC().Format(time.RFC3339) // Using UTC is a good practice

	data["_id"] = key // Ensure the _id field
	data["updated_at"] = now

	if !found {
		// --- CREATE ---
		// The item is new, so we set created_at
		data["created_at"] = now
	} else {
		// --- UPDATE ---
		// The item exists, so we preserve the original created_at
		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err == nil {
			if originalCreatedAt, ok := existingData["created_at"]; ok {
				data["created_at"] = originalCreatedAt
			} else {
				// If for some reason the old data didn't have created_at, add it now
				data["created_at"] = now
			}
		}
	}

	// 3. Marshal the object back to JSON with the new fields
	finalValue, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshalling final value with timestamps for key '%s': %v", key, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to process value with timestamps", nil)
		return
	}

	// --- END OF NEW TIMESTAMP LOGIC ---

	// We use finalValue instead of the original 'value'
	colStore.Set(key, finalValue, ttl)

	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence async)", key, collectionName), nil)
}

// handleCollectionItemUpdate processes the CmdCollectionItemUpdate command.
func (h *ConnectionHandler) handleCollectionItemUpdate(conn net.Conn) {
	collectionName, key, patchValue, err := protocol.ReadCollectionItemUpdateCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_UPDATE command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_UPDATE command format", nil)
		return
	}

	if collectionName == "" || key == "" || len(patchValue) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or patch value cannot be empty", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	existingValue, found := colStore.Get(key)
	if !found {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection '%s'", key, collectionName), nil)
		return
	}

	var existingData map[string]any
	if err := json.Unmarshal(existingValue, &existingData); err != nil {
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to unmarshal existing document. Cannot apply patch.", nil)
		return
	}

	var patchData map[string]any
	if err := json.Unmarshal(patchValue, &patchData); err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch JSON format.", nil)
		return
	}

	for k, v := range patchData {
		// Prevent the client from overwriting _id and created_at
		if k == "_id" || k == "created_at" {
			continue
		}
		existingData[k] = v
	}

	// --- ADD/UPDATE updated_at ---
	existingData["updated_at"] = time.Now().UTC().Format(time.RFC3339)
	// --- END ---

	updatedValue, err := json.Marshal(existingData)
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal updated document.", nil)
		return
	}

	// TTL 0 so it doesn't expire on an update
	colStore.Set(key, updatedValue, 0)

	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' updated in collection '%s' (persistence async)", key, collectionName), updatedValue)
}

type updateManyPayload struct {
	ID    string         `json:"_id"`
	Patch map[string]any `json:"patch"`
}

// handleCollectionItemUpdateMany processes the CmdCollectionItemUpdateMany command.
func (h *ConnectionHandler) handleCollectionItemUpdateMany(conn net.Conn) {
	collectionName, value, err := protocol.ReadCollectionItemUpdateManyCommand(conn)
	if err != nil {
		log.Printf("Error reading UPDATE_COLLECTION_ITEMS_MANY command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid UPDATE_COLLECTION_ITEMS_MANY command format", nil)
		return
	}

	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	var payloads []updateManyPayload
	if err := json.Unmarshal(value, &payloads); err != nil {
		log.Printf("Error unmarshalling JSON array for UPDATE_MANY in collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format. Expected an array of `{\"_id\": \"...\", \"patch\": {...}}`.", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	updatedCount := 0
	failedKeys := []string{}
	now := time.Now().UTC().Format(time.RFC3339) // Get current time once for the whole batch

	for _, p := range payloads {
		if p.ID == "" || p.Patch == nil {
			log.Printf("Skipping invalid payload in UPDATE_MANY batch for collection '%s': missing _id or patch.", collectionName)
			continue
		}

		existingValue, found := colStore.Get(p.ID)
		if !found {
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err != nil {
			log.Printf("Failed to unmarshal existing document for key '%s' in UPDATE_MANY. Skipping.", p.ID)
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		for k, v := range p.Patch {
			// Prevent the client from overwriting _id and created_at
			if k == "_id" || k == "created_at" {
				continue
			}
			existingData[k] = v
		}

		// --- ADD/UPDATE updated_at ---
		existingData["updated_at"] = now
		// --- END ---

		updatedValue, err := json.Marshal(existingData)
		if err != nil {
			log.Printf("Failed to marshal updated document for key '%s' in UPDATE_MANY. Skipping.", p.ID)
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		// TTL 0 so it doesn't expire on an update
		colStore.Set(p.ID, updatedValue, 0)
		updatedCount++
	}

	if updatedCount > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}

	summary := fmt.Sprintf("OK: %d items updated. %d items failed or not found in collection '%s'.", updatedCount, len(failedKeys), collectionName)
	var responseData []byte
	if len(failedKeys) > 0 {
		responseData, _ = json.Marshal(map[string][]string{"failed_keys": failedKeys})
	}

	protocol.WriteResponse(conn, protocol.StatusOk, summary, responseData)
}

// handleCollectionItemGet processes the CmdCollectionItemGet command.
func (h *ConnectionHandler) handleCollectionItemGet(conn net.Conn) {
	collectionName, key, err := protocol.ReadCollectionItemGetCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_GET command: %v", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_GET command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "read") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	value, found := colStore.Get(key)
	if found {
		// Sanitize system user data before sending it over the wire
		if collectionName == SystemCollectionName && strings.HasPrefix(key, UserPrefix) {
			var userInfo UserInfo
			if err := json.Unmarshal(value, &userInfo); err == nil {
				sanitizedInfo := map[string]any{
					"username":    userInfo.Username,
					"is_root":     userInfo.IsRoot,
					"permissions": userInfo.Permissions,
				}
				sanitizedBytes, _ := json.Marshal(sanitizedInfo)
				protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s' (sanitized)", key, collectionName), sanitizedBytes)
				return
			}
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from collection '%s'", key, collectionName), value)
	} else {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in collection '%s'", key, collectionName), nil)
	}
}

// handleCollectionItemDelete processes the CmdCollectionItemDelete command.
func (h *ConnectionHandler) handleCollectionItemDelete(conn net.Conn) {
	collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_DELETE command: %v", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		return
	}
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.Delete(key)

	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s' (persistence async)", key, collectionName), nil)
}

// handleCollectionItemList processes the CmdCollectionItemList command.
func (h *ConnectionHandler) handleCollectionItemList(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionItemListCommand(conn)
	if err != nil {
		log.Printf("Error reading COLLECTION_ITEM_LIST command: %v", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.IsRoot || !h.IsLocalhostConn {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Listing all items is a privileged operation for root@localhost. Please use 'collection query' with limit/offset for data retrieval.", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "read") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	allData := colStore.GetAll()

	// Sanitize system collection data
	if collectionName == SystemCollectionName {
		sanitizedData := make(map[string]map[string]any)
		for key, val := range allData {
			if strings.HasPrefix(key, UserPrefix) {
				var userInfo UserInfo
				if err := json.Unmarshal(val, &userInfo); err == nil {
					sanitizedData[key] = map[string]any{
						"username":    userInfo.Username,
						"is_root":     userInfo.IsRoot,
						"permissions": userInfo.Permissions,
					}
				}
			} else {
				// Hide non-user system data for security
				sanitizedData[key] = map[string]any{"data": "non-user system data (omitted)"}
			}
		}
		jsonResponseData, _ := json.Marshal(sanitizedData)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Sanitized items from collection '%s' retrieved", collectionName), jsonResponseData)
	} else {
		jsonResponseData, err := json.Marshal(allData)
		if err != nil {
			log.Printf("Error marshalling collection items to JSON for '%s': %v", collectionName, err)
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection items", nil)
			return
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Items from collection '%s' retrieved", collectionName), jsonResponseData)
	}
}

// handleCollectionItemSetMany processes the CmdCollectionItemSetMany command.
func (h *ConnectionHandler) handleCollectionItemSetMany(conn net.Conn) {
	collectionName, value, err := protocol.ReadCollectionItemSetManyCommand(conn)
	if err != nil {
		log.Printf("Error reading SET_COLLECTION_ITEMS_MANY command: %v", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET_COLLECTION_ITEMS_MANY command format", nil)
		return
	}

	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	var records []map[string]any
	if err := json.Unmarshal(value, &records); err != nil {
		log.Printf("Error unmarshalling JSON array for SET_MANY: %v", err)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	insertedCount := 0
	now := time.Now().UTC().Format(time.RFC3339) // Get current time once for the whole batch

	for _, record := range records {
		var key string
		if id, ok := record["_id"].(string); ok && id != "" {
			key = id
		} else {
			key = uuid.New().String()
		}

		// --- ADD TIMESTAMP LOGIC ---
		record["_id"] = key
		record["created_at"] = now
		record["updated_at"] = now
		// --- END TIMESTAMP LOGIC ---

		updatedValue, err := json.Marshal(record)
		if err != nil {
			log.Printf("Error marshalling record for SET_MANY: %v", err)
			continue
		}
		// TTL 0 so it doesn't expire
		colStore.Set(key, updatedValue, 0)
		insertedCount++
	}

	if insertedCount > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d items set in collection '%s' (persistence async)", insertedCount, collectionName), nil)
}

// handleCollectionItemDeleteMany processes the CmdCollectionItemDeleteMany command.
func (h *ConnectionHandler) handleCollectionItemDeleteMany(conn net.Conn) {
	collectionName, keys, err := protocol.ReadCollectionItemDeleteManyCommand(conn)
	if err != nil {
		log.Printf("Error reading DELETE_COLLECTION_ITEMS_MANY command: %v", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_ITEMS_MANY command format", nil)
		return
	}

	if collectionName == "" || len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty and keys must be provided", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	for _, key := range keys {
		colStore.Delete(key)
	}

	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys deleted from collection '%s' (persistence async)", len(keys), collectionName), nil)
}
