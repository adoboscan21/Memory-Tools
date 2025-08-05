package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
)

// handleCollectionItemSet processes the CmdCollectionItemSet command.
func (h *ConnectionHandler) handleCollectionItemSet(conn net.Conn) {
	collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(conn)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_SET command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
		return
	}
	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection item set attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if key == "" {
		key = uuid.New().String()
		slog.Debug("Empty key received for SET, generated new UUID", "key", key, "collection", collectionName)
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	var data map[string]any
	if err := json.Unmarshal(value, &data); err != nil {
		slog.Warn("Failed to unmarshal item data for SET", "error", err, "collection", collectionName, "user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid value. Must be a JSON object.", nil)
		return
	}

	existingValue, found := colStore.Get(key)
	now := time.Now().UTC().Format(time.RFC3339)

	data["_id"] = key
	data["updated_at"] = now

	if !found {
		data["created_at"] = now
	} else {
		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err == nil {
			if originalCreatedAt, ok := existingData["created_at"]; ok {
				data["created_at"] = originalCreatedAt
			} else {
				data["created_at"] = now
			}
		}
	}

	finalValue, err := json.Marshal(data)
	if err != nil {
		slog.Error("Failed to marshal final value with timestamps", "key", key, "collection", collectionName, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to process value with timestamps", nil)
		return
	}

	colStore.Set(key, finalValue, ttl)
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Item set in collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key, "operation", boolToString(found, "update", "create"))
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence async)", key, collectionName), nil)
}

// handleCollectionItemUpdate processes the CmdCollectionItemUpdate command.
func (h *ConnectionHandler) handleCollectionItemUpdate(conn net.Conn) {
	collectionName, key, patchValue, err := protocol.ReadCollectionItemUpdateCommand(conn)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_UPDATE command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_UPDATE command format", nil)
		return
	}
	if collectionName == "" || key == "" || len(patchValue) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or patch value cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection item update attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	existingValue, found := colStore.Get(key)
	if !found {
		slog.Warn("Item update failed: key not found", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection '%s'", key, collectionName), nil)
		return
	}

	var existingData map[string]any
	if err := json.Unmarshal(existingValue, &existingData); err != nil {
		slog.Error("Failed to unmarshal existing document for update", "key", key, "collection", collectionName, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to unmarshal existing document. Cannot apply patch.", nil)
		return
	}

	var patchData map[string]any
	if err := json.Unmarshal(patchValue, &patchData); err != nil {
		slog.Warn("Failed to unmarshal patch data for update", "key", key, "collection", collectionName, "error", err, "user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch JSON format.", nil)
		return
	}

	for k, v := range patchData {
		if k == "_id" || k == "created_at" {
			continue
		}
		existingData[k] = v
	}

	existingData["updated_at"] = time.Now().UTC().Format(time.RFC3339)

	updatedValue, err := json.Marshal(existingData)
	if err != nil {
		slog.Error("Failed to marshal updated document", "key", key, "collection", collectionName, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal updated document.", nil)
		return
	}

	colStore.Set(key, updatedValue, 0)
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Item updated in collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
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
		slog.Error("Failed to read UPDATE_MANY command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid UPDATE_COLLECTION_ITEMS_MANY command format", nil)
		return
	}
	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection item update-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	var payloads []updateManyPayload
	if err := json.Unmarshal(value, &payloads); err != nil {
		slog.Warn("Failed to unmarshal JSON array for UPDATE_MANY", "collection", collectionName, "error", err, "user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format. Expected an array of `{\"_id\": \"...\", \"patch\": {...}}`.", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	updatedCount := 0
	failedKeys := []string{}
	now := time.Now().UTC().Format(time.RFC3339)

	for _, p := range payloads {
		if p.ID == "" || p.Patch == nil {
			slog.Debug("Skipping invalid payload in UPDATE_MANY batch", "collection", collectionName)
			continue
		}

		existingValue, found := colStore.Get(p.ID)
		if !found {
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err != nil {
			slog.Warn("Failed to unmarshal existing document in UPDATE_MANY batch", "key", p.ID, "collection", collectionName, "error", err)
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		for k, v := range p.Patch {
			if k == "_id" || k == "created_at" {
				continue
			}
			existingData[k] = v
		}

		existingData["updated_at"] = now

		updatedValue, err := json.Marshal(existingData)
		if err != nil {
			slog.Warn("Failed to marshal updated document in UPDATE_MANY batch", "key", p.ID, "collection", collectionName, "error", err)
			failedKeys = append(failedKeys, p.ID)
			continue
		}

		colStore.Set(p.ID, updatedValue, 0)
		updatedCount++
	}

	if updatedCount > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}

	slog.Info("Update-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "updated_count", updatedCount, "failed_count", len(failedKeys))
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
		slog.Error("Failed to read GET_ITEM command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_GET command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "read") {
		slog.Warn("Unauthorized collection item get attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	value, found := colStore.Get(key)
	slog.Debug("Get item from collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key, "found", found)

	if found {
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
		slog.Error("Failed to read DELETE_ITEM command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
		return
	}
	if collectionName == "" || key == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection item delete attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
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
	slog.Info("Item deleted from collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s' (persistence async)", key, collectionName), nil)
}

// handleCollectionItemList processes the CmdCollectionItemList command.
func (h *ConnectionHandler) handleCollectionItemList(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionItemListCommand(conn)
	if err != nil {
		slog.Error("Failed to read LIST_ITEMS command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.IsRoot || !h.IsLocalhostConn {
		slog.Warn("Unauthorized list-all-items attempt", "user", h.AuthenticatedUser, "collection", collectionName, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Listing all items is a privileged operation for root@localhost. Please use 'collection query' with limit/offset for data retrieval.", nil)
		return
	}

	if !h.hasPermission(collectionName, "read") {
		slog.Warn("Unauthorized collection item list attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for listing items", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	allData := colStore.GetAll()

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
				sanitizedData[key] = map[string]any{"data": "non-user system data (omitted)"}
			}
		}
		jsonResponseData, _ := json.Marshal(sanitizedData)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Sanitized items from collection '%s' retrieved", collectionName), jsonResponseData)
	} else {
		jsonResponseData, err := json.Marshal(allData)
		if err != nil {
			slog.Error("Failed to marshal collection items to JSON", "collection", collectionName, "error", err)
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection items", nil)
			return
		}
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Items from collection '%s' retrieved", collectionName), jsonResponseData)
	}
	slog.Info("All items listed from collection", "user", h.AuthenticatedUser, "collection", collectionName, "item_count", len(allData))
}

// handleCollectionItemSetMany processes the CmdCollectionItemSetMany command.
func (h *ConnectionHandler) handleCollectionItemSetMany(conn net.Conn) {
	collectionName, value, err := protocol.ReadCollectionItemSetManyCommand(conn)
	if err != nil {
		slog.Error("Failed to read SET_MANY command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET_COLLECTION_ITEMS_MANY command format", nil)
		return
	}
	if collectionName == "" || len(value) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection item set-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	var records []map[string]any
	if err := json.Unmarshal(value, &records); err != nil {
		slog.Warn("Failed to unmarshal JSON array for SET_MANY", "collection", collectionName, "error", err, "user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format", nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	insertedCount := 0
	now := time.Now().UTC().Format(time.RFC3339)

	for _, record := range records {
		var key string
		if id, ok := record["_id"].(string); ok && id != "" {
			key = id
		} else {
			key = uuid.New().String()
		}

		record["_id"] = key
		record["created_at"] = now
		record["updated_at"] = now

		updatedValue, err := json.Marshal(record)
		if err != nil {
			slog.Warn("Failed to marshal record in SET_MANY batch", "key", key, "collection", collectionName, "error", err)
			continue
		}
		colStore.Set(key, updatedValue, 0)
		insertedCount++
	}

	if insertedCount > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}
	slog.Info("Set-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "item_count", insertedCount)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d items set in collection '%s' (persistence async)", insertedCount, collectionName), nil)
}

// handleCollectionItemDeleteMany processes the CmdCollectionItemDeleteMany command.
func (h *ConnectionHandler) handleCollectionItemDeleteMany(conn net.Conn) {
	collectionName, keys, err := protocol.ReadCollectionItemDeleteManyCommand(conn)
	if err != nil {
		slog.Error("Failed to read DELETE_MANY command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_ITEMS_MANY command format", nil)
		return
	}
	if collectionName == "" || len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty and keys must be provided", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection item delete-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
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
	slog.Info("Delete-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "key_count", len(keys))
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys deleted from collection '%s' (persistence async)", len(keys), collectionName), nil)
}

// boolToString is a small helper for clearer logs.
func boolToString(b bool, trueStr, falseStr string) string {
	if b {
		return trueStr
	}
	return falseStr
}
