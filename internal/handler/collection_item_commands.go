package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/globalconst"
	"memory-tools/internal/persistence"
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

	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
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

	data[globalconst.ID] = key
	data[globalconst.UPDATED_AT] = now

	if !found {
		data[globalconst.CREATED_AT] = now
	} else {
		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err == nil {
			if originalCreatedAt, ok := existingData[globalconst.CREATED_AT]; ok {
				data[globalconst.CREATED_AT] = originalCreatedAt
			} else {
				data[globalconst.CREATED_AT] = now
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
// It now handles updating both hot (in-memory) and cold (on-disk) items.
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

	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		slog.Warn("Unauthorized collection item update attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	existingValue, found := colStore.Get(key)
	if found {
		// The item is in RAM, so the operation is fast.
		slog.Debug("Updating hot item in RAM", "collection", collectionName, "key", key)

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
			if k == globalconst.ID || k == globalconst.CREATED_AT {
				continue
			}
			existingData[k] = v
		}

		existingData[globalconst.UPDATED_AT] = time.Now().UTC().Format(time.RFC3339)

		updatedValue, _ := json.Marshal(existingData)
		colStore.Set(key, updatedValue, 0)
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

		slog.Info("Item updated in collection (hot)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' updated in collection '%s'", key, collectionName), updatedValue)
		return
	}

	slog.Debug("Item not in RAM, attempting to update in cold storage", "collection", collectionName, "key", key)
	fileLock := h.CollectionManager.GetFileLock(collectionName)
	fileLock.Lock()
	updated, err := persistence.UpdateColdItem(collectionName, key, patchValue)
	fileLock.Unlock()
	if err != nil {
		slog.Error("Failed to update cold item on disk", "collection", collectionName, "key", key, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to update item on disk", nil)
		return
	}

	if !updated {
		slog.Warn("Item update failed: key not found in hot or cold storage", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection '%s'", key, collectionName), nil)
		return
	}

	slog.Info("Item updated in collection (cold)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Cold item '%s' updated in collection '%s'", key, collectionName), nil)
}

type updateManyPayload struct {
	ID    string         `json:"_id"`
	Patch map[string]any `json:"patch"`
}

// handleCollectionItemUpdateMany processes the CmdCollectionItemUpdateMany command.
// It now processes batches against both hot (RAM) and cold (disk) storage.
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

	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
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
	hotPayloads := []updateManyPayload{}
	coldPayloads := []persistence.ColdUpdatePayload{}

	// 1. Split payloads into hot and cold batches.
	for _, p := range payloads {
		if _, found := colStore.Get(p.ID); found {
			hotPayloads = append(hotPayloads, p)
		} else {
			coldPayloads = append(coldPayloads, persistence.ColdUpdatePayload{ID: p.ID, Patch: p.Patch})
		}
	}

	slog.Debug("Split update-many batch", "hot_count", len(hotPayloads), "cold_count", len(coldPayloads))

	updatedHotCount := 0
	failedHotKeys := []string{}
	now := time.Now().UTC().Format(time.RFC3339)

	// 2. Process hot payloads in memory (fast).
	for _, p := range hotPayloads {
		existingValue, _ := colStore.Get(p.ID)
		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err != nil {
			failedHotKeys = append(failedHotKeys, p.ID)
			continue
		}
		for k, v := range p.Patch {
			if k == globalconst.ID || k == globalconst.CREATED_AT {
				continue
			}
			existingData[k] = v
		}
		existingData[globalconst.UPDATED_AT] = now
		updatedValue, err := json.Marshal(existingData)
		if err != nil {
			failedHotKeys = append(failedHotKeys, p.ID)
			continue
		}
		colStore.Set(p.ID, updatedValue, 0)
		updatedHotCount++
	}

	if updatedHotCount > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}

	// 3. Process cold payloads on disk (slower, single operation).
	updatedColdCount := 0
	if len(coldPayloads) > 0 {
		fileLock := h.CollectionManager.GetFileLock(collectionName)
		fileLock.Lock()
		count, err := persistence.UpdateManyColdItems(collectionName, coldPayloads)
		fileLock.Unlock()
		if err != nil {
			slog.Error("Failed to update cold items batch", "collection", collectionName, "error", err)
			protocol.WriteResponse(conn, protocol.StatusError, "An error occurred during the cold batch update.", nil)
			return
		}
		updatedColdCount = count

	}

	// 4. Consolidate results and respond.
	totalUpdated := updatedHotCount + updatedColdCount
	totalFailed := (len(payloads) - totalUpdated)

	slog.Info("Update-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "updated_count", totalUpdated, "failed_count", totalFailed)
	summary := fmt.Sprintf("OK: %d items updated. %d items failed or not found.", totalUpdated, totalFailed)

	var responseData []byte
	if len(failedHotKeys) > 0 {
		responseData, _ = json.Marshal(map[string][]string{"failed_hot_keys": failedHotKeys})
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

	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
		slog.Warn("Unauthorized collection item get attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	value, found := colStore.Get(key)
	slog.Debug("Get item from collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key, "found", found)

	if found {
		if collectionName == globalconst.SystemCollectionName && strings.HasPrefix(key, globalconst.UserPrefix) {
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
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		slog.Warn("Unauthorized collection item delete attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	// --- LOGIC FOR "HOT" ITEMS (IN RAM) ---
	if _, foundInRam := colStore.Get(key); foundInRam {
		slog.Debug("Deleting hot item from RAM", "collection", collectionName, "key", key)
		colStore.Delete(key)                                          // Deletes from memory and indexes
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore) // Enqueue saving the new state

		slog.Info("Item deleted from collection (hot)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s'", key, collectionName), nil)
		return // IMPORTANT: We finish here, do not continue to the "cold" logic.
	}

	// --- LOGIC FOR "COLD" ITEMS (ON DISK ONLY) ---
	slog.Debug("Item not in RAM, attempting to mark for deletion in cold storage", "collection", collectionName, "key", key)

	fileLock := h.CollectionManager.GetFileLock(collectionName)
	fileLock.Lock()
	marked, err := persistence.DeleteColdItem(collectionName, key)
	fileLock.Unlock()

	if err != nil {
		slog.Error("Failed to mark item as deleted on disk", "collection", collectionName, "key", key, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to perform delete operation on disk", nil)
		return
	}

	if !marked {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection", key), nil)
		return
	}

	slog.Info("Item marked for deletion in collection (cold)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' marked for deletion from collection '%s'", key, collectionName), nil)
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

	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
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

	if collectionName == globalconst.SystemCollectionName {
		sanitizedData := make(map[string]map[string]any)
		for key, val := range allData {
			if strings.HasPrefix(key, globalconst.UserPrefix) {
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

	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
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
		if id, ok := record[globalconst.ID].(string); ok && id != "" {
			key = id
		} else {
			key = uuid.New().String()
		}

		record[globalconst.ID] = key
		record[globalconst.CREATED_AT] = now
		record[globalconst.UPDATED_AT] = now

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
	// ... (initial validations without changes) ...
	if err != nil {
		slog.Error("Failed to read DELETE_MANY command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_ITEMS_MANY command format", nil)
		return
	}
	if collectionName == "" || len(keys) == 0 {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty and keys must be provided", nil)
		return
	}
	if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
		slog.Warn("Unauthorized collection item delete-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	hotKeysToDelete := []string{}
	coldKeysToTombstone := []string{}

	// 1. Separate keys into "hot" and "cold"
	for _, key := range keys {
		if _, foundInRam := colStore.Get(key); foundInRam {
			hotKeysToDelete = append(hotKeysToDelete, key)
		} else {
			coldKeysToTombstone = append(coldKeysToTombstone, key)
		}
	}

	// 2. Process "hot" deletions
	if len(hotKeysToDelete) > 0 {
		slog.Debug("Deleting hot items from RAM in batch", "count", len(hotKeysToDelete))
		for _, key := range hotKeysToDelete {
			colStore.Delete(key)
		}
		// Enqueue a single save for all deletions in RAM
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}

	// 3. Process "cold" deletions (tombstones)
	var markedCount int
	if len(coldKeysToTombstone) > 0 {
		slog.Debug("Marking cold items for deletion in batch", "count", len(coldKeysToTombstone))
		fileLock := h.CollectionManager.GetFileLock(collectionName)
		fileLock.Lock()
		markedCount, err = persistence.DeleteManyColdItems(collectionName, coldKeysToTombstone)
		fileLock.Unlock()

		if err != nil {
			slog.Error("Failed to mark items for deletion in cold storage", "collection", collectionName, "error", err)
			protocol.WriteResponse(conn, protocol.StatusError, "An error occurred during the batch delete operation.", nil)
			return
		}
	}

	totalProcessed := len(hotKeysToDelete) + markedCount
	slog.Info("Delete-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "processed_count", totalProcessed)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys processed for deletion from collection '%s'.", totalProcessed, collectionName), nil)
}

// boolToString is a small helper for clearer logs.
func boolToString(b bool, trueStr, falseStr string) string {
	if b {
		return trueStr
	}
	return falseStr
}
