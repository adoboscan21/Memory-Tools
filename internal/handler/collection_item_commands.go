package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/globalconst"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
)

// handleCollectionItemSet procesa el CmdCollectionItemSet. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionItemSet(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, key, value, ttl, err := protocol.ReadCollectionItemSetCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_SET command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_SET command format", nil)
		}
		return
	}

	wasKeyGenerated := false
	if key == "" {
		if conn == nil {
			slog.Error("CRITICAL: SET command with empty key received during WAL replay.", "collection", collectionName)
			return
		}

		wasKeyGenerated = true
		const maxGenerateRetries = 5
		colStore := h.CollectionManager.GetCollection(collectionName)

		for i := range maxGenerateRetries {
			newKey := uuid.New().String()
			if _, found := colStore.Get(newKey); !found {
				key = newKey
				break
			}
			slog.Warn("UUID collision detected during server-side generation. This is extremely rare. Retrying...", "attempt", i+1, "collection", collectionName)
		}

		if key == "" {
			errMessage := "Failed to generate a unique ID after several attempts. This indicates a highly unusual system state."
			slog.Error(errMessage, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusError, errMessage, nil)

			return
		}
	}
	// --- FIN: LÓGICA DE GENERACIÓN DE ID ---

	// Validación de existencia para claves PROPORCIONADAS POR EL CLIENTE en operaciones NO transaccionales.
	if conn != nil && !wasKeyGenerated && h.CurrentTransactionID == "" {
		colStore := h.CollectionManager.GetCollection(collectionName)
		if _, found := colStore.Get(key); found {
			slog.Warn("Set item failed because key already exists", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Key '%s' already exists in collection '%s'. Use 'collection item update' to modify.", key, collectionName), nil)
			return
		}
	}

	if conn != nil {
		if collectionName == "" || len(value) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item set attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if h.CurrentTransactionID == "" && !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Set item failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist. Please create it first.", collectionName), nil)
			return
		}
	}

	var data map[string]any
	if err := json.Unmarshal(value, &data); err != nil {
		slog.Warn("Failed to unmarshal item data for SET", "error", err, "collection", collectionName, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid value. Must be a JSON object.", nil)
		}
		return
	}
	data[globalconst.ID] = key

	// Lógica transaccional
	if h.CurrentTransactionID != "" {
		finalValueForTx, err := json.Marshal(data)
		if err != nil {
			slog.Error("Failed to marshal enriched value for transaction", "key", key, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "Internal server error preparing data for transaction", nil)
			}
			return
		}

		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			Value:      finalValueForTx,
			OpType:     store.OpTypeSet,
		}

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record operation in transaction: "+err.Error(), nil)
			}
			return
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, "OK: Operation queued in transaction.", finalValueForTx)
		}
		return
	}

	// Lógica no transaccional
	colStore := h.CollectionManager.GetCollection(collectionName)
	now := time.Now().UTC().Format(time.RFC3339)
	data[globalconst.UPDATED_AT] = now
	data[globalconst.CREATED_AT] = now

	finalValue, err := json.Marshal(data)
	if err != nil {
		slog.Error("Failed to marshal final value with timestamps", "key", key, "collection", collectionName, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to process value with timestamps", nil)
		}
		return
	}

	colStore.Set(key, finalValue, ttl)
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Item set in collection", "user", h.AuthenticatedUser, "collection", collectionName, "key", key, "operation", "create")
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in collection '%s' (persistence async)", key, collectionName), finalValue)
	}
}

// handleCollectionItemUpdate procesa el CmdCollectionItemUpdate. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionItemUpdate(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, key, patchValue, err := protocol.ReadCollectionItemUpdateCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_ITEM_UPDATE command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_UPDATE command format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || key == "" || len(patchValue) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name, key, or patch value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item update attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Update item failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Lógica transaccional
	if h.CurrentTransactionID != "" {
		colStore := h.CollectionManager.GetCollection(collectionName)
		existingValue, found := colStore.Get(key)
		if !found {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusNotFound, "Item not found in memory. Updates inside a transaction currently only support hot data.", nil)
			}
			return
		}
		var existingData, patchData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "Failed to unmarshal existing document for update.", nil)
			}
			return
		}
		if err := json.Unmarshal(patchValue, &patchData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch JSON format.", nil)
			}
			return
		}
		for k, v := range patchData {
			if k != globalconst.ID && k != globalconst.CREATED_AT {
				existingData[k] = v
			}
		}
		finalValue, _ := json.Marshal(existingData)

		// --- MODIFICACIÓN: Usar OpType en lugar de IsDelete ---
		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			Value:      finalValue,
			OpType:     store.OpTypeUpdate,
		}
		// --- FIN DE LA MODIFICACIÓN ---

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record update in transaction: "+err.Error(), nil)
			}
			return
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, "OK: Update operation queued in transaction.", nil)
		}
		return
	}

	// Lógica no transaccional (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	if existingValue, found := colStore.Get(key); found {
		var existingData, patchData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "Failed to unmarshal existing document. Cannot apply patch.", nil)
			}
			return
		}
		if err := json.Unmarshal(patchValue, &patchData); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid patch JSON format.", nil)
			}
			return
		}
		for k, v := range patchData {
			if k != globalconst.ID && k != globalconst.CREATED_AT {
				existingData[k] = v
			}
		}
		existingData[globalconst.UPDATED_AT] = time.Now().UTC().Format(time.RFC3339)
		updatedValue, _ := json.Marshal(existingData)
		colStore.Set(key, updatedValue, 0)
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
		slog.Info("Item updated in collection (hot)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' updated in collection '%s'", key, collectionName), updatedValue)
		}
		return
	}

	fileLock := h.CollectionManager.GetFileLock(collectionName)
	fileLock.Lock()
	updated, err := persistence.UpdateColdItem(collectionName, key, patchValue)
	fileLock.Unlock()

	if err != nil {
		slog.Error("Failed to update cold item on disk", "collection", collectionName, "key", key, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to update item on disk", nil)
		}
		return
	}
	if !updated {
		slog.Warn("Item update failed: key not found in hot or cold storage", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection '%s'", key, collectionName), nil)
		}
		return
	}
	slog.Info("Item updated in collection (cold)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Cold item '%s' updated in collection '%s'", key, collectionName), nil)
	}
}

type updateManyPayload struct {
	ID    string         `json:"_id"`
	Patch map[string]any `json:"patch"`
}

// handleCollectionItemUpdateMany procesa el CmdCollectionItemUpdateMany. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionItemUpdateMany(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, value, err := protocol.ReadCollectionItemUpdateManyCommand(r)
	if err != nil {
		slog.Error("Failed to read UPDATE_MANY command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid UPDATE_COLLECTION_ITEMS_MANY command format", nil)
		}
		return
	}

	var payloads []updateManyPayload
	if err := json.Unmarshal(value, &payloads); err != nil {
		slog.Warn("Failed to unmarshal JSON array for UPDATE_MANY", "collection", collectionName, "error", err, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format. Expected an array of `{\"_id\": \"...\", \"patch\": {...}}`.", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || len(value) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item update-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Update-many failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Lógica transaccional
	if h.CurrentTransactionID != "" {
		colStore := h.CollectionManager.GetCollection(collectionName)
		for _, p := range payloads {
			existingValue, found := colStore.Get(p.ID)
			if !found {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("Item '%s' not found in memory for transactional update.", p.ID), nil)
				}
				return
			}
			var existingData map[string]any
			json.Unmarshal(existingValue, &existingData)
			for k, v := range p.Patch {
				if k != globalconst.ID && k != globalconst.CREATED_AT {
					existingData[k] = v
				}
			}
			finalValue, _ := json.Marshal(existingData)

			// --- MODIFICACIÓN: Usar OpType en lugar de IsDelete ---
			op := store.WriteOperation{
				Collection: collectionName,
				Key:        p.ID,
				Value:      finalValue,
				OpType:     store.OpTypeUpdate,
			}
			// --- FIN DE LA MODIFICACIÓN ---

			if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record update-many op: "+err.Error(), nil)
				}
				return
			}
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d update operations queued in transaction.", len(payloads)), nil)
		}
		return
	}

	// Lógica no transaccional (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	var hotPayloads []updateManyPayload
	var coldPayloads []persistence.ColdUpdatePayload
	for _, p := range payloads {
		if _, found := colStore.Get(p.ID); found {
			hotPayloads = append(hotPayloads, p)
		} else {
			coldPayloads = append(coldPayloads, persistence.ColdUpdatePayload{ID: p.ID, Patch: p.Patch})
		}
	}
	slog.Debug("Split update-many batch", "hot_count", len(hotPayloads), "cold_count", len(coldPayloads))
	updatedHotCount := 0
	var failedHotKeys []string
	now := time.Now().UTC().Format(time.RFC3339)
	for _, p := range hotPayloads {
		existingValue, _ := colStore.Get(p.ID)
		var existingData map[string]any
		if err := json.Unmarshal(existingValue, &existingData); err != nil {
			failedHotKeys = append(failedHotKeys, p.ID)
			continue
		}
		for k, v := range p.Patch {
			if k != globalconst.ID && k != globalconst.CREATED_AT {
				existingData[k] = v
			}
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
	updatedColdCount := 0
	if len(coldPayloads) > 0 {
		fileLock := h.CollectionManager.GetFileLock(collectionName)
		fileLock.Lock()
		count, err := persistence.UpdateManyColdItems(collectionName, coldPayloads)
		fileLock.Unlock()
		if err != nil {
			slog.Error("Failed to update cold items batch", "collection", collectionName, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "An error occurred during the cold batch update.", nil)
			}
			return
		}
		updatedColdCount = count
	}
	totalUpdated := updatedHotCount + updatedColdCount
	totalFailed := (len(payloads) - totalUpdated)
	slog.Info("Update-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "updated_count", totalUpdated, "failed_count", totalFailed)
	if conn != nil {
		summary := fmt.Sprintf("OK: %d items updated. %d items failed or not found.", totalUpdated, totalFailed)
		var responseData []byte
		if len(failedHotKeys) > 0 {
			responseData, _ = json.Marshal(map[string][]string{"failed_hot_keys": failedHotKeys})
		}
		protocol.WriteResponse(conn, protocol.StatusOk, summary, responseData)
	}
}

// handleCollectionItemGet procesa el CmdCollectionItemGet. Es una operación de solo lectura.
func (h *ConnectionHandler) handleCollectionItemGet(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Read operations like GET are not supported inside a transaction in this version.", nil)
		return
	}
	collectionName, key, err := protocol.ReadCollectionItemGetCommand(r)
	if err != nil {
		slog.Error("Failed to read GET_ITEM command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
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

// handleCollectionItemDelete procesa el CmdCollectionItemDelete. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionItemDelete(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, key, err := protocol.ReadCollectionItemDeleteCommand(r)
	if err != nil {
		slog.Error("Failed to read DELETE_ITEM command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_DELETE command format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || key == "" {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or key cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item delete attempt", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Delete item failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Lógica transaccional
	if h.CurrentTransactionID != "" {
		// --- MODIFICACIÓN: Usar OpType en lugar de IsDelete ---
		op := store.WriteOperation{
			Collection: collectionName,
			Key:        key,
			OpType:     store.OpTypeDelete,
		}
		// --- FIN DE LA MODIFICACIÓN ---

		if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record delete in transaction: "+err.Error(), nil)
			}
			return
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, "OK: Delete operation queued in transaction.", nil)
		}
		return
	}

	// Lógica no transaccional (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	if _, foundInRam := colStore.Get(key); foundInRam {
		colStore.Delete(key)
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
		slog.Info("Item deleted from collection (hot)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' deleted from collection '%s'", key, collectionName), nil)
		}
		return
	}

	fileLock := h.CollectionManager.GetFileLock(collectionName)
	fileLock.Lock()
	marked, err := persistence.DeleteColdItem(collectionName, key)
	fileLock.Unlock()

	if err != nil {
		slog.Error("Failed to mark item as deleted on disk", "collection", collectionName, "key", key, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "Failed to perform delete operation on disk", nil)
		}
		return
	}
	if !marked {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found in collection", key), nil)
		}
		return
	}
	slog.Info("Item marked for deletion in collection (cold)", "user", h.AuthenticatedUser, "collection", collectionName, "key", key)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' marked for deletion from collection '%s'", key, collectionName), nil)
	}
}

// handleCollectionItemList procesa el CmdCollectionItemList. Es una operación de solo lectura.
func (h *ConnectionHandler) handleCollectionItemList(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: LIST command is not allowed inside a transaction in this version.", nil)
		return
	}
	collectionName, err := protocol.ReadCollectionItemListCommand(r)
	if err != nil {
		slog.Error("Failed to read LIST_ITEMS command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_ITEM_LIST command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}
	if !h.IsRoot || !h.IsLocalhostConn {
		slog.Warn("Unauthorized list-all-items attempt", "user", h.AuthenticatedUser, "collection", collectionName, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Listing all items is a privileged operation for root@localhost. Please use 'collection query' for data retrieval.", nil)
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

// handleCollectionItemSetMany procesa el CmdCollectionItemSetMany. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionItemSetMany(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, value, err := protocol.ReadCollectionItemSetManyCommand(r)
	if err != nil {
		slog.Error("Failed to read SET_MANY command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET_COLLECTION_ITEMS_MANY command format", nil)
		}
		return
	}

	var records []map[string]any
	if err := json.Unmarshal(value, &records); err != nil {
		slog.Warn("Failed to unmarshal JSON array for SET_MANY", "collection", collectionName, "error", err, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid JSON array format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || len(value) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name or value cannot be empty", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item set-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if h.CurrentTransactionID == "" && !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Set-many operation failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist. Please create it first.", collectionName), nil)
			return
		}
	}

	// --- INICIO: LÓGICA DE GENERACIÓN Y VALIDACIÓN DE IDs ---
	colStore := h.CollectionManager.GetCollection(collectionName)
	recordsToProcess := make([]map[string]any, 0, len(records))
	duplicateKeys := make([]string, 0)
	invalidRecordsCount := 0

	for _, record := range records {
		key, ok := record[globalconst.ID].(string)

		// Caso 1: El cliente no proporciona ID, el servidor lo genera.
		if !ok || key == "" {
			if conn == nil { // No se puede generar ID durante el replay de WAL.
				slog.Error("CRITICAL: SET_MANY record with empty key received during WAL replay.", "collection", collectionName)
				invalidRecordsCount++
				continue
			}

			const maxGenerateRetries = 5
			generatedKey := ""
			for i := 0; i < maxGenerateRetries; i++ {
				newKey := uuid.New().String()
				if _, found := colStore.Get(newKey); !found {
					generatedKey = newKey
					break
				}
			}

			if generatedKey == "" {
				slog.Error("Failed to generate unique ID for a record in SET_MANY batch.", "collection", collectionName)
				invalidRecordsCount++
				continue
			}
			record[globalconst.ID] = generatedKey
			recordsToProcess = append(recordsToProcess, record)
			continue
		}

		// Caso 2: El cliente proporciona ID. Validamos si ya existe (solo para no-transaccional).
		if h.CurrentTransactionID == "" {
			if _, found := colStore.Get(key); found {
				duplicateKeys = append(duplicateKeys, key)
			} else {
				recordsToProcess = append(recordsToProcess, record)
			}
		} else {
			// En una transacción, todos los registros con ID pasan al commit para su validación final.
			recordsToProcess = append(recordsToProcess, record)
		}
	}

	if len(recordsToProcess) == 0 && conn != nil && h.CurrentTransactionID == "" {
		msg := fmt.Sprintf("OK: 0 items processed. %d records were skipped due to existing keys and %d were invalid or failed ID generation.", len(duplicateKeys), invalidRecordsCount)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
		return
	}
	// --- FIN DE LA LÓGICA DE GENERACIÓN Y VALIDACIÓN ---

	// Lógica transaccional
	if h.CurrentTransactionID != "" {
		for _, record := range recordsToProcess {
			key := record[globalconst.ID].(string)
			valBytes, err := json.Marshal(record)
			if err != nil {
				slog.Warn("Failed to marshal record in SET_MANY (transaction)", "key", key, "error", err)
				continue // Skip this record if it can't be marshaled
			}
			op := store.WriteOperation{
				Collection: collectionName, Key: key, Value: valBytes, OpType: store.OpTypeSet,
			}
			if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record set-many op in transaction: "+err.Error(), nil)
				}
				return
			}
		}
		if conn != nil {
			finalDocsBytes, _ := json.Marshal(recordsToProcess)
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d set operations queued in transaction.", len(recordsToProcess)), finalDocsBytes)
		}
		return
	}

	// Lógica no transaccional
	now := time.Now().UTC().Format(time.RFC3339)
	for _, record := range recordsToProcess {
		// El ID ya está garantizado en el record
		record[globalconst.CREATED_AT] = now
		record[globalconst.UPDATED_AT] = now
		updatedValue, err := json.Marshal(record)
		if err != nil {
			slog.Warn("Failed to marshal record in SET_MANY batch, skipping", "key", record[globalconst.ID], "error", err)
			continue
		}
		colStore.Set(record[globalconst.ID].(string), updatedValue, 0)
	}

	if len(recordsToProcess) > 0 {
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}
	slog.Info("Set-many operation completed", "user", h.AuthenticatedUser, "inserted_count", len(recordsToProcess), "duplicates_skipped", len(duplicateKeys), "invalid_skipped", invalidRecordsCount)
	if conn != nil {
		finalDocsBytes, _ := json.Marshal(recordsToProcess)
		msg := fmt.Sprintf("OK: %d items set in collection '%s'. %d records were skipped due to existing keys. %d were invalid or failed ID generation.", len(recordsToProcess), collectionName, len(duplicateKeys), invalidRecordsCount)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, finalDocsBytes)
	}
}

// handleCollectionItemDeleteMany procesa el CmdCollectionItemDeleteMany. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionItemDeleteMany(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, keys, err := protocol.ReadCollectionItemDeleteManyCommand(r)
	if err != nil {
		slog.Error("Failed to read DELETE_MANY command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_ITEMS_MANY command format", nil)
		}
		return
	}

	if conn != nil {
		if collectionName == "" || len(keys) == 0 {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty and keys must be provided", nil)
			return
		}
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection item delete-many attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
		if !h.CollectionManager.CollectionExists(collectionName) {
			slog.Warn("Delete-many failed because collection does not exist", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist.", collectionName), nil)
			return
		}
	}

	// Lógica transaccional
	if h.CurrentTransactionID != "" {
		for _, key := range keys {
			// --- MODIFICACIÓN: Usar OpType en lugar de IsDelete ---
			op := store.WriteOperation{
				Collection: collectionName,
				Key:        key,
				OpType:     store.OpTypeDelete,
			}
			// --- FIN DE LA MODIFICACIÓN ---

			if err := h.TransactionManager.RecordWrite(h.CurrentTransactionID, op); err != nil {
				if conn != nil {
					protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Failed to record delete-many op in transaction: "+err.Error(), nil)
				}
				return
			}
		}
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d delete operations queued in transaction.", len(keys)), nil)
		}
		return
	}

	// Lógica no transaccional (hot/cold)
	colStore := h.CollectionManager.GetCollection(collectionName)
	var hotKeysToDelete, coldKeysToTombstone []string
	for _, key := range keys {
		if _, foundInRam := colStore.Get(key); foundInRam {
			hotKeysToDelete = append(hotKeysToDelete, key)
		} else {
			coldKeysToTombstone = append(coldKeysToTombstone, key)
		}
	}
	if len(hotKeysToDelete) > 0 {
		for _, key := range hotKeysToDelete {
			colStore.Delete(key)
		}
		h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	}
	var markedCount int
	if len(coldKeysToTombstone) > 0 {
		fileLock := h.CollectionManager.GetFileLock(collectionName)
		fileLock.Lock()
		markedCount, err = persistence.DeleteManyColdItems(collectionName, coldKeysToTombstone)
		fileLock.Unlock()
		if err != nil {
			slog.Error("Failed to mark items for deletion in cold storage", "collection", collectionName, "error", err)
			if conn != nil {
				protocol.WriteResponse(conn, protocol.StatusError, "An error occurred during the batch delete operation.", nil)
			}
			return
		}
	}
	totalProcessed := len(hotKeysToDelete) + markedCount
	slog.Info("Delete-many operation completed", "user", h.AuthenticatedUser, "collection", collectionName, "processed_count", totalProcessed)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: %d keys processed for deletion from collection '%s'.", totalProcessed, collectionName), nil)
	}
}

// boolToString es un pequeño helper para logs más claros.
func boolToString(b bool, trueStr, falseStr string) string {
	if b {
		return trueStr
	}
	return falseStr
}
