package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
)

// handleCollectionCreate processes the CmdCollectionCreate command.
func (h *ConnectionHandler) handleCollectionCreate(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionCreateCommand(conn)
	if err != nil {
		slog.Error("Failed to read CREATE_COLLECTION command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection create attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Collection created/ensured", "user", h.AuthenticatedUser, "collection", collectionName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' ensured (persistence will be handled asynchronously)", collectionName), nil)
}

// handleCollectionDelete processes the CmdCollectionDelete command.
func (h *ConnectionHandler) handleCollectionDelete(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionDeleteCommand(conn)
	if err != nil {
		slog.Error("Failed to read DELETE_COLLECTION command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized collection delete attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		slog.Warn("Collection delete failed: collection not found", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		return
	}

	h.CollectionManager.DeleteCollection(collectionName)
	h.CollectionManager.EnqueueDeleteTask(collectionName)

	slog.Info("Collection deleted", "user", h.AuthenticatedUser, "collection", collectionName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted (persistence will be handled asynchronously)", collectionName), nil)
}

// handleCollectionList processes the CmdCollectionList command.
func (h *ConnectionHandler) handleCollectionList(conn net.Conn) {
	allCollectionNames := h.CollectionManager.ListCollections()
	accessibleCollections := []string{}

	for _, name := range allCollectionNames {
		if h.hasPermission(name, "read") {
			accessibleCollections = append(accessibleCollections, name)
		}
	}

	jsonNames, err := json.Marshal(accessibleCollections)
	if err != nil {
		slog.Error("Failed to marshal collection names to JSON", "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection names", nil)
		return
	}

	slog.Debug("User listed collections", "user", h.AuthenticatedUser, "count", len(accessibleCollections))
	if err := protocol.WriteResponse(conn, protocol.StatusOk, "OK: Accessible collections listed", jsonNames); err != nil {
		slog.Error("Failed to write collection list response", "error", err, "remote_addr", conn.RemoteAddr().String())
	}
}

// handleCollectionIndexCreate processes the CmdCollectionIndexCreate command.
func (h *ConnectionHandler) handleCollectionIndexCreate(conn net.Conn) {
	collectionName, fieldName, err := protocol.ReadCollectionIndexCreateCommand(conn)
	if err != nil {
		slog.Error("Failed to read CREATE_INDEX command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION_INDEX command format", nil)
		return
	}
	if collectionName == "" || fieldName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name and field name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized index create attempt", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		slog.Warn("Index create failed: collection not found", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.CreateIndex(fieldName)

	slog.Info("Index created on collection", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index creation process for field '%s' on collection '%s' completed.", fieldName, collectionName), nil)
}

// handleCollectionIndexDelete processes the CmdCollectionIndexDelete command.
func (h *ConnectionHandler) handleCollectionIndexDelete(conn net.Conn) {
	collectionName, fieldName, err := protocol.ReadCollectionIndexDeleteCommand(conn)
	if err != nil {
		slog.Error("Failed to read DELETE_INDEX command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_INDEX command format", nil)
		return
	}
	if collectionName == "" || fieldName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name and field name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "write") {
		slog.Warn("Unauthorized index delete attempt", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		slog.Warn("Index delete failed: collection not found", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.DeleteIndex(fieldName)

	slog.Info("Index deleted from collection", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index for field '%s' on collection '%s' deleted.", fieldName, collectionName), nil)
}

// handleCollectionIndexList processes the CmdCollectionIndexList command.
func (h *ConnectionHandler) handleCollectionIndexList(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionIndexListCommand(conn)
	if err != nil {
		slog.Error("Failed to read LIST_INDEXES command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid LIST_COLLECTION_INDEXES command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, "read") {
		slog.Warn("Unauthorized index list attempt", "user", h.AuthenticatedUser, "collection", collectionName)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	indexedFields := colStore.ListIndexes()

	jsonResponse, err := json.Marshal(indexedFields)
	if err != nil {
		slog.Error("Failed to marshal index list", "collection", collectionName, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal index list", nil)
		return
	}

	slog.Debug("User listed indexes for collection", "user", h.AuthenticatedUser, "collection", collectionName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Indexes for collection '%s' retrieved.", collectionName), jsonResponse)
}
