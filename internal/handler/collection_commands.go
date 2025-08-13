package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/globalconst"
	"memory-tools/internal/protocol"
	"net"
)

// handleCollectionCreate procesa el CmdCollectionCreate. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionCreate(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, err := protocol.ReadCollectionCreateCommand(r)
	if err != nil {
		slog.Error("Failed to read CREATE_COLLECTION command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION command format", nil)
		}
		return
	}
	if collectionName == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		}
		return
	}

	if conn != nil {
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection create attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
	}

	if h.CollectionManager.CollectionExists(collectionName) {
		slog.Info("Collection create command on existing collection", "user", h.AuthenticatedUser, "collection", collectionName)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' already exists.", collectionName), nil)
		}
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Collection created/ensured", "user", h.AuthenticatedUser, "collection", collectionName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' ensured (persistence will be handled asynchronously)", collectionName), nil)
	}
}

// handleCollectionDelete procesa el CmdCollectionDelete. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionDelete(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, err := protocol.ReadCollectionDeleteCommand(r)
	if err != nil {
		slog.Error("Failed to read DELETE_COLLECTION command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION command format", nil)
		}
		return
	}
	if collectionName == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		}
		return
	}

	if conn != nil {
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized collection delete attempt", "user", h.AuthenticatedUser, "collection", collectionName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		slog.Warn("Collection delete failed: collection not found", "user", h.AuthenticatedUser, "collection", collectionName)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		}
		return
	}

	h.CollectionManager.DeleteCollection(collectionName)
	h.CollectionManager.EnqueueDeleteTask(collectionName)

	slog.Info("Collection deleted", "user", h.AuthenticatedUser, "collection", collectionName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted (persistence will be handled asynchronously)", collectionName), nil)
	}
}

// handleCollectionList procesa el CmdCollectionList. Es una operación de solo lectura.
func (h *ConnectionHandler) handleCollectionList(r io.Reader, conn net.Conn) {
	allCollectionNames := h.CollectionManager.ListCollections()
	accessibleCollections := []string{}

	for _, name := range allCollectionNames {
		if h.hasPermission(name, globalconst.PermissionRead) {
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

// handleCollectionIndexCreate procesa el CmdCollectionIndexCreate. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionIndexCreate(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, fieldName, err := protocol.ReadCollectionIndexCreateCommand(r)
	if err != nil {
		slog.Error("Failed to read CREATE_INDEX command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION_INDEX command format", nil)
		}
		return
	}
	if collectionName == "" || fieldName == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name and field name cannot be empty", nil)
		}
		return
	}

	if conn != nil {
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized index create attempt", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		slog.Warn("Index create failed: collection not found", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		}
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.CreateIndex(fieldName)

	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Index created on collection", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index creation process for field '%s' on collection '%s' completed.", fieldName, collectionName), nil)
	}
}

// handleCollectionIndexDelete procesa el CmdCollectionIndexDelete. Es una operación de escritura.
func (h *ConnectionHandler) HandleCollectionIndexDelete(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	collectionName, fieldName, err := protocol.ReadCollectionIndexDeleteCommand(r)
	if err != nil {
		slog.Error("Failed to read DELETE_INDEX command payload", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_INDEX command format", nil)
		}
		return
	}
	if collectionName == "" || fieldName == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name and field name cannot be empty", nil)
		}
		return
	}

	if conn != nil {
		if !h.hasPermission(collectionName, globalconst.PermissionWrite) {
			slog.Warn("Unauthorized index delete attempt", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
			return
		}
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		slog.Warn("Index delete failed: collection not found", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		}
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.DeleteIndex(fieldName)

	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)

	slog.Info("Index deleted from collection", "user", h.AuthenticatedUser, "collection", collectionName, "field", fieldName)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index for field '%s' on collection '%s' deleted.", fieldName, collectionName), nil)
	}
}

// handleCollectionIndexList procesa el CmdCollectionIndexList. Es una operación de solo lectura.
func (h *ConnectionHandler) handleCollectionIndexList(r io.Reader, conn net.Conn) {
	collectionName, err := protocol.ReadCollectionIndexListCommand(r)
	if err != nil {
		slog.Error("Failed to read LIST_INDEXES command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid LIST_COLLECTION_INDEXES command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
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
