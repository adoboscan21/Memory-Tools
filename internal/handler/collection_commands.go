package handler

import (
	"fmt"
	"log"
	"memory-tools/internal/protocol"
	"net"
)

// handleCollectionCreate processes the CmdCollectionCreate command.
func (h *ConnectionHandler) handleCollectionCreate(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionCreateCommand(conn)
	if err != nil {
		log.Printf("Error reading CREATE_COLLECTION command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}
	// Specific authorization check for _system collection (even if authenticated)
	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can create collection '%s'", SystemCollectionName), nil)
		return
	}

	colStore := h.CollectionManager.GetCollection(collectionName)

	// --- ASYNC SAVE: Enqueue save task instead of saving synchronously ---
	h.CollectionManager.EnqueueSaveTask(collectionName, colStore)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' ensured (persistence will be handled asynchronously)", collectionName), nil)
}

// handleCollectionDelete processes the CmdCollectionDelete command.
func (h *ConnectionHandler) handleCollectionDelete(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionDeleteCommand(conn)
	if err != nil {
		log.Printf("Error reading DELETE_COLLECTION command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}
	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		return
	}
	// Specific authorization check for _system collection (even if authenticated)
	if collectionName == SystemCollectionName && !(h.AuthenticatedUser == "root" && h.IsLocalhostConn) {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: Only 'root' from localhost can delete collection '%s'", SystemCollectionName), nil)
		return
	}

	h.CollectionManager.DeleteCollection(collectionName)

	// --- ASYNC DELETE: Enqueue file deletion task asynchronously ---
	h.CollectionManager.EnqueueDeleteTask(collectionName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted (persistence will be handled asynchronously)", collectionName), nil)
}

// handleCollectionList processes the CmdCollectionList command.
func (h *ConnectionHandler) handleCollectionList(conn net.Conn) {
	collectionNames := h.CollectionManager.ListCollections()
	jsonNames, err := json.Marshal(collectionNames)
	if err != nil {
		log.Printf("Error marshalling collection names to JSON: %v", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection names", nil)
		return
	}
	if err := protocol.WriteResponse(conn, protocol.StatusOk, "OK: Collections listed", jsonNames); err != nil {
		log.Printf("Error writing collection list response to %s: %v", conn.RemoteAddr(), err)
	}
}

// handleCollectionIndexCreate processes the CmdCollectionIndexCreate command.
func (h *ConnectionHandler) handleCollectionIndexCreate(conn net.Conn) {
	collectionName, fieldName, err := protocol.ReadCollectionIndexCreateCommand(conn)
	if err != nil {
		log.Printf("Error reading CREATE_COLLECTION_INDEX command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid CREATE_COLLECTION_INDEX command format", nil)
		return
	}
	if collectionName == "" || fieldName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name and field name cannot be empty", nil)
		return
	}
	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	// This operation can be long, in a real-world scenario, you might do this in the background.
	// For now, we do it synchronously.
	log.Printf("User '%s' requested to create index on '%s' for collection '%s'", h.AuthenticatedUser, fieldName, collectionName)
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.CreateIndex(fieldName)

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index creation process for field '%s' on collection '%s' completed.", fieldName, collectionName), nil)
}

// NEW: handleCollectionIndexDelete processes the CmdCollectionIndexDelete command.
func (h *ConnectionHandler) handleCollectionIndexDelete(conn net.Conn) {
	collectionName, fieldName, err := protocol.ReadCollectionIndexDeleteCommand(conn)
	if err != nil {
		log.Printf("Error reading DELETE_COLLECTION_INDEX command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid DELETE_COLLECTION_INDEX command format", nil)
		return
	}
	if collectionName == "" || fieldName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name and field name cannot be empty", nil)
		return
	}
	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	log.Printf("User '%s' requested to delete index on '%s' for collection '%s'", h.AuthenticatedUser, fieldName, collectionName)
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.DeleteIndex(fieldName)

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index for field '%s' on collection '%s' deleted.", fieldName, collectionName), nil)
}

// NEW: handleCollectionIndexList processes the CmdCollectionIndexList command.
func (h *ConnectionHandler) handleCollectionIndexList(conn net.Conn) {
	collectionName, err := protocol.ReadCollectionIndexListCommand(conn)
	if err != nil {
		log.Printf("Error reading LIST_COLLECTION_INDEXES command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid LIST_COLLECTION_INDEXES command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
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
		log.Printf("Error marshalling index list for collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal index list", nil)
		return
	}

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Indexes for collection '%s' retrieved.", collectionName), jsonResponse)
}
