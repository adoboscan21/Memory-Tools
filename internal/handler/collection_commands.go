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

	// Authorization check
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
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

	// Authorization check
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for deletion", collectionName), nil)
		return
	}

	h.CollectionManager.DeleteCollection(collectionName)

	// --- ASYNC DELETE: Enqueue file deletion task asynchronously ---
	h.CollectionManager.EnqueueDeleteTask(collectionName)
	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted (persistence will be handled asynchronously)", collectionName), nil)
}

// handleCollectionList processes the CmdCollectionList command.
func (h *ConnectionHandler) handleCollectionList(conn net.Conn) {
	allCollectionNames := h.CollectionManager.ListCollections()
	accessibleCollections := []string{}

	// Filter collections based on read permission
	for _, name := range allCollectionNames {
		if h.hasPermission(name, "read") {
			accessibleCollections = append(accessibleCollections, name)
		}
	}

	jsonNames, err := json.Marshal(accessibleCollections)
	if err != nil {
		log.Printf("Error marshalling collection names to JSON: %v", err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal collection names", nil)
		return
	}
	if err := protocol.WriteResponse(conn, protocol.StatusOk, "OK: Accessible collections listed", jsonNames); err != nil {
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

	// Authorization check: Requires write permission to modify collection structure
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist", collectionName), nil)
		return
	}

	log.Printf("User '%s' requested to create index on '%s' for collection '%s'", h.AuthenticatedUser, fieldName, collectionName)
	colStore := h.CollectionManager.GetCollection(collectionName)
	colStore.CreateIndex(fieldName)

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Index creation process for field '%s' on collection '%s' completed.", fieldName, collectionName), nil)
}

// handleCollectionIndexDelete processes the CmdCollectionIndexDelete command.
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

	// Authorization check: Requires write permission
	if !h.hasPermission(collectionName, "write") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have write permission for collection '%s'", collectionName), nil)
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

// handleCollectionIndexList processes the CmdCollectionIndexList command.
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

	// Authorization check: Requires read permission
	if !h.hasPermission(collectionName, "read") {
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
		log.Printf("Error marshalling index list for collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal index list", nil)
		return
	}

	protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Indexes for collection '%s' retrieved.", collectionName), jsonResponse)
}
