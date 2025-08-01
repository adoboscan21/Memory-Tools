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
	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
		log.Printf("Error saving new/ensured collection '%s' to disk: %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to ensure collection '%s' persistence", collectionName), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' ensured and persisted", collectionName), nil)
	}
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
	if err := h.CollectionManager.DeleteCollectionFromDisk(collectionName); err != nil {
		log.Printf("Error deleting collection file for '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to delete collection '%s' from disk", collectionName), nil)
	} else {
		protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Collection '%s' deleted", collectionName), nil)
	}
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
