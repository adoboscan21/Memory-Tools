package handler

import (
	"fmt"
	"log"
	"memory-tools/internal/protocol"
	"net"
)

// handleMainStoreSet processes the CmdSet command.
func (h *ConnectionHandler) handleMainStoreSet(conn net.Conn) {
	key, value, ttl, err := protocol.ReadSetCommand(conn)
	if err != nil {
		log.Printf("Error reading SET command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET command format", nil)
		return
	}
	h.MainStore.Set(key, value, ttl)
	if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in main store", key), nil); err != nil {
		log.Printf("Error writing SET response to %s: %v", conn.RemoteAddr(), err)
	}
}

// handleMainStoreGet processes the CmdGet command.
func (h *ConnectionHandler) handleMainStoreGet(conn net.Conn) {
	key, err := protocol.ReadGetCommand(conn)
	if err != nil {
		log.Printf("Error reading GET command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid GET command format", nil)
		return
	}
	value, found := h.MainStore.Get(key)
	if found {
		if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from main store", key), value); err != nil {
			log.Printf("Error writing GET success response to %s: %v", conn.RemoteAddr(), err)
		}
	} else {
		if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in main store", key), nil); err != nil {
			log.Printf("Error writing GET not found response to %s: %v", conn.RemoteAddr(), err)
		}
	}
}
