package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
)

// handleMainStoreSet processes the CmdSet command.
func (h *ConnectionHandler) handleMainStoreSet(conn net.Conn) {
	// Authorization check: Only root can use the main store.
	if !h.IsRoot {
		slog.Warn("Unauthorized main store SET attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can operate on the main store.", nil)
		return
	}

	key, value, ttl, err := protocol.ReadSetCommand(conn)
	if err != nil {
		slog.Error("Failed to read SET command", "remote_addr", conn.RemoteAddr().String(), "error", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET command format", nil)
		return
	}

	h.MainStore.Set(key, value, ttl)
	slog.Debug("Main store SET successful", "key", key, "user", h.AuthenticatedUser)

	if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in main store", key), nil); err != nil {
		slog.Error("Failed to write SET response", "remote_addr", conn.RemoteAddr().String(), "error", err)
	}
}

// handleMainStoreGet processes the CmdGet command.
func (h *ConnectionHandler) handleMainStoreGet(conn net.Conn) {
	// Authorization check: Only root can use the main store.
	if !h.IsRoot {
		slog.Warn("Unauthorized main store GET attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can operate on the main store.", nil)
		return
	}

	key, err := protocol.ReadGetCommand(conn)
	if err != nil {
		slog.Error("Failed to read GET command", "remote_addr", conn.RemoteAddr().String(), "error", err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid GET command format", nil)
		return
	}

	value, found := h.MainStore.Get(key)
	slog.Debug("Main store GET", "key", key, "user", h.AuthenticatedUser, "found", found)

	if found {
		if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' retrieved from main store", key), value); err != nil {
			slog.Error("Failed to write GET success response", "remote_addr", conn.RemoteAddr().String(), "error", err)
		}
	} else {
		if err := protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Key '%s' not found or expired in main store", key), nil); err != nil {
			slog.Error("Failed to write GET not found response", "remote_addr", conn.RemoteAddr().String(), "error", err)
		}
	}
}
