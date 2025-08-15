package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
)

// HandleMainStoreSet processes the CmdSet command.
// It reads the payload from 'r' and writes the response to 'conn'.
func (h *ConnectionHandler) HandleMainStoreSet(r io.Reader, conn net.Conn) {
	// During WAL recovery, conn is nil and authorization is skipped.
	if conn != nil {
		if !h.IsRoot {
			slog.Warn("Unauthorized main store SET attempt",
				"user", h.AuthenticatedUser,
				"remote_addr", conn.RemoteAddr().String(),
			)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can operate on the main store.", nil)
			return
		}
	}

	key, value, ttl, err := protocol.ReadSetCommand(r)
	if err != nil {
		remoteAddr := "recovery"
		if conn != nil {
			remoteAddr = conn.RemoteAddr().String()
		}
		slog.Error("Failed to read SET command payload", "remote_addr", remoteAddr, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid SET command format", nil)
		}
		return
	}

	h.MainStore.Set(key, value, ttl)
	slog.Debug("Main store SET successful", "key", key, "user", h.AuthenticatedUser)

	if conn != nil {
		if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Key '%s' set in main store", key), nil); err != nil {
			slog.Error("Failed to write SET response", "remote_addr", conn.RemoteAddr().String(), "error", err)
		}
	}
}

// handleMainStoreGet processes the CmdGet command.
// It reads the payload from 'r' and writes the response to 'conn'.
func (h *ConnectionHandler) handleMainStoreGet(r io.Reader, conn net.Conn) {
	// GET is a read-only command, so it doesn't need a conn == nil check
	// as it will never be replayed from the WAL. Still, it's good practice to keep the signature consistent.
	if !h.IsRoot {
		slog.Warn("Unauthorized main store GET attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can operate on the main store.", nil)
		return
	}

	key, err := protocol.ReadGetCommand(r)
	if err != nil {
		slog.Error("Failed to read GET command payload", "remote_addr", conn.RemoteAddr().String(), "error", err)
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
