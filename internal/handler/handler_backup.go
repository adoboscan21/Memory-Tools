package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"net"
)

// handleBackup handles the command for a manual backup.
func (h *ConnectionHandler) handleBackup(conn net.Conn) {
	if !h.IsRoot {
		slog.Warn("Unauthorized backup attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a manual backup.", nil)
		return
	}

	slog.Info("Manual backup initiated", "user", h.AuthenticatedUser, "remote_addr", conn.RemoteAddr().String())
	if err := h.BackupManager.PerformBackup(); err != nil {
		slog.Error("Manual backup failed", "user", h.AuthenticatedUser, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Backup failed: %v", err), nil)
		return
	}

	slog.Info("Manual backup completed successfully", "user", h.AuthenticatedUser)
	protocol.WriteResponse(conn, protocol.StatusOk, "OK: Manual backup completed successfully.", nil)
}

// handleRestore handles the command to restore from a backup.
func (h *ConnectionHandler) handleRestore(conn net.Conn) {
	if !h.IsRoot {
		slog.Warn("Unauthorized restore attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a restore.", nil)
		return
	}

	backupName, err := protocol.ReadRestoreCommand(conn)
	if err != nil {
		slog.Error("Failed to read RESTORE command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid RESTORE command format.", nil)
		return
	}
	if backupName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Backup name cannot be empty.", nil)
		return
	}

	slog.Warn("DESTRUCTIVE ACTION: Restore initiated",
		"user", h.AuthenticatedUser,
		"backup_name", backupName,
		"remote_addr", conn.RemoteAddr().String(),
	)

	err = persistence.PerformRestore(backupName, h.MainStore, h.CollectionManager)
	if err != nil {
		slog.Error("Restore failed", "backup_name", backupName, "user", h.AuthenticatedUser, "error", err)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Restore failed: %v. Server might be in an inconsistent state.", err), nil)
		return
	}

	slog.Info("Restore complete. Enqueuing persistence tasks for all restored collections.")

	if err := persistence.SaveData(h.MainStore); err != nil {
		slog.Error("Failed to persist main store after restore", "error", err)
	}

	restoredCollections := h.CollectionManager.ListCollections()
	for _, colName := range restoredCollections {
		colStore := h.CollectionManager.GetCollection(colName)
		h.CollectionManager.EnqueueSaveTask(colName, colStore)
	}

	slog.Info("Restore completed successfully", "backup_name", backupName, "user", h.AuthenticatedUser)
	msg := fmt.Sprintf("OK: Restore from '%s' completed successfully. A server restart is recommended to ensure consistency.", backupName)
	protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
}
