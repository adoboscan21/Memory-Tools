package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"net"
)

// handleBackup handles the command for a manual backup.
// This operation does not modify data state, so it is not logged to the WAL.
func (h *ConnectionHandler) handleBackup(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	if !h.IsRoot {
		slog.Warn("Unauthorized backup attempt",
			"user", h.AuthenticatedUser,
			"remote_addr", remoteAddr,
		)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a manual backup.", nil)
		}
		return
	}

	slog.Info("Manual backup initiated", "user", h.AuthenticatedUser, "remote_addr", remoteAddr)
	if err := h.BackupManager.PerformBackup(); err != nil {
		slog.Error("Manual backup failed", "user", h.AuthenticatedUser, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Backup failed: %v", err), nil)
		}
		return
	}

	slog.Info("Manual backup completed successfully", "user", h.AuthenticatedUser)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Manual backup completed successfully.", nil)
	}
}

// HandleRestore handles the command to restore from a backup.
// This is a bulk write operation and is logged to the WAL.
func (h *ConnectionHandler) HandleRestore(r io.Reader, conn net.Conn) {
	remoteAddr := "recovery"
	if conn != nil {
		remoteAddr = conn.RemoteAddr().String()
	}

	// During WAL recovery, conn is nil and authorization is skipped.
	if conn != nil {
		if !h.IsRoot {
			slog.Warn("Unauthorized restore attempt",
				"user", h.AuthenticatedUser,
				"remote_addr", remoteAddr,
			)
			protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a restore.", nil)
			return
		}
	}

	backupName, err := protocol.ReadRestoreCommand(r)
	if err != nil {
		slog.Error("Failed to read RESTORE command payload", "remote_addr", remoteAddr, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid RESTORE command format.", nil)
		}
		return
	}
	if backupName == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusBadRequest, "Backup name cannot be empty.", nil)
		}
		return
	}

	slog.Warn("DESTRUCTIVE ACTION: Restore initiated",
		"user", h.AuthenticatedUser,
		"backup_name", backupName,
		"remote_addr", remoteAddr,
	)

	err = persistence.PerformRestore(backupName, h.MainStore, h.CollectionManager)
	if err != nil {
		slog.Error("Restore failed", "backup_name", backupName, "user", h.AuthenticatedUser, "error", err)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Restore failed: %v. Server might be in an inconsistent state.", err), nil)
		}
		return
	}

	slog.Info("Restore complete. Enqueuing persistence tasks for all restored collections.")

	// After a restore, it's vital to save the new state to the snapshots.
	if err := persistence.SaveData(h.MainStore); err != nil {
		slog.Error("Failed to persist main store after restore", "error", err)
	}

	restoredCollections := h.CollectionManager.ListCollections()
	for _, colName := range restoredCollections {
		colStore := h.CollectionManager.GetCollection(colName)
		h.CollectionManager.EnqueueSaveTask(colName, colStore)
	}

	slog.Info("Restore completed successfully", "backup_name", backupName, "user", h.AuthenticatedUser)
	if conn != nil {
		msg := fmt.Sprintf("OK: Restore from '%s' completed successfully. A server restart is recommended to ensure consistency.", backupName)
		protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
	}
}
