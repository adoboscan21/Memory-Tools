package handler

import (
	"fmt"
	"log"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"net"
)

// handleBackup handles the command for a manual backup.
func (h *ConnectionHandler) handleBackup(conn net.Conn) {
	if !h.IsRoot {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a manual backup.", nil)
		return
	}

	log.Printf("Root user '%s' initiated a manual backup.", h.AuthenticatedUser)
	if err := h.BackupManager.PerformBackup(); err != nil {
		log.Printf("Manual backup failed: %v", err)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Backup failed: %v", err), nil)
		return
	}

	protocol.WriteResponse(conn, protocol.StatusOk, "OK: Manual backup completed successfully.", nil)
}

// handleRestore handles the command to restore from a backup.
func (h *ConnectionHandler) handleRestore(conn net.Conn) {
	if !h.IsRoot {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, "UNAUTHORIZED: Only root can trigger a restore.", nil)
		return
	}

	backupName, err := protocol.ReadRestoreCommand(conn)
	if err != nil {
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid RESTORE command format.", nil)
		return
	}
	if backupName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Backup name cannot be empty.", nil)
		return
	}

	log.Printf("!!! DESTRUCTIVE ACTION: Root user '%s' initiated a restore from '%s' !!!", h.AuthenticatedUser, backupName)

	// NOTE: In a production system, the server should be paused or
	// put into read-only mode here to prevent inconsistencies.

	err = persistence.PerformRestore(backupName, h.MainStore, h.CollectionManager)
	if err != nil {
		log.Printf("Restore from '%s' failed: %v", backupName, err)
		// It is crucial not to leave the server in an inconsistent state.
		// The best action here would be a forced restart or a severe alert.
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Restore failed: %v. Server might be in an inconsistent state.", err), nil)
		return
	}

	msg := fmt.Sprintf("OK: Restore from '%s' completed successfully. A server restart is recommended to ensure consistency.", backupName)
	protocol.WriteResponse(conn, protocol.StatusOk, msg, nil)
}
