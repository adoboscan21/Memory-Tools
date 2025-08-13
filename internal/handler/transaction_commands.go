package handler

import (
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
)

// handleBegin inicia una nueva transacción para la conexión actual.
// No es una operación de escritura en el WAL, ya que solo modifica el estado de la conexión.
func (h *ConnectionHandler) handleBegin(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID != "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: A transaction is already in progress.", nil)
		}
		return
	}

	txID, err := h.TransactionManager.Begin()
	if err != nil {
		remoteAddr := "recovery"
		if conn != nil {
			remoteAddr = conn.RemoteAddr().String()
		}
		slog.Error("Failed to begin new transaction", "error", err, "remote_addr", remoteAddr)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Could not start a new transaction.", nil)
		}
		return
	}

	h.CurrentTransactionID = txID
	slog.Info("Transaction started", "txID", txID, "user", h.AuthenticatedUser)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Transaction started.", []byte(txID))
	}
}

// handleCommit intenta confirmar la transacción actual. Es una operación de escritura en el WAL.
func (h *ConnectionHandler) HandleCommit(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: No transaction in progress to commit.", nil)
		}
		return
	}

	txID := h.CurrentTransactionID
	// Limpiar el ID de la transacción de la conexión inmediatamente.
	h.CurrentTransactionID = ""

	err := h.TransactionManager.Commit(txID)

	if err != nil {
		slog.Error("Transaction failed to commit and was rolled back", "txID", txID, "error", err, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Transaction failed and was rolled back: %v", err), nil)
		}
		return
	}

	slog.Info("Transaction committed successfully", "txID", txID, "user", h.AuthenticatedUser)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Transaction committed successfully.", nil)
	}
}

// handleRollback cancela explícitamente la transacción actual.
// No es una operación de escritura en el WAL.
func (h *ConnectionHandler) handleRollback(r io.Reader, conn net.Conn) {
	if h.CurrentTransactionID == "" {
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, "ERROR: No transaction in progress to roll back.", nil)
		}
		return
	}

	txID := h.CurrentTransactionID
	h.CurrentTransactionID = "" // Limpiar estado de la conexión

	err := h.TransactionManager.Rollback(txID)
	if err != nil {
		slog.Error("Error during transaction rollback", "txID", txID, "error", err, "user", h.AuthenticatedUser)
		if conn != nil {
			protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: An error occurred during rollback: %v", err), nil)
		}
		return
	}

	slog.Info("Transaction rolled back by user", "txID", txID, "user", h.AuthenticatedUser)
	if conn != nil {
		protocol.WriteResponse(conn, protocol.StatusOk, "OK: Transaction rolled back successfully.", nil)
	}
}
