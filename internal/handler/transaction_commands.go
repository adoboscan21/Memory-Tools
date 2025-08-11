// ./internal/handler/transaction_commands.go

package handler

import (
	"fmt"
	"log/slog"
	"memory-tools/internal/protocol"
	"net"
)

// handleBegin inicia una nueva transacción para la conexión actual.
func (h *ConnectionHandler) handleBegin(conn net.Conn) {
	if h.CurrentTransactionID != "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: A transaction is already in progress.", nil)
		return
	}

	txID, err := h.TransactionManager.Begin()
	if err != nil {
		slog.Error("Failed to begin new transaction", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: Could not start a new transaction.", nil)
		return
	}

	h.CurrentTransactionID = txID
	slog.Info("Transaction started", "txID", txID, "user", h.AuthenticatedUser)
	protocol.WriteResponse(conn, protocol.StatusOk, "OK: Transaction started.", []byte(txID))
}

// handleCommit intenta confirmar la transacción actual usando un protocolo de 2 fases.
func (h *ConnectionHandler) handleCommit(conn net.Conn) {
	if h.CurrentTransactionID == "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: No transaction in progress to commit.", nil)
		return
	}

	txID := h.CurrentTransactionID
	// Limpiar el ID de la transacción de la conexión inmediatamente.
	// La transacción se cerrará, ya sea con éxito o con un rollback.
	h.CurrentTransactionID = ""

	// Aquí ocurre la magia del 2PC, coordinado por el TransactionManager.
	err := h.TransactionManager.Commit(txID)

	if err != nil {
		slog.Error("Transaction failed to commit and was rolled back", "txID", txID, "error", err, "user", h.AuthenticatedUser)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: Transaction failed and was rolled back: %v", err), nil)
		return
	}

	slog.Info("Transaction committed successfully", "txID", txID, "user", h.AuthenticatedUser)
	protocol.WriteResponse(conn, protocol.StatusOk, "OK: Transaction committed successfully.", nil)
}

// handleRollback cancela explícitamente la transacción actual.
func (h *ConnectionHandler) handleRollback(conn net.Conn) {
	if h.CurrentTransactionID == "" {
		protocol.WriteResponse(conn, protocol.StatusError, "ERROR: No transaction in progress to roll back.", nil)
		return
	}

	txID := h.CurrentTransactionID
	h.CurrentTransactionID = "" // Limpiar estado de la conexión

	err := h.TransactionManager.Rollback(txID)
	if err != nil {
		slog.Error("Error during transaction rollback", "txID", txID, "error", err, "user", h.AuthenticatedUser)
		// Aunque falle, la transacción se considera cerrada.
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("ERROR: An error occurred during rollback: %v", err), nil)
		return
	}

	slog.Info("Transaction rolled back by user", "txID", txID, "user", h.AuthenticatedUser)
	protocol.WriteResponse(conn, protocol.StatusOk, "OK: Transaction rolled back successfully.", nil)
}
