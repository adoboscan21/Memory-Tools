// ./internal/store/transaction.go

package store

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TransactionState define los posibles estados de una transacción.
type TransactionState int

const (
	StateActive    TransactionState = iota // La transacción está en curso.
	StatePreparing                         // Fase 1 de 2PC (votación) iniciada.
	StateCommitted                         // La transacción se confirmó con éxito.
	StateAborted                           // La transacción se abortó (rollback).
)

// WriteOperation representa una única operación de escritura dentro de una transacción.
type WriteOperation struct {
	Collection string
	Key        string
	Value      []byte // Será nil para operaciones de DELETE.
	IsDelete   bool
}

// Transaction mantiene el estado y las operaciones de una transacción.
type Transaction struct {
	ID        string
	State     TransactionState
	WriteSet  []WriteOperation // El "diario" de operaciones de la transacción.
	startTime time.Time
	mu        sync.RWMutex
}

// TransactionManager es el coordinador central de todas las transacciones.
type TransactionManager struct {
	transactions map[string]*Transaction // Mapa de ID de transacción -> *Transaction
	mu           sync.RWMutex
	cm           *CollectionManager // Referencia al CollectionManager para acceder a los shards.
}

// NewTransactionManager crea una nueva instancia del gestor de transacciones.
func NewTransactionManager(cm *CollectionManager) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		cm:           cm,
	}
}

// Begin inicia una nueva transacción y la registra, devolviendo su ID único.
func (tm *TransactionManager) Begin() (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := uuid.New().String()
	tx := &Transaction{
		ID:        txID,
		State:     StateActive,
		WriteSet:  make([]WriteOperation, 0),
		startTime: time.Now(),
	}

	tm.transactions[txID] = tx
	slog.Debug("TransactionManager: new transaction begun", "txID", txID)
	return txID, nil
}

// RecordWrite añade una operación de escritura al diario de una transacción activa.
func (tm *TransactionManager) RecordWrite(txID string, op WriteOperation) error {
	tx, err := tm.getTransaction(txID)
	if err != nil {
		return err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != StateActive {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	tx.WriteSet = append(tx.WriteSet, op)
	return nil
}

// getTransaction es un helper interno para obtener una transacción de forma segura.
func (tm *TransactionManager) getTransaction(txID string) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return nil, fmt.Errorf("transaction with ID %s not found", txID)
	}
	return tx, nil
}

// removeTransaction es un helper interno para limpiar una transacción finalizada.
func (tm *TransactionManager) removeTransaction(txID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.transactions, txID)
}

func (tm *TransactionManager) Commit(txID string) error {
	tx, err := tm.getTransaction(txID)
	if err != nil {
		return err
	}

	tx.mu.Lock()
	if tx.State != StateActive {
		tx.mu.Unlock()
		return fmt.Errorf("cannot commit transaction %s; state is not active", txID)
	}
	tx.State = StatePreparing
	tx.mu.Unlock()

	slog.Debug("TransactionManager: entering Prepare Phase", "txID", txID)

	opsByShard := make(map[*Shard][]WriteOperation)
	keysByShard := make(map[*Shard][]string)

	for _, op := range tx.WriteSet {
		col := tm.cm.GetCollection(op.Collection).(*InMemStore)
		shard := col.getShard(op.Key)
		opsByShard[shard] = append(opsByShard[shard], op)
		keysByShard[shard] = append(keysByShard[shard], op.Key)
	}

	// Versión corregida sin la variable innecesaria:
	for shard, keys := range keysByShard {
		if err := shard.lockKeys(txID, keys); err != nil {
			slog.Warn("TransactionManager: lock failed during Prepare Phase, initiating rollback", "txID", txID, "shard", shard, "error", err)
			tm.Rollback(txID)
			return fmt.Errorf("prepare failed: %w", err)
		}
	}

	for shard, ops := range opsByShard {
		for _, op := range ops {
			if err := shard.prepareWrite(txID, op); err != nil {
				slog.Warn("TransactionManager: prepareWrite failed, initiating rollback", "txID", txID, "shard", shard, "error", err)
				tm.Rollback(txID)
				return fmt.Errorf("prepare failed: %w", err)
			}
		}
	}

	slog.Debug("TransactionManager: Prepare Phase successful. Entering Commit Phase.", "txID", txID)

	tx.mu.Lock()
	tx.State = StateCommitted
	tx.mu.Unlock()

	indexManagers := make(map[*IndexManager]bool)

	for _, op := range tx.WriteSet {
		col := tm.cm.GetCollection(op.Collection).(*InMemStore)
		tm.cm.EnqueueSaveTask(op.Collection, col)
		indexManagers[col.indexes] = true
	}

	for shard := range keysByShard {
		var associatedIndexManager *IndexManager
		for im := range indexManagers {
			associatedIndexManager = im
			break
		}
		shard.commitAppliedChanges(txID, associatedIndexManager)
	}

	tm.removeTransaction(txID)
	return nil
}

// Rollback revierte una transacción, descartando todos sus cambios.
func (tm *TransactionManager) Rollback(txID string) error {
	tx, err := tm.getTransaction(txID)
	if err != nil {
		// Es posible que ya se haya eliminado, por lo que no es un error fatal.
		return nil
	}

	tx.mu.Lock()
	if tx.State == StateCommitted || tx.State == StateAborted {
		tx.mu.Unlock()
		return nil // La transacción ya está finalizada.
	}
	tx.State = StateAborted
	tx.mu.Unlock()

	slog.Debug("TransactionManager: rolling back transaction", "txID", txID)

	// Agrupar operaciones por shard para saber a quién notificar.
	keysByShard := make(map[*Shard][]string)
	for _, op := range tx.WriteSet {
		col := tm.cm.GetCollection(op.Collection).(*InMemStore)
		shard := col.getShard(op.Key)
		keysByShard[shard] = append(keysByShard[shard], op.Key)
	}

	// Notificar a cada shard involucrado que debe revertir los cambios.
	for shard := range keysByShard {
		shard.rollbackChanges(txID)
	}

	// Limpiar la transacción del gestor.
	tm.removeTransaction(txID)
	return nil
}
