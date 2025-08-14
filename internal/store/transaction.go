// ./internal/store/transaction.go

package store

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"memory-tools/internal/globalconst"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type TransactionState int

const (
	StateActive TransactionState = iota
	StatePreparing
	StateCommitted
	StateAborted
)

// NUEVO: Un enum para el tipo de operación
type TransactionOpType int

const (
	OpTypeSet TransactionOpType = iota
	OpTypeUpdate
	OpTypeDelete
)

type WriteOperation struct {
	Collection string
	Key        string
	Value      []byte
	OpType     TransactionOpType // Campo actualizado
}

type Transaction struct {
	ID        string
	State     TransactionState
	WriteSet  []WriteOperation
	startTime time.Time
	mu        sync.RWMutex
}

// TransactionManager es el coordinador central de todas las transacciones.
type TransactionManager struct {
	transactions map[string]*Transaction
	mu           sync.RWMutex
	cm           *CollectionManager
	gcQuitChan   chan struct{}
	wg           sync.WaitGroup
}

// NewTransactionManager crea una nueva instancia del gestor de transacciones.
func NewTransactionManager(cm *CollectionManager) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		cm:           cm,
		gcQuitChan:   make(chan struct{}),
	}
}

// StartGC inicia el goroutine del recolector de basura.
func (tm *TransactionManager) StartGC(timeout, interval time.Duration) {
	tm.wg.Add(1)
	go tm.runGC(timeout, interval)
	slog.Info("Transaction garbage collector started", "timeout", timeout, "interval", interval)
}

// StopGC detiene el recolector de basura y espera a que termine.
func (tm *TransactionManager) StopGC() {
	close(tm.gcQuitChan)
	tm.wg.Wait()
	slog.Info("Transaction garbage collector stopped.")
}

// runGC es el bucle principal del recolector de basura.
func (tm *TransactionManager) runGC(timeout, interval time.Duration) {
	defer tm.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Debug("Running transaction garbage collection scan...")
			var txIDsToRollback []string
			tm.mu.RLock()
			for txID, tx := range tm.transactions {
				tx.mu.RLock()
				if tx.State == StateActive && time.Since(tx.startTime) > timeout {
					txIDsToRollback = append(txIDsToRollback, txID)
				}
				tx.mu.RUnlock()
			}
			tm.mu.RUnlock()
			if len(txIDsToRollback) > 0 {
				slog.Warn("Found abandoned transactions to roll back", "count", len(txIDsToRollback))
				for _, txID := range txIDsToRollback {
					slog.Info("Rolling back abandoned transaction", "txID", txID)
					if err := tm.Rollback(txID); err != nil {
						slog.Error("Error rolling back abandoned transaction", "txID", txID, "error", err)
					}
				}
			}
		case <-tm.gcQuitChan:
			return
		}
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

// Commit procesa el guardado final de la transacción.
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
	writeSetToProcess := tx.WriteSet
	tx.State = StatePreparing
	tx.mu.Unlock()

	// --- INICIO DE LA MODIFICACIÓN CLAVE: BARRIDO DE PRE-VALIDACIÓN ---
	slog.Debug("TransactionManager: starting pre-commit validation", "txID", txID)
	for _, op := range writeSetToProcess {
		col := tm.cm.GetCollection(op.Collection)
		_, keyExists := col.Get(op.Key)

		// Regla 1: Si la operación es un SET, la clave NO debe existir.
		if op.OpType == OpTypeSet && keyExists {
			slog.Warn("Commit failed: attempt to SET a key that already exists", "txID", txID, "key", op.Key)
			tm.Rollback(txID) // Deshacer la transacción
			return fmt.Errorf("commit failed: key '%s' in collection '%s' already exists. Use update instead", op.Key, op.Collection)
		}

		// Regla 2: Si la operación es un UPDATE o DELETE, la clave SÍ debe existir.
		if (op.OpType == OpTypeUpdate || op.OpType == OpTypeDelete) && !keyExists {
			slog.Warn("Commit failed: attempt to UPDATE/DELETE a key that does not exist", "txID", txID, "key", op.Key)
			tm.Rollback(txID) // Deshacer la transacción
			return fmt.Errorf("commit failed: key '%s' in collection '%s' does not exist to be updated or deleted", op.Key, op.Collection)
		}
	}
	slog.Debug("TransactionManager: pre-commit validation successful", "txID", txID)
	// --- FIN DE LA MODIFICACIÓN CLAVE ---

	tx.mu.Lock()
	tx.WriteSet = nil
	tx.mu.Unlock()

	slog.Debug("TransactionManager: enriching WriteSet with timestamps", "txID", txID)
	now := time.Now().UTC().Format(time.RFC3339)

	enrichedWriteSet := make([]WriteOperation, 0, len(writeSetToProcess))
	for _, op := range writeSetToProcess {
		if op.OpType == OpTypeDelete {
			enrichedWriteSet = append(enrichedWriteSet, op)
			continue
		}

		col := tm.cm.GetCollection(op.Collection)
		var data map[string]any
		if err := json.Unmarshal(op.Value, &data); err != nil {
			slog.Warn("Could not unmarshal value during commit, skipping enrichment", "key", op.Key)
			enrichedWriteSet = append(enrichedWriteSet, op)
			continue
		}

		data[globalconst.UPDATED_AT] = now
		if _, found := col.Get(op.Key); !found {
			data[globalconst.CREATED_AT] = now
		}

		enrichedValue, err := json.Marshal(data)
		if err != nil {
			slog.Error("Could not marshal enriched value during commit", "key", op.Key, "error", err)
			tm.Rollback(txID)
			return fmt.Errorf("failed to marshal enriched data for key %s: %w", op.Key, err)
		}

		op.Value = enrichedValue
		enrichedWriteSet = append(enrichedWriteSet, op)
	}

	slog.Debug("TransactionManager: entering Prepare Phase", "txID", txID, "op_count", len(enrichedWriteSet))
	opsByShard := make(map[*Shard][]WriteOperation)
	keysByShard := make(map[*Shard][]string)

	for _, op := range enrichedWriteSet {
		col := tm.cm.GetCollection(op.Collection).(*InMemStore)
		shard := col.getShard(op.Key)
		opsByShard[shard] = append(opsByShard[shard], op)
		keysByShard[shard] = append(keysByShard[shard], op.Key)
	}

	for shard, keys := range keysByShard {
		if err := shard.lockKeys(txID, keys); err != nil {
			slog.Warn("TransactionManager: lock failed during Prepare Phase, initiating rollback", "txID", txID, "error", err)
			tm.Rollback(txID)
			return fmt.Errorf("prepare failed: %w", err)
		}
	}

	for shard, ops := range opsByShard {
		for _, op := range ops {
			if err := shard.prepareWrite(txID, op); err != nil {
				slog.Warn("TransactionManager: prepareWrite failed, initiating rollback", "txID", txID, "error", err)
				tm.Rollback(txID)
				return fmt.Errorf("prepare failed: %w", err)
			}
		}
	}

	slog.Debug("TransactionManager: Prepare Phase successful. Entering Commit Phase.", "txID", txID)

	tx.mu.Lock()
	tx.State = StateCommitted
	tx.mu.Unlock()

	for shard := range keysByShard {
		var associatedIndexManager *IndexManager
		if len(opsByShard[shard]) > 0 {
			firstOp := opsByShard[shard][0]
			col := tm.cm.GetCollection(firstOp.Collection).(*InMemStore)
			associatedIndexManager = col.indexes
		}
		shard.commitAppliedChanges(txID, associatedIndexManager)
	}

	collectionsToSave := make(map[string]DataStore)
	for _, op := range enrichedWriteSet {
		if _, exists := collectionsToSave[op.Collection]; !exists {
			collectionsToSave[op.Collection] = tm.cm.GetCollection(op.Collection)
		}
	}

	for name, store := range collectionsToSave {
		tm.cm.EnqueueSaveTask(name, store)
	}

	tm.removeTransaction(txID)
	return nil
}

// Rollback revierte una transacción, descartando todos sus cambios.
func (tm *TransactionManager) Rollback(txID string) error {
	tx, err := tm.getTransaction(txID)
	if err != nil {
		return nil
	}

	tx.mu.Lock()
	if tx.State == StateCommitted || tx.State == StateAborted {
		tx.mu.Unlock()
		return nil
	}
	tx.State = StateAborted
	tx.mu.Unlock()

	slog.Debug("TransactionManager: rolling back transaction", "txID", txID)

	keysByShard := make(map[*Shard][]string)
	for _, op := range tx.WriteSet {
		col := tm.cm.GetCollection(op.Collection).(*InMemStore)
		shard := col.getShard(op.Key)
		keysByShard[shard] = append(keysByShard[shard], op.Key)
	}

	for shard := range keysByShard {
		shard.rollbackChanges(txID)
	}

	tm.removeTransaction(txID)
	return nil
}
