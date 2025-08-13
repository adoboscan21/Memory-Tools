// internal/wal/wal.go
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/protocol"
	"os"
	"sync"
)

// WalEntry representa una única operación registrada en el log.
type WalEntry struct {
	CommandType protocol.CommandType
	Payload     []byte
}

// WAL (Write-Ahead Log) gestiona la escritura y lectura del log de durabilidad.
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
}

// New crea e inicializa una nueva instancia del WAL en la ruta especificada.
func New(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   path,
	}, nil
}

// Write escribe una entrada de log al archivo de forma síncrona.
// Esta es la operación crítica que garantiza la durabilidad.
func (w *WAL) Write(entry WalEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadLen := len(entry.Payload)
	// Formato: [Longitud Total (4 bytes)] [Tipo de Comando (1 byte)] [Payload]
	totalLen := 1 + payloadLen

	// Escribimos la longitud total de la entrada (sin incluir estos 4 bytes).
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(totalLen)); err != nil {
		return fmt.Errorf("failed to write WAL entry length: %w", err)
	}

	// Escribimos el tipo de comando.
	if err := w.writer.WriteByte(byte(entry.CommandType)); err != nil {
		return fmt.Errorf("failed to write WAL command type: %w", err)
	}

	// Escribimos el payload.
	if _, err := w.writer.Write(entry.Payload); err != nil {
		return fmt.Errorf("failed to write WAL payload: %w", err)
	}

	// Flush fuerza la escritura desde el buffer al sistema operativo.
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL writer: %w", err)
	}

	// Sync garantiza que los datos se escriban físicamente en el disco.
	// ¡Este es el paso que nos da la durabilidad!
	return w.file.Sync()
}

// Close cierra el archivo del WAL de forma segura.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		w.file.Close() // Intentamos cerrar incluso si el flush falla.
		return fmt.Errorf("failed to flush WAL on close: %w", err)
	}
	return w.file.Close()
}

// Replay lee todas las entradas del archivo WAL y las envía a un canal.
// Esta función se usa durante el arranque para recuperar el estado.
func Replay(path string) (<-chan WalEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		// Si el archivo no existe, no hay nada que reproducir, lo cual es normal.
		if os.IsNotExist(err) {
			slog.Info("WAL file not found, skipping replay.", "path", path)
			closeChan := make(chan WalEntry)
			close(closeChan)
			return closeChan, nil
		}
		return nil, fmt.Errorf("failed to open WAL file for replay: %w", err)
	}

	entriesChan := make(chan WalEntry, 100) // Un buffer para no bloquear.

	go func() {
		defer file.Close()
		defer close(entriesChan)

		reader := bufio.NewReader(file)
		for {
			var totalLen uint32
			if err := binary.Read(reader, binary.LittleEndian, &totalLen); err != nil {
				if err != io.EOF {
					slog.Error("Failed to read WAL entry length during replay", "error", err)
				}
				break // Fin del archivo o error.
			}

			// Leemos la entrada completa en un buffer.
			entryData := make([]byte, totalLen)
			if _, err := io.ReadFull(reader, entryData); err != nil {
				slog.Error("Failed to read full WAL entry during replay", "error", err)
				break
			}

			entry := WalEntry{
				CommandType: protocol.CommandType(entryData[0]),
				Payload:     entryData[1:],
			}
			entriesChan <- entry
		}
		slog.Info("WAL replay finished.", "path", path)
	}()

	return entriesChan, nil
}

func (w *WAL) Path() string {
	return w.path
}

func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Asegurarse de que todo lo que está en el buffer se escriba antes de cerrar.
	if err := w.writer.Flush(); err != nil {
		w.file.Close() // Intentar cerrar de todos modos
		return fmt.Errorf("failed to flush WAL before rotation: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close current WAL file for rotation: %w", err)
	}

	// Eliminar el archivo de log antiguo.
	if err := os.Remove(w.path); err != nil {
		return fmt.Errorf("failed to remove old WAL file: %w", err)
	}

	// Abrir un nuevo archivo de log en la misma ruta.
	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new WAL file after rotation: %w", err)
	}

	w.file = file
	w.writer.Reset(file) // Apuntar el writer al nuevo archivo.

	slog.Info("WAL file rotated successfully.")
	return nil
}
