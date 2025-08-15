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

// WalEntry represents a single operation recorded in the log.
type WalEntry struct {
	CommandType protocol.CommandType
	Payload     []byte
}

// WAL (Write-Ahead Log) manages the writing and reading of the durability log.
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
}

// New creates and initializes a new WAL instance at the specified path.
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

// Write writes a log entry to the file synchronously.
// This is the critical operation that ensures durability.
func (w *WAL) Write(entry WalEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadLen := len(entry.Payload)
	// Format: [Total Length (4 bytes)] [Command Type (1 byte)] [Payload]
	totalLen := 1 + payloadLen

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(totalLen)); err != nil {
		return fmt.Errorf("failed to write WAL entry length: %w", err)
	}

	if err := w.writer.WriteByte(byte(entry.CommandType)); err != nil {
		return fmt.Errorf("failed to write WAL command type: %w", err)
	}

	if _, err := w.writer.Write(entry.Payload); err != nil {
		return fmt.Errorf("failed to write WAL payload: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL writer: %w", err)
	}

	return w.file.Sync()
}

// Close closes the WAL file safely.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to flush WAL on close: %w", err)
	}
	return w.file.Close()
}

// Replay reads all entries from the WAL file and sends them to a channel.
// This function is used during startup to recover state.
func Replay(path string) (<-chan WalEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("WAL file not found, skipping replay.", "path", path)
			closeChan := make(chan WalEntry)
			close(closeChan)
			return closeChan, nil
		}
		return nil, fmt.Errorf("failed to open WAL file for replay: %w", err)
	}

	entriesChan := make(chan WalEntry, 100)

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
				break
			}

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

// Path returns the file path of the WAL.
func (w *WAL) Path() string {
	return w.path
}

// Rotate closes the current WAL file, deletes it, and opens a new one in its place.
func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to flush WAL before rotation: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close current WAL file for rotation: %w", err)
	}

	if err := os.Remove(w.path); err != nil {
		return fmt.Errorf("failed to remove old WAL file: %w", err)
	}

	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new WAL file after rotation: %w", err)
	}

	w.file = file
	w.writer.Reset(file)

	slog.Info("WAL file rotated successfully.")
	return nil
}
