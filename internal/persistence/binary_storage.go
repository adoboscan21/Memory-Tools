// persistence/binary_storage.go
package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/store" // DataStore interface now expects []byte for values
	"os"
	"time"
)

const dataFile = "database.mtdb"
const snapshotTempFile = "database.mtdb.tmp"

// SaveData saves all data from the DataStore to a binary file.
// It now handles values as []byte (raw JSON).
func SaveData(s store.DataStore) error {
	data := s.GetAll() // data is now map[string][]byte

	file, err := os.Create(snapshotTempFile)
	if err != nil {
		return fmt.Errorf("failed to create temporary snapshot file '%s': %w", snapshotTempFile, err)
	}
	defer file.Close()

	// Write the total number of key-value entries.
	if err := binary.Write(file, binary.LittleEndian, uint32(len(data))); err != nil {
		os.Remove(snapshotTempFile)
		return fmt.Errorf("failed to write data count to temporary file: %w", err)
	}

	// Write each key-value pair. Value is now []byte.
	for key, value := range data {
		// Write key length and key bytes.
		if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write key length for '%s': %w", key, err)
		}
		if _, err := file.WriteString(key); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write key '%s': %w", key, err)
		}

		// Write value length (which is now the length of the JSON in bytes).
		if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write value length for '%s': %w", key, err)
		}
		// Write the value bytes (raw JSON).
		if _, err := file.Write(value); err != nil { // Use file.Write for []byte
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write value for '%s': %w", key, err)
		}
	}

	// Ensure all buffered writes are flushed to the disk.
	if err := file.Sync(); err != nil {
		os.Remove(snapshotTempFile)
		return fmt.Errorf("failed to sync temporary snapshot file to disk: %w", err)
	}
	file.Close()

	// Atomically rename the temporary file to the final data file.
	if err := os.Rename(snapshotTempFile, dataFile); err != nil {
		os.Remove(snapshotTempFile)
		return fmt.Errorf("failed to rename temporary snapshot file to '%s': %w", dataFile, err)
	}

	log.Printf("Data successfully saved to %s", dataFile)
	return nil
}

// LoadData loads data from a binary file and populates the InMemStore.
// It now reads values as []byte (raw JSON).
func LoadData(s *store.InMemStore) error {
	file, err := os.Open(dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Data file '%s' not found, initializing with empty data.", dataFile)
			return nil
		}
		return fmt.Errorf("failed to open data file '%s': %w", dataFile, err)
	}
	defer file.Close()

	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries from '%s': %w", dataFile, err)
	}

	loadedData := make(map[string][]byte, numEntries) // map[string][]byte for loaded values
	for i := 0; i < int(numEntries); i++ {
		// Read key length and key bytes.
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length for entry %d: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return fmt.Errorf("failed to read key for entry %d: %w", i, err)
		}
		key := string(keyBytes)

		// Read value length and value bytes (raw JSON).
		var valLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length for key '%s': %w", key, err)
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(file, valBytes); err != nil {
			return fmt.Errorf("failed to read value for key '%s': %w", key, err)
		}
		value := valBytes // Value is already []byte

		loadedData[key] = value
	}

	s.LoadData(loadedData)
	log.Printf("Data successfully loaded from %s. Total keys: %d", dataFile, len(loadedData))
	return nil
}

// SnapshotManager manages the scheduling and execution of data snapshots. (No changes needed)
type SnapshotManager struct {
	Store            store.DataStore
	Interval         time.Duration
	Quit             chan struct{}
	SnapshotsEnabled bool
}

func NewSnapshotManager(s store.DataStore, interval time.Duration, enabled bool) *SnapshotManager {
	return &SnapshotManager{
		Store:            s,
		Interval:         interval,
		Quit:             make(chan struct{}),
		SnapshotsEnabled: enabled,
	}
}

func (sm *SnapshotManager) Start() {
	if !sm.SnapshotsEnabled || sm.Interval <= 0 {
		log.Println("Snapshots are disabled or interval is invalid. Skipping scheduled snapshots.")
		return
	}

	log.Printf("Scheduled snapshots enabled every %s.", sm.Interval)
	ticker := time.NewTicker(sm.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("Performing scheduled snapshot...")
			if err := SaveData(sm.Store); err != nil {
				log.Printf("Error performing scheduled snapshot: %v", err)
			}
		case <-sm.Quit:
			log.Println("Snapshot manager received quit signal. Stopping.")
			return
		}
	}
}

func (sm *SnapshotManager) Stop() {
	if sm.SnapshotsEnabled {
		close(sm.Quit)
	}
}
