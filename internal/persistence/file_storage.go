package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/store"
	"os"
	"time"
)

// dataFile is the name of the file for persistent binary data.
const dataFile = "data.mtdb"

// snapshotTempFile is a temporary file used during safe data saving.
// This ensures data integrity in case of a crash during write operations.
const snapshotTempFile = "data.mtdb.tmp"

// SaveData saves all data from the DataStore to a binary file.
// It uses a temporary file and an atomic rename to ensure data integrity.
func SaveData(s store.DataStore) error {
	// Get a copy of all data from the store. This allows the store to
	// continue serving requests while data is being serialized.
	data := s.GetAll()

	// Create a temporary file for writing.
	file, err := os.Create(snapshotTempFile)
	if err != nil {
		return fmt.Errorf("failed to create temporary snapshot file '%s': %w", snapshotTempFile, err)
	}
	// Ensure the temporary file is closed, regardless of success or failure.
	defer file.Close()

	// Write the total number of key-value entries as a uint32.
	// We use binary.LittleEndian, a common byte order for x86/x64 architectures.
	if err := binary.Write(file, binary.LittleEndian, uint32(len(data))); err != nil {
		// Clean up the temporary file if the initial write fails.
		os.Remove(snapshotTempFile)
		return fmt.Errorf("failed to write data count to temporary file: %w", err)
	}

	// Iterate over each key-value pair and write them to the file.
	for key, value := range data {
		// Write the length of the key (as uint32) before the key itself.
		if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write key length for '%s': %w", key, err)
		}
		// Write the key bytes.
		if _, err := file.WriteString(key); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write key '%s': %w", key, err)
		}

		// Write the length of the value (as uint32) before the value itself.
		if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write value length for '%s': %w", key, err)
		}
		// Write the value bytes.
		if _, err := file.WriteString(value); err != nil {
			os.Remove(snapshotTempFile)
			return fmt.Errorf("failed to write value for '%s': %w", key, err)
		}
	}

	// Ensure all buffered writes are flushed to the disk.
	if err := file.Sync(); err != nil {
		os.Remove(snapshotTempFile)
		return fmt.Errorf("failed to sync temporary snapshot file to disk: %w", err)
	}
	// Explicitly close the file before renaming, especially important on Windows.
	file.Close()

	// Atomically rename the temporary file to the final data file.
	// This ensures that the main data file is always a complete and valid snapshot.
	if err := os.Rename(snapshotTempFile, dataFile); err != nil {
		// Try to clean up the temporary file if the rename fails.
		os.Remove(snapshotTempFile)
		return fmt.Errorf("failed to rename temporary snapshot file to '%s': %w", dataFile, err)
	}

	log.Printf("Data successfully saved to %s", dataFile)
	return nil
}

// LoadData loads data from a binary file and populates the InMemStore.
func LoadData(s *store.InMemStore) error {
	// Open the data file.
	file, err := os.Open(dataFile)
	if err != nil {
		// If the file does not exist, it's not a critical error;
		// it simply means we start with an empty store.
		if os.IsNotExist(err) {
			log.Printf("Data file '%s' not found, initializing with empty data.", dataFile)
			return nil
		}
		// For other errors, return them wrapped for context.
		return fmt.Errorf("failed to open data file '%s': %w", dataFile, err)
	}
	// Ensure the file is closed after reading.
	defer file.Close()

	var numEntries uint32
	// Read the total number of entries from the beginning of the file.
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries from '%s': %w", dataFile, err)
	}

	// Initialize a map with a pre-allocated capacity for efficiency.
	loadedData := make(map[string]string, numEntries)
	// Read each key-value pair based on the number of entries.
	for i := 0; i < int(numEntries); i++ {
		var keyLen uint32
		// Read the length of the key.
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length for entry %d: %w", i, err)
		}
		// Create a byte slice of the appropriate size and read the key bytes.
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return fmt.Errorf("failed to read key for entry %d: %w", i, err)
		}
		key := string(keyBytes) // Convert byte slice to string.

		var valLen uint32
		// Read the length of the value.
		if err := binary.Read(file, binary.LittleEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length for key '%s': %w", key, err)
		}
		// Create a byte slice for the value and read its bytes.
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(file, valBytes); err != nil {
			return fmt.Errorf("failed to read value for key '%s': %w", key, err)
		}
		value := string(valBytes) // Convert byte slice to string.

		// Store the loaded key-value pair.
		loadedData[key] = value
	}

	// Load the deserialized data into the in-memory store instance.
	s.LoadData(loadedData)
	log.Printf("Data successfully loaded from %s. Total keys: %d", dataFile, len(loadedData))
	return nil
}

// SnapshotManager manages the scheduling and execution of data snapshots.
type SnapshotManager struct {
	Store            store.DataStore // The data store to snapshot.
	Interval         time.Duration   // The frequency at which snapshots should be taken.
	Quit             chan struct{}   // Channel to signal the manager to stop.
	SnapshotsEnabled bool            // Flag to enable/disable scheduled snapshots.
}

// NewSnapshotManager creates and returns a new instance of SnapshotManager.
func NewSnapshotManager(s store.DataStore, interval time.Duration, enabled bool) *SnapshotManager {
	return &SnapshotManager{
		Store:            s,
		Interval:         interval,
		Quit:             make(chan struct{}),
		SnapshotsEnabled: enabled,
	}
}

// Start begins the scheduled snapshot process.
// It runs in a separate goroutine and takes snapshots at the configured interval.
func (sm *SnapshotManager) Start() {
	// If snapshots are disabled or the interval is invalid, log and exit.
	if !sm.SnapshotsEnabled || sm.Interval <= 0 {
		log.Println("Snapshots are disabled or interval is invalid. Skipping scheduled snapshots.")
		return
	}

	log.Printf("Scheduled snapshots enabled every %s.", sm.Interval)
	// Create a new ticker that sends events on a channel at the specified interval.
	ticker := time.NewTicker(sm.Interval)
	// Ensure the ticker is stopped when the goroutine exits to prevent resource leaks.
	defer ticker.Stop()

	// Infinite loop to continuously perform snapshots or wait for a quit signal.
	for {
		select {
		// When the ticker fires, perform a snapshot.
		case <-ticker.C:
			log.Println("Performing scheduled snapshot...")
			if err := SaveData(sm.Store); err != nil {
				log.Printf("Error performing scheduled snapshot: %v", err)
			}
		// When the quit channel receives a signal, stop the manager.
		case <-sm.Quit:
			log.Println("Snapshot manager received quit signal. Stopping.")
			return
		}
	}
}

// Stop signals the SnapshotManager to cease scheduled snapshot operations.
func (sm *SnapshotManager) Stop() {
	// Only attempt to close the quit channel if snapshots were enabled,
	// to avoid sending on a nil or already closed channel.
	if sm.SnapshotsEnabled {
		close(sm.Quit)
	}
}
