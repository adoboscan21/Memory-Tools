package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/store"
	"os"
	"path/filepath"
	"time"
)

// Constants for main data persistence.
const mainDataFile = "in-memory.mtdb"
const mainSnapshotTempFile = "in-memory.mtdb.tmp"

// SaveData saves all non-expired data from the main DataStore to a binary file.
func SaveData(s store.DataStore) error {
	data := s.GetAll()

	file, err := os.Create(mainSnapshotTempFile)
	if err != nil {
		return fmt.Errorf("failed to create temporary main snapshot file '%s': %w", mainSnapshotTempFile, err)
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, uint32(len(data))); err != nil {
		os.Remove(mainSnapshotTempFile)
		return fmt.Errorf("failed to write data count to temporary main file: %w", err)
	}

	for key, value := range data {
		if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
			os.Remove(mainSnapshotTempFile)
			return fmt.Errorf("failed to write key length for '%s' in main store: %w", key, err)
		}
		if _, err := file.WriteString(key); err != nil {
			os.Remove(mainSnapshotTempFile)
			return fmt.Errorf("failed to write key '%s' in main store: %w", key, err)
		}
		if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
			os.Remove(mainSnapshotTempFile)
			return fmt.Errorf("failed to write value length for '%s' in main store: %w", key, err)
		}
		if _, err := file.Write(value); err != nil {
			os.Remove(mainSnapshotTempFile)
			return fmt.Errorf("failed to write value for '%s' in main store: %w", key, err)
		}
	}

	if err := file.Sync(); err != nil {
		os.Remove(mainSnapshotTempFile)
		return fmt.Errorf("failed to sync temporary main snapshot file to disk: %w", err)
	}
	file.Close()

	if err := os.Rename(mainSnapshotTempFile, mainDataFile); err != nil {
		os.Remove(mainSnapshotTempFile)
		return fmt.Errorf("failed to rename temporary main snapshot file to '%s': %w", mainDataFile, err)
	}

	log.Printf("Main data successfully saved to %s", mainDataFile)
	return nil
}

// LoadData loads data from the main binary file and populates the InMemStore.
func LoadData(s store.DataStore) error {
	file, err := os.Open(mainDataFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Main data file '%s' not found, initializing with empty data.", mainDataFile)
			return nil
		}
		return fmt.Errorf("failed to open main data file '%s': %w", mainDataFile, err)
	}
	defer file.Close()

	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries from '%s': %w", mainDataFile, err)
	}

	loadedData := make(map[string][]byte, numEntries)
	for i := 0; i < int(numEntries); i++ {
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length for entry %d in main store: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return fmt.Errorf("failed to read key for entry %d in main store: %w", i, err)
		}
		key := string(keyBytes)

		var valLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length for key '%s' in main store: %w", key, err)
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(file, valBytes); err != nil {
			return fmt.Errorf("failed to read value for key '%s' in main store: %w", key, err)
		}
		value := valBytes

		loadedData[key] = value
	}

	s.LoadData(loadedData)
	log.Printf("Main data successfully loaded from %s. Total keys: %d", mainDataFile, len(loadedData))
	return nil
}

// SnapshotManager manages the scheduling and execution of data snapshots for the main InMemStore.
type SnapshotManager struct {
	Store            store.DataStore // Refers to the main DataStore.
	Interval         time.Duration
	Quit             chan struct{}
	SnapshotsEnabled bool
}

// NewSnapshotManager creates a new instance of SnapshotManager for the main store.
func NewSnapshotManager(s store.DataStore, interval time.Duration, enabled bool) *SnapshotManager {
	return &SnapshotManager{
		Store:            s,
		Interval:         interval,
		Quit:             make(chan struct{}),
		SnapshotsEnabled: enabled,
	}
}

// Start begins the scheduled snapshot process for the main store.
func (sm *SnapshotManager) Start() {
	if !sm.SnapshotsEnabled || sm.Interval <= 0 {
		log.Println("Main store snapshots are disabled or interval is invalid. Skipping scheduled snapshots.")
		return
	}

	log.Printf("Scheduled main store snapshots enabled every %s.", sm.Interval)
	ticker := time.NewTicker(sm.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("Performing scheduled main store snapshot...")
			if err := SaveData(sm.Store); err != nil {
				log.Printf("Error performing scheduled main store snapshot: %v", err)
			}
		case <-sm.Quit:
			log.Println("Main store snapshot manager received quit signal. Stopping.")
			return
		}
	}
}

// Stop signals the SnapshotManager to cease scheduled snapshot operations for the main store.
func (sm *SnapshotManager) Stop() {
	if sm.SnapshotsEnabled {
		close(sm.Quit)
	}
}

// Constants for collections persistence.
const collectionsDir = "collections"
const collectionFileExtension = ".mtdb"

// CollectionPersisterImpl implements the store.CollectionPersister interface.
type CollectionPersisterImpl struct{}

// SaveCollectionData saves all non-expired data from a single collection (DataStore) to a file.
// MODIFIED: Now also saves index metadata.
func (p *CollectionPersisterImpl) SaveCollectionData(collectionName string, s store.DataStore) error {
	// Ensure the collections directory exists.
	if err := os.MkdirAll(collectionsDir, 0755); err != nil {
		return fmt.Errorf("failed to create collections directory '%s': %w", collectionsDir, err)
	}

	data := s.GetAll()
	indexedFields := s.ListIndexes() // Get the list of indexed fields.

	filePath := filepath.Join(collectionsDir, collectionName+collectionFileExtension)
	tempFilePath := filePath + ".tmp"

	file, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file for collection '%s': %w", collectionName, err)
	}
	defer file.Close()

	// --- NEW: Write index metadata header ---
	// 1. Write number of indexes.
	if err := binary.Write(file, binary.LittleEndian, uint32(len(indexedFields))); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write index count for collection '%s': %w", collectionName, err)
	}
	// 2. Write each indexed field name.
	for _, field := range indexedFields {
		if err := binary.Write(file, binary.LittleEndian, uint32(len(field))); err != nil {
			os.Remove(tempFilePath)
			return fmt.Errorf("failed to write index field name length for '%s': %w", field, err)
		}
		if _, err := file.WriteString(field); err != nil {
			os.Remove(tempFilePath)
			return fmt.Errorf("failed to write index field name '%s': %w", field, err)
		}
	}
	// --- END NEW ---

	// Write data (as before).
	if err := binary.Write(file, binary.LittleEndian, uint32(len(data))); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write data count for collection '%s': %w", collectionName, err)
	}

	for key, value := range data {
		if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
			file.Close()
			os.Remove(tempFilePath)
			return fmt.Errorf("failed to write key length for '%s' in collection '%s': %w", key, collectionName, err)
		}
		if _, err := file.WriteString(key); err != nil {
			file.Close()
			os.Remove(tempFilePath)
			return fmt.Errorf("failed to write key '%s' in collection '%s': %w", key, collectionName, err)
		}
		if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
			file.Close()
			os.Remove(tempFilePath)
			return fmt.Errorf("failed to write value length for '%s' in collection '%s': %w", key, collectionName, err)
		}
		if _, err := file.Write(value); err != nil {
			file.Close()
			os.Remove(tempFilePath)
			return fmt.Errorf("failed to write value for '%s' in collection '%s': %w", key, collectionName, err)
		}
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to sync temporary file for collection '%s' to disk: %w", collectionName, err)
	}
	file.Close()

	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary file to '%s' for collection '%s': %w", filePath, collectionName, err)
	}

	log.Printf("Collection '%s' with %d indexes successfully saved to %s", collectionName, len(indexedFields), filePath)
	return nil
}

// DeleteCollectionFile removes a collection's data file from disk.
func (p *CollectionPersisterImpl) DeleteCollectionFile(collectionName string) error {
	filePath := filepath.Join(collectionsDir, collectionName+collectionFileExtension)
	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			log.Printf("Collection file '%s' does not exist, no need to delete.", filePath)
			return nil
		}
		return fmt.Errorf("failed to delete collection file '%s': %w", filePath, err)
	}
	log.Printf("Collection file '%s' successfully deleted from disk.", filePath)
	return nil
}

// LoadCollectionData loads data for a single collection from its file.
// MODIFIED: Now also loads index metadata and rebuilds indexes.
func LoadCollectionData(collectionName string, s store.DataStore) error {
	filePath := filepath.Join(collectionsDir, collectionName+collectionFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Collection file '%s' for '%s' not found, initializing with empty data.", filePath, collectionName)
			return nil
		}
		return fmt.Errorf("failed to open collection file '%s': %w", filePath, err)
	}
	defer file.Close()

	// --- NEW: Read index metadata header ---
	var numIndexes uint32
	if err := binary.Read(file, binary.LittleEndian, &numIndexes); err != nil {
		// For backward compatibility, if this read fails, assume old format with no indexes.
		log.Printf("Warning: could not read index header for collection '%s'. Assuming old file format. Error: %v", collectionName, err)
		// Reset file pointer to the beginning.
		if _, seekErr := file.Seek(0, 0); seekErr != nil {
			return fmt.Errorf("failed to seek back to start of file for '%s': %w", collectionName, seekErr)
		}
		numIndexes = 0 // Proceed as if there were no indexes.
	}

	indexedFields := make([]string, numIndexes)
	for i := 0; i < int(numIndexes); i++ {
		var fieldLen uint32
		if err := binary.Read(file, binary.LittleEndian, &fieldLen); err != nil {
			return fmt.Errorf("failed to read index field length for collection '%s': %w", collectionName, err)
		}
		fieldBytes := make([]byte, fieldLen)
		if _, err := io.ReadFull(file, fieldBytes); err != nil {
			return fmt.Errorf("failed to read index field name for collection '%s': %w", collectionName, err)
		}
		indexedFields[i] = string(fieldBytes)
	}
	// --- END NEW ---

	// Read data (as before).
	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries from collection '%s': %w", collectionName, err)
	}

	collectionData := make(map[string][]byte, numEntries)
	for i := 0; i < int(numEntries); i++ {
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length for entry %d in collection '%s': %w", i, collectionName, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return fmt.Errorf("failed to read key for entry %d in collection '%s': %w", i, collectionName, err)
		}
		key := string(keyBytes)

		var valLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length for key '%s' in collection '%s': %w", key, collectionName, err)
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(file, valBytes); err != nil {
			return fmt.Errorf("failed to read value for key '%s' in collection '%s': %w", key, collectionName, err)
		}
		value := valBytes
		collectionData[key] = value
	}

	s.LoadData(collectionData)
	log.Printf("Collection '%s' successfully loaded from %s. Total keys: %d", collectionName, filePath, len(collectionData))

	// --- NEW: Rebuild indexes based on loaded metadata ---
	if len(indexedFields) > 0 {
		log.Printf("Rebuilding %d indexes for collection '%s'...", len(indexedFields), collectionName)
		for _, field := range indexedFields {
			s.CreateIndex(field)
		}
		log.Printf("Finished rebuilding indexes for collection '%s'.", collectionName)
	}
	// --- END NEW ---

	return nil
}

// ListCollectionFiles returns a list of all collection names found on disk.
func ListCollectionFiles() ([]string, error) {
	if _, err := os.Stat(collectionsDir); os.IsNotExist(err) {
		return []string{}, nil // Directory doesn't exist, no collections.
	}

	files, err := filepath.Glob(filepath.Join(collectionsDir, "*"+collectionFileExtension))
	if err != nil {
		return nil, fmt.Errorf("failed to list collection files in '%s': %w", collectionsDir, err)
	}

	names := make([]string, 0, len(files))
	for _, filePath := range files {
		baseName := filepath.Base(filePath)
		colName := baseName[:len(baseName)-len(collectionFileExtension)]
		names = append(names, colName)
	}
	log.Printf("Found %d collection files on disk.", len(names))
	return names, nil
}

// LoadAllCollectionsIntoManager loads all existing collections from disk into the CollectionManager.
func LoadAllCollectionsIntoManager(cm *store.CollectionManager) error {
	collectionNames, err := ListCollectionFiles()
	if err != nil {
		return fmt.Errorf("failed to get list of collection files: %w", err)
	}

	for _, colName := range collectionNames {
		// GetCollection will create a new, empty InMemStore instance.
		colStore := cm.GetCollection(colName)
		// LoadCollectionData will populate it with data AND rebuild its indexes.
		if err := LoadCollectionData(colName, colStore); err != nil {
			log.Printf("Warning: Failed to load data for collection '%s': %v", colName, err)
			continue // Continue with the next collection even if this one fails.
		}
	}

	log.Printf("Finished attempting to load all collections into CollectionManager.")
	return nil
}

// SaveAllCollectionsFromManager saves all currently active collections from the CollectionManager to disk.
func SaveAllCollectionsFromManager(cm *store.CollectionManager) error {
	cm.GetAllCollectionsDataForPersistence() // This is map[string]map[string][]byte

	persister := &CollectionPersisterImpl{}

	// We need to get the store object itself to access the indexes, not just the data.
	// This approach is a bit tricky. A better way would be for the collection manager
	// to expose the stores directly. For now, we'll iterate through the known collections.

	activeCollections := cm.ListCollections()

	for _, colName := range activeCollections {
		colStore := cm.GetCollection(colName)
		if err := persister.SaveCollectionData(colName, colStore); err != nil {
			log.Printf("Error saving collection '%s': %v", colName, err)
		}
	}

	// This logic might need adjustment for deleting collections that are no longer active.
	// We'll rely on the existing logic that gets all collections from the manager.
	existingFiles, err := filepath.Glob(filepath.Join(collectionsDir, "*"+collectionFileExtension))
	if err != nil {
		log.Printf("Warning: Failed to list existing collection files for cleanup: %v", err)
	}
	activeFileMap := make(map[string]bool)
	for _, f := range activeCollections {
		activeFileMap[f] = true
	}

	for _, f := range existingFiles {
		baseName := filepath.Base(f)
		colName := baseName[:len(baseName)-len(collectionFileExtension)]
		if !activeFileMap[colName] {
			if err := persister.DeleteCollectionFile(colName); err != nil {
				log.Printf("Warning: Failed to remove old collection file for '%s': %v", colName, err)
			}
		}
	}

	log.Printf("All active collections from manager successfully saved to disk.")
	return nil
}
