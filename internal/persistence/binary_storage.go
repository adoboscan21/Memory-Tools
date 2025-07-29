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
func (p *CollectionPersisterImpl) SaveCollectionData(collectionName string, s store.DataStore) error {
	// Ensure the collections directory exists.
	if err := os.MkdirAll(collectionsDir, 0755); err != nil {
		return fmt.Errorf("failed to create collections directory '%s': %w", collectionsDir, err)
	}

	data := s.GetAll()
	filePath := filepath.Join(collectionsDir, collectionName+collectionFileExtension)
	tempFilePath := filePath + ".tmp"

	file, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file for collection '%s': %w", collectionName, err)
	}
	defer file.Close()

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

	log.Printf("Collection '%s' successfully saved to %s", collectionName, filePath)
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

	s.LoadData(collectionData) // Load data into the provided DataStore (InMemStore).
	log.Printf("Collection '%s' successfully loaded from %s. Total keys: %d", collectionName, filePath, len(collectionData))
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

	// Temporary map to load data before passing to the manager.
	allLoadedCollectionsData := make(map[string]map[string][]byte)

	for _, colName := range collectionNames {
		tempStore := store.NewInMemStore() // Create a temporary InMemStore to load data.
		if err := LoadCollectionData(colName, tempStore); err != nil {
			log.Printf("Warning: Failed to load data for collection '%s': %v", colName, err)
			continue // Continue with the next collection even if this one fails.
		}
		allLoadedCollectionsData[colName] = tempStore.GetAll() // Get loaded data from tempStore.
	}

	// Pass all loaded data to the CollectionManager.
	cm.LoadAllCollectionData(allLoadedCollectionsData)

	log.Printf("Finished attempting to load data for %d collections into CollectionManager.", len(collectionNames))
	return nil
}

// SaveAllCollectionsFromManager saves all currently active collections from the CollectionManager to disk.
func SaveAllCollectionsFromManager(cm *store.CollectionManager) error {
	dataToSave := cm.GetAllCollectionsDataForPersistence()

	// Get a list of existing collection files on disk to detect and remove old ones.
	existingFiles, err := filepath.Glob(filepath.Join(collectionsDir, "*"+collectionFileExtension))
	if err != nil {
		log.Printf("Warning: Failed to list existing collection files for cleanup: %v", err)
	}
	existingFileMap := make(map[string]bool)
	for _, f := range existingFiles {
		baseName := filepath.Base(f)
		colName := baseName[:len(baseName)-len(collectionFileExtension)]
		existingFileMap[colName] = true
	}

	// Create an instance of the persister to use its methods.
	persister := &CollectionPersisterImpl{}

	for colName, colData := range dataToSave {
		// Create a tempStore and load collection data for saving.
		tempStore := store.NewInMemStore() // Create a new InMemStore instance.
		tempStore.LoadData(colData)        // Load the specific collection data into this temp store.

		if err := persister.SaveCollectionData(colName, tempStore); err != nil { // Use the persister instance.
			log.Printf("Error saving collection '%s': %v", colName, err)
		}
		delete(existingFileMap, colName) // Mark this file as saved, don't delete it.
	}

	// Remove any collection files that no longer exist in memory.
	for colName := range existingFileMap {
		if err := persister.DeleteCollectionFile(colName); err != nil { // Use the persister instance.
			log.Printf("Warning: Failed to remove old collection file for '%s': %v", colName, err)
		}
	}

	log.Printf("All active collections from manager successfully saved to disk.")
	return nil
}
