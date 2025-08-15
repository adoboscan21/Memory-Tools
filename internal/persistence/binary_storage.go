package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/globalconst"
	"memory-tools/internal/store"
	"os"
	"path/filepath"
	"time"

	jsoniter "github.com/json-iterator/go"
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

	slog.Info("Main data successfully saved", "path", mainDataFile, "item_count", len(data))
	return nil
}

// LoadData loads data from the main binary file and populates the InMemStore.
func LoadData(s store.DataStore) error {
	file, err := os.Open(mainDataFile)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("Main data file not found, initializing with empty data", "path", mainDataFile)
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
	slog.Info("Main data successfully loaded", "path", mainDataFile, "total_keys", len(loadedData))
	return nil
}

// SnapshotManager manages the scheduling and execution of data snapshots for the main InMemStore.
type SnapshotManager struct {
	Store            store.DataStore
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
		slog.Info("Main store snapshots are disabled.")
		return
	}

	slog.Info("Scheduled main store snapshots enabled", "interval", sm.Interval.String())
	ticker := time.NewTicker(sm.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Info("Performing scheduled main store snapshot...")
			if err := SaveData(sm.Store); err != nil {
				slog.Error("Error performing scheduled main store snapshot", "error", err)
			}
		case <-sm.Quit:
			slog.Info("Main store snapshot manager received quit signal. Stopping.")
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

// CollectionPersisterImpl implements the store.CollectionPersister interface.
type CollectionPersisterImpl struct{}

// SaveCollectionData saves all non-expired data from a single collection (DataStore) to a file.
func (p *CollectionPersisterImpl) SaveCollectionData(collectionName string, s store.DataStore) error {
	if err := os.MkdirAll(globalconst.CollectionsDirName, 0755); err != nil {
		return fmt.Errorf("failed to create collections directory '%s': %w", globalconst.CollectionsDirName, err)
	}

	data := s.GetAll()
	indexedFields := s.ListIndexes()

	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	tempFilePath := filePath + globalconst.TempFileSuffix

	file, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file for collection '%s': %w", collectionName, err)
	}
	defer file.Close()

	if err := binary.Write(file, binary.LittleEndian, uint32(len(indexedFields))); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write index count for collection '%s': %w", collectionName, err)
	}
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

	slog.Info("Collection data saved", "collection", collectionName, "path", filePath, "indexes", len(indexedFields), "items", len(data))
	return nil
}

// DeleteCollectionFile removes a collection's data file from disk.
func (p *CollectionPersisterImpl) DeleteCollectionFile(collectionName string) error {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			slog.Debug("Collection file does not exist, no need to delete", "path", filePath)
			return nil
		}
		return fmt.Errorf("failed to delete collection file '%s': %w", filePath, err)
	}
	slog.Info("Collection file deleted from disk", "path", filePath)
	return nil
}

// LoadCollectionData loads data for a single collection from its file.
func LoadCollectionData(collectionName string, s store.DataStore, hotThreshold time.Time) error {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("Collection file not found, initializing empty collection", "collection", collectionName, "path", filePath)
			return nil
		}
		return fmt.Errorf("failed to open collection file '%s': %w", filePath, err)
	}
	defer file.Close()

	var numIndexes uint32
	if err := binary.Read(file, binary.LittleEndian, &numIndexes); err != nil {
		slog.Warn("Could not read index header, assuming old file format", "collection", collectionName, "error", err)
		if _, seekErr := file.Seek(0, 0); seekErr != nil {
			return fmt.Errorf("failed to seek back to start of file for '%s': %w", collectionName, seekErr)
		}
		numIndexes = 0
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

	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries from collection '%s': %w", collectionName, err)
	}

	collectionData := make(map[string][]byte, numEntries)
	hotDataCount := 0
	coldDataCount := 0

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

		if !hotThreshold.IsZero() {
			var doc map[string]any
			if err := jsoniter.Unmarshal(valBytes, &doc); err == nil {
				if createdAtStr, ok := doc[globalconst.CREATED_AT].(string); ok {
					createdAt, err := time.Parse(time.RFC3339, createdAtStr)
					if err == nil && createdAt.Before(hotThreshold) {
						coldDataCount++
						continue
					}
				}
			}
		}

		collectionData[key] = valBytes
		hotDataCount++
	}

	s.LoadData(collectionData)
	slog.Info("Collection data loaded",
		"collection", collectionName,
		"path", filePath,
		"hot_items_in_ram", hotDataCount,
		"cold_items_on_disk", coldDataCount)

	if len(indexedFields) > 0 {
		slog.Info("Rebuilding indexes for hot data in collection", "collection", collectionName, "index_count", len(indexedFields))
		for _, field := range indexedFields {
			s.CreateIndex(field)
		}
		slog.Info("Finished rebuilding indexes for hot data", "collection", collectionName)
	}

	return nil
}

// ListCollectionFiles returns a list of all collection names found on disk.
func ListCollectionFiles() ([]string, error) {
	if _, err := os.Stat(globalconst.CollectionsDirName); os.IsNotExist(err) {
		return []string{}, nil
	}

	files, err := filepath.Glob(filepath.Join(globalconst.CollectionsDirName, "*"+globalconst.DBFileExtension))
	if err != nil {
		return nil, fmt.Errorf("failed to list collection files in '%s': %w", globalconst.CollectionsDirName, err)
	}

	names := make([]string, 0, len(files))
	for _, filePath := range files {
		baseName := filepath.Base(filePath)
		colName := baseName[:len(baseName)-len(globalconst.DBFileExtension)]
		names = append(names, colName)
	}
	slog.Info("Found collection files on disk", "count", len(names))
	return names, nil
}

// LoadAllCollectionsIntoManager loads all existing collections from disk into the CollectionManager.
func LoadAllCollectionsIntoManager(cm *store.CollectionManager, coldStorageMonths int) error {
	collectionNames, err := ListCollectionFiles()
	if err != nil {
		return fmt.Errorf("failed to get list of collection files: %w", err)
	}

	var hotThreshold time.Time
	if coldStorageMonths > 0 {
		hotThreshold = time.Now().AddDate(0, -coldStorageMonths, 0)
		slog.Info("Hot/Cold storage enabled", "hot_threshold", hotThreshold.Format(time.RFC3339))
	} else {
		slog.Info("Hot/Cold storage is disabled. All data will be loaded into RAM.")
	}

	for _, colName := range collectionNames {
		colStore := cm.GetCollection(colName)
		if err := LoadCollectionData(colName, colStore, hotThreshold); err != nil {
			slog.Warn("Failed to load data for collection, skipping", "collection", colName, "error", err)
			continue
		}
	}

	slog.Info("Finished loading all collections into manager.")
	return nil
}

// SaveAllCollectionsFromManager saves all currently active collections from the CollectionManager to disk.
func SaveAllCollectionsFromManager(cm *store.CollectionManager) error {
	activeCollections := cm.ListCollections()
	persister := &CollectionPersisterImpl{}

	activeMap := make(map[string]bool)
	var finalErr error

	// 1. Save all collections that are currently active in memory.
	for _, colName := range activeCollections {
		activeMap[colName] = true
		colStore := cm.GetCollection(colName)
		if err := persister.SaveCollectionData(colName, colStore); err != nil {
			slog.Error("Error saving collection during shutdown/checkpoint", "collection", colName, "error", err)
			finalErr = err
		}
	}

	// 2. Clean up files of collections that are no longer active (orphaned).
	slog.Debug("Checking for orphaned collection files to clean up...")
	existingFiles, err := ListCollectionFiles()
	if err != nil {
		slog.Warn("Failed to list existing collection files for cleanup", "error", err)
		return err
	}

	deletedCount := 0
	for _, fileName := range existingFiles {
		if _, isActive := activeMap[fileName]; !isActive {
			if err := persister.DeleteCollectionFile(fileName); err != nil {
				slog.Warn("Failed to remove orphaned collection file", "collection", fileName, "error", err)
				finalErr = err
			} else {
				slog.Info("Cleaned up orphaned collection file", "collection", fileName)
				deletedCount++
			}
		}
	}
	if deletedCount > 0 {
		slog.Info("Orphaned file cleanup complete", "deleted_count", deletedCount)
	}

	slog.Info("All active collections from manager successfully synchronized to disk.")
	return finalErr
}
