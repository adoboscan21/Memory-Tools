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
)

// PerformRestore performs a full restore from a specific backup directory.
// WARNING: This is a destructive operation that replaces all in-memory data.
func PerformRestore(backupName string, mainStore store.DataStore, colManager *store.CollectionManager) error {
	backupPath := filepath.Join(globalconst.BackupsDirName, backupName)
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory '%s' not found", backupName)
	}

	slog.Warn("--- STARTING RESTORE ---", "backup_name", backupName)

	if err := restoreMainStore(backupPath, mainStore); err != nil {
		return fmt.Errorf("failed to restore main store: %w", err)
	}

	if err := restoreCollections(backupPath, colManager); err != nil {
		return fmt.Errorf("failed to restore collections: %w", err)
	}

	slog.Info("--- RESTORE COMPLETED SUCCESSFULLY ---", "backup_name", backupName)
	return nil
}

// restoreMainStore loads the main store's data from its backup file.
func restoreMainStore(backupPath string, s store.DataStore) error {
	filePath := filepath.Join(backupPath, "in-memory.mtdb")
	slog.Info("Restoring main store...", "path", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Warn("Main store backup file not found, skipping.", "path", filePath)
			s.LoadData(make(map[string][]byte))
			return nil
		}
		return fmt.Errorf("failed to open main backup file '%s': %w", filePath, err)
	}
	defer file.Close()

	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read number of entries from main backup: %w", err)
	}

	loadedData := make(map[string][]byte, numEntries)
	for i := 0; i < int(numEntries); i++ {
		keyBytes, err := readLengthPrefixed(file)
		if err != nil {
			return fmt.Errorf("failed to read key for entry %d in main backup: %w", i, err)
		}
		key := string(keyBytes)

		valBytes, err := readLengthPrefixed(file)
		if err != nil {
			return fmt.Errorf("failed to read value for key '%s' in main backup: %w", key, err)
		}
		loadedData[key] = valBytes
	}

	s.LoadData(loadedData)
	slog.Info("Main store restored.", "key_count", len(loadedData))
	return nil
}

// restoreCollections loads all collections from the backup directory.
func restoreCollections(backupPath string, cm *store.CollectionManager) error {
	activeCollections := cm.ListCollections()
	for _, colName := range activeCollections {
		cm.DeleteCollection(colName)
	}
	slog.Info("Cleared all active in-memory collections before restore.")

	collectionsBackupDir := filepath.Join(backupPath, "collections")
	files, err := filepath.Glob(filepath.Join(collectionsBackupDir, "*"+globalconst.DBFileExtension))
	if err != nil {
		return fmt.Errorf("failed to list collection backup files in '%s': %w", collectionsBackupDir, err)
	}

	slog.Info("Found collection files in backup, starting restore...", "count", len(files))
	for _, filePath := range files {
		baseName := filepath.Base(filePath)
		colName := baseName[:len(baseName)-len(globalconst.DBFileExtension)]

		slog.Info("Restoring collection...", "collection", colName, "path", filePath)
		colStore := cm.GetCollection(colName)

		if err := loadCollectionDataFromBackup(filePath, colStore); err != nil {
			slog.Warn("Failed to restore collection, skipping.", "collection", colName, "error", err)
			cm.DeleteCollection(colName)
			continue
		}
	}

	return nil
}

// loadCollectionDataFromBackup loads a single collection and rebuilds its indexes.
func loadCollectionDataFromBackup(filePath string, s store.DataStore) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open collection backup file '%s': %w", filePath, err)
	}
	defer file.Close()

	var numIndexes uint32
	if err := binary.Read(file, binary.LittleEndian, &numIndexes); err != nil {
		return fmt.Errorf("failed to read index count from '%s': %w", filePath, err)
	}

	indexedFields := make([]string, numIndexes)
	for i := 0; i < int(numIndexes); i++ {
		fieldBytes, err := readLengthPrefixed(file)
		if err != nil {
			return fmt.Errorf("failed to read index field name from '%s': %w", filePath, err)
		}
		indexedFields[i] = string(fieldBytes)
	}

	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("failed to read entry count from '%s': %w", filePath, err)
	}

	collectionData := make(map[string][]byte, numEntries)
	for i := 0; i < int(numEntries); i++ {
		keyBytes, err := readLengthPrefixed(file)
		if err != nil {
			return fmt.Errorf("failed to read key for entry %d in '%s': %w", i, filePath, err)
		}
		key := string(keyBytes)

		valBytes, err := readLengthPrefixed(file)
		if err != nil {
			return fmt.Errorf("failed to read value for key '%s' in '%s': %w", key, filePath, err)
		}
		collectionData[key] = valBytes
	}

	s.LoadData(collectionData)
	slog.Info("Collection data loaded.", "key_count", len(collectionData))

	if len(indexedFields) > 0 {
		slog.Info("Rebuilding indexes...", "index_count", len(indexedFields))
		for _, field := range indexedFields {
			s.CreateIndex(field)
		}
		slog.Info("Finished rebuilding indexes.")
	}

	return nil
}

// readLengthPrefixed is a helper function to read length-prefixed data.
func readLengthPrefixed(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}
