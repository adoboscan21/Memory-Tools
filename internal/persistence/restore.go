package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/store"
	"os"
	"path/filepath"
)

// PerformRestore realiza una restauración completa desde un directorio de backup específico.
// ADVERTENCIA: Esta es una operación destructiva que reemplaza todos los datos en memoria.
func PerformRestore(backupName string, mainStore store.DataStore, colManager *store.CollectionManager) error {
	backupPath := filepath.Join(backupDir, backupName)
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory '%s' not found", backupName)
	}

	log.Printf("--- INICIANDO RESTAURACIÓN DESDE '%s' ---", backupName)

	// Restaurar el Almacén Principal
	if err := restoreMainStore(backupPath, mainStore); err != nil {
		return fmt.Errorf("failed to restore main store: %w", err)
	}

	// Restaurar las Colecciones
	if err := restoreCollections(backupPath, colManager); err != nil {
		return fmt.Errorf("failed to restore collections: %w", err)
	}

	log.Printf("--- RESTAURACIÓN DESDE '%s' COMPLETADA EXITOSAMENTE ---", backupName)
	return nil
}

// restoreMainStore carga los datos del almacén principal desde su archivo de backup.
func restoreMainStore(backupPath string, s store.DataStore) error {
	filePath := filepath.Join(backupPath, "in-memory.mtdb")
	log.Printf("Restoring main store from '%s'...", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Main store backup file not found in '%s'. Skipping.", backupPath)
			s.LoadData(make(map[string][]byte)) // Carga datos vacíos
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
	log.Printf("Main store restored with %d keys.", len(loadedData))
	return nil
}

// restoreCollections carga todas las colecciones desde el directorio de backup.
func restoreCollections(backupPath string, cm *store.CollectionManager) error {
	// 1. Limpiar todas las colecciones existentes en memoria para evitar conflictos
	activeCollections := cm.ListCollections()
	for _, colName := range activeCollections {
		cm.DeleteCollection(colName)
	}
	log.Println("Cleared all active in-memory collections before restore.")

	// 2. Cargar las colecciones desde los archivos de backup
	collectionsBackupDir := filepath.Join(backupPath, "collections")
	files, err := filepath.Glob(filepath.Join(collectionsBackupDir, "*"+collectionFileExtension))
	if err != nil {
		return fmt.Errorf("failed to list collection backup files in '%s': %w", collectionsBackupDir, err)
	}

	log.Printf("Found %d collection files in backup. Starting restore...", len(files))
	for _, filePath := range files {
		baseName := filepath.Base(filePath)
		colName := baseName[:len(baseName)-len(collectionFileExtension)]

		log.Printf("Restoring collection '%s' from '%s'...", colName, filePath)
		colStore := cm.GetCollection(colName)

		if err := loadCollectionDataFromBackup(filePath, colStore); err != nil {
			log.Printf("WARNING: Failed to restore collection '%s', skipping. Error: %v", colName, err)
			cm.DeleteCollection(colName) // Eliminar la colección parcialmente creada
			continue
		}
	}

	return nil
}

// loadCollectionDataFromBackup carga una sola colección y reconstruye sus índices.
func loadCollectionDataFromBackup(filePath string, s store.DataStore) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open collection backup file '%s': %w", filePath, err)
	}
	defer file.Close()

	// 1. Leer metadata de los índices
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

	// 2. Leer los datos de la colección
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

	// 3. Cargar datos en el store y reconstruir índices
	s.LoadData(collectionData)
	log.Printf("Collection data loaded with %d keys.", len(collectionData))

	if len(indexedFields) > 0 {
		log.Printf("Rebuilding %d indexes...", len(indexedFields))
		for _, field := range indexedFields {
			s.CreateIndex(field) // Esto reconstruye el índice a partir de los datos recién cargados
		}
		log.Printf("Finished rebuilding indexes.")
	}

	return nil
}

// readLengthPrefixed es una función de ayuda para leer datos con prefijo de longitud.
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
