// ./internal/persistence/cold_storage_ops.go

package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/globalconst"
	"os"
	"path/filepath"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// rewriteCollectionFile atomically rewrites a collection's data file.
// It iterates through the existing file and uses the updateFunc to decide
// what to do with each item (keep, modify, or skip).
func rewriteCollectionFile(collectionName string, updateFunc func(key string, data []byte) ([]byte, error)) error {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	tempFilePath := filePath + ".tmp"

	// Open the original file for reading.
	sourceFile, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, nothing to rewrite.
		}
		return fmt.Errorf("failed to open source collection file '%s': %w", filePath, err)
	}
	defer sourceFile.Close()

	// Create a new temporary file for writing.
	destFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temporary collection file '%s': %w", tempFilePath, err)
	}
	defer destFile.Close()

	// We must preserve the index header.
	var numIndexes uint32
	if err := binary.Read(sourceFile, binary.LittleEndian, &numIndexes); err != nil {
		return fmt.Errorf("rewrite: failed to read index header count: %w", err)
	}
	if err := binary.Write(destFile, binary.LittleEndian, numIndexes); err != nil {
		return fmt.Errorf("rewrite: failed to write index header count: %w", err)
	}

	indexedFields := make([][]byte, numIndexes)
	for i := 0; i < int(numIndexes); i++ {
		fieldBytes, err := readPrefixedBytes(sourceFile)
		if err != nil {
			return fmt.Errorf("rewrite: failed to read index field name: %w", err)
		}
		indexedFields[i] = fieldBytes
		if err := writePrefixedBytes(destFile, fieldBytes); err != nil {
			return fmt.Errorf("rewrite: failed to write index field name: %w", err)
		}
	}

	var numEntries uint32
	if err := binary.Read(sourceFile, binary.LittleEndian, &numEntries); err != nil {
		return fmt.Errorf("rewrite: failed to read entry count: %w", err)
	}

	// We don't know the final number of entries yet, so we write a placeholder.
	if err := binary.Write(destFile, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("rewrite: failed to write placeholder entry count: %w", err)
	}

	var finalCount uint32
	for i := 0; i < int(numEntries); i++ {
		keyBytes, err := readPrefixedBytes(sourceFile)
		if err != nil {
			return fmt.Errorf("rewrite: failed to read key at entry %d: %w", i, err)
		}
		valBytes, err := readPrefixedBytes(sourceFile)
		if err != nil {
			return fmt.Errorf("rewrite: failed to read value at entry %d: %w", i, err)
		}

		newValBytes, err := updateFunc(string(keyBytes), valBytes)
		if err != nil {
			return fmt.Errorf("rewrite: update function failed for key '%s': %w", string(keyBytes), err)
		}

		// If the update function returns nil, it means we should skip/delete this record.
		if newValBytes != nil {
			if err := writePrefixedBytes(destFile, keyBytes); err != nil {
				return fmt.Errorf("rewrite: failed to write key for '%s': %w", string(keyBytes), err)
			}
			if err := writePrefixedBytes(destFile, newValBytes); err != nil {
				return fmt.Errorf("rewrite: failed to write value for '%s': %w", string(keyBytes), err)
			}
			finalCount++
		}
	}

	// Go back to the beginning of the file to write the final count.
	if _, err := destFile.Seek(0, 0); err != nil {
		return fmt.Errorf("rewrite: failed to seek to start of temp file: %w", err)
	}

	// Re-write the header (indexes and final count).
	if err := binary.Write(destFile, binary.LittleEndian, numIndexes); err != nil {
		return fmt.Errorf("rewrite: failed to write final index count: %w", err)
	}
	for _, fieldBytes := range indexedFields {
		if err := writePrefixedBytes(destFile, fieldBytes); err != nil {
			return fmt.Errorf("rewrite: failed to write final index field name: %w", err)
		}
	}
	if err := binary.Write(destFile, binary.LittleEndian, finalCount); err != nil {
		return fmt.Errorf("rewrite: failed to write final entry count: %w", err)
	}

	// Atomically replace the old file with the new one.
	if err := destFile.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("rewrite: failed to close temp file: %w", err)
	}
	if err := os.Rename(tempFilePath, filePath); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("rewrite: failed to rename temp file: %w", err)
	}

	return nil
}

// UpdateColdItem finds a cold item by key and applies a patch to it on disk.
func UpdateColdItem(collectionName, key string, patchValue []byte) (bool, error) {
	found := false
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		if itemKey != key {
			return data, nil // Keep this item as is.
		}

		found = true
		var existingData map[string]any
		if err := jsoniter.Unmarshal(data, &existingData); err != nil {
			return nil, fmt.Errorf("could not unmarshal existing cold data: %w", err)
		}

		var patchData map[string]any
		if err := jsoniter.Unmarshal(patchValue, &patchData); err != nil {
			return nil, fmt.Errorf("could not unmarshal patch data: %w", err)
		}

		for k, v := range patchData {
			if k == globalconst.ID || k == globalconst.CREATED_AT {
				continue
			}
			existingData[k] = v
		}
		existingData[globalconst.UPDATED_AT] = time.Now().UTC().Format(time.RFC3339)

		return jsoniter.Marshal(existingData)
	})

	return found, err
}

// DeleteColdItem finds a cold item by key and marks it as deleted on disk (tombstone).
func DeleteColdItem(collectionName, key string) (bool, error) {
	found := false
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		if itemKey != key {
			return data, nil // Keep this item as is.
		}

		found = true
		var doc map[string]any
		if err := jsoniter.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("could not unmarshal cold data for deletion: %w", err)
		}

		doc[globalconst.DELETED_FLAG] = true // Add the tombstone flag.
		doc[globalconst.UPDATED_AT] = time.Now().UTC().Format(time.RFC3339)

		return jsoniter.Marshal(doc)
	})

	return found, err
}

// CompactCollectionFile rewrites a collection file, permanently removing tombstones.
func CompactCollectionFile(collectionName string) error {
	slog.Info("Compacting collection file", "collection", collectionName)
	return rewriteCollectionFile(collectionName, func(key string, data []byte) ([]byte, error) {
		var doc map[string]any
		if err := jsoniter.Unmarshal(data, &doc); err != nil {
			return data, nil // Keep malformed data just in case.
		}

		if deleted, ok := doc[globalconst.DELETED_FLAG].(bool); ok && deleted {
			return nil, nil // Return nil to skip/delete this record permanently.
		}

		return data, nil // Keep this record.
	})
}

// writePrefixedBytes is a helper for the rewriter.
func writePrefixedBytes(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// ColdUpdatePayload defines the structure for a single update operation in a batch.
type ColdUpdatePayload struct {
	ID    string
	Patch map[string]any
}

// --- NEW FUNCTION: UpdateManyColdItems ---
// Updates multiple cold items in a single file rewrite operation.
func UpdateManyColdItems(collectionName string, payloads []ColdUpdatePayload) (int, error) {
	// Create a map for quick O(1) lookups of patches by key.
	patches := make(map[string]map[string]any, len(payloads))
	for _, p := range payloads {
		if p.ID != "" {
			patches[p.ID] = p.Patch
		}
	}

	updatedCount := 0
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		// Check if the current item's key is in our batch of updates.
		if patchData, ok := patches[itemKey]; ok {
			updatedCount++
			var existingData map[string]any
			if err := jsoniter.Unmarshal(data, &existingData); err != nil {
				return nil, fmt.Errorf("could not unmarshal existing cold data for batch update: %w", err)
			}

			for k, v := range patchData {
				if k == globalconst.ID || k == globalconst.CREATED_AT {
					continue
				}
				existingData[k] = v
			}
			existingData[globalconst.UPDATED_AT] = time.Now().UTC().Format(time.RFC3339)

			return jsoniter.Marshal(existingData)
		}

		// If the key is not in the batch, return the original data.
		return data, nil
	})

	return updatedCount, err
}

// --- NEW FUNCTION: DeleteManyColdItems ---
// Marks multiple cold items as deleted (tombstone) in a single file rewrite.
func DeleteManyColdItems(collectionName string, keys []string) (int, error) {
	// Create a map for quick O(1) lookups of keys to delete.
	keysToDelete := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keysToDelete[k] = struct{}{}
	}

	markedCount := 0
	err := rewriteCollectionFile(collectionName, func(itemKey string, data []byte) ([]byte, error) {
		// Check if the current item's key is in our batch of deletions.
		if _, shouldDelete := keysToDelete[itemKey]; shouldDelete {
			markedCount++
			var doc map[string]any
			if err := jsoniter.Unmarshal(data, &doc); err != nil {
				return nil, fmt.Errorf("could not unmarshal cold data for batch deletion: %w", err)
			}

			doc[globalconst.DELETED_FLAG] = true // Add the tombstone flag.
			doc[globalconst.UPDATED_AT] = time.Now().UTC().Format(time.RFC3339)

			return jsoniter.Marshal(doc)
		}

		// If the key is not in the batch, return the original data.
		return data, nil
	})

	return markedCount, err
}

// CheckColdKeyExists verifica si una clave específica existe en el archivo de persistencia de una colección.
// Es una operación optimizada que solo lee las claves y evita decodificar los valores.
func CheckColdKeyExists(collectionName, keyToFind string) (bool, error) {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // El archivo no existe, por lo tanto la clave tampoco.
		}
		return false, fmt.Errorf("failed to open cold data file '%s': %w", filePath, err)
	}
	defer file.Close()

	// Omitir cabecera de índices
	var numIndexes uint32
	if err := binary.Read(file, binary.LittleEndian, &numIndexes); err != nil {
		return false, nil // Archivo vacío o corrupto, asumimos que la clave no existe.
	}
	for i := 0; i < int(numIndexes); i++ {
		fieldBytes, err := readPrefixedBytes(file)
		if err != nil {
			return false, fmt.Errorf("could not read index field name: %w", err)
		}
		_ = fieldBytes // Descartamos el nombre del campo.
	}

	// Leer número de entradas
	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return false, nil // No hay entradas para leer.
	}

	// Iterar solo sobre las claves
	for i := 0; i < int(numEntries); i++ {
		keyBytes, err := readPrefixedBytes(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, fmt.Errorf("error reading key at position %d: %w", i, err)
		}

		if string(keyBytes) == keyToFind {
			return true, nil // ¡Clave encontrada!
		}

		// Omitir el valor para pasar al siguiente registro rápidamente
		var valLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valLen); err != nil {
			return false, fmt.Errorf("error reading value length for key '%s': %w", string(keyBytes), err)
		}
		if _, err := file.Seek(int64(valLen), io.SeekCurrent); err != nil {
			return false, fmt.Errorf("error seeking past value for key '%s': %w", string(keyBytes), err)
		}
	}

	return false, nil // La clave no fue encontrada.
}

// CheckManyColdKeysExist verifica la existencia de múltiples claves en un archivo de colección en una sola pasada.
// Devuelve un mapa con las claves que sí fueron encontradas.
func CheckManyColdKeysExist(collectionName string, keysToFind []string) (map[string]bool, error) {
	foundKeys := make(map[string]bool)
	if len(keysToFind) == 0 {
		return foundKeys, nil
	}

	keysMap := make(map[string]struct{}, len(keysToFind))
	for _, k := range keysToFind {
		keysMap[k] = struct{}{}
	}

	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return foundKeys, nil
		}
		return nil, fmt.Errorf("failed to open cold data file '%s': %w", filePath, err)
	}
	defer file.Close()

	// Omitir cabecera de índices (misma lógica que la función singular)
	var numIndexes uint32
	binary.Read(file, binary.LittleEndian, &numIndexes)
	for i := 0; i < int(numIndexes); i++ {
		fieldBytes, _ := readPrefixedBytes(file)
		_ = fieldBytes
	}

	// Leer número de entradas
	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return foundKeys, nil
	}

	// Iterar una sola vez
	for i := 0; i < int(numEntries); i++ {
		keyBytes, err := readPrefixedBytes(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		keyStr := string(keyBytes)
		if _, needed := keysMap[keyStr]; needed {
			foundKeys[keyStr] = true
		}

		// Omitir valor
		var valLen uint32
		binary.Read(file, binary.LittleEndian, &valLen)
		file.Seek(int64(valLen), io.SeekCurrent)

		// Optimización: si ya encontramos todas las claves, salimos.
		if len(foundKeys) == len(keysToFind) {
			break
		}
	}

	return foundKeys, nil
}
