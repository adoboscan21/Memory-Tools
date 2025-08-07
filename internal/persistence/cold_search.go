// ./internal/persistence/cold_search.go

package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// MatcherFunc is a function signature that defines how to determine if a document matches a filter.
// This allows us to reuse the `matchFilter` logic from the `handler` package.
type MatcherFunc func(item map[string]any) bool

// SearchColdData searches a collection's persistence file for items that match a filter.
// This is an I/O-intensive operation that sequentially reads the file.
// SearchColdData searches a collection's persistence file for items that match a filter.
// It now correctly skips items marked with a tombstone.
func SearchColdData(collectionName string, matcher MatcherFunc) ([]map[string]any, error) {
	filePath := filepath.Join(collectionsDir, collectionName+collectionFileExtension)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []map[string]any{}, nil // No file, so no cold data.
		}
		return nil, fmt.Errorf("failed to open cold data file '%s': %w", filePath, err)
	}
	defer file.Close()

	// We skip the index header, as we don't use it for the cold search.
	var numIndexes uint32
	if err := binary.Read(file, binary.LittleEndian, &numIndexes); err != nil {
		// Handle old files that might not have an index header
		if err == io.EOF {
			numIndexes = 0
			if _, seekErr := file.Seek(0, 0); seekErr != nil {
				return nil, fmt.Errorf("failed to seek back to start of file for '%s': %w", collectionName, seekErr)
			}
		} else {
			return nil, fmt.Errorf("failed to read index header from cold file '%s': %w", filePath, err)
		}
	}

	for i := 0; i < int(numIndexes); i++ {
		var fieldLen uint32
		if err := binary.Read(file, binary.LittleEndian, &fieldLen); err != nil {
			return nil, fmt.Errorf("failed to read index field length from cold file: %w", err)
		}
		// Read and discard the field name.
		if _, err := io.CopyN(io.Discard, file, int64(fieldLen)); err != nil {
			return nil, fmt.Errorf("failed to discard index field name from cold file: %w", err)
		}
	}

	// Read the number of entries and begin the search.
	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		// If we can't read the entry count after the header, the file might be empty or corrupt.
		if err == io.EOF {
			return []map[string]any{}, nil
		}
		return nil, fmt.Errorf("failed to read number of entries from cold file '%s': %w", filePath, err)
	}

	var results []map[string]any
	for i := 0; i < int(numEntries); i++ {
		// Read the key and value for each record.
		_, err := readPrefixedBytes(file) // Read and discard the key
		if err != nil {
			if err == io.EOF {
				break // Clean end of file
			}
			slog.Warn("Failed to read key in cold search, skipping record", "collection", collectionName, "error", err)
			continue
		}
		valBytes, err := readPrefixedBytes(file)
		if err != nil {
			slog.Warn("Failed to read value in cold search, skipping record", "collection", collectionName, "error", err)
			continue
		}

		// Unmarshal the value into a map so we can apply the filter.
		var doc map[string]any
		if err := json.Unmarshal(valBytes, &doc); err != nil {
			continue // If it's not valid JSON, we can't filter it.
		}

		// --- MODIFICACIÓN CLAVE ---
		// Check for the tombstone flag. If present and true, skip this record.
		if deleted, ok := doc["_deleted"].(bool); ok && deleted {
			continue
		}

		// Use the `matcher` function to see if the document matches.
		if matcher(doc) {
			results = append(results, doc)
		}
	}

	slog.Debug("Cold data search complete", "collection", collectionName, "found_matches", len(results))
	return results, nil
}

// readPrefixedBytes is a helper function to read length-prefixed data.
func readPrefixedBytes(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("could not read length prefix: %w", err)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("could not read full data bytes: %w", err)
	}
	return data, nil
}
