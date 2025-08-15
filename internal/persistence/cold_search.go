package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/globalconst"
	"os"
	"path/filepath"
)

// MatcherFunc is a function signature that defines how to determine if a document matches a filter.
// This allows us to reuse the `matchFilter` logic from the `handler` package.
type MatcherFunc func(item map[string]any) bool

// SearchColdData searches a collection's persistence file for items that match a filter.
// This is an I/O-intensive operation that sequentially reads the file.
func SearchColdData(collectionName string, matcher MatcherFunc) ([]map[string]any, error) {
	filePath := filepath.Join(globalconst.CollectionsDirName, collectionName+globalconst.DBFileExtension)
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
		if _, err := io.CopyN(io.Discard, file, int64(fieldLen)); err != nil {
			return nil, fmt.Errorf("failed to discard index field name from cold file: %w", err)
		}
	}

	var numEntries uint32
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		if err == io.EOF {
			return []map[string]any{}, nil
		}
		return nil, fmt.Errorf("failed to read number of entries from cold file '%s': %w", filePath, err)
	}

	var results []map[string]any
	for i := 0; i < int(numEntries); i++ {
		_, err := readPrefixedBytes(file) // Read and discard the key
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Warn("Failed to read key in cold search, skipping record", "collection", collectionName, "error", err)
			continue
		}
		valBytes, err := readPrefixedBytes(file)
		if err != nil {
			slog.Warn("Failed to read value in cold search, skipping record", "collection", collectionName, "error", err)
			continue
		}

		var doc map[string]any
		if err := json.Unmarshal(valBytes, &doc); err != nil {
			continue
		}

		if deleted, ok := doc[globalconst.DELETED_FLAG].(bool); ok && deleted {
			continue
		}

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
