package persistence

import (
	"encoding/json"
	"fmt" // New import for io.ReadAll
	"log"
	"memory-tools/internal/store"
	"os" // New import for os.ReadFile and os.WriteFile
)

const dataFile = "data.json" // Name of the file for persistence

// SaveData saves all data from the DataStore to a JSON file.
func SaveData(s store.DataStore) error {
	data := s.GetAll() // Get a copy of all data from the store
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		// Wrap error for better context
		return fmt.Errorf("failed to serialize data for saving: %w", err)
	}

	// Use os.WriteFile instead of ioutil.WriteFile
	err = os.WriteFile(dataFile, jsonData, 0644) // 0644 for read/write by owner, read by others
	if err != nil {
		return fmt.Errorf("failed to write data file '%s': %w", dataFile, err)
	}
	log.Printf("Data successfully saved to %s", dataFile) // Structured logging
	return nil
}

// LoadData loads data from a JSON file and puts it into the InMemStore.
func LoadData(s *store.InMemStore) error {
	// Use os.ReadFile instead of ioutil.ReadFile
	content, err := os.ReadFile(dataFile)
	if err != nil {
		// It's normal for the file not to exist the first time the application runs.
		// We don't treat this as a fatal error; just start with an empty store.
		if os.IsNotExist(err) {
			log.Printf("Data file '%s' not found, initializing with empty data.", dataFile)
			return nil // Not a critical error if the file doesn't exist
		}
		// For other errors, wrap and return
		return fmt.Errorf("failed to read data file '%s': %w", dataFile, err)
	}

	var data map[string]string
	err = json.Unmarshal(content, &data)
	if err != nil {
		return fmt.Errorf("failed to deserialize data from '%s': %w", dataFile, err)
	}

	s.LoadData(data)                                         // Load the data into the InMemStore instance
	log.Printf("Data successfully loaded from %s", dataFile) // Structured logging
	return nil
}
