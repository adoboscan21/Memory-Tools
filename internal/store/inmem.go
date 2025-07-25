package store

import (
	"log"
	"maps" // Used for maps.Copy, available from Go 1.21.
	"sync" // Used for RWMutex for concurrent safety.
)

// DataStore is the interface that defines the basic operations for our key-value store.
// This interface promotes loose coupling, allowing different implementations of the store.
type DataStore interface {
	Set(key string, value string)
	Get(key string) (string, bool)
	GetAll() map[string]string       // Used for persistence, returns a copy of all data.
	LoadData(data map[string]string) // Used by persistence to load data into the store.
}

// InMemStore implements DataStore for in-memory storage.
// It uses a RWMutex to ensure concurrent-safe access to the underlying map.
type InMemStore struct {
	data map[string]string // The actual in-memory map holding key-value pairs.
	mu   sync.RWMutex      // RWMutex to protect concurrent access to the map.
	// RWMutex allows multiple readers concurrently but only one writer at a time,
	// and blocks readers when a writer is active.
}

// NewInMemStore creates and returns a new instance of InMemStore.
func NewInMemStore() *InMemStore {
	return &InMemStore{
		data: make(map[string]string), // Initialize the map.
	}
}

// Set saves a key-value pair in the in-memory store securely.
func (s *InMemStore) Set(key string, value string) {
	s.mu.Lock()         // Acquire a write lock before modifying the map.
	defer s.mu.Unlock() // Release the write lock when the function exits.
	s.data[key] = value
	log.Printf("SET: Key='%s', Value='%s'", key, value) // Structured logging.
}

// Get retrieves a value from the in-memory store by its key securely.
func (s *InMemStore) Get(key string) (string, bool) {
	s.mu.RLock()         // Acquire a read lock. Multiple readers can hold this lock concurrently.
	defer s.mu.RUnlock() // Release the read lock when the function exits.
	value, ok := s.data[key]
	log.Printf("GET: Key='%s' -> Value='%s' (found: %t)", key, value, ok) // Structured logging.
	return value, ok
}

// GetAll returns a copy of all data currently in the store.
// This is used primarily for persistence operations or debugging.
// Returning a copy prevents external modifications from bypassing the mutex.
func (s *InMemStore) GetAll() map[string]string {
	s.mu.RLock()         // Acquire a read lock before reading the map.
	defer s.mu.RUnlock() // Release the read lock.
	// Create a copy of the map to ensure that the caller cannot directly modify
	// the internal state of the InMemStore without acquiring the appropriate locks.
	copyData := make(map[string]string, len(s.data))
	maps.Copy(copyData, s.data) // Efficiently copies elements from s.data to copyData.
	return copyData
}

// LoadData loads data into the in-memory store.
// This method is used during application startup to restore state from a persistent source.
func (s *InMemStore) LoadData(data map[string]string) {
	s.mu.Lock()                                                        // Acquire a write lock as we are modifying the internal map entirely.
	defer s.mu.Unlock()                                                // Release the write lock.
	s.data = data                                                      // Replace the current map with the loaded data.
	log.Printf("Data loaded into memory. Total keys: %d", len(s.data)) // Structured logging.
}
