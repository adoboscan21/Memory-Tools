// store/inmem.go
package store

import (
	"log"  // Used for maps.Copy, available from Go 1.21.
	"sync" // Used for RWMutex for concurrent safety.
)

// DataStore is the interface that defines the basic operations for our key-value store.
// This interface promotes loose coupling, allowing different implementations of the store.
type DataStore interface {
	// Value type changed to []byte to accommodate raw JSON documents.
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	// Map type changed to map[string][]byte for persistence and internal operations.
	GetAll() map[string][]byte
	LoadData(data map[string][]byte)
}

// InMemStore implements DataStore for in-memory storage.
// It uses a RWMutex to ensure concurrent-safe access to the underlying map.
type InMemStore struct {
	// The actual in-memory map now holds key-value pairs where values are byte slices.
	data map[string][]byte
	mu   sync.RWMutex // RWMutex to protect concurrent access to the map.
}

// NewInMemStore creates and returns a new instance of InMemStore.
func NewInMemStore() *InMemStore {
	return &InMemStore{
		data: make(map[string][]byte), // Initialize the map.
	}
}

// Set saves a key-value pair in the in-memory store securely.
// It now accepts a byte slice for the value.
func (s *InMemStore) Set(key string, value []byte) {
	s.mu.Lock()         // Acquire a write lock before modifying the map.
	defer s.mu.Unlock() // Release the write lock when the function exits.
	s.data[key] = value
	// For JSON values, logging the full value can be too verbose. Log length instead.
	log.Printf("SET: Key='%s', ValueLength=%d bytes", key, len(value))
}

// Get retrieves a value from the in-memory store by its key securely.
// It now returns a byte slice for the value.
func (s *InMemStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()         // Acquire a read lock. Multiple readers can hold this lock concurrently.
	defer s.mu.RUnlock() // Release the read lock when the function exits.
	value, ok := s.data[key]
	log.Printf("GET: Key='%s' (found: %t)", key, ok) // Log presence, not full value.
	return value, ok
}

// GetAll returns a copy of all data currently in the store.
// It returns a deep copy of the map, including copies of the byte slices,
// to prevent external modifications from affecting the internal store state.
func (s *InMemStore) GetAll() map[string][]byte {
	s.mu.RLock()         // Acquire a read lock before reading the map.
	defer s.mu.RUnlock() // Release the read lock.
	// Create a copy of the map to ensure that the caller cannot directly modify
	// the internal state of the InMemStore without acquiring the appropriate locks.
	copyData := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		// Crucially, make a deep copy of the byte slice itself.
		// A slice is a reference, so a shallow copy would still point to the same underlying array.
		copyValue := make([]byte, len(v))
		copy(copyValue, v)
		copyData[k] = copyValue
	}
	return copyData
}

// LoadData loads data into the in-memory store.
// This method is used during application startup to restore state from a persistent source.
// It now expects values as byte slices.
func (s *InMemStore) LoadData(data map[string][]byte) {
	s.mu.Lock()                                                        // Acquire a write lock as we are modifying the internal map entirely.
	defer s.mu.Unlock()                                                // Release the write lock.
	s.data = data                                                      // Replace the current map with the loaded data.
	log.Printf("Data loaded into memory. Total keys: %d", len(s.data)) // Structured logging.
}
