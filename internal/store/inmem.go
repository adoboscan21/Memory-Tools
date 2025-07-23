package store

import (
	"log"
	"maps"
	"sync"
)

// DataStore is the interface that defines the basic operations for our key-value store.
type DataStore interface {
	Set(key string, value string)
	Get(key string) (string, bool)
	GetAll() map[string]string // Used for persistence, returns a copy of all data
}

// InMemStore implements DataStore for in-memory storage with concurrent-safe access.
type InMemStore struct {
	data map[string]string
	mu   sync.RWMutex // RWMutex to protect concurrent access to the map
}

// NewInMemStore creates and returns a new instance of InMemStore.
func NewInMemStore() *InMemStore {
	return &InMemStore{
		data: make(map[string]string),
	}
}

// Set saves a key-value pair in the in-memory store securely.
func (s *InMemStore) Set(key string, value string) {
	s.mu.Lock() // Acquire a write lock
	defer s.mu.Unlock()
	s.data[key] = value
	log.Printf("SET: Key='%s', Value='%s'", key, value) // Use structured logging
}

// Get retrieves a value from the in-memory store by its key securely.
func (s *InMemStore) Get(key string) (string, bool) {
	s.mu.RLock() // Acquire a read lock
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	log.Printf("GET: Key='%s' -> Value='%s' (found: %t)", key, value, ok) // Structured logging
	return value, ok
}

// GetAll returns a copy of all data currently in the store.
// This is useful for persistence operations or debugging.
func (s *InMemStore) GetAll() map[string]string {
	s.mu.RLock() // Acquire a read lock
	defer s.mu.RUnlock()
	// Create a copy to prevent direct external modifications
	copyData := make(map[string]string, len(s.data))
	maps.Copy(copyData, s.data)
	return copyData
}

// LoadData loads data into the in-memory store, used for initialization
// from a persistent source.
func (s *InMemStore) LoadData(data map[string]string) {
	s.mu.Lock() // Acquire a write lock
	defer s.mu.Unlock()
	s.data = data
	log.Printf("Data loaded into memory. Total keys: %d", len(s.data)) // Structured logging
}
