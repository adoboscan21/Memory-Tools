package store

import (
	"log"  // Used for maps.Copy, available from Go 1.21.
	"sync" // Used for RWMutex for concurrent safety.
	"time" // Added for time.Time and time.Duration
)

// Item represents an individual key-value entry stored in the in-memory database.
// It includes metadata for TTL management.
type Item struct {
	Value     []byte        // The actual value, stored as raw JSON bytes.
	CreatedAt time.Time     // The timestamp when this item was created or last updated.
	TTL       time.Duration // The time-to-live for this item. A value of 0 means no expiration.
}

// DataStore is the interface that defines the basic operations for our key-value store.
// It now includes a TTL parameter for the Set method, and a Delete method.
type DataStore interface {
	// Set now accepts a time.Duration for TTL. If ttl is 0, the item does not expire.
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	// Delete removes a key-value pair from the store.
	Delete(key string)
	// GetAll now returns map[string][]byte, containing only non-expired items for persistence.
	GetAll() map[string][]byte
	LoadData(data map[string][]byte) // Still loads map[string][]byte (raw values, no TTL on load)
}

// InMemStore implements DataStore for in-memory storage.
// It uses a RWMutex to ensure concurrent-safe access to the underlying map.
type InMemStore struct {
	// The actual in-memory map now holds key-value pairs where values are Item structs.
	data map[string]Item
	mu   sync.RWMutex // RWMutex to protect concurrent access to the map.
}

// NewInMemStore creates and returns a new instance of InMemStore.
func NewInMemStore() *InMemStore {
	return &InMemStore{
		data: make(map[string]Item), // Initialize the map to store Item structs.
	}
}

// Set saves a key-value pair in the in-memory store securely.
// It now accepts a byte slice for the value and a time.Duration for its Time-To-Live.
// If ttl is 0, the item will not expire.
func (s *InMemStore) Set(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()         // Acquire a write lock before modifying the map.
	defer s.mu.Unlock() // Release the write lock when the function exits.

	s.data[key] = Item{
		Value:     value,
		CreatedAt: time.Now(), // Record the current time of creation/update.
		TTL:       ttl,
	}
	log.Printf("SET: Key='%s', ValueLength=%d bytes, TTL=%s", key, len(value), ttl)
}

// Get retrieves a value from the in-memory store by its key securely.
// It now returns a byte slice for the value, after checking for expiration.
func (s *InMemStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()         // Acquire a read lock. Multiple readers can hold this lock concurrently.
	defer s.mu.RUnlock() // Release the read lock when the function exits.

	item, found := s.data[key]
	if !found {
		log.Printf("GET: Key='%s' (not found)", key)
		return nil, false
	}

	// Check if the item has expired.
	// An item expires if TTL is positive AND the current time is after CreatedAt + TTL.
	if item.TTL > 0 && time.Since(item.CreatedAt) > item.TTL {
		// The item has expired. Log and treat it as not found.
		// Actual deletion from the map will be handled by the background clean-up goroutine.
		log.Printf("GET: Key='%s' (found, but expired)", key)
		return nil, false
	}

	log.Printf("GET: Key='%s' (found, not expired)", key)
	return item.Value, true
}

// Delete removes a key-value pair from the in-memory store.
func (s *InMemStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	log.Printf("DELETE: Key='%s'", key)
}

// GetAll returns a copy of all data currently in the store for persistence.
// This method now filters out expired items and only returns their raw values.
func (s *InMemStore) GetAll() map[string][]byte {
	s.mu.RLock()         // Acquire a read lock before reading the map.
	defer s.mu.RUnlock() // Release the read lock.

	// Create a copy of the map to ensure that the caller cannot directly modify
	// the internal state of the InMemStore without acquiring the appropriate locks.
	snapshotData := make(map[string][]byte, len(s.data))
	now := time.Now()

	for k, item := range s.data {
		// Only include non-expired items in the snapshot for persistence.
		if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
			// Make a deep copy of the byte slice value.
			copyValue := make([]byte, len(item.Value))
			copy(copyValue, item.Value)
			snapshotData[k] = copyValue
		}
	}
	return snapshotData
}

// LoadData loads data into the in-memory store from a persistent source.
// Items loaded from persistence will have no TTL by default upon loading,
// as TTL metadata is not stored in the binary file for simplicity in this iteration.
func (s *InMemStore) LoadData(data map[string][]byte) {
	s.mu.Lock()         // Acquire a write lock as we are modifying the internal map entirely.
	defer s.mu.Unlock() // Release the write lock.

	s.data = make(map[string]Item) // Clear existing data and prepare for loaded items.
	for k, v := range data {
		s.data[k] = Item{
			Value:     v,
			CreatedAt: time.Now(), // Assume loaded items are "created" at load time.
			TTL:       0,          // Loaded items have no TTL by default.
		}
	}
	log.Printf("Data loaded into memory. Total keys: %d", len(s.data)) // Structured logging.
}

// CleanExpiredItems iterates through the store and physically deletes expired items.
// This method should be called periodically by a background goroutine.
func (s *InMemStore) CleanExpiredItems() {
	s.mu.Lock() // Requires a write lock to modify the map.
	defer s.mu.Unlock()

	deletedCount := 0
	now := time.Now()
	for key, item := range s.data {
		// Check if the item has a TTL and if it has expired.
		if item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
			delete(s.data, key)
			deletedCount++
			log.Printf("TTL Cleaner: Deleted expired item: '%s'", key)
		}
	}
	if deletedCount > 0 {
		log.Printf("TTL Cleaner: %d expired items removed.", deletedCount)
	}
}
