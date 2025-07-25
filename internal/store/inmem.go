package store

import (
	"hash/fnv" // Import for FNV hash function (standard library)
	"log"
	"sync"
	"time"
)

// Item represents an individual key-value entry stored in the in-memory database.
// It includes metadata for TTL management.
type Item struct {
	Value     []byte        // The actual value, stored as raw JSON bytes.
	CreatedAt time.Time     // The timestamp when this item was created or last updated.
	TTL       time.Duration // The time-to-live for this item. A value of 0 means no expiration.
}

// Shard represents a segment of the in-memory store.
// Each shard contains a portion of the data map and its own mutex.
type Shard struct {
	data map[string]Item // The actual map for this shard.
	mu   sync.RWMutex    // Mutex to protect concurrent access to this specific shard.
}

// DataStore is the interface that defines the basic operations for our key-value store.
// The interface methods remain the same, but their internal implementation will change due to sharding.
type DataStore interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	Delete(key string)
	GetAll() map[string][]byte
	LoadData(data map[string][]byte)
}

// InMemStore implements DataStore for in-memory storage, now with sharding for better concurrency.
// It uses an array of Shard structs to distribute data and reduce mutex contention.
type InMemStore struct {
	shards []*Shard // Array of pointers to Shard structs, each holding a part of the data.
	// No global mutex for the whole InMemStore data map itself, as shards have their own.
	// A global mutex might still be needed for operations that affect *all* shards, like LoadData.
	// For LoadData, we'll acquire individual shard locks sequentially for simplicity, or
	// manage a global lock if the data map replacement is truly atomic across shards.
	// For this sharding implementation, LoadData will acquire individual shard locks.
}

// Default number of shards. This value should be a power of 2 for efficient hashing.
// Adjust based on expected concurrency and profiling. More shards can reduce contention,
// but add overhead.
const defaultNumShards = 256

// NewInMemStore creates and returns a new instance of InMemStore with sharding enabled.
func NewInMemStore() *InMemStore {
	s := &InMemStore{
		shards: make([]*Shard, defaultNumShards), // Initialize the slice of shards.
	}
	// Initialize each shard with its own map.
	for i := 0; i < defaultNumShards; i++ {
		s.shards[i] = &Shard{
			data: make(map[string]Item),
		}
	}
	log.Printf("InMemStore initialized with %d shards.", defaultNumShards)
	return s
}

// getShard determines which shard a given key belongs to.
// It uses FNV-64a hash function for key distribution.
func (s *InMemStore) getShard(key string) *Shard {
	// FNV-64a hash is fast and generally provides good distribution.
	h := fnv.New64a()
	h.Write([]byte(key))
	// Use the hash sum modulo the number of shards to get the shard index.
	shardIndex := h.Sum64() % uint64(defaultNumShards)
	return s.shards[shardIndex]
}

// Set saves a key-value pair in the in-memory store securely within its respective shard.
// It now accepts a byte slice for the value and a time.Duration for its Time-To-Live.
// If ttl is 0, the item will not expire.
func (s *InMemStore) Set(key string, value []byte, ttl time.Duration) {
	shard := s.getShard(key) // Get the specific shard for this key.
	shard.mu.Lock()          // Acquire a write lock ONLY for this shard.
	defer shard.mu.Unlock()  // Release the shard's write lock.

	shard.data[key] = Item{
		Value:     value,
		CreatedAt: time.Now(), // Record the current time of creation/update.
		TTL:       ttl,
	}
	// Log for the specific shard, indicating improved concurrency.
	log.Printf("SET [Shard %d]: Key='%s', ValueLength=%d bytes, TTL=%s", shard.getShardIndex(key), key, len(value), ttl)
}

// Helper to get shard index for logging purposes (optional, for better logs).
func (sh *Shard) getShardIndex(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64() % uint64(defaultNumShards)
}

// Get retrieves a value from the in-memory store by its key securely within its respective shard.
// It returns a byte slice for the value, after checking for expiration.
func (s *InMemStore) Get(key string) ([]byte, bool) {
	shard := s.getShard(key) // Get the specific shard for this key.
	shard.mu.RLock()         // Acquire a read lock ONLY for this shard.
	defer shard.mu.RUnlock() // Release the shard's read lock.

	item, found := shard.data[key] // Access data within the specific shard.
	if !found {
		log.Printf("GET [Shard %d]: Key='%s' (not found)", shard.getShardIndex(key), key)
		return nil, false
	}

	// Check if the item has expired.
	if item.TTL > 0 && time.Since(item.CreatedAt) > item.TTL {
		log.Printf("GET [Shard %d]: Key='%s' (found, but expired)", shard.getShardIndex(key), key)
		return nil, false
	}

	log.Printf("GET [Shard %d]: Key='%s' (found, not expired)", shard.getShardIndex(key), key)
	return item.Value, true
}

// Delete removes a key-value pair from the in-memory store within its respective shard.
func (s *InMemStore) Delete(key string) {
	shard := s.getShard(key) // Get the specific shard for this key.
	shard.mu.Lock()          // Acquire a write lock ONLY for this shard.
	defer shard.mu.Unlock()  // Release the shard's write lock.

	delete(shard.data, key) // Delete from the specific shard's map.
	log.Printf("DELETE [Shard %d]: Key='%s'", shard.getShardIndex(key), key)
}

// GetAll returns a copy of all non-expired data from ALL shards for persistence.
// This operation requires locking ALL shards, so it can be slower and more impactful on performance.
// For extremely high-throughput, consider offloading this with a copy-on-write strategy or similar.
func (s *InMemStore) GetAll() map[string][]byte {
	snapshotData := make(map[string][]byte) // Map to hold combined snapshot data.
	now := time.Now()

	// Iterate over all shards and acquire their read locks sequentially.
	// This is the bottleneck for GetAll in a sharded setup.
	for i, shard := range s.shards {
		shard.mu.RLock() // Acquire read lock for current shard.
		// Note: The defer here would release the lock after the entire function returns,
		// which is incorrect for sequential locking.
		// We must release the lock *before* moving to the next shard.
		// So, no defer here; explicit Unlock is needed below.

		for k, item := range shard.data {
			// Only include non-expired items in the snapshot for persistence.
			if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
				// Make a deep copy of the byte slice value.
				copyValue := make([]byte, len(item.Value))
				copy(copyValue, item.Value)
				snapshotData[k] = copyValue
			}
		}
		shard.mu.RUnlock() // Explicitly release read lock for current shard.
		log.Printf("GetAll: Processed Shard %d", i)
	}
	log.Printf("GetAll: Combined snapshot data from all %d shards. Total items: %d", defaultNumShards, len(snapshotData))
	return snapshotData
}

// LoadData loads data into the in-memory store across its shards from a persistent source.
// Items loaded from persistence will have no TTL by default upon loading.
// This operation requires writing to shards, so it will acquire write locks for each.
func (s *InMemStore) LoadData(data map[string][]byte) {
	// Clear all existing data from all shards before loading new data.
	for _, shard := range s.shards {
		shard.mu.Lock()                    // Acquire write lock for each shard.
		shard.data = make(map[string]Item) // Clear map in shard.
		shard.mu.Unlock()                  // Release write lock.
	}
	log.Println("LoadData: All shards cleared.")

	// Distribute loaded data into appropriate shards.
	loadedCount := 0
	for k, v := range data {
		shard := s.getShard(k) // Determine which shard the key belongs to.
		shard.mu.Lock()        // Acquire write lock for that specific shard.
		shard.data[k] = Item{
			Value:     v,
			CreatedAt: time.Now(), // Assume loaded items are "created" at load time.
			TTL:       0,          // Loaded items have no TTL by default.
		}
		shard.mu.Unlock() // Release write lock for that shard.
		loadedCount++
	}
	log.Printf("LoadData: Data successfully loaded into %d shards. Total keys: %d", defaultNumShards, loadedCount)
}

// CleanExpiredItems iterates through each shard and physically deletes expired items.
// It acquires write locks for each shard individually, allowing other shards to remain accessible.
func (s *InMemStore) CleanExpiredItems() {
	totalDeletedCount := 0
	now := time.Now()

	// Iterate over all shards to clean expired items.
	for i, shard := range s.shards {
		shard.mu.Lock() // Acquire a write lock for the current shard.
		deletedInShard := 0
		for key, item := range shard.data {
			if item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
				delete(shard.data, key)
				deletedInShard++
			}
		}
		shard.mu.Unlock() // Explicitly release the write lock for the current shard.

		if deletedInShard > 0 {
			totalDeletedCount += deletedInShard
			log.Printf("TTL Cleaner [Shard %d]: Removed %d expired items.", i, deletedInShard)
		}
	}
	if totalDeletedCount > 0 {
		log.Printf("TTL Cleaner: Total %d expired items removed across all shards.", totalDeletedCount)
	} else {
		log.Println("TTL Cleaner: No expired items found to remove.")
	}
}
