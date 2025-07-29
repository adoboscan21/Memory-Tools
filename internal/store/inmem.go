package store

import (
	"hash/fnv"
	"log"
	"maps"
	"sync"
	"time"
)

// Item represents an individual key-value entry.
type Item struct {
	Value     []byte
	CreatedAt time.Time
	TTL       time.Duration // 0 means no expiration.
}

// Shard represents a segment of the in-memory store.
type Shard struct {
	data map[string]Item
	mu   sync.RWMutex // Mutex to protect concurrent access.
}

// DataStore is the interface that defines basic key-value store operations.
type DataStore interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	Delete(key string)
	GetAll() map[string][]byte // Get all non-expired data.
	LoadData(data map[string][]byte)
	CleanExpiredItems() bool // Returns true if any items were cleaned.
	Size() int               // Returns the current number of items.
}

// InMemStore implements DataStore for in-memory storage, with sharding.
type InMemStore struct {
	shards    []*Shard
	numShards int
}

const defaultNumShards = 16

// NewInMemStore creates a new InMemStore with default sharding.
func NewInMemStore() *InMemStore {
	return NewInMemStoreWithShards(defaultNumShards)
}

// NewInMemStoreWithShards creates a new InMemStore with a specified number of shards.
func NewInMemStoreWithShards(numShards int) *InMemStore {
	s := &InMemStore{
		shards:    make([]*Shard, numShards),
		numShards: numShards,
	}
	for i := range numShards {
		s.shards[i] = &Shard{
			data: make(map[string]Item),
		}
	}
	log.Printf("InMemStore initialized with %d shards.", numShards)
	return s
}

// getShard determines which shard a given key belongs to.
func (s *InMemStore) getShard(key string) *Shard {
	h := fnv.New64a()
	h.Write([]byte(key))
	shardIndex := h.Sum64() % uint64(s.numShards)
	return s.shards[shardIndex]
}

// getShardIndex for logging purposes.
func (s *InMemStore) getShardIndex(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64() % uint64(s.numShards)
}

// Set saves a key-value pair in the store within its respective shard.
func (s *InMemStore) Set(key string, value []byte, ttl time.Duration) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = Item{
		Value:     value,
		CreatedAt: time.Now(),
		TTL:       ttl,
	}
	log.Printf("SET [Shard %d]: Key='%s', ValueLength=%d bytes, TTL=%s", s.getShardIndex(key), key, len(value), ttl)
}

// Get retrieves a value from the store by its key within its respective shard.
func (s *InMemStore) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	item, found := shard.data[key]
	if !found {
		log.Printf("GET [Shard %d]: Key='%s' (not found)", s.getShardIndex(key), key)
		return nil, false
	}

	if item.TTL > 0 && time.Since(item.CreatedAt) > item.TTL {
		log.Printf("GET [Shard %d]: Key='%s' (found, but expired)", s.getShardIndex(key), key)
		return nil, false
	}

	log.Printf("GET [Shard %d]: Key='%s' (found, not expired)", s.getShardIndex(key), key)
	return item.Value, true
}

// Delete removes a key-value pair from the store within its respective shard.
func (s *InMemStore) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.data, key)
	log.Printf("DELETE [Shard %d]: Key='%s'", s.getShardIndex(key), key)
}

// GetAll returns a copy of all non-expired data from ALL shards for persistence.
func (s *InMemStore) GetAll() map[string][]byte {
	snapshotData := make(map[string][]byte)
	now := time.Now()

	for i, shard := range s.shards {
		shard.mu.RLock()
		for k, item := range shard.data {
			if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
				copyValue := make([]byte, len(item.Value))
				copy(copyValue, item.Value)
				snapshotData[k] = copyValue
			}
		}
		shard.mu.RUnlock()
		log.Printf("GetAll: Processed Shard %d", i)
	}
	log.Printf("GetAll: Combined snapshot data from all %d shards. Total items: %d", s.numShards, len(snapshotData))
	return snapshotData
}

// LoadData loads data into the store across its shards from a persistent source.
func (s *InMemStore) LoadData(data map[string][]byte) {
	// Clear all existing data from all shards before loading new data.
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.data = make(map[string]Item) // Clear map in shard.
		shard.mu.Unlock()
	}
	log.Println("LoadData: All shards cleared.")

	loadedCount := 0
	for k, v := range data {
		shard := s.getShard(k)
		shard.mu.Lock()
		shard.data[k] = Item{
			Value:     v,
			CreatedAt: time.Now(), // Assume loaded items are "created" at load time.
			TTL:       0,          // Loaded items have no TTL by default.
		}
		shard.mu.Unlock()
		loadedCount++
	}
	log.Printf("LoadData: Data successfully loaded into %d shards. Total keys: %d", s.numShards, loadedCount)
}

// CleanExpiredItems iterates through each shard and physically deletes expired items.
// Returns true if any items were deleted.
func (s *InMemStore) CleanExpiredItems() bool {
	totalDeletedCount := 0
	now := time.Now()
	wasModified := false

	for i, shard := range s.shards {
		shard.mu.Lock()
		deletedInShard := 0
		for key, item := range shard.data {
			if item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
				delete(shard.data, key)
				deletedInShard++
				wasModified = true
			}
		}
		shard.mu.Unlock()

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
	return wasModified
}

// Size returns the total number of items in the store across all shards.
func (s *InMemStore) Size() int {
	total := 0
	for _, shard := range s.shards {
		shard.mu.RLock()
		total += len(shard.data)
		shard.mu.RUnlock()
	}
	return total
}

// CollectionPersister defines the interface for persistence operations specific to collections.
type CollectionPersister interface {
	SaveCollectionData(collectionName string, s DataStore) error
	DeleteCollectionFile(collectionName string) error
}

// CollectionManager manages multiple named InMemStore instances, each representing a collection.
type CollectionManager struct {
	collections map[string]DataStore // Map of collection names to their DataStore instances.
	mu          sync.RWMutex         // Mutex to protect the 'collections' map.
	persister   CollectionPersister  // Interface for persistence operations.
}

// NewCollectionManager creates a new instance of CollectionManager.
func NewCollectionManager(persister CollectionPersister) *CollectionManager {
	return &CollectionManager{
		collections: make(map[string]DataStore),
		persister:   persister, // Inject the persister.
	}
}

// GetCollection retrieves an existing collection (InMemStore) by name, or creates a new one.
func (cm *CollectionManager) GetCollection(name string) DataStore {
	// Attempt to get with RLock first.
	cm.mu.RLock()
	col, found := cm.collections[name]
	cm.mu.RUnlock()

	if found {
		return col
	}

	// Acquire write lock to create if not found.
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check after acquiring lock.
	col, found = cm.collections[name]
	if found {
		return col
	}

	// Create new collection.
	newCol := NewInMemStore() // Each collection gets its own sharded in-memory store.
	cm.collections[name] = newCol
	log.Printf("Collection '%s' created and added to CollectionManager.", name)
	return newCol
}

// DeleteCollection removes a collection entirely from the manager.
func (cm *CollectionManager) DeleteCollection(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, exists := cm.collections[name]; exists {
		delete(cm.collections, name)
		log.Printf("Collection '%s' deleted from CollectionManager (in-memory).", name)
	} else {
		log.Printf("Attempted to delete non-existent collection '%s'.", name)
	}
}

// ListCollections returns the names of all active collections.
func (cm *CollectionManager) ListCollections() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	names := make([]string, 0, len(cm.collections))
	for name := range cm.collections {
		names = append(names, name)
	}
	log.Printf("ListCollections: Returning %d collection names.", len(names))
	return names
}

// CollectionExists checks if a collection with the given name exists in the manager.
func (cm *CollectionManager) CollectionExists(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, exists := cm.collections[name]
	return exists
}

// LoadAllCollectionData loads data into the respective InMemStore instances within the manager.
func (cm *CollectionManager) LoadAllCollectionData(allCollectionsData map[string]map[string][]byte) {
	cm.mu.Lock() // Acquire write lock for the entire duration of loading collections.
	defer cm.mu.Unlock()

	// Clear existing in-memory collection instances before loading.
	cm.collections = make(map[string]DataStore) // Re-initialize to clear.

	for colName, data := range allCollectionsData {
		// Directly create and load the new InMemStore for this collection.
		newCol := NewInMemStore()        // Create a new InMemStore for this collection.
		newCol.LoadData(data)            // Load data into this specific InMemStore.
		cm.collections[colName] = newCol // Directly assign to the map.
		log.Printf("Loaded collection '%s' with %d items into CollectionManager from persistence.", colName, newCol.Size())
	}
	log.Printf("CollectionManager: Successfully loaded/updated %d collections from persistence.", len(allCollectionsData))
}

// GetAllCollectionsDataForPersistence gets data from all managed InMemStore instances for persistence.
func (cm *CollectionManager) GetAllCollectionsDataForPersistence() map[string]map[string][]byte {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	dataToSave := make(map[string]map[string][]byte)
	for colName, col := range cm.collections {
		dataToSave[colName] = col.GetAll() // Get all non-expired data from each collection's InMemStore.
	}
	log.Printf("CollectionManager: Retrieved data from %d collections for persistence.", len(dataToSave))
	return dataToSave
}

// SaveCollectionToDisk saves a single collection's data to disk using the injected persister.
func (cm *CollectionManager) SaveCollectionToDisk(collectionName string, col DataStore) error {
	log.Printf("Attempting to save collection '%s' to disk (via injected persister)...", collectionName)
	return cm.persister.SaveCollectionData(collectionName, col)
}

// DeleteCollectionFromDisk removes a collection's file from disk using the injected persister.
func (cm *CollectionManager) DeleteCollectionFromDisk(collectionName string) error {
	log.Printf("Attempting to delete collection file for '%s' from disk (via injected persister)...", collectionName)
	return cm.persister.DeleteCollectionFile(collectionName)
}

// CleanExpiredItemsAndSave triggers TTL cleanup on all managed collections and saves modified ones to disk.
func (cm *CollectionManager) CleanExpiredItemsAndSave() {
	cm.mu.RLock() // Read lock to iterate collections without blocking new collection creation.
	collectionsAndNames := make(map[string]DataStore, len(cm.collections))
	maps.Copy(collectionsAndNames, cm.collections)
	cm.mu.RUnlock()

	log.Println("TTL Cleaner (Collections): Starting sweep across all managed collections.")
	for name, col := range collectionsAndNames {
		if col.CleanExpiredItems() { // CleanExpiredItems now returns true if any item was deleted.
			// If items were deleted, save the modified collection to disk.
			if err := cm.SaveCollectionToDisk(name, col); err != nil {
				log.Printf("Error saving collection '%s' to disk after TTL cleanup: %v", name, err)
			}
		}
	}
	log.Println("TTL Cleaner (Collections): Finished sweep across all managed collections.")
}
