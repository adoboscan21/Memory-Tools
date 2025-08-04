package store

import (
	"hash/fnv"
	"log"
	"maps"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// tryUnmarshal unmarshals a byte slice into a map, returning nil if it fails.
func tryUnmarshal(value []byte) map[string]any {
	var data map[string]any
	if err := json.Unmarshal(value, &data); err != nil {
		return nil // Not a JSON object, cannot be indexed.
	}
	return data
}

// --- Indexing Structures ---

// Index represents the index data for a single field.
// It maps a field's value to a set of document keys (_id).
// map[indexedValue] -> map[documentKey] -> struct{}
type Index map[any]map[string]struct{}

// IndexManager manages all indexes for a single InMemStore (collection).
type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]Index // map[fieldName] -> Index
}

// NewIndexManager creates a new index manager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]Index),
	}
}

// CreateIndex initializes a new index for a given field.
func (im *IndexManager) CreateIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; !exists {
		im.indexes[field] = make(Index)
		log.Printf("Index created for field '%s'.", field)
	}
}

// NEW: DeleteIndex removes an index for a given field.
func (im *IndexManager) DeleteIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; exists {
		delete(im.indexes, field)
		log.Printf("Index for field '%s' deleted.", field)
	}
}

// NEW: ListIndexes returns a slice of all indexed field names.
func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()

	indexedFields := make([]string, 0, len(im.indexes))
	for field := range im.indexes {
		indexedFields = append(indexedFields, field)
	}
	return indexedFields
}

// Update updates all relevant indexes for a given document.
// It handles both removal of old values and addition of new ones.
func (im *IndexManager) Update(docKey string, oldData, newData map[string]any) {
	im.mu.Lock()
	defer im.mu.Unlock()

	// If no indexes are defined, do nothing.
	if len(im.indexes) == 0 {
		return
	}

	for field, index := range im.indexes {
		oldVal, oldOk := oldData[field]
		newVal, newOk := newData[field]

		// If value hasn't changed, do nothing for this field.
		if oldOk && newOk && oldVal == newVal {
			continue
		}

		// Remove old value from index if it existed.
		if oldOk {
			if docKeySet, valueExists := index[oldVal]; valueExists {
				delete(docKeySet, docKey)
				// Clean up the map if the set becomes empty.
				if len(docKeySet) == 0 {
					delete(index, oldVal)
				}
			}
		}

		// Add new value to index if it exists.
		if newOk {
			if _, valueExists := index[newVal]; !valueExists {
				index[newVal] = make(map[string]struct{})
			}
			index[newVal][docKey] = struct{}{}
		}
	}
}

// Remove removes a document from all indexes.
func (im *IndexManager) Remove(docKey string, data map[string]any) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if data == nil || len(im.indexes) == 0 {
		return
	}

	for field, index := range im.indexes {
		if val, ok := data[field]; ok {
			if docKeySet, valueExists := index[val]; valueExists {
				delete(docKeySet, docKey)
				if len(docKeySet) == 0 {
					delete(index, val)
				}
			}
		}
	}
}

// Lookup performs a value lookup on an index and returns matching document keys.
func (im *IndexManager) Lookup(field string, value any) ([]string, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false // No index for this field.
	}

	keySet, exists := index[value]
	if !exists {
		return []string{}, true // Index exists, but value not found.
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	return keys, true
}

// HasIndex checks if an index exists for a given field.
func (im *IndexManager) HasIndex(field string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, exists := im.indexes[field]
	return exists
}

// --- MODIFIED: DataStore and InMemStore ---

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
	GetMany(keys []string) map[string][]byte
	Delete(key string)
	GetAll() map[string][]byte
	LoadData(data map[string][]byte)
	CleanExpiredItems() bool
	Size() int
	// Indexing interface methods.
	CreateIndex(field string)
	DeleteIndex(field string) // NEW
	ListIndexes() []string    // NEW
	HasIndex(field string) bool
	Lookup(field string, value any) ([]string, bool)
}

// InMemStore implements DataStore for in-memory storage, with sharding and indexing.
type InMemStore struct {
	shards    []*Shard
	numShards int
	indexes   *IndexManager // NEW: Index manager for the store.
}

// NewInMemStoreWithShards creates a new InMemStore with a specified number of shards.
func NewInMemStoreWithShards(numShards int) *InMemStore {
	s := &InMemStore{
		shards:    make([]*Shard, numShards),
		numShards: numShards,
		indexes:   NewIndexManager(), // NEW: Initialize the index manager.
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

// Set saves a key-value pair and updates any relevant indexes.
func (s *InMemStore) Set(key string, value []byte, ttl time.Duration) {
	shard := s.getShard(key)
	shard.mu.Lock()

	// MODIFIED: Handle index update.
	// Get old value for index removal before overwriting.
	oldItem, itemExists := shard.data[key]
	var oldData map[string]any
	if itemExists {
		oldData = tryUnmarshal(oldItem.Value)
	}

	shard.data[key] = Item{
		Value:     value,
		CreatedAt: time.Now(),
		TTL:       ttl,
	}
	shard.mu.Unlock() // Unlock early before index update.

	// Update indexes outside the shard lock.
	newData := tryUnmarshal(value)
	s.indexes.Update(key, oldData, newData)

	log.Printf("SET [Shard %d]: Key='%s', ValueLength=%d bytes, TTL=%s", s.getShardIndex(key), key, len(value), ttl)
}

// Get retrieves a value from the store by its key.
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
		return nil, false // Note: expired items are not proactively deleted here, but by CleanExpiredItems
	}

	log.Printf("GET [Shard %d]: Key='%s' (found, not expired)", s.getShardIndex(key), key)
	return item.Value, true
}

// GetMany retrieves multiple keys concurrently by grouping them by shard.
func (s *InMemStore) GetMany(keys []string) map[string][]byte {
	if len(keys) == 0 {
		return make(map[string][]byte)
	}

	// 1. Group keys by the shard they belong to.
	keysByShard := make([][]string, s.numShards)
	for _, key := range keys {
		h := fnv.New64a()
		h.Write([]byte(key))
		shardIndex := h.Sum64() % uint64(s.numShards)
		keysByShard[shardIndex] = append(keysByShard[shardIndex], key)
	}

	// 2. Prepare for concurrent fetching.
	resultsChan := make(chan map[string][]byte, s.numShards)
	var wg sync.WaitGroup

	now := time.Now()

	// 3. Launch a goroutine for each shard that has keys to fetch.
	for i, shardKeys := range keysByShard {
		if len(shardKeys) > 0 {
			wg.Add(1)
			go func(shardIndex int, keysInShard []string) {
				defer wg.Done()

				shard := s.shards[shardIndex]
				shardResults := make(map[string][]byte, len(keysInShard))

				shard.mu.RLock()
				for _, key := range keysInShard {
					if item, found := shard.data[key]; found {
						// Check TTL
						if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
							shardResults[key] = item.Value
						}
					}
				}
				shard.mu.RUnlock()

				resultsChan <- shardResults
			}(i, shardKeys)
		}
	}

	// 4. Wait for all goroutines to finish and close the channel.
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// 5. Combine results from all shards.
	finalResults := make(map[string][]byte, len(keys))
	for shardResults := range resultsChan {
		for key, value := range shardResults {
			finalResults[key] = value
		}
	}

	return finalResults
}

// Delete removes a key-value pair and updates any relevant indexes.
func (s *InMemStore) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()

	// MODIFIED: Get data for index removal before deleting.
	item, itemExists := shard.data[key]
	delete(shard.data, key)
	shard.mu.Unlock() // Unlock early.

	if itemExists {
		data := tryUnmarshal(item.Value)
		s.indexes.Remove(key, data)
	}

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
	// Note: A more advanced implementation would also remove expired items from indexes here.
	// For simplicity, we rely on the fact that Get/GetAll checks TTL, so expired
	// items won't be returned, and they'll be removed from indexes upon next Set/Delete.
	totalDeletedCount := 0
	now := time.Now()
	wasModified := false

	for i, shard := range s.shards {
		shard.mu.Lock()
		deletedInShard := 0
		for key, item := range shard.data {
			if item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
				// To correctly update indexes, we would need to unmarshal the value here.
				// This simplified version omits that for performance.
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

// --- Indexing method implementations for InMemStore ---

// CreateIndex creates an index on a field and backfills it with existing data.
func (s *InMemStore) CreateIndex(field string) {
	if s.HasIndex(field) {
		log.Printf("Index on field '%s' already exists.", field)
		return
	}

	s.indexes.CreateIndex(field)

	// Backfill the index with existing data.
	log.Printf("Backfilling index for field '%s'...", field)
	allData := s.GetAll()
	count := 0
	for key, value := range allData {
		data := tryUnmarshal(value)
		if data != nil {
			// Using the Update method with nil oldData to just add the new one.
			s.indexes.Update(key, nil, data)
			count++
		}
	}
	log.Printf("Index for field '%s' backfilled with %d items.", field, count)
}

// NEW: DeleteIndex removes an index from the store.
func (s *InMemStore) DeleteIndex(field string) {
	s.indexes.DeleteIndex(field)
}

// NEW: ListIndexes returns a list of indexed fields.
func (s *InMemStore) ListIndexes() []string {
	return s.indexes.ListIndexes()
}

// HasIndex checks if an index exists on a field.
func (s *InMemStore) HasIndex(field string) bool {
	return s.indexes.HasIndex(field)
}

// Lookup uses the index manager to find document keys.
func (s *InMemStore) Lookup(field string, value any) ([]string, bool) {
	return s.indexes.Lookup(field, value)
}

// CollectionPersister defines the interface for persistence operations specific to collections.
type CollectionPersister interface {
	SaveCollectionData(collectionName string, s DataStore) error
	DeleteCollectionFile(collectionName string) error
}

// saveTask encapsulates a request to save a collection.
type saveTask struct {
	collectionName string
	collection     DataStore
}

// deleteTask encapsulates a request to delete a collection file.
type deleteTask struct {
	collectionName string
}

// CollectionManager manages multiple named InMemStore instances, each representing a collection.
type CollectionManager struct {
	collections map[string]DataStore // Map of collection names to their DataStore instances.
	mu          sync.RWMutex         // Mutex to protect the 'collections' map.
	persister   CollectionPersister  // Interface for persistence operations.

	saveQueue   chan saveTask   // Channel to send save tasks to a goroutine.
	deleteQueue chan deleteTask // Channel to send delete tasks.
	quit        chan struct{}   // Channel to signal the goroutines to stop.
	wg          sync.WaitGroup  // WaitGroup to wait for goroutines to finish.
	numShards   int
}

// NewCollectionManager creates a new instance of CollectionManager.
func NewCollectionManager(persister CollectionPersister, numShards int) *CollectionManager {
	cm := &CollectionManager{
		collections: make(map[string]DataStore),
		persister:   persister,
		saveQueue:   make(chan saveTask, 100),
		deleteQueue: make(chan deleteTask, 10),
		quit:        make(chan struct{}),
		numShards:   numShards,
	}
	cm.StartAsyncWorker()
	return cm
}

// StartAsyncWorker launches a background goroutine to process tasks from both queues.
func (cm *CollectionManager) StartAsyncWorker() {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		log.Println("Async collection worker started.")
		for {
			select {
			case task, ok := <-cm.saveQueue:
				if !ok {
					// Channel is closed, stop.
					log.Println("Async save queue channel closed, stopping worker.")
					return
				}
				// Execute the save operation.
				if err := cm.persister.SaveCollectionData(task.collectionName, task.collection); err != nil {
					log.Printf("Error saving collection '%s' from async task: %v", task.collectionName, err)
				}
			case task, ok := <-cm.deleteQueue:
				if !ok {
					// Channel is closed, stop.
					log.Println("Async delete queue channel closed, stopping worker.")
					return
				}
				// Execute the delete operation.
				if err := cm.persister.DeleteCollectionFile(task.collectionName); err != nil {
					log.Printf("Error deleting collection file '%s' from async task: %v", task.collectionName, err)
				}
			case <-cm.quit:
				// Signal to stop and drain remaining tasks.
				log.Println("Async worker received quit signal. Draining queues...")
				// Draining the save queue.
				for len(cm.saveQueue) > 0 {
					task := <-cm.saveQueue
					if err := cm.persister.SaveCollectionData(task.collectionName, task.collection); err != nil {
						log.Printf("Error saving collection '%s' while draining save queue: %v", task.collectionName, err)
					}
				}
				// Draining the delete queue.
				for len(cm.deleteQueue) > 0 {
					task := <-cm.deleteQueue
					if err := cm.persister.DeleteCollectionFile(task.collectionName); err != nil {
						log.Printf("Error deleting collection file '%s' while draining delete queue: %v", task.collectionName, err)
					}
				}
				log.Println("Async collection worker stopped.")
				return
			}
		}
	}()
}

// Wait blocks until all outstanding tasks are complete and the worker stops.
func (cm *CollectionManager) Wait() {
	close(cm.quit)
	cm.wg.Wait()
}

// EnqueueSaveTask adds a collection save request to the asynchronous queue.
func (cm *CollectionManager) EnqueueSaveTask(collectionName string, col DataStore) {
	// Use a temporary `InMemStore` to get a consistent `GetAll()` snapshot.
	tempStore := NewInMemStoreWithShards(cm.numShards)
	tempStore.LoadData(col.GetAll())

	task := saveTask{
		collectionName: collectionName,
		collection:     tempStore,
	}
	select {
	case cm.saveQueue <- task:
		log.Printf("Save task for collection '%s' enqueued.", collectionName)
	default:
		log.Printf("Warning: Save queue for collection '%s' is full. Dropping task.", collectionName)
	}
}

// EnqueueDeleteTask adds a collection delete request to the asynchronous queue.
func (cm *CollectionManager) EnqueueDeleteTask(collectionName string) {
	task := deleteTask{
		collectionName: collectionName,
	}
	select {
	case cm.deleteQueue <- task:
		log.Printf("Delete task for collection '%s' enqueued.", collectionName)
	default:
		log.Printf("Warning: Delete queue for collection '%s' is full. Dropping task.", collectionName)
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
	newCol := NewInMemStoreWithShards(cm.numShards)
	cm.collections[name] = newCol
	log.Printf("Collection '%s' created with %d shards and added to CollectionManager.", name, cm.numShards)
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
		newCol := NewInMemStoreWithShards(cm.numShards) // Create a new InMemStore for this collection.
		newCol.LoadData(data)                           // Load data into this specific InMemStore.
		cm.collections[colName] = newCol                // Directly assign to the map.
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
	cm.mu.RLock()
	collectionsAndNames := make(map[string]DataStore, len(cm.collections))
	maps.Copy(collectionsAndNames, cm.collections)
	cm.mu.RUnlock()

	log.Println("TTL Cleaner (Collections): Starting sweep across all managed collections.")
	for name, col := range collectionsAndNames {
		if col.CleanExpiredItems() {
			cm.EnqueueSaveTask(name, col)
		}
	}
	log.Println("TTL Cleaner (Collections): Finished sweep across all managed collections.")
}
