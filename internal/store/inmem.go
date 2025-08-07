package store

import (
	"bytes"
	"hash/fnv"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/google/btree"
	jsoniter "github.com/json-iterator/go"
)

// tryUnmarshal unmarshals a byte slice into a map.
// It ensures that all numbers from JSON are converted to float64 for consistent indexing.
func tryUnmarshal(value []byte) map[string]any {
	var data map[string]any

	// Use a decoder that treats all numbers as json.Number first.
	decoder := jsoniter.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return nil // Not a JSON object, cannot be indexed.
	}

	// Post-process to convert json.Number to float64.
	for k, v := range data {
		if num, ok := v.(jsoniter.Number); ok {
			if f, err := num.Float64(); err == nil {
				data[k] = f
			} else {
				// If it fails to be a float, store it as a string.
				data[k] = num.String()
			}
		}
	}
	return data
}

// valueToFloat64 is a helper to safely convert various numeric types to float64.
func valueToFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case jsoniter.Number:
		f, err := val.Float64()
		return f, err == nil
	case string:
		// Also try to parse a string that might represent a number.
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// --- NEW: B-Tree Indexing Structures ---

const btreeDegree = 32 // Degree of the B-Tree, can be tuned for performance.

// NumericKey implements the item for the numeric B-Tree.
// It holds a float64 value and the set of associated document keys.
type NumericKey struct {
	Value float64
	Keys  map[string]struct{}
}

// StringKey implements the item for the string B-Tree.
type StringKey struct {
	Value string
	Keys  map[string]struct{}
}

// --- FIX: Create LessFunc functions instead of methods ---

// numericLess provides the comparison logic for NumericKey items.
func numericLess(a, b NumericKey) bool {
	return a.Value < b.Value
}

// stringLess provides the comparison logic for StringKey items.
func stringLess(a, b StringKey) bool {
	return a.Value < b.Value
}

// Index now contains two B-Trees, one for each supported data type.
type Index struct {
	numericTree *btree.BTreeG[NumericKey]
	stringTree  *btree.BTreeG[StringKey]
}

// NewIndex creates a new index structure with initialized B-Trees.
func NewIndex() *Index {
	return &Index{
		// --- FIX: Pass the LessFunc as the second argument to NewG ---
		numericTree: btree.NewG[NumericKey](btreeDegree, numericLess),
		stringTree:  btree.NewG[StringKey](btreeDegree, stringLess),
	}
}

// --- REWRITTEN: IndexManager for B-Trees ---

// IndexManager manages all indexes for a single InMemStore.
type IndexManager struct {
	mu      sync.RWMutex
	indexes map[string]*Index // map[fieldName] -> *Index
}

// NewIndexManager creates a new index manager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
	}
}

// CreateIndex initializes a new B-Tree index for a given field.
func (im *IndexManager) CreateIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; !exists {
		im.indexes[field] = NewIndex()
		slog.Info("B-Tree Index created", "field", field)
	}
}

// DeleteIndex removes an index for a given field.
func (im *IndexManager) DeleteIndex(field string) {
	im.mu.Lock()
	defer im.mu.Unlock()
	if _, exists := im.indexes[field]; exists {
		delete(im.indexes, field)
		slog.Info("Index deleted", "field", field)
	}
}

// ListIndexes returns the names of all indexed fields.
func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	indexedFields := make([]string, 0, len(im.indexes))
	for field := range im.indexes {
		indexedFields = append(indexedFields, field)
	}
	return indexedFields
}

// addToIndex adds a document key to an index for a specific value.
func (im *IndexManager) addToIndex(index *Index, docKey string, value any) {
	if fVal, ok := valueToFloat64(value); ok {
		key := NumericKey{Value: fVal}
		item, found := index.numericTree.Get(key)
		if !found {
			item = NumericKey{Value: fVal, Keys: make(map[string]struct{})}
		}
		item.Keys[docKey] = struct{}{}
		index.numericTree.ReplaceOrInsert(item)
	} else if sVal, ok := value.(string); ok {
		key := StringKey{Value: sVal}
		item, found := index.stringTree.Get(key)
		if !found {
			item = StringKey{Value: sVal, Keys: make(map[string]struct{})}
		}
		item.Keys[docKey] = struct{}{}
		index.stringTree.ReplaceOrInsert(item)
	}
}

// removeFromIndex removes a document key from an index.
func (im *IndexManager) removeFromIndex(index *Index, docKey string, value any) {
	if fVal, ok := valueToFloat64(value); ok {
		key := NumericKey{Value: fVal}
		if item, found := index.numericTree.Get(key); found {
			delete(item.Keys, docKey)
			if len(item.Keys) == 0 {
				// If no more documents are associated with this value, remove it from the B-Tree.
				index.numericTree.Delete(item)
			} else {
				// Otherwise, update the item in the tree.
				index.numericTree.ReplaceOrInsert(item)
			}
		}
	} else if sVal, ok := value.(string); ok {
		key := StringKey{Value: sVal}
		if item, found := index.stringTree.Get(key); found {
			delete(item.Keys, docKey)
			if len(item.Keys) == 0 {
				index.stringTree.Delete(item)
			} else {
				index.stringTree.ReplaceOrInsert(item)
			}
		}
	}
}

// Update updates the indexes for a given document.
func (im *IndexManager) Update(docKey string, oldData, newData map[string]any) {
	im.mu.Lock()
	defer im.mu.Unlock()

	if len(im.indexes) == 0 {
		return
	}

	for field, index := range im.indexes {
		oldVal, oldOk := oldData[field]
		newVal, newOk := newData[field]

		if oldOk && newOk && oldVal == newVal {
			continue // No change in the indexed field.
		}

		if oldOk {
			im.removeFromIndex(index, docKey, oldVal)
		}
		if newOk {
			im.addToIndex(index, docKey, newVal)
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
			im.removeFromIndex(index, docKey, val)
		}
	}
}

// Lookup performs an equality lookup on an index.
func (im *IndexManager) Lookup(field string, value any) ([]string, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

	var foundKeys map[string]struct{}
	if fVal, ok := valueToFloat64(value); ok {
		if item, found := index.numericTree.Get(NumericKey{Value: fVal}); found {
			foundKeys = item.Keys
		}
	} else if sVal, ok := value.(string); ok {
		if item, found := index.stringTree.Get(StringKey{Value: sVal}); found {
			foundKeys = item.Keys
		}
	}

	if foundKeys == nil {
		return []string{}, true // Value not found in index.
	}

	keys := make([]string, 0, len(foundKeys))
	for k := range foundKeys {
		keys = append(keys, k)
	}
	return keys, true
}

// LookupRange performs a range scan on a B-Tree index.
func (im *IndexManager) LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	index, exists := im.indexes[field]
	if !exists {
		return nil, false
	}

	unionKeys := make(map[string]struct{})

	// Determine if we should query the numeric or string tree.
	var isNumericQuery bool
	if low != nil {
		if _, ok := valueToFloat64(low); ok {
			isNumericQuery = true
		}
	} else if high != nil {
		if _, ok := valueToFloat64(high); ok {
			isNumericQuery = true
		}
	}

	if isNumericQuery {
		var lowKey, highKey NumericKey
		hasLowBound, hasHighBound := low != nil, high != nil
		if hasLowBound {
			lowKey.Value, _ = valueToFloat64(low)
		}
		if hasHighBound {
			highKey.Value, _ = valueToFloat64(high)
		}

		iterator := func(item NumericKey) bool {
			// Check upper bound
			if hasHighBound {
				if item.Value > highKey.Value {
					return false // Stop iteration
				}
				if !highInclusive && item.Value == highKey.Value {
					return false // Stop iteration
				}
			}
			// Add keys to the result set
			for k := range item.Keys {
				unionKeys[k] = struct{}{}
			}
			return true
		}

		startKey := lowKey
		if !hasLowBound {
			if minItem, ok := index.numericTree.Min(); ok {
				startKey = minItem
			} else {
				return []string{}, true // Tree is empty
			}
		}

		index.numericTree.AscendGreaterOrEqual(startKey, iterator)

		// Handle non-inclusive lower bound by removing keys from the startKey.
		if hasLowBound && !lowInclusive {
			if item, found := index.numericTree.Get(lowKey); found {
				for k := range item.Keys {
					delete(unionKeys, k)
				}
			}
		}

	} else {
		// Logic for string range scan
		var lowKey, highKey StringKey
		hasLowBound, hasHighBound := low != nil, high != nil
		if hasLowBound {
			lowKey.Value, _ = low.(string)
		}
		if hasHighBound {
			highKey.Value, _ = high.(string)
		}

		iterator := func(item StringKey) bool {
			if hasHighBound {
				if item.Value > highKey.Value {
					return false
				}
				if !highInclusive && item.Value == highKey.Value {
					return false
				}
			}
			for k := range item.Keys {
				unionKeys[k] = struct{}{}
			}
			return true
		}

		startKey := lowKey
		if !hasLowBound {
			if minItem, ok := index.stringTree.Min(); ok {
				startKey = minItem
			} else {
				return []string{}, true // Tree is empty
			}
		}

		index.stringTree.AscendGreaterOrEqual(startKey, iterator)

		if hasLowBound && !lowInclusive {
			if item, found := index.stringTree.Get(lowKey); found {
				for k := range item.Keys {
					delete(unionKeys, k)
				}
			}
		}
	}

	finalKeys := make([]string, 0, len(unionKeys))
	for k := range unionKeys {
		finalKeys = append(finalKeys, k)
	}
	return finalKeys, true
}

// HasIndex checks if an index exists for a given field.
func (im *IndexManager) HasIndex(field string) bool {
	im.mu.RLock()
	defer im.mu.RUnlock()
	_, exists := im.indexes[field]
	return exists
}

// Item represents an individual key-value entry.
type Item struct {
	Value     []byte
	CreatedAt time.Time
	TTL       time.Duration // 0 means no expiration.
}

// Shard represents a segment of the in-memory store.
type Shard struct {
	data map[string]Item
	mu   sync.RWMutex
}

// --- UPDATED: DataStore Interface ---
type DataStore interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	GetMany(keys []string) map[string][]byte
	Delete(key string)
	GetAll() map[string][]byte
	LoadData(data map[string][]byte)
	CleanExpiredItems() bool
	Size() int

	// Indexing interface methods
	CreateIndex(field string)
	DeleteIndex(field string)
	ListIndexes() []string
	HasIndex(field string) bool
	Lookup(field string, value any) ([]string, bool)
	// NEW METHOD IN THE INTERFACE!
	LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool)
}

// InMemStore implements DataStore for in-memory storage, with sharding and indexing.
type InMemStore struct {
	shards    []*Shard
	numShards int
	indexes   *IndexManager
}

// NewInMemStoreWithShards creates a new InMemStore with a specified number of shards.
func NewInMemStoreWithShards(numShards int) *InMemStore {
	s := &InMemStore{
		shards:    make([]*Shard, numShards),
		numShards: numShards,
		indexes:   NewIndexManager(),
	}
	for i := range numShards {
		s.shards[i] = &Shard{
			data: make(map[string]Item),
		}
	}
	slog.Info("InMemStore initialized", "num_shards", numShards)
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
// Definitive and Corrected Version of InMemStore.Set
func (s *InMemStore) Set(key string, value []byte, ttl time.Duration) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 1. Capture the old state BEFORE any modifications.
	//    This is crucial so the index update knows what to delete.
	var oldDataForIndex map[string]any
	var oldCreatedAt time.Time
	isUpdate := false
	if oldItem, exists := shard.data[key]; exists {
		oldDataForIndex = tryUnmarshal(oldItem.Value)
		oldCreatedAt = oldItem.CreatedAt // We preserve the original creation date on an update.
		isUpdate = true
	}

	// 2. Prepare the new item's data.
	//    If it's an update, use the original creation date.
	newItemCreatedAt := time.Now()
	if isUpdate {
		newItemCreatedAt = oldCreatedAt
	}

	newItem := Item{
		Value:     value,
		CreatedAt: newItemCreatedAt,
		TTL:       ttl,
	}

	// 3. Commit the new item to the main data store.
	//    Now, shard.data[] holds the most recent information.
	shard.data[key] = newItem

	// 4. Finally, update the index to reflect the change.
	newDataForIndex := tryUnmarshal(value)
	if oldDataForIndex != nil || newDataForIndex != nil {
		s.indexes.Update(key, oldDataForIndex, newDataForIndex)
	}

	slog.Debug("Item set", "shard_id", s.getShardIndex(key), "key", key, "is_update", isUpdate)
}

// Get retrieves a value from the store by its key.
func (s *InMemStore) Get(key string) ([]byte, bool) {
	// 1. Correctly identifies the shard for the key.
	shard := s.getShard(key)
	// 2. Uses a Read Lock, which is optimal for performance.
	shard.mu.RLock()
	// 3. Ensures the lock is always released.
	defer shard.mu.RUnlock()

	// 4. Performs an efficient and standard map lookup.
	item, found := shard.data[key]
	if !found {
		// Correctly handles the "not found" case.
		slog.Debug("Item get", "shard_id", s.getShardIndex(key), "key", key, "status", "not_found")
		return nil, false
	}

	// 5. Implements a robust TTL check.
	if item.TTL > 0 && time.Since(item.CreatedAt) > item.TTL {
		// Correctly treats an expired item as "not found".
		slog.Debug("Item get", "shard_id", s.getShardIndex(key), "key", key, "status", "expired")
		return nil, false
	}

	// 6. Returns the value if found and not expired.
	slog.Debug("Item get", "shard_id", s.getShardIndex(key), "key", key, "status", "found")
	return item.Value, true
}

// GetMany retrieves multiple keys concurrently by grouping them by shard.
func (s *InMemStore) GetMany(keys []string) map[string][]byte {
	if len(keys) == 0 {
		return make(map[string][]byte)
	}

	keysByShard := make([][]string, s.numShards)
	for _, key := range keys {
		h := fnv.New64a()
		h.Write([]byte(key))
		shardIndex := h.Sum64() % uint64(s.numShards)
		keysByShard[shardIndex] = append(keysByShard[shardIndex], key)
	}

	resultsChan := make(chan map[string][]byte, s.numShards)
	var wg sync.WaitGroup
	now := time.Now()

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

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	finalResults := make(map[string][]byte, len(keys))
	for shardResults := range resultsChan {
		maps.Copy(finalResults, shardResults)
	}

	return finalResults
}

// Delete removes a key-value pair and updates any relevant indexes.
func (s *InMemStore) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()

	var data map[string]any
	if item, exists := shard.data[key]; exists {
		data = tryUnmarshal(item.Value)
	}
	delete(shard.data, key)
	shard.mu.Unlock()

	if data != nil {
		s.indexes.Remove(key, data)
	}

	slog.Debug("Item deleted", "shard_id", s.getShardIndex(key), "key", key)
}

// GetAll returns a copy of all non-expired data from ALL shards for persistence.
func (s *InMemStore) GetAll() map[string][]byte {
	snapshotData := make(map[string][]byte)
	now := time.Now()

	for _, shard := range s.shards {
		shard.mu.RLock()
		for k, item := range shard.data {
			if item.TTL == 0 || now.Before(item.CreatedAt.Add(item.TTL)) {
				copyValue := make([]byte, len(item.Value))
				copy(copyValue, item.Value)
				snapshotData[k] = copyValue
			}
		}
		shard.mu.RUnlock()
	}
	slog.Debug("Snapshot data combined", "num_shards", s.numShards, "total_items", len(snapshotData))
	return snapshotData
}

// LoadData loads data into the store across its shards from a persistent source.
func (s *InMemStore) LoadData(data map[string][]byte) {
	for _, shard := range s.shards {
		shard.mu.Lock()
		shard.data = make(map[string]Item)
		shard.mu.Unlock()
	}
	slog.Info("All shards cleared for data load")

	for k, v := range data {
		shard := s.getShard(k)
		shard.mu.Lock()
		shard.data[k] = Item{
			Value:     v,
			CreatedAt: time.Now(), // Assume loaded items are "created" at load time.
			TTL:       0,          // Loaded items have no TTL by default.
		}
		shard.mu.Unlock()
	}
	slog.Info("Data loaded into shards", "num_shards", s.numShards, "total_keys", len(data))
}

// CleanExpiredItems iterates through each shard and physically deletes expired items.
func (s *InMemStore) CleanExpiredItems() bool {
	totalDeletedCount := 0
	now := time.Now()
	wasModified := false

	for i, shard := range s.shards {
		shard.mu.Lock()
		deletedInShard := 0
		for key, item := range shard.data {
			if item.TTL > 0 && now.After(item.CreatedAt.Add(item.TTL)) {
				// To avoid re-locking, we must remove from index here
				data := tryUnmarshal(item.Value)
				if data != nil {
					s.indexes.Remove(key, data)
				}
				delete(shard.data, key)
				deletedInShard++
				wasModified = true
			}
		}
		shard.mu.Unlock()

		if deletedInShard > 0 {
			totalDeletedCount += deletedInShard
			slog.Info("TTL cleaner removed expired items from shard", "shard_id", i, "count", deletedInShard)
		}
	}

	if totalDeletedCount > 0 {
		slog.Info("TTL cleaner finished run", "total_removed", totalDeletedCount)
	} else {
		slog.Debug("TTL cleaner run complete: no items to remove")
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
		slog.Debug("Index creation skipped: already exists", "field", field)
		return
	}
	s.indexes.CreateIndex(field)

	slog.Info("Backfilling index", "field", field)
	allData := s.GetAll()
	count := 0
	for key, value := range allData {
		if data := tryUnmarshal(value); data != nil {
			s.indexes.Update(key, nil, data)
			count++
		}
	}
	slog.Info("Index backfill complete", "field", field, "item_count", count)
}

// DeleteIndex removes an index from the store.
func (s *InMemStore) DeleteIndex(field string) {
	s.indexes.DeleteIndex(field)
}

// ListIndexes returns a list of indexed fields.
func (s *InMemStore) ListIndexes() []string {
	return s.indexes.ListIndexes()
}

// HasIndex checks if an index exists on a field.
func (s *InMemStore) HasIndex(field string) bool {
	return s.indexes.HasIndex(field)
}

// Lookup uses the index manager to find document keys for an exact value.
func (s *InMemStore) Lookup(field string, value any) ([]string, bool) {
	return s.indexes.Lookup(field, value)
}

// LookupRange uses the index manager to find document keys within a range.
func (s *InMemStore) LookupRange(field string, low, high any, lowInclusive, highInclusive bool) ([]string, bool) {
	return s.indexes.LookupRange(field, low, high, lowInclusive, highInclusive)
}

// --- The rest of the file (CollectionManager, etc.) does not need changes ---

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
	collections map[string]DataStore
	mu          sync.RWMutex
	persister   CollectionPersister
	saveQueue   chan saveTask
	deleteQueue chan deleteTask
	quit        chan struct{}
	wg          sync.WaitGroup
	numShards   int
	fileLocks   map[string]*sync.Mutex
	fileLocksMu sync.RWMutex
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
		fileLocks:   make(map[string]*sync.Mutex),
	}
	cm.StartAsyncWorker()
	return cm
}

// StartAsyncWorker launches a background goroutine to process tasks from both queues.
func (cm *CollectionManager) StartAsyncWorker() {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		slog.Info("Async collection worker started.")
		for {
			select {
			case task, ok := <-cm.saveQueue:
				if !ok {
					slog.Info("Async save queue closed, stopping worker.")
					return
				}
				fileLock := cm.GetFileLock(task.collectionName)
				fileLock.Lock()
				if err := cm.persister.SaveCollectionData(task.collectionName, task.collection); err != nil {
					slog.Error("Error saving collection from async task", "collection", task.collectionName, "error", err)
				}
				fileLock.Unlock()

			case task, ok := <-cm.deleteQueue:
				if !ok {
					slog.Info("Async delete queue closed, stopping worker.")
					return
				}
				fileLock := cm.GetFileLock(task.collectionName)
				fileLock.Lock()
				if err := cm.persister.DeleteCollectionFile(task.collectionName); err != nil {
					slog.Error("Error deleting collection file from async task", "collection", task.collectionName, "error", err)
				}
				fileLock.Unlock()

			case <-cm.quit:
				slog.Info("Async worker received quit signal. Draining queues...")
				for len(cm.saveQueue) > 0 {
					task := <-cm.saveQueue
					fileLock := cm.GetFileLock(task.collectionName)
					fileLock.Lock()
					if err := cm.persister.SaveCollectionData(task.collectionName, task.collection); err != nil {
						slog.Error("Error saving collection while draining save queue", "collection", task.collectionName, "error", err)
					}
					fileLock.Unlock()
				}
				for len(cm.deleteQueue) > 0 {
					task := <-cm.deleteQueue
					fileLock := cm.GetFileLock(task.collectionName)
					fileLock.Lock()
					if err := cm.persister.DeleteCollectionFile(task.collectionName); err != nil {
						slog.Error("Error deleting collection file while draining delete queue", "collection", task.collectionName, "error", err)
					}
					fileLock.Unlock()
				}
				slog.Info("Async collection worker stopped.")
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
	// Create a temporary snapshot of the collection data to be saved.
	// This avoids holding a lock on the main collection while I/O happens.
	tempStore := NewInMemStoreWithShards(cm.numShards)
	tempStore.LoadData(col.GetAll())

	originalIndexes := col.ListIndexes()
	if len(originalIndexes) > 0 {

		for _, fieldName := range originalIndexes {
			tempStore.CreateIndex(fieldName)
		}
	}

	task := saveTask{
		collectionName: collectionName,
		collection:     tempStore,
	}
	select {
	case cm.saveQueue <- task:
		slog.Debug("Save task enqueued", "collection", collectionName)
	default:
		slog.Warn("Save queue is full, dropping task", "collection", collectionName)
	}
}

// EnqueueDeleteTask adds a collection delete request to the asynchronous queue.
func (cm *CollectionManager) EnqueueDeleteTask(collectionName string) {
	task := deleteTask{
		collectionName: collectionName,
	}
	select {
	case cm.deleteQueue <- task:
		slog.Debug("Delete task enqueued", "collection", collectionName)
	default:
		slog.Warn("Delete queue is full, dropping task", "collection", collectionName)
	}
}

// GetCollection retrieves an existing collection (InMemStore) by name, or creates a new one.
func (cm *CollectionManager) GetCollection(name string) DataStore {
	cm.mu.RLock()
	col, found := cm.collections[name]
	cm.mu.RUnlock()
	if found {
		return col
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check in case another goroutine created it while we waited for the lock.
	col, found = cm.collections[name]
	if found {
		return col
	}

	newCol := NewInMemStoreWithShards(cm.numShards)
	cm.collections[name] = newCol
	slog.Info("Collection created", "name", name, "num_shards", cm.numShards)
	return newCol
}

// DeleteCollection removes a collection entirely from the manager.
func (cm *CollectionManager) DeleteCollection(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, exists := cm.collections[name]; exists {
		delete(cm.collections, name)
		slog.Info("Collection deleted from memory", "name", name)
	} else {
		slog.Warn("Attempted to delete non-existent collection", "name", name)
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
	slog.Debug("Listing collections", "count", len(names))
	return names
}

// CollectionExists checks if a collection with the given name exists in the manager.
func (cm *CollectionManager) CollectionExists(name string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, exists := cm.collections[name]
	return exists
}

// GetAllCollectionsDataForPersistence gets data from all managed InMemStore instances for persistence.
func (cm *CollectionManager) GetAllCollectionsDataForPersistence() map[string]map[string][]byte {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	dataToSave := make(map[string]map[string][]byte)
	for colName, col := range cm.collections {
		dataToSave[colName] = col.GetAll()
	}
	slog.Debug("Retrieved all collections data for persistence", "collection_count", len(dataToSave))
	return dataToSave
}

// CleanExpiredItemsAndSave triggers TTL cleanup on all managed collections.
func (cm *CollectionManager) CleanExpiredItemsAndSave() {
	cm.mu.RLock()
	// Create a copy of the collections map to avoid holding the lock during the long-running loop.
	collectionsAndNames := make(map[string]DataStore, len(cm.collections))
	maps.Copy(collectionsAndNames, cm.collections)
	cm.mu.RUnlock()

	slog.Info("Starting TTL sweep across all collections")
	for name, col := range collectionsAndNames {
		if col.CleanExpiredItems() {
			cm.EnqueueSaveTask(name, col)
		}
	}
	slog.Info("Finished TTL sweep across all collections")
}

// EvictColdData iterates over all collections and removes "cold" data from RAM.
func (cm *CollectionManager) EvictColdData(threshold time.Time) {
	cm.mu.RLock()
	// Copy the map to avoid holding the lock during the operation.
	collectionsToClean := make(map[string]DataStore, len(cm.collections))
	maps.Copy(collectionsToClean, cm.collections)
	cm.mu.RUnlock()

	for name, col := range collectionsToClean {
		// Delegate the eviction logic to the collection itself (InMemStore).
		if inMemStore, ok := col.(*InMemStore); ok {
			inMemStore.EvictColdData(name, threshold)
		}
	}
}

// EvictColdData iterates through all shards and removes items that have become "cold".
func (s *InMemStore) EvictColdData(collectionName string, threshold time.Time) {
	totalEvicted := 0
	for i, shard := range s.shards {
		shard.mu.Lock()
		evictedInShard := 0
		for key, item := range shard.data {
			var doc map[string]any
			if err := jsoniter.Unmarshal(item.Value, &doc); err != nil {
				continue
			}

			createdAtStr, ok := doc["created_at"].(string)
			if !ok {
				continue
			}

			createdAt, err := time.Parse(time.RFC3339, createdAtStr)
			if err != nil {
				continue
			}

			if createdAt.Before(threshold) {
				// Data is cold, remove it from RAM and from the indexes.
				s.indexes.Remove(key, doc)
				delete(shard.data, key)
				evictedInShard++
			}
		}
		shard.mu.Unlock()
		if evictedInShard > 0 {
			totalEvicted += evictedInShard
			slog.Debug("Evicted cold items from shard", "collection", collectionName, "shard_id", i, "count", evictedInShard)
		}
	}
	if totalEvicted > 0 {
		slog.Info("Finished evicting cold data from collection", "collection", collectionName, "total_evicted", totalEvicted)
	}
}

func (cm *CollectionManager) GetFileLock(collectionName string) *sync.Mutex {
	cm.fileLocksMu.RLock()
	lock, exists := cm.fileLocks[collectionName]
	cm.fileLocksMu.RUnlock()

	if exists {
		return lock
	}

	cm.fileLocksMu.Lock()
	defer cm.fileLocksMu.Unlock()
	// Double-check in case another goroutine created it while we waited for the lock
	lock, exists = cm.fileLocks[collectionName]
	if exists {
		return lock
	}

	newLock := &sync.Mutex{}
	cm.fileLocks[collectionName] = newLock
	return newLock
}
