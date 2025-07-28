package api

import (
	"fmt"
	"log"
	"net/http"
	"strings" // Required for path parsing
	"time"

	stdjson "encoding/json"
	"memory-tools/internal/store" // Import the store package

	jsoniter "github.com/json-iterator/go"
)

// Configure jsoniter for standard library compatibility.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// APIResponse defines the base structure for all JSON responses.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// SendJSONResponse is a helper function to send any JSON response.
func SendJSONResponse(w http.ResponseWriter, success bool, message string, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	resp := APIResponse{
		Success: success,
		Message: message,
		Data:    data,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// SetRequest is the structure for the request body of POST /set.
type SetRequest struct {
	Key        string             `json:"key"`
	Value      stdjson.RawMessage `json:"value"`
	TTLSeconds int64              `json:"ttl_seconds,omitempty"`
}

// Handlers struct groups our API handlers.
type Handlers struct {
	MainStore         store.DataStore
	CollectionManager *store.CollectionManager
}

// NewHandlers creates a new instance of Handlers.
func NewHandlers(mainS store.DataStore, cm *store.CollectionManager) *Handlers {
	return &Handlers{
		MainStore:         mainS,
		CollectionManager: cm,
	}
}

// SetHandler handles POST requests to save key-value pairs in the main in-memory store.
func (h *Handlers) SetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /set cancelled or timed out: %v", ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	if r.Method != http.MethodPost {
		SendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 50*1024*1024)
	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		log.Printf("Bad request: invalid JSON body or unknown fields: %v", err)
		SendJSONResponse(w, false, "Invalid JSON request body or unknown fields", nil, http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		log.Print("Bad request: key cannot be empty")
		SendJSONResponse(w, false, "Key cannot be empty", nil, http.StatusBadRequest)
		return
	}
	if len(req.Value) == 0 {
		log.Print("Bad request: value cannot be empty (e.g., use {} or [])")
		SendJSONResponse(w, false, "Value cannot be empty (e.g., use {} or [])", nil, http.StatusBadRequest)
		return
	}
	if !json.Valid(req.Value) {
		log.Printf("Bad request: 'value' field is not a valid JSON document: %s", string(req.Value))
		SendJSONResponse(w, false, "'value' field must be a valid JSON document", nil, http.StatusBadRequest)
		return
	}

	ttl := max(time.Duration(req.TTLSeconds)*time.Second, 0)

	h.MainStore.Set(req.Key, req.Value, ttl) // Operates on the main store
	SendJSONResponse(w, true, fmt.Sprintf("Data saved for Key='%s' in main store", req.Key), nil, http.StatusOK)
}

// GetHandler handles GET requests to retrieve a value from the main in-memory store.
func (h *Handlers) GetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /get cancelled or timed out: %v", ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	if r.Method != http.MethodGet {
		SendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		log.Print("Bad request: 'key' query parameter is required")
		SendJSONResponse(w, false, "'key' query parameter is required", nil, http.StatusBadRequest)
		return
	}

	valueBytes, ok := h.MainStore.Get(key) // Operates on the main store
	if !ok {
		log.Printf("Not found: Key='%s' not found or expired in main store", key)
		SendJSONResponse(w, false, fmt.Sprintf("Key '%s' not found or expired in main store", key), nil, http.StatusNotFound)
		return
	}

	responseData := map[string]stdjson.RawMessage{
		"key":   stdjson.RawMessage(fmt.Sprintf(`"%s"`, key)),
		"value": stdjson.RawMessage(valueBytes),
	}
	SendJSONResponse(w, true, "Data retrieved successfully from main store", responseData, http.StatusOK)
}

// --- New Collection CRUD Handlers (modified for net/http) ---

// getCollectionNameFromPath extracts the collection name from the request URL path.
func getCollectionNameFromPath(r *http.Request) string {
	path := strings.TrimPrefix(r.URL.Path, "/collections/")
	pathParts := strings.Split(path, "/")
	if len(pathParts) > 0 {
		// Ensure the first part is not empty.
		if pathParts[0] != "" {
			return pathParts[0]
		}
	}
	return ""
}

// CreateCollectionHandler handles POST /collections/{collectionName}.
func (h *Handlers) CreateCollectionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/create cancelled: %v", ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	collectionName := getCollectionNameFromPath(r) // Extract from path
	if collectionName == "" {
		SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
		return
	}

	// Ensures collection existence in memory and loads from disk if it exists.
	colStore := h.CollectionManager.GetCollection(collectionName)

	// Save the empty or newly loaded collection to disk immediately.
	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, colStore); err != nil {
		log.Printf("Error saving new/ensured collection '%s' to disk: %v", collectionName, err)
		SendJSONResponse(w, false, fmt.Sprintf("Failed to ensure collection '%s' persistence", collectionName), nil, http.StatusInternalServerError)
		return
	}

	SendJSONResponse(w, true, fmt.Sprintf("Collection '%s' ensured and persisted on disk.", collectionName), nil, http.StatusCreated)
}

// DeleteCollectionHandler handles DELETE /collections/{collectionName}.
func (h *Handlers) DeleteCollectionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/delete cancelled: %v", ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	collectionName := getCollectionNameFromPath(r) // Extract from path
	if collectionName == "" {
		SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
		return
	}

	// Delete from the manager's memory.
	h.CollectionManager.DeleteCollection(collectionName)

	// Delete the associated collection file from disk.
	if err := h.CollectionManager.DeleteCollectionFromDisk(collectionName); err != nil {
		log.Printf("Error deleting collection file for '%s': %v", collectionName, err)
		SendJSONResponse(w, false, fmt.Sprintf("Failed to delete collection '%s' from disk", collectionName), nil, http.StatusInternalServerError)
		return
	}

	SendJSONResponse(w, true, fmt.Sprintf("Collection '%s' deleted from memory and disk.", collectionName), nil, http.StatusOK)
}

// ListCollectionsHandler handles GET /collections.
func (h *Handlers) ListCollectionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/list all cancelled: %v", ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	collectionNames := h.CollectionManager.ListCollections()
	SendJSONResponse(w, true, "Collections retrieved successfully", collectionNames, http.StatusOK)
}

// SetCollectionItemHandler handles POST /collections/{collectionName}/set.
func (h *Handlers) SetCollectionItemHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	collectionName := getCollectionNameFromPath(r) // Extract from path
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/%s/set cancelled: %v", collectionName, ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	if r.Method != http.MethodPost {
		SendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	if collectionName == "" {
		SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
		return
	}

	collectionStore := h.CollectionManager.GetCollection(collectionName)

	r.Body = http.MaxBytesReader(w, r.Body, 50*1024*1024)
	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		log.Printf("Bad request for collection '%s': invalid JSON body: %v", collectionName, err)
		SendJSONResponse(w, false, "Invalid JSON request body or unknown fields", nil, http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		log.Printf("Bad request for collection '%s': key cannot be empty", collectionName)
		SendJSONResponse(w, false, "Key cannot be empty", nil, http.StatusBadRequest)
		return
	}
	if len(req.Value) == 0 {
		log.Printf("Bad request for collection '%s': value cannot be empty (e.g., use {} or [])", collectionName)
		SendJSONResponse(w, false, "Value cannot be empty (e.g., use {} or [])", nil, http.StatusBadRequest)
		return
	}
	if !json.Valid(req.Value) {
		log.Printf("Bad request for collection '%s': 'value' field is not a valid JSON document: %s", collectionName, string(req.Value))
		SendJSONResponse(w, false, "'value' field must be a valid JSON document", nil, http.StatusBadRequest)
		return
	}

	ttl := max(time.Duration(req.TTLSeconds)*time.Second, 0)

	collectionStore.Set(req.Key, req.Value, ttl)

	// Save the collection to disk after a SET operation.
	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, collectionStore); err != nil {
		log.Printf("Error saving collection '%s' to disk after SET operation: %v", collectionName, err)
	}

	SendJSONResponse(w, true, fmt.Sprintf("Data saved for Key='%s' in collection '%s'", req.Key, collectionName), nil, http.StatusOK)
}

// GetCollectionItemHandler handles GET /collections/{collectionName}/get?key=...
func (h *Handlers) GetCollectionItemHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	collectionName := getCollectionNameFromPath(r) // Extract from path
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/%s/get cancelled: %v", collectionName, ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	if r.Method != http.MethodGet {
		SendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	if collectionName == "" {
		SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		log.Printf("Bad request for collection '%s': 'key' query parameter is required", collectionName)
		SendJSONResponse(w, false, "'key' query parameter is required", nil, http.StatusBadRequest)
		return
	}

	collectionStore := h.CollectionManager.GetCollection(collectionName)
	valueBytes, ok := collectionStore.Get(key)
	if !ok {
		log.Printf("Not found for collection '%s': Key='%s' not found or expired", collectionName, key)
		SendJSONResponse(w, false, fmt.Sprintf("Key '%s' not found or expired in collection '%s'", key, collectionName), nil, http.StatusNotFound)
		return
	}

	responseData := map[string]stdjson.RawMessage{
		"key":   stdjson.RawMessage(fmt.Sprintf(`"%s"`, key)),
		"value": stdjson.RawMessage(valueBytes),
	}
	SendJSONResponse(w, true, "Data retrieved successfully", responseData, http.StatusOK)
}

// DeleteCollectionItemHandler handles DELETE /collections/{collectionName}/delete?key=...
func (h *Handlers) DeleteCollectionItemHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	collectionName := getCollectionNameFromPath(r) // Extract from path
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/%s/delete cancelled: %v", collectionName, ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	if r.Method != http.MethodDelete {
		SendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	if collectionName == "" {
		SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		log.Printf("Bad request for collection '%s': 'key' query parameter is required for delete", collectionName)
		SendJSONResponse(w, false, "'key' query parameter is required", nil, http.StatusBadRequest)
		return
	}

	collectionStore := h.CollectionManager.GetCollection(collectionName)
	collectionStore.Delete(key)

	// Save the collection to disk after a DELETE operation.
	if err := h.CollectionManager.SaveCollectionToDisk(collectionName, collectionStore); err != nil {
		log.Printf("Error saving collection '%s' to disk after DELETE operation: %v", collectionName, err)
	}

	SendJSONResponse(w, true, fmt.Sprintf("Key '%s' deleted from collection '%s'", key, collectionName), nil, http.StatusOK)
}

// ListCollectionItemsHandler handles GET /collections/{collectionName}/list.
func (h *Handlers) ListCollectionItemsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	collectionName := getCollectionNameFromPath(r) // Extract from path
	select {
	case <-ctx.Done():
		log.Printf("Request /collections/%s/list cancelled: %v", collectionName, ctx.Err())
		SendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
	}

	if r.Method != http.MethodGet {
		SendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	if collectionName == "" {
		SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
		return
	}

	// Check if the collection exists before retrieving items
	if !h.CollectionManager.CollectionExists(collectionName) { // New check
		log.Printf("Collection '%s' not found for listing items.", collectionName)
		SendJSONResponse(w, false, fmt.Sprintf("Collection '%s' not found.", collectionName), nil, http.StatusNotFound)
		return
	}

	collectionStore := h.CollectionManager.GetCollection(collectionName)
	allData := collectionStore.GetAll() // GetAll returns non-expired items

	responseMap := make(map[string]stdjson.RawMessage)
	for k, v := range allData {
		responseMap[k] = stdjson.RawMessage(v)
	}

	SendJSONResponse(w, true, fmt.Sprintf("Items from collection '%s' retrieved successfully", collectionName), responseMap, http.StatusOK)
}

// LogRequest is a middleware for logging incoming HTTP requests.
func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("Request: Method='%s', Path='%s', Duration='%s'", r.Method, r.URL.Path, time.Since(start))
	})
}
