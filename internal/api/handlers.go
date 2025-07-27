package api

import (
	"fmt"
	"log"
	"net/http"
	"time"

	stdjson "encoding/json"       // Import standard json as 'stdjson' to access RawMessage
	"memory-tools/internal/store" // Updated store package interface

	jsoniter "github.com/json-iterator/go" // Import jsoniter
)

// Configure jsoniter to be compatible with the standard library's behavior.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// APIResponse defines the base structure for all JSON responses.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"` // Message is optional for success, but good for errors.
	Data    interface{} `json:"data,omitempty"`    // Generic data field for successful responses.
}

// sendJSONResponse is a helper function to send any JSON response.
func sendJSONResponse(w http.ResponseWriter, success bool, message string, data interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	resp := APIResponse{
		Success: success,
		Message: message,
		Data:    data,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
		// Fallback to text error if JSON encoding fails for some reason
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// SetRequest is the structure for the request body of POST /set.
// 'Value' now uses stdjson.RawMessage from the standard library's json package.
type SetRequest struct {
	Key        string             `json:"key"`
	Value      stdjson.RawMessage `json:"value"`                 // Use stdjson.RawMessage
	TTLSeconds int64              `json:"ttl_seconds,omitempty"` // New optional field for TTL
}

// Handlers struct groups our API handlers and the DataStore.
// Using an interface (store.DataStore) promotes loose coupling and testability.
type Handlers struct {
	Store store.DataStore
}

// NewHandlers creates a new instance of Handlers with the provided DataStore.
func NewHandlers(s store.DataStore) *Handlers {
	return &Handlers{Store: s}
}

// SetHandler handles POST requests to save key-value pairs with JSON values and optional TTL.
func (h *Handlers) SetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /set cancelled or timed out: %v", ctx.Err())
		sendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
		// Continue with handler logic
	}

	if r.Method != http.MethodPost {
		sendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 50*1024*1024) // Limit body to 50 MB
	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		log.Printf("Bad request: invalid JSON body or unknown fields: %v", err)
		sendJSONResponse(w, false, "Invalid JSON request body or unknown fields", nil, http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		log.Print("Bad request: key cannot be empty")
		sendJSONResponse(w, false, "Key cannot be empty", nil, http.StatusBadRequest)
		return
	}
	if len(req.Value) == 0 {
		log.Print("Bad request: value cannot be empty (e.g., use {} or [])")
		sendJSONResponse(w, false, "Value cannot be empty (e.g., use {} or [])", nil, http.StatusBadRequest)
		return
	}
	if !json.Valid(req.Value) {
		log.Printf("Bad request: 'value' field is not a valid JSON document: %s", string(req.Value))
		sendJSONResponse(w, false, "'value' field must be a valid JSON document", nil, http.StatusBadRequest)
		return
	}

	ttl := max(time.Duration(req.TTLSeconds)*time.Second, 0)

	h.Store.Set(req.Key, req.Value, ttl)
	// Success response for SetHandler
	sendJSONResponse(w, true, fmt.Sprintf("Data saved for Key='%s'", req.Key), nil, http.StatusOK)
}

// GetHandler handles GET requests to retrieve a value by key.
func (h *Handlers) GetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /get cancelled or timed out: %v", ctx.Err())
		sendJSONResponse(w, false, "Request cancelled or timed out", nil, http.StatusServiceUnavailable)
		return
	default:
		// Continue with handler logic
	}

	if r.Method != http.MethodGet {
		sendJSONResponse(w, false, "Method not allowed", nil, http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		log.Print("Bad request: 'key' query parameter is required")
		sendJSONResponse(w, false, "'key' query parameter is required", nil, http.StatusBadRequest)
		return
	}

	valueBytes, ok := h.Store.Get(key)
	if !ok {
		log.Printf("Not found: Key='%s' not found or expired", key)
		sendJSONResponse(w, false, fmt.Sprintf("Key '%s' not found or expired", key), nil, http.StatusNotFound)
		return
	}

	// For successful GET, we want to include the retrieved data.
	// We'll create an anonymous struct or map for the data field.
	responseData := map[string]stdjson.RawMessage{
		"key":   stdjson.RawMessage(fmt.Sprintf(`"%s"`, key)),
		"value": stdjson.RawMessage(valueBytes),
	}
	sendJSONResponse(w, true, "Data retrieved successfully", responseData, http.StatusOK)
}

// LogRequest is a middleware for logging incoming HTTP requests.
func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("Request: Method='%s', Path='%s', Duration='%s'", r.Method, r.URL.Path, time.Since(start))
	})
}
