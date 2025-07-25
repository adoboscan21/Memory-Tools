// api/handlers.go
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"memory-tools/internal/store"
	"net/http"
	"time"
)

// SetRequest is the structure for the request body of POST /set.
// 'Value' is now json.RawMessage to capture any valid JSON structure directly as bytes.
type SetRequest struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"` // json.RawMessage captures raw JSON bytes without unmarshalling
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

// SetHandler handles POST requests to save key-value pairs with JSON values.
func (h *Handlers) SetHandler(w http.ResponseWriter, r *http.Request) {
	// Use context for request-scoped values, cancellation, and deadlines.
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /set cancelled or timed out: %v", ctx.Err())
		http.Error(w, "Request cancelled or timed out", http.StatusServiceUnavailable)
		return
	default:
		// Continue with handler logic if context is not done.
	}

	// Ensure the request method is POST.
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Wrap the request body with http.MaxBytesReader to prevent large payloads.
	// Increased limit to 1MB to accommodate larger JSON documents. Adjust as needed.
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024) // Limit to 1MB

	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	// Disallow unknown fields in the JSON request body for stricter parsing.
	decoder.DisallowUnknownFields()

	// Decode the request body into the SetRequest struct.
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Bad request: invalid JSON body or unknown fields: %v", err)
		http.Error(w, "Invalid JSON request body or unknown fields", http.StatusBadRequest)
		return
	}

	// Validate that the key is not empty.
	if req.Key == "" {
		log.Print("Bad request: key cannot be empty")
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}
	// Validate that the value (raw JSON) is not empty.
	// An empty JSON object like {} or empty array [] is allowed, but not a completely empty string.
	if len(req.Value) == 0 {
		log.Print("Bad request: value cannot be empty (e.g., use {} or [])")
		http.Error(w, "Value cannot be empty", http.StatusBadRequest)
		return
	}
	// Optional: Validate that the json.RawMessage itself is a valid JSON document.
	// This catches cases where the 'value' field might contain plain text instead of JSON.
	if !json.Valid(req.Value) {
		log.Printf("Bad request: 'value' field is not a valid JSON document: %s", string(req.Value))
		http.Error(w, "'value' field must be a valid JSON document", http.StatusBadRequest)
		return
	}

	// Pass the raw JSON bytes (json.RawMessage is an alias for []byte) to the store.
	h.Store.Set(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "Data saved: Key='%s'", req.Key) // No longer show the full value in success log.
}

// GetHandler handles GET requests to retrieve a value by key.
func (h *Handlers) GetHandler(w http.ResponseWriter, r *http.Request) {
	// Check request context for cancellation or timeout.
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /get cancelled or timed out: %v", ctx.Err())
		http.Error(w, "Request cancelled or timed out", http.StatusServiceUnavailable)
		return
	default:
		// Continue with handler logic.
	}

	// Ensure the request method is GET.
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract the 'key' query parameter from the URL.
	key := r.URL.Query().Get("key")
	// Validate that the 'key' parameter is provided.
	if key == "" {
		log.Print("Bad request: 'key' query parameter is required")
		http.Error(w, "'key' query parameter is required", http.StatusBadRequest)
		return
	}

	// Retrieve the value from the store; it will be raw JSON bytes.
	valueBytes, ok := h.Store.Get(key)
	if !ok {
		log.Printf("Not found: Key='%s' not found", key)
		http.Error(w, fmt.Sprintf("Key '%s' not found", key), http.StatusNotFound)
		return
	}

	// Construct the JSON response. The 'value' field will embed the raw JSON bytes.
	// We use json.RawMessage to ensure the 'valueBytes' are inserted directly as JSON
	// without being re-escaped as a string.
	responseMap := map[string]json.RawMessage{
		"key":   json.RawMessage(fmt.Sprintf(`"%s"`, key)), // Embed key as a JSON string
		"value": json.RawMessage(valueBytes),               // Embed the raw JSON value
	}

	w.Header().Set("Content-Type", "application/json")
	// Encode the response map to JSON and write it to the response writer.
	if err := json.NewEncoder(w).Encode(responseMap); err != nil {
		log.Printf("Error encoding JSON response for GET request: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// LogRequest is a middleware for logging incoming HTTP requests. (No changes needed here)
func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()  // Record the start time of the request.
		next.ServeHTTP(w, r) // Serve the actual request.
		// Log request details including method, path, and duration.
		log.Printf("Request: Method='%s', Path='%s', Duration='%s'", r.Method, r.URL.Path, time.Since(start))
	})
}
