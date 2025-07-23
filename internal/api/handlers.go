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
type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Handlers struct groups our API handlers and the DataStore.
// Using an interface (store.DataStore) promotes loose coupling.
type Handlers struct {
	Store store.DataStore
}

// NewHandlers creates a new instance of Handlers with the provided DataStore.
func NewHandlers(s store.DataStore) *Handlers {
	return &Handlers{Store: s}
}

// SetHandler handles POST requests to save key-value pairs.
func (h *Handlers) SetHandler(w http.ResponseWriter, r *http.Request) {
	// Use context for request-scoped values, cancellation, and deadlines.
	// For a simple set operation, it might not seem critical, but it's good practice.
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /set cancelled or timed out: %v", ctx.Err())
		http.Error(w, "Request cancelled or timed out", http.StatusServiceUnavailable)
		return
	default:
		// Continue with handler logic
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Max bytes reader to prevent large payloads
	r.Body = http.MaxBytesReader(w, r.Body, 1024*64) // Limit to 64KB

	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	// Disallow unknown fields for stricter parsing
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Bad request: invalid JSON body: %v", err)
		http.Error(w, "Invalid JSON request body or unknown fields", http.StatusBadRequest)
		return
	}

	if req.Key == "" || req.Value == "" {
		log.Print("Bad request: key or value cannot be empty")
		http.Error(w, "Key and value cannot be empty", http.StatusBadRequest)
		return
	}

	h.Store.Set(req.Key, req.Value) // Use the injected store
	w.WriteHeader(http.StatusOK)
	// Always set Content-Type header for JSON responses, even for simple messages
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "Data saved: Key='%s', Value='%s'", req.Key, req.Value)
}

// GetHandler handles GET requests to retrieve a value by key.
func (h *Handlers) GetHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		log.Printf("Request /get cancelled or timed out: %v", ctx.Err())
		http.Error(w, "Request cancelled or timed out", http.StatusServiceUnavailable)
		return
	default:
		// Continue with handler logic
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		log.Print("Bad request: 'key' query parameter is required")
		http.Error(w, "'key' query parameter is required", http.StatusBadRequest)
		return
	}

	value, ok := h.Store.Get(key) // Use the injected store
	if !ok {
		log.Printf("Not found: Key='%s' not found", key)
		http.Error(w, fmt.Sprintf("Key '%s' not found", key), http.StatusNotFound)
		return
	}

	response := map[string]string{"key": key, "value": value}
	w.Header().Set("Content-Type", "application/json") // Always set Content-Type header for JSON
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding JSON response for GET request: %v", err)
		// It might be too late to change status code if headers already sent.
		// Log and try to write a generic error, but client might not receive it cleanly.
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// LogRequest is a middleware for logging incoming requests.
// This is a common pattern in Go for cross-cutting concerns.
func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("Request: Method='%s', Path='%s', Duration='%s'", r.Method, r.URL.Path, time.Since(start))
	})
}
