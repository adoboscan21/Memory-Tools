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
// Using an interface (store.DataStore) promotes loose coupling and testability.
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
	// This checks if the client has cancelled the request or if the server's
	// read/write timeouts (configured in main.go) have occurred.
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

	// Wrap the request body with http.MaxBytesReader to prevent large payloads
	// that could lead to resource exhaustion attacks (e.g., 64KB limit).
	r.Body = http.MaxBytesReader(w, r.Body, 1024*64) // Limit to 64KB

	var req SetRequest
	decoder := json.NewDecoder(r.Body)
	log.Println(decoder)
	// Disallow unknown fields in the JSON request body for stricter parsing.
	// This helps prevent unexpected input and client errors.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		log.Printf("Bad request: invalid JSON body or unknown fields: %v", err)
		http.Error(w, "Invalid JSON request body or unknown fields", http.StatusBadRequest)
		return
	}

	// Validate that key and value are not empty.
	if req.Key == "" || req.Value == "" {
		log.Print("Bad request: key or value cannot be empty")
		http.Error(w, "Key and value cannot be empty", http.StatusBadRequest)
		return
	}

	// Use the injected DataStore to set the key-value pair.
	h.Store.Set(req.Key, req.Value)
	w.WriteHeader(http.StatusOK)
	// Always set Content-Type header for JSON responses, even for simple messages.
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "Data saved: Key='%s', Value='%s'", req.Key, req.Value)
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

	// Use the injected DataStore to get the value by key.
	value, ok := h.Store.Get(key)
	// If the key is not found, return a 404 Not Found status.
	if !ok {
		log.Printf("Not found: Key='%s' not found", key)
		http.Error(w, fmt.Sprintf("Key '%s' not found", key), http.StatusNotFound)
		return
	}

	// Prepare the JSON response.
	response := map[string]string{"key": key, "value": value}
	// Set the Content-Type header to application/json.
	w.Header().Set("Content-Type", "application/json")
	// Encode the response map to JSON and write it to the response writer.
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding JSON response for GET request: %v", err)
		// If an error occurs here, it might be too late to change the status code
		// if headers have already been sent. Log and try to write a generic error.
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// LogRequest is a middleware for logging incoming HTTP requests.
// This is a common pattern in Go for applying cross-cutting concerns like logging.
func LogRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()  // Record the start time of the request.
		next.ServeHTTP(w, r) // Serve the actual request.
		// Log request details including method, path, and duration.
		log.Printf("Request: Method='%s', Path='%s', Duration='%s'", r.Method, r.URL.Path, time.Since(start))
	})
}
