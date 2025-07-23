package main

import (
	"context"
	"log"
	"memory-tools/internal/api"
	"memory-tools/internal/persistence"

	"memory-tools/internal/store"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Config holds application-wide configuration.
type Config struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

func main() {
	// Configure logging format to include timestamps
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Application Configuration
	cfg := Config{
		Port:            ":8080",
		ReadTimeout:     5 * time.Second,   // Max time to read the request body
		WriteTimeout:    10 * time.Second,  // Max time to write the response
		IdleTimeout:     120 * time.Second, // Max time a connection can remain idle
		ShutdownTimeout: 10 * time.Second,  // Max time for graceful shutdown
	}

	// 1. Initialize the in-memory data store
	inMemStore := store.NewInMemStore()

	// 2. Load persistent data on application start
	if err := persistence.LoadData(inMemStore); err != nil {
		log.Fatalf("Fatal error loading persistent data: %v", err) // Use Fatalf to exit if data cannot be loaded
	}

	// 3. Create an instance of the API handlers, injecting the store
	apiHandlers := api.NewHandlers(inMemStore) // Pass the inMemStore instance

	// 4. Create a new ServeMux for routing, allowing for middleware
	mux := http.NewServeMux()

	// 5. Register HTTP routes with a logging middleware
	mux.Handle("/set", api.LogRequest(http.HandlerFunc(apiHandlers.SetHandler)))
	mux.Handle("/get", api.LogRequest(http.HandlerFunc(apiHandlers.GetHandler)))

	// 6. Configure the HTTP server with timeouts and the router
	server := &http.Server{
		Addr:         cfg.Port,
		Handler:      mux, // Our configured ServeMux
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// 7. Start the HTTP server in a goroutine to not block main thread
	go func() {
		log.Printf("Server listening on http://localhost%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Use Fatalf for unrecoverable errors during server start
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	// 8. Set up graceful shutdown: Save data before exiting
	// Create a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify this channel for interrupt (Ctrl+C) and termination signals
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan // Block until a termination signal is received

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// Create a context with a timeout for the server shutdown
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown() // Ensure context cancellation

	// Shut down the HTTP server gracefully
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		// If shutdown fails, force quit after timeout
		log.Println("Forcing server shutdown due to error.")
	} else {
		log.Println("HTTP server gracefully stopped.")
	}

	// Save persistent data after server has stopped accepting new connections
	log.Println("Saving data before application exit...")
	if err := persistence.SaveData(inMemStore); err != nil {
		// Log but don't fatal, as server is already down and exiting anyway
		log.Printf("Error saving data during shutdown: %v", err)
	} else {
		log.Println("Data saved. Application exiting.")
	}
}
