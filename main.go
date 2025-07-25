package main

import (
	"context"
	"log"
	"memory-tools/internal/api"
	"memory-tools/internal/persistence" // Updated to use binary persistence
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
	// New configurations for snapshot functionality.
	SnapshotInterval time.Duration // How often to take snapshots (e.g., 5 * time.Minute)
	EnableSnapshots  bool          // Whether scheduled snapshots are enabled.
}

func main() {
	// Configure logging format to include date, time, and file/line number.
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Application Configuration.
	cfg := Config{
		Port:            ":8080",
		ReadTimeout:     5 * time.Second,   // Max time to read the request body.
		WriteTimeout:    10 * time.Second,  // Max time to write the response.
		IdleTimeout:     120 * time.Second, // Max time a connection can remain idle.
		ShutdownTimeout: 10 * time.Second,  // Max time for graceful shutdown.
		// Snapshot configuration:
		SnapshotInterval: 5 * time.Minute, // Take a snapshot every 5 minutes.
		EnableSnapshots:  true,            // Enable scheduled snapshots by default.
	}

	// 1. Initialize the in-memory data store.
	inMemStore := store.NewInMemStore()

	// 2. Load persistent data from the binary file on application start.
	// If loading fails, it's a fatal error as the application cannot run without its data.
	if err := persistence.LoadData(inMemStore); err != nil {
		log.Fatalf("Fatal error loading persistent data: %v", err)
	}

	// 3. Create an instance of the API handlers, injecting the store dependency.
	apiHandlers := api.NewHandlers(inMemStore)

	// 4. Create a new ServeMux for routing HTTP requests.
	mux := http.NewServeMux()

	// 5. Register HTTP routes with a logging middleware.
	mux.Handle("/set", api.LogRequest(http.HandlerFunc(apiHandlers.SetHandler)))
	mux.Handle("/get", api.LogRequest(http.HandlerFunc(apiHandlers.GetHandler)))

	// 6. Configure the HTTP server with timeouts and the router.
	server := &http.Server{
		Addr:         cfg.Port,
		Handler:      mux, // Our configured ServeMux.
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// 7. Initialize and start the snapshot manager in a separate goroutine.
	// This goroutine will periodically save the data to the .mtdb file.
	snapshotManager := persistence.NewSnapshotManager(inMemStore, cfg.SnapshotInterval, cfg.EnableSnapshots)
	go snapshotManager.Start()

	// 8. Start the HTTP server in a goroutine to not block the main thread.
	go func() {
		log.Printf("Server listening on http://localhost%s", cfg.Port)
		// ListenAndServe returns an error, typically http.ErrServerClosed during graceful shutdown.
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Use Fatalf for unrecoverable errors during server startup.
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	// 9. Set up graceful shutdown mechanism.
	// Create a channel to listen for OS signals.
	sigChan := make(chan os.Signal, 1)
	// Notify this channel for interrupt (Ctrl+C) and termination signals.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// Block the main goroutine until a termination signal is received.
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// 10. Stop the snapshot manager first.
	// This ensures no new snapshots are initiated while the server is shutting down.
	snapshotManager.Stop()

	// Create a context with a timeout for the server shutdown.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	// Ensure the context cancellation function is called to release resources.
	defer cancelShutdown()

	// Shut down the HTTP server gracefully. This attempts to close active connections.
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		// If shutdown fails, log the error and indicate a forced exit.
		log.Println("Forcing server shutdown due to error.")
	} else {
		log.Println("HTTP server gracefully stopped.")
	}

	// 11. Save final data to disk before application exit.
	// This ensures the latest state is persisted, even if no scheduled snapshot occurred recently.
	log.Println("Saving final data before application exit...")
	if err := persistence.SaveData(inMemStore); err != nil {
		// Log the error but don't fatal, as the server is already down and exiting anyway.
		log.Printf("Error saving final data during shutdown: %v", err)
	} else {
		log.Println("Final data saved. Application exiting.")
	}
}
