package main

import (
	"context"
	"log"
	"memory-tools/internal/api"
	"memory-tools/internal/config"
	"memory-tools/internal/persistence"
	"memory-tools/internal/store" // Store package with new TTL features
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Configure logging format to include date, time, and file/line number.
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Application Configuration.
	cfg := config.Config{
		Port:            ":8080",
		ReadTimeout:     5 * time.Second,   // Max time to read the request body.
		WriteTimeout:    10 * time.Second,  // Max time to write the response.
		IdleTimeout:     120 * time.Second, // Max time a connection can remain idle.
		ShutdownTimeout: 10 * time.Second,  // Max time for graceful shutdown.
		// Snapshot configuration:
		SnapshotInterval: 5 * time.Minute, // Take a snapshot every 5 minutes.
		EnableSnapshots:  true,            // Enable scheduled snapshots by default.
		// TTL Cleaner configuration:
		TtlCleanInterval: 1 * time.Minute, // Run TTL cleaner every 1 minute.
	}

	// 1. Initialize the in-memory data store.
	inMemStore := store.NewInMemStore()

	// 2. Load persistent data from the binary file on application start.
	// If loading fails, it's a fatal error as the application cannot run without its data.
	if err := persistence.LoadData(inMemStore); err != nil {
		log.Fatalf("Fatal error loading persistent data: %v", err)
	}

	// 3. Create an instance of the API handlers, injecting the store dependency.
	apiHandlers := api.NewHandlers(inMemStore) // Handlers need to be updated to pass TTL

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

	// 8. Start the TTL cleaner goroutine.
	// This goroutine will periodically remove expired items from the in-memory store.
	ttlCleanStopChan := make(chan struct{}) // Channel to signal the TTL cleaner to stop.
	go func() {
		ticker := time.NewTicker(cfg.TtlCleanInterval)
		defer ticker.Stop()
		log.Printf("Starting TTL cleaner with interval of %s", cfg.TtlCleanInterval)

		for {
			select {
			case <-ticker.C:
				inMemStore.CleanExpiredItems()
			case <-ttlCleanStopChan:
				log.Println("TTL cleaner received stop signal. Stopping.")
				return
			}
		}
	}()

	// 9. Start the HTTP server in a goroutine to not block the main thread.
	go func() {
		log.Printf("Server listening on http://localhost%s", cfg.Port)
		// ListenAndServe returns an error, typically http.ErrServerClosed during graceful shutdown.
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Use Fatalf for unrecoverable errors during server startup.
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	// 10. Set up graceful shutdown mechanism.
	// Create a channel to listen for OS signals.
	sigChan := make(chan os.Signal, 1)
	// Notify this channel for interrupt (Ctrl+C) and termination signals.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// Block the main goroutine until a termination signal is received.
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// 11. Stop the snapshot manager first.
	snapshotManager.Stop()

	// 12. Stop the TTL cleaner goroutine.
	close(ttlCleanStopChan)

	// Create a context with a timeout for the server shutdown.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	// Ensure the context cancellation function is called to release resources.
	defer cancelShutdown()

	// Shut down the HTTP server gracefully.
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		log.Println("Forcing server shutdown due to error.")
	} else {
		log.Println("HTTP server gracefully stopped.")
	}

	// 13. Save final data to disk before application exit.
	log.Println("Saving final data before application exit...")
	if err := persistence.SaveData(inMemStore); err != nil {
		log.Printf("Error saving final data during shutdown: %v", err)
	} else {
		log.Println("Final data saved. Application exiting.")
	}
}
