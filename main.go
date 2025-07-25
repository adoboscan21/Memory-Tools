package main

import (
	"context"
	"flag" // Used for command-line argument parsing.
	"log"
	"memory-tools/internal/api"
	"memory-tools/internal/config" // Import the config package.
	"memory-tools/internal/persistence"
	"memory-tools/internal/store" // Store package with TTL features.
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Configure logging format to include date, time, and file/line number.
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Define a command-line flag for the config file path.
	// Default to "config.json" in the current directory.
	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	flag.Parse() // Parse command-line flags.

	// Load application configuration.
	// This will first get default values and then attempt to load from the specified JSON file,
	// overriding defaults with values found in the file.
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Fatal error loading configuration: %v", err)
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
	// This goroutine will periodically save the data to the .mtdb file based on config.
	snapshotManager := persistence.NewSnapshotManager(inMemStore, cfg.SnapshotInterval, cfg.EnableSnapshots)
	go snapshotManager.Start()

	// 8. Start the TTL cleaner goroutine.
	// This goroutine will periodically remove expired items from the in-memory store based on config.
	ttlCleanStopChan := make(chan struct{}) // Channel to signal the TTL cleaner to stop.
	go func() {
		ticker := time.NewTicker(cfg.TtlCleanInterval)
		defer ticker.Stop() // Ensure the ticker is stopped when the goroutine exits.
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
		log.Printf("Server listening on %s", cfg.Port)
		// ListenAndServe returns an error, typically http.ErrServerClosed during graceful shutdown.
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Use Fatalf for unrecoverable errors during server startup.
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	// 10. Set up graceful shutdown mechanism.
	// Create a channel to listen for OS signals (Interrupt/Ctrl+C and Terminate).
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// Block the main goroutine until a termination signal is received.
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// 11. Stop the snapshot manager first.
	// This ensures no new snapshots are initiated while the server is shutting down.
	snapshotManager.Stop()

	// 12. Stop the TTL cleaner goroutine.
	// Closing the channel signals the goroutine to exit cleanly.
	close(ttlCleanStopChan)

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

	// 13. Save final data to disk before application exit.
	// This ensures the latest state is persisted, even if no scheduled snapshot occurred recently.
	log.Println("Saving final data before application exit...")
	if err := persistence.SaveData(inMemStore); err != nil {
		// Log the error but don't fatal, as the server is already down and exiting anyway.
		log.Printf("Error saving final data during shutdown: %v", err)
	} else {
		log.Println("Final data saved. Application exiting.")
	}
}
