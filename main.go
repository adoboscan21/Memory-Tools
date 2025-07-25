// main.go (Sin cambios necesarios)
package main

import (
	"context"
	"flag"
	"log"
	"memory-tools/internal/api"
	"memory-tools/internal/config"
	"memory-tools/internal/persistence"
	"memory-tools/internal/store"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Define a command-line flag for the config file path.
	// Default to "config.json" now.
	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	flag.Parse()

	// Load application configuration from the specified file.
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Fatal error loading configuration: %v", err)
	}

	// 1. Initialize the in-memory data store.
	inMemStore := store.NewInMemStore()

	// 2. Load persistent data from the binary file on application start.
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
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// 7. Initialize and start the snapshot manager in a separate goroutine.
	snapshotManager := persistence.NewSnapshotManager(inMemStore, cfg.SnapshotInterval, cfg.EnableSnapshots)
	go snapshotManager.Start()

	// 8. Start the TTL cleaner goroutine.
	ttlCleanStopChan := make(chan struct{})
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
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	// 10. Set up graceful shutdown mechanism.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// 11. Stop the snapshot manager first.
	snapshotManager.Stop()

	// 12. Stop the TTL cleaner goroutine.
	close(ttlCleanStopChan)

	// Create a context with a timeout for the server shutdown.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
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
