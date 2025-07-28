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
	"strings" // Required for path parsing
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	configPath := flag.String("config", "config.json", "Path to the JSON configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Fatal error loading configuration: %v", err)
	}

	// Initialize the main in-memory data store.
	mainInMemStore := store.NewInMemStore()

	// Initialize the CollectionManager.
	collectionPersister := &persistence.CollectionPersisterImpl{}
	collectionManager := store.NewCollectionManager(collectionPersister)

	// Load persistent data for the main store.
	if err := persistence.LoadData(mainInMemStore); err != nil {
		log.Fatalf("Fatal error loading main persistent data: %v", err)
	}

	// Load persistent data for all collections into the CollectionManager.
	if err := persistence.LoadAllCollectionsIntoManager(collectionManager); err != nil {
		log.Fatalf("Fatal error loading persistent collections data: %v", err)
	}

	apiHandlers := api.NewHandlers(mainInMemStore, collectionManager)

	// Use http.NewServeMux for standard routing
	mux := http.NewServeMux()

	// Register HTTP routes for the main in-memory store.
	mux.Handle("/set", api.LogRequest(http.HandlerFunc(apiHandlers.SetHandler)))
	mux.Handle("/get", api.LogRequest(http.HandlerFunc(apiHandlers.GetHandler)))

	// Register HTTP routes for Collections.
	mux.Handle("/collections/", api.LogRequest(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/collections/")
		pathParts := strings.Split(path, "/")

		// Handle /collections (list all collections)
		if path == "" && r.Method == http.MethodGet {
			apiHandlers.ListCollectionsHandler(w, r)
			return
		}

		// collectionName is the first part of the path
		collectionName := pathParts[0]
		if collectionName == "" {
			api.SendJSONResponse(w, false, "Collection name cannot be empty", nil, http.StatusBadRequest)
			return
		}

		// Route based on method and path parts
		switch {
		case len(pathParts) == 1: // /collections/{collectionName}
			switch r.Method {
			case http.MethodPost: // POST /collections/{collectionName} (Create/Ensure Collection)
				apiHandlers.CreateCollectionHandler(w, r)
				return
			case http.MethodDelete: // DELETE /collections/{collectionName} (Delete Collection)
				apiHandlers.DeleteCollectionHandler(w, r)
				return
			}
		case len(pathParts) == 2: // /collections/{collectionName}/subPath
			subPath := pathParts[1]
			switch subPath {
			case "set": // POST /collections/{collectionName}/set
				if r.Method == http.MethodPost {
					apiHandlers.SetCollectionItemHandler(w, r)
					return
				}
			case "get": // GET /collections/{collectionName}/get?key=...
				if r.Method == http.MethodGet {
					apiHandlers.GetCollectionItemHandler(w, r)
					return
				}
			case "delete": // DELETE /collections/{collectionName}/delete?key=...
				if r.Method == http.MethodDelete {
					apiHandlers.DeleteCollectionItemHandler(w, r)
					return
				}
			case "list": // GET /collections/{collectionName}/list
				if r.Method == http.MethodGet {
					apiHandlers.ListCollectionItemsHandler(w, r)
					return
				}
			}
		}

		// If no route matches, send 404
		api.SendJSONResponse(w, false, "Not Found", nil, http.StatusNotFound)
	})))

	// Configure the HTTP server.
	server := &http.Server{
		Addr:         cfg.Port,
		Handler:      mux, // Use our standard ServeMux
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Initialize and start the snapshot manager for the main store.
	snapshotManager := persistence.NewSnapshotManager(mainInMemStore, cfg.SnapshotInterval, cfg.EnableSnapshots)
	go snapshotManager.Start()

	// Start the TTL cleaner goroutine.
	ttlCleanStopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cfg.TtlCleanInterval)
		defer ticker.Stop()
		log.Printf("Starting TTL cleaner for main store and collections with interval of %s", cfg.TtlCleanInterval)

		for {
			select {
			case <-ticker.C:
				mainInMemStore.CleanExpiredItems()
				collectionManager.CleanExpiredItemsAndSave()
			case <-ttlCleanStopChan:
				log.Println("TTL cleaner received stop signal. Stopping.")
				return
			}
		}
	}()

	// Start the HTTP server.
	go func() {
		log.Printf("Server listening on %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %v", err)
		}
	}()

	// Set up graceful shutdown mechanism.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Termination signal received. Attempting graceful shutdown...")

	// Stop the snapshot manager.
	snapshotManager.Stop()

	// Stop the TTL cleaner goroutine.
	close(ttlCleanStopChan)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
		log.Println("Forcing server shutdown due to error.")
	} else {
		log.Println("HTTP server gracefully stopped.")
	}

	// Save final data to disk for the main store.
	log.Println("Saving final data for main store before application exit...")
	if err := persistence.SaveData(mainInMemStore); err != nil {
		log.Printf("Error saving final data for main store during shutdown: %v", err)
	} else {
		log.Println("Final main store data saved.")
	}

	// Save final data for all collections to disk.
	log.Println("Saving final data for all collections before application exit...")
	if err := persistence.SaveAllCollectionsFromManager(collectionManager); err != nil {
		log.Printf("Error saving final data for collections during shutdown: %v", err)
	} else {
		log.Println("Final collection data saved. Application exiting.")
	}
}
