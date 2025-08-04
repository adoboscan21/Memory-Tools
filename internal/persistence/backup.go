package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"memory-tools/internal/store"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	backupDir       = "backups"
	backupRetention = 7 * 24 * time.Hour // 7 days retention
	backupInterval  = 1 * time.Minute    // Backup frequency
)

// BackupManager handles backup operations
type BackupManager struct {
	mainStore      store.DataStore
	colManager     *store.CollectionManager
	backupLock     sync.RWMutex
	lastBackupTime time.Time
	backupRunning  bool
	stopChan       chan struct{}
	wg             sync.WaitGroup
}

// NewBackupManager creates a new instance of the backup manager
func NewBackupManager(mainStore store.DataStore, colManager *store.CollectionManager) *BackupManager {
	return &BackupManager{
		mainStore:  mainStore,
		colManager: colManager,
		stopChan:   make(chan struct{}),
	}
}

// Start initiates the periodic backup service
func (bm *BackupManager) Start() {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		log.Printf("Error creating backup directory: %v", err)
		return
	}

	bm.wg.Add(1)
	go bm.runPeriodicBackups()
}

// Stop terminates the backup service
func (bm *BackupManager) Stop() {
	close(bm.stopChan)
	bm.wg.Wait()
}

// runPeriodicBackups executes backups according to the configured interval
func (bm *BackupManager) runPeriodicBackups() {
	defer bm.wg.Done()

	// Execute a backup immediately upon starting
	if err := bm.PerformBackup(); err != nil {
		log.Printf("Error in initial backup: %v", err)
	}

	ticker := time.NewTicker(backupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := bm.PerformBackup(); err != nil {
				log.Printf("Error in periodic backup: %v", err)
			}
		case <-bm.stopChan:
			log.Println("Backup manager received stop signal. Stopping.")
			return
		}
	}
}

// PerformBackup executes a full backup of all data
func (bm *BackupManager) PerformBackup() error {
	bm.backupLock.Lock()
	defer bm.backupLock.Unlock()

	if bm.backupRunning {
		return fmt.Errorf("backup already in progress")
	}

	bm.backupRunning = true
	defer func() { bm.backupRunning = false }()

	// Create directory for this backup
	backupTime := time.Now().Format("2006-01-02_15-04-05")
	backupPath := filepath.Join(backupDir, backupTime)
	if err := os.Mkdir(backupPath, 0755); err != nil {
		return fmt.Errorf("error creating backup directory: %w", err)
	}

	// Backup the main store
	if err := bm.backupMainStore(backupPath); err != nil {
		// If backup fails, remove the partial backup directory to avoid confusion
		os.RemoveAll(backupPath)
		return fmt.Errorf("error in main store backup: %w", err)
	}

	// Backup the collections
	if err := bm.backupCollections(backupPath); err != nil {
		os.RemoveAll(backupPath)
		return fmt.Errorf("error in collections backup: %w", err)
	}

	// Clean up old backups
	go bm.cleanOldBackups()

	bm.lastBackupTime = time.Now()
	log.Printf("Backup completed successfully at %s", backupPath)

	// Verify the backup
	if err := bm.verifyBackup(backupPath); err != nil {
		// Log verification failure but don't delete the backup automatically
		log.Printf("CRITICAL: Backup verification failed for '%s': %v", backupPath, err)
		return fmt.Errorf("backup verification failed: %w", err)
	}

	return nil
}

// backupMainStore performs the backup of the main store
func (bm *BackupManager) backupMainStore(backupPath string) error {
	snapshot := bm.mainStore.GetAll()
	// Consistent naming with the main persistence logic
	backupFile := filepath.Join(backupPath, "in-memory.mtdb")

	return bm.saveBackupFile(backupFile, func(w io.Writer) error {
		// 1. Write number of entries
		if err := binary.Write(w, binary.LittleEndian, uint32(len(snapshot))); err != nil {
			return fmt.Errorf("error writing data count: %w", err)
		}

		// 2. Write each entry (key-value)
		for key, value := range snapshot {
			// Write key (length-prefixed)
			if err := writeLengthPrefixed(w, []byte(key)); err != nil {
				return fmt.Errorf("error writing key '%s': %w", key, err)
			}

			// Write value (length-prefixed)
			if err := writeLengthPrefixed(w, value); err != nil {
				return fmt.Errorf("error writing value for key '%s': %w", key, err)
			}
		}
		return nil
	})
}

// backupCollections performs the backup of all collections, now including index metadata.
func (bm *BackupManager) backupCollections(backupPath string) error {
	collectionsBackupDir := filepath.Join(backupPath, "collections")
	if err := os.Mkdir(collectionsBackupDir, 0755); err != nil {
		return fmt.Errorf("error creating collections backup directory: %w", err)
	}

	// Get a list of all active collection names
	collectionNames := bm.colManager.ListCollections()

	for _, colName := range collectionNames {
		// Get the full DataStore object for the collection to access its methods
		colStore := bm.colManager.GetCollection(colName)

		// Get data and index metadata from the store
		data := colStore.GetAll()
		indexedFields := colStore.ListIndexes()

		backupFile := filepath.Join(collectionsBackupDir, colName+".mtdb")

		log.Printf("Backing up collection '%s' with %d indexes and %d items.", colName, len(indexedFields), len(data))

		if err := bm.saveBackupFile(backupFile, func(w io.Writer) error {
			// --- NEW: Write index metadata header (consistent with main persistence) ---
			// 1. Write number of indexes.
			if err := binary.Write(w, binary.LittleEndian, uint32(len(indexedFields))); err != nil {
				return fmt.Errorf("failed to write index count for collection '%s': %w", colName, err)
			}
			// 2. Write each indexed field name.
			for _, field := range indexedFields {
				if err := writeLengthPrefixed(w, []byte(field)); err != nil {
					return fmt.Errorf("failed to write index field name '%s': %w", field, err)
				}
			}
			// --- END NEW ---

			// 3. Write number of data entries
			if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
				return fmt.Errorf("error writing data count for collection '%s': %w", colName, err)
			}

			// 4. Write each key-value pair
			for key, value := range data {
				// Write key (length-prefixed)
				if err := writeLengthPrefixed(w, []byte(key)); err != nil {
					return fmt.Errorf("error writing key '%s' for collection '%s': %w", key, colName, err)
				}

				// Write value (length-prefixed)
				if err := writeLengthPrefixed(w, value); err != nil {
					return fmt.Errorf("error writing value for key '%s' in collection '%s': %w", key, colName, err)
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("error in backup of collection '%s': %w", colName, err)
		}
	}
	return nil
}

// saveBackupFile saves a backup file securely
func (bm *BackupManager) saveBackupFile(path string, writeFunc func(io.Writer) error) error {
	// Write to a temporary file to ensure atomicity
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("error creating temporary file: %w", err)
	}

	// Write data using the provided function
	if err := writeFunc(file); err != nil {
		file.Close() // Best effort close
		os.Remove(tempPath)
		return fmt.Errorf("error writing data: %w", err)
	}

	// Sync data to disk and close file
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("error syncing data: %w", err)
	}
	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("error closing file: %w", err)
	}

	// Rename the temp file to the final destination file
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("error renaming backup file: %w", err)
	}

	return nil
}

// verifyBackup verifies the integrity of the backup
func (bm *BackupManager) verifyBackup(backupPath string) error {
	// Verify main file
	mainFile := filepath.Join(backupPath, "in-memory.mtdb")
	if info, err := os.Stat(mainFile); err != nil {
		if os.IsNotExist(err) {
			// This is only an error if the main store was supposed to have data.
			// For simplicity, we assume it should always exist.
			return fmt.Errorf("main backup file '%s' does not exist", mainFile)
		}
		return fmt.Errorf("error verifying main file: %w", err)
	} else if info.Size() == 0 {
		// A size of 4 bytes (for 0 entries) is a valid empty state, but 0 is not.
		if bm.mainStore.Size() > 0 {
			return fmt.Errorf("main backup file is empty but store is not")
		}
	}

	// Verify collections directory and its contents
	collectionsDir := filepath.Join(backupPath, "collections")
	if _, err := os.Stat(collectionsDir); err != nil {
		return fmt.Errorf("error verifying collections directory: %w", err)
	}

	entries, err := os.ReadDir(collectionsDir)
	if err != nil {
		return fmt.Errorf("error reading collections directory: %w", err)
	}

	// Check if the number of backed-up collections matches active collections
	if len(entries) != len(bm.colManager.ListCollections()) {
		log.Printf("Warning: number of backed-up collection files (%d) does not match active collections (%d)", len(entries), len(bm.colManager.ListCollections()))
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip directories
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("error getting file info for '%s': %w", entry.Name(), err)
		}
		// A collection file can be validly small if it only has index metadata,
		// but a size of 0 is an error.
		if info.Size() == 0 {
			return fmt.Errorf("collection backup file '%s' is empty", entry.Name())
		}
	}

	return nil
}

// cleanOldBackups removes backups older than the retention period
func (bm *BackupManager) cleanOldBackups() {
	cutoffTime := time.Now().Add(-backupRetention)

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		log.Printf("Error reading backup directory for cleanup: %v", err)
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoffTime) {
			path := filepath.Join(backupDir, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				log.Printf("Error deleting old backup '%s': %v", path, err)
			} else {
				log.Printf("Old backup deleted: %s", path)
			}
		}
	}
}

// GetLastBackupTime returns the time of the last successful backup
func (bm *BackupManager) GetLastBackupTime() time.Time {
	bm.backupLock.RLock()
	defer bm.backupLock.RUnlock()
	return bm.lastBackupTime
}

// GetBackupStatus returns the current status of the backup system
func (bm *BackupManager) GetBackupStatus() string {
	bm.backupLock.RLock()
	defer bm.backupLock.RUnlock()

	if bm.lastBackupTime.IsZero() {
		return "A backup has never been performed"
	}

	if bm.backupRunning {
		return "Backup in progress"
	}

	return fmt.Sprintf("Last successful backup: %s", bm.lastBackupTime.Format(time.RFC1123))
}

// writeLengthPrefixed is a helper function to write length-prefixed byte slices.
func writeLengthPrefixed(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}
