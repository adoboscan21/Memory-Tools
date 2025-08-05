package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"memory-tools/internal/store"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	backupDir = "backups"
)

// BackupManager handles backup operations
type BackupManager struct {
	mainStore       store.DataStore
	colManager      *store.CollectionManager
	backupLock      sync.RWMutex
	lastBackupTime  time.Time
	backupRunning   bool
	stopChan        chan struct{}
	wg              sync.WaitGroup
	backupInterval  time.Duration
	backupRetention time.Duration
}

// NewBackupManager creates a new instance of the backup manager
func NewBackupManager(mainStore store.DataStore, colManager *store.CollectionManager, interval time.Duration, retention time.Duration) *BackupManager {
	return &BackupManager{
		mainStore:       mainStore,
		colManager:      colManager,
		stopChan:        make(chan struct{}),
		backupInterval:  interval,
		backupRetention: retention,
	}
}

// Start initiates the periodic backup service
func (bm *BackupManager) Start() {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		slog.Error("Failed to create backup directory", "path", backupDir, "error", err)
		return
	}
	slog.Info("Backup manager starting...", "interval", bm.backupInterval.String(), "retention", bm.backupRetention.String())
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

	slog.Info("Performing initial backup on startup...")
	if err := bm.PerformBackup(); err != nil {
		slog.Error("Error in initial backup", "error", err)
	}

	ticker := time.NewTicker(bm.backupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Info("Performing periodic backup...")
			if err := bm.PerformBackup(); err != nil {
				slog.Error("Error in periodic backup", "error", err)
			}
		case <-bm.stopChan:
			slog.Info("Backup manager received stop signal. Stopping.")
			return
		}
	}
}

// PerformBackup executes a full backup of all data
func (bm *BackupManager) PerformBackup() error {
	bm.backupLock.Lock()
	defer bm.backupLock.Unlock()

	if bm.backupRunning {
		slog.Warn("Backup skipped: another backup is already in progress.")
		return fmt.Errorf("backup already in progress")
	}

	bm.backupRunning = true
	defer func() { bm.backupRunning = false }()

	backupTime := time.Now().Format("2006-01-02_15-04-05")
	backupPath := filepath.Join(backupDir, backupTime)
	slog.Info("Starting new backup", "path", backupPath)

	if err := os.Mkdir(backupPath, 0755); err != nil {
		return fmt.Errorf("error creating backup directory: %w", err)
	}

	if err := bm.backupMainStore(backupPath); err != nil {
		os.RemoveAll(backupPath)
		return fmt.Errorf("error in main store backup: %w", err)
	}

	if err := bm.backupCollections(backupPath); err != nil {
		os.RemoveAll(backupPath)
		return fmt.Errorf("error in collections backup: %w", err)
	}

	go bm.cleanOldBackups()

	bm.lastBackupTime = time.Now()
	slog.Info("Backup completed successfully", "path", backupPath)

	if err := bm.verifyBackup(backupPath); err != nil {
		slog.Error("CRITICAL: Backup verification failed", "path", backupPath, "error", err)
		return fmt.Errorf("backup verification failed: %w", err)
	}

	slog.Debug("Backup verified successfully", "path", backupPath)
	return nil
}

// backupMainStore performs the backup of the main store
func (bm *BackupManager) backupMainStore(backupPath string) error {
	snapshot := bm.mainStore.GetAll()
	backupFile := filepath.Join(backupPath, "in-memory.mtdb")

	return bm.saveBackupFile(backupFile, func(w io.Writer) error {
		if err := binary.Write(w, binary.LittleEndian, uint32(len(snapshot))); err != nil {
			return fmt.Errorf("error writing data count: %w", err)
		}

		for key, value := range snapshot {
			if err := writeLengthPrefixed(w, []byte(key)); err != nil {
				return fmt.Errorf("error writing key '%s': %w", key, err)
			}
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

	collectionNames := bm.colManager.ListCollections()

	for _, colName := range collectionNames {
		colStore := bm.colManager.GetCollection(colName)
		data := colStore.GetAll()
		indexedFields := colStore.ListIndexes()
		backupFile := filepath.Join(collectionsBackupDir, colName+".mtdb")

		slog.Debug("Backing up collection", "collection", colName, "indexes", len(indexedFields), "items", len(data))

		if err := bm.saveBackupFile(backupFile, func(w io.Writer) error {
			if err := binary.Write(w, binary.LittleEndian, uint32(len(indexedFields))); err != nil {
				return fmt.Errorf("failed to write index count for collection '%s': %w", colName, err)
			}
			for _, field := range indexedFields {
				if err := writeLengthPrefixed(w, []byte(field)); err != nil {
					return fmt.Errorf("failed to write index field name '%s': %w", field, err)
				}
			}

			if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
				return fmt.Errorf("error writing data count for collection '%s': %w", colName, err)
			}

			for key, value := range data {
				if err := writeLengthPrefixed(w, []byte(key)); err != nil {
					return fmt.Errorf("error writing key '%s' for collection '%s': %w", key, colName, err)
				}
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
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("error creating temporary file: %w", err)
	}

	if err := writeFunc(file); err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("error writing data: %w", err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("error syncing data: %w", err)
	}
	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("error closing file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("error renaming backup file: %w", err)
	}
	return nil
}

// verifyBackup verifies the integrity of the backup
func (bm *BackupManager) verifyBackup(backupPath string) error {
	mainFile := filepath.Join(backupPath, "in-memory.mtdb")
	if info, err := os.Stat(mainFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("main backup file '%s' does not exist", mainFile)
		}
		return fmt.Errorf("error verifying main file: %w", err)
	} else if info.Size() == 0 {
		if bm.mainStore.Size() > 0 {
			return fmt.Errorf("main backup file is empty but store is not")
		}
	}

	collectionsDir := filepath.Join(backupPath, "collections")
	if _, err := os.Stat(collectionsDir); err != nil {
		return fmt.Errorf("error verifying collections directory: %w", err)
	}

	entries, err := os.ReadDir(collectionsDir)
	if err != nil {
		return fmt.Errorf("error reading collections directory: %w", err)
	}

	if len(entries) != len(bm.colManager.ListCollections()) {
		slog.Warn("Backup verification mismatch", "backed_up_collections", len(entries), "active_collections", len(bm.colManager.ListCollections()))
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("error getting file info for '%s': %w", entry.Name(), err)
		}
		if info.Size() == 0 {
			return fmt.Errorf("collection backup file '%s' is empty", entry.Name())
		}
	}
	return nil
}

// cleanOldBackups removes backups older than the retention period
func (bm *BackupManager) cleanOldBackups() {
	cutoffTime := time.Now().Add(-bm.backupRetention)
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		slog.Error("Failed to read backup directory for cleanup", "error", err)
		return
	}

	cleanedCount := 0
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
				slog.Error("Failed to delete old backup", "path", path, "error", err)
			} else {
				slog.Info("Old backup deleted", "path", path)
				cleanedCount++
			}
		}
	}
	if cleanedCount > 0 {
		slog.Info("Backup cleanup finished", "deleted_count", cleanedCount)
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
