package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"redigo/pkg/utils"
	"time"
)

// Runs a background goroutine that periodically
// creates database snapshots at configured intervals
func (database *RedigoDB) StartSnapshotListener() {
	ticker := time.NewTicker(database.envs.SnapshotSaveInterval)
	for range ticker.C {
		if err := database.UpdateSnapshot(); err != nil {
			fmt.Printf("Error when updating snapshot: %v\n", err)
		} else {
			fmt.Println("Snapshot updated successfully")
		}
	}
}

// Creates a point-in-time snapshot of the database
// Cleans up expired keys, serializes data to JSON, and truncates AOF
func (database *RedigoDB) UpdateSnapshot() error {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	now := time.Now().Unix()
	snapshot := make(map[string]map[string]any)

	// Iterate through all stored data
	for key, value := range database.store {
		// Remove expired keys during snapshot creation
		if expireTime, exists := database.expirationKeys[key]; exists && now > expireTime {
			database.unsafeRemoveKey(key)
			continue // Skip expired keys from snapshot
		}

		// Store key-value pair with type information for proper restoration
		snapshot[key] = map[string]any{
			"value": value,
		}
	}

	// Serialize snapshot to pretty-printed JSON
	jsonData, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	// Get snapshot file path from configuration
	snapshotPath, err := utils.GetSnapshotFilePath()
	if err != nil {
		return err
	}

	// Write to temporary file first for atomic operation
	tempSnapshotPath := snapshotPath + ".tmp"
	if err := os.WriteFile(tempSnapshotPath, jsonData, 0644); err != nil {
		return err
	}

	// Atomically replace old snapshot with new one
	if err := os.Rename(tempSnapshotPath, snapshotPath); err != nil {
		return err
	}

	// Truncate AOF file since snapshot is now the authoritative source
	aofPath, err := utils.GetAOFPath()
	if err != nil {
		return err
	}

	if err := os.Truncate(aofPath, 0); err != nil {
		return fmt.Errorf("failed to truncate AOF file: %w", err)
	}

	if err := database.DumpIndexesToFile(); err != nil {
		return fmt.Errorf("failed to dump indexes: %w", err)
	}

	return nil
}

// Restores database state from the most recent snapshot file
// Creates an empty snapshot file if none exists
func (database *RedigoDB) LoadFromSnapshot() error {
	snapshotPath, err := utils.GetSnapshotFilePath()
	if err != nil {
		return err
	}

	// Create empty snapshot file if it doesn't exist
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		//0600 - read/write for owner only
		if err := os.WriteFile(snapshotPath, []byte("{}"), 0600); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// Read snapshot file contents
	snapshotFileContent, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	// Parse JSON snapshot data
	var snapshot map[string]map[string]any
	if err := json.Unmarshal(snapshotFileContent, &snapshot); err != nil {
		return err
	}

	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	// Initialize fresh store for loading
	database.store = make(map[string]any)

	// Restore each key-value pair from snapshot
	for key, item := range snapshot {
		rawValue, exists := item["value"]
		if !exists {
			continue
		}

		switch value := rawValue.(type) {
		case string, bool, int, int64:
			database.store[key] = value
		case float64:
			if value == float64(int(value)) {
				database.store[key] = int(value)
			} else {
				database.store[key] = value
			}
		default:
			fmt.Printf("Warning: unsupported value type %T for key '%s', skipping\n", value, key)
		}
	}

	fmt.Printf("Database loaded from snapshot: %s (%d keys)\n", snapshotPath, len(database.store))
	return nil
}
