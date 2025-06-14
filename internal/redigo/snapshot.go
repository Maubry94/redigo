package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"time"
)

// Runs a background goroutine that periodically
// creates database snapshots at configured intervals
func (db *RedigoDB) StartSnapshotListener() {
	ticker := time.NewTicker(db.envs.SnapshotSaveInterval)
	for range ticker.C {
		if err := db.UpdateSnapshot(); err != nil {
			fmt.Printf("Error when updating snapshot: %v\n", err)
		} else {
			fmt.Println("Snapshot updated successfully")
		}
	}
}

// Creates a point-in-time snapshot of the database
// Cleans up expired keys, serializes data to JSON, and truncates AOF
func (db *RedigoDB) UpdateSnapshot() error {
	db.storeMutex.Lock()
	defer db.storeMutex.Unlock()

	now := time.Now().Unix()
	snapshot := make(map[string]map[string]any)

	// Iterate through all stored data
	for key, value := range db.store {
		// Remove expired keys during snapshot creation
		if expireTime, exists := db.expirationKeys[key]; exists && now > expireTime {
			delete(db.store, key)
			delete(db.expirationKeys, key)
			continue // Skip expired keys from snapshot
		}

		var valueType string
		var rawValue any

		// Convert typed values to serializable format
		switch v := value.(type) {
		case types.RedigoString:
			valueType = "string"
			rawValue = string(v)
		case types.RedigoBool:
			valueType = "bool"
			rawValue = bool(v)
		case types.RedigoInt:
			valueType = "int"
			rawValue = int(v)
		}

		// Store key-value pair with type information for proper restoration
		snapshot[key] = map[string]any{
			"type":  valueType,
			"value": rawValue,
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

	return nil
}

// Restores database state from the most recent snapshot file
// Creates an empty snapshot file if none exists
func (db *RedigoDB) LoadFromSnapshot() error {
	snapshotPath, err := utils.GetSnapshotFilePath()
	if err != nil {
		return err
	}

	// Create empty snapshot file if it doesn't exist
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		if err := os.WriteFile(snapshotPath, []byte("{}"), 0644); err != nil {
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
	var snapshot map[string]map[string]interface{}
	if err := json.Unmarshal(snapshotFileContent, &snapshot); err != nil {
		return err
	}

	db.storeMutex.Lock()
	defer db.storeMutex.Unlock()

	// Initialize fresh store for loading
	db.store = make(map[string]types.RedigoStorableValues)

	// Restore each key-value pair from snapshot
	for key, item := range snapshot {
		valueType, ok := item["type"].(string)
		if !ok {
			continue // Skip malformed entries
		}

		// Convert back to proper typed values based on stored type
		switch valueType {
		case "string":
			if value, ok := item["value"].(string); ok {
				db.store[key] = types.RedigoString(value)
			}
		case "bool":
			if value, ok := item["value"].(bool); ok {
				db.store[key] = types.RedigoBool(value)
			}
		case "int":
			// JSON unmarshals numbers as float64, convert to int
			if value, ok := item["value"].(float64); ok {
				db.store[key] = types.RedigoInt(int(value))
			}
		}
	}

	fmt.Printf("Database loaded from snapshot: %s\n", snapshotPath)
	return nil
}
