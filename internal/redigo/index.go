package redigo

import (
	"fmt"
	"os"
	"redigo/envs"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"sync"
)

var envsConfig envs.Envs

type RedigoDB struct {
	store                  map[string]any   // Main key-value store
	expirationKeys         map[string]int64 // Maps keys to their expiration timestamps
	storeMutex             sync.Mutex       // Protects concurrent access to store and expirationKeys
	aofFile                *os.File         // Handle to the AOF for persistence
	aofMutex               sync.Mutex       // Protects concurrent writes to AOF file
	envs                   envs.Envs        // Configuration loaded from environment variables
	aofCommandsBuffer      []types.Command  // Buffer for AOF commands before flushing to disk
	aofCommandsBufferMutex sync.Mutex       // Protects concurrent access to the AOF buffer

	// Reverse indexes for efficient value-based searching
	valueIndex  *types.ReverseIndex // Index for searching by exact value
	prefixIndex *types.ReverseIndex // Index for searching by key prefix
	suffixIndex *types.ReverseIndex // Index for searching by key suffix
	indexMutex  sync.RWMutex        // Protects concurrent access to indexes
}

// Creates and initializes a new RedigoDB instance
// It loads configuration, restores data from snapshot and AOF, and starts background processes
func InitializeRedigo() (*RedigoDB, error) {
	envs := envs.Gets()

	// Create database instance with default values
	database := &RedigoDB{
		store:             make(map[string]any),
		expirationKeys:    make(map[string]int64),
		envs:              envs,
		aofCommandsBuffer: make([]types.Command, 0),

		// Initialize reverse indexes
		valueIndex: &types.ReverseIndex{
			Type:    types.VALUE_INDEX,
			Entries: make(map[string]*types.IndexEntry),
		},
		prefixIndex: &types.ReverseIndex{
			Type:    types.PREFIX_INDEX,
			Entries: make(map[string]*types.IndexEntry),
		},
		suffixIndex: &types.ReverseIndex{
			Type:    types.SUFFIX_INDEX,
			Entries: make(map[string]*types.IndexEntry),
		},
	}

	if err := database.LoadFromSnapshot(); err != nil {
		return nil, fmt.Errorf("error loading snapshot: %w", err)
	}

	aofPath, err := utils.GetAOFPath()

	if err != nil {
		return nil, err
	}

	loadedAOF, err := os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	database.aofFile = loadedAOF

	if err := database.LoadFromAof(); err != nil {
		return nil, fmt.Errorf("error loading AOF: %w", err)
	}

	if err := database.LoadIndexesFromFile(); err != nil {
		fmt.Printf("Failed to load indexes: %v\n", err)
	}

	go database.StartSnapshotListener()
	go database.StartDataExpirationListener()
	go database.StartBufferListener()

	return database, nil
}

// Adds a key-value pair to the reverse indexes
func (database *RedigoDB) addToIndex(key string, value any) {
	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	valueStr := utils.ValueToString(value)

	// --- VALUE INDEX ---
	if entry, exists := database.valueIndex.Entries[valueStr]; exists {
		entry.Keys[key] = true
	} else {
		database.valueIndex.Entries[valueStr] = &types.IndexEntry{
			Keys: map[string]bool{key: true},
		}
	}

	// --- PREFIX INDEX ---
	for i := 1; i <= len(key); i++ {
		prefix := key[:i]
		if entry, exists := database.prefixIndex.Entries[prefix]; exists {
			entry.Keys[key] = true
		} else {
			database.prefixIndex.Entries[prefix] = &types.IndexEntry{
				Keys: map[string]bool{key: true},
			}
		}
	}

	// --- SUFFIX INDEX ---
	for i := 0; i < len(key); i++ {
		suffix := key[i:]
		if entry, exists := database.suffixIndex.Entries[suffix]; exists {
			entry.Keys[key] = true
		} else {
			database.suffixIndex.Entries[suffix] = &types.IndexEntry{
				Keys: map[string]bool{key: true},
			}
		}
	}
}

// Removes a key from all reverse indexes
func (database *RedigoDB) removeFromIndex(key string, value any) {
	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	valueStr := utils.ValueToString(value)

	// --- VALUE INDEX ---
	if entry, exists := database.valueIndex.Entries[valueStr]; exists {
		delete(entry.Keys, key)
		if len(entry.Keys) == 0 {
			delete(database.valueIndex.Entries, valueStr)
		}
	}

	// --- PREFIX INDEX ---
	for i := 1; i <= len(key); i++ {
		prefix := key[:i]
		if entry, exists := database.prefixIndex.Entries[prefix]; exists {
			delete(entry.Keys, key)
			if len(entry.Keys) == 0 {
				delete(database.prefixIndex.Entries, prefix)
			}
		}
	}

	// --- SUFFIX INDEX ---
	for i := 0; i < len(key); i++ {
		suffix := key[i:]
		if entry, exists := database.suffixIndex.Entries[suffix]; exists {
			delete(entry.Keys, key)
			if len(entry.Keys) == 0 {
				delete(database.suffixIndex.Entries, suffix)
			}
		}
	}
}

func (database *RedigoDB) SafeRemoveKey(key string) {
	database.storeMutex.Lock()

	delete(database.store, key)
	delete(database.expirationKeys, key)

	database.storeMutex.Unlock()

}

func (database *RedigoDB) UnsafeRemoveKey(key string) {
	delete(database.store, key)
	delete(database.expirationKeys, key)
}

// Forces the database to save its state to a snapshot file
func (database *RedigoDB) ForceSave() error {
	if err := database.UpdateSnapshot(); err != nil {
		return fmt.Errorf("Error creating snapshot: %w", err)
	}

	return nil
}
