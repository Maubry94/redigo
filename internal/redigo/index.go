package redigo

import (
	"fmt"
	"os"
	"redigo/envs"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"strings"
	"sync"
)

var envsConfig envs.Envs

type RedigoDB struct {
    store      map[string]types.RedigoStorableValues  // Main key-value store
    expirationKeys     map[string]int64                // Maps keys to their expiration timestamps
    storeMutex sync.Mutex                             // Protects concurrent access to store and expirationKeys
    aofFile    *os.File                               // Handle to the AOF for persistence
    aofMutex   sync.Mutex                             // Protects concurrent writes to AOF file
    envs       envs.Envs                              // Configuration loaded from environment variables
    aofCommandsBuffer  []types.Command                // Buffer for AOF commands before flushing to disk
    aofCommandsBufferMutex   sync.Mutex               // Protects concurrent access to the AOF buffer
    
    // Reverse indexes for efficient value-based searching
    valueIndex    *types.ReverseIndex  // Index for searching by exact value
    prefixIndex   *types.ReverseIndex  // Index for searching by key prefix
    suffixIndex   *types.ReverseIndex  // Index for searching by key suffix
    indexMutex    sync.RWMutex         // Protects concurrent access to indexes
}

// Creates and initializes a new RedigoDB instance
// It loads configuration, restores data from snapshot and AOF, and starts background processes
func InitializeRedigo() (*RedigoDB, error) {
    envs := envs.Gets()

    // Create database instance with default values
    database := &RedigoDB{
        store:  make(map[string]types.RedigoStorableValues),
        expirationKeys: make(map[string]int64),
        envs:   envs,
        aofCommandsBuffer:  make([]types.Command, 0),
        
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
    
    go database.StartSnapshotListener()
    go database.StartDataExpirationListener()
    go database.StartBufferListener()
    
    return database, nil
}

// Adds a key-value pair to the reverse indexes
func (db *RedigoDB) addToIndex(key string, value types.RedigoStorableValues) {
    db.indexMutex.Lock()
    defer db.indexMutex.Unlock()
    
    // Convert value to string for indexing
    valueStr := db.valueToString(value)
    
    // Add to value index (exact match)
    if entry, exists := db.valueIndex.Entries[valueStr]; exists {
        entry.Keys = append(entry.Keys, key)
    } else {
        db.valueIndex.Entries[valueStr] = &types.IndexEntry{
            Keys: []string{key},
        }
    }
    
    // Add to prefix index (all prefixes of the key)
    for i := 1; i <= len(key); i++ {
        prefix := key[:i]
        if entry, exists := db.prefixIndex.Entries[prefix]; exists {
            entry.Keys = append(entry.Keys, key)
        } else {
            db.prefixIndex.Entries[prefix] = &types.IndexEntry{
                Keys: []string{key},
            }
        }
    }
    
    // Add to suffix index (all suffixes of the key)
    for i := 0; i < len(key); i++ {
        suffix := key[i:]
        if entry, exists := db.suffixIndex.Entries[suffix]; exists {
            entry.Keys = append(entry.Keys, key)
        } else {
            db.suffixIndex.Entries[suffix] = &types.IndexEntry{
                Keys: []string{key},
            }
        }
    }
}

// Removes a key from all reverse indexes
func (db *RedigoDB) removeFromIndex(key string, value types.RedigoStorableValues) {
    db.indexMutex.Lock()
    defer db.indexMutex.Unlock()
    
    valueStr := db.valueToString(value)
    
    // Remove from value index
    if entry, exists := db.valueIndex.Entries[valueStr]; exists {
        entry.Keys = db.removeKeyFromSlice(entry.Keys, key)
        if len(entry.Keys) == 0 {
            delete(db.valueIndex.Entries, valueStr)
        }
    }
    
    // Remove from prefix index
    for i := 1; i <= len(key); i++ {
        prefix := key[:i]
        if entry, exists := db.prefixIndex.Entries[prefix]; exists {
            entry.Keys = db.removeKeyFromSlice(entry.Keys, key)
            if len(entry.Keys) == 0 {
                delete(db.prefixIndex.Entries, prefix)
            }
        }
    }
    
    // Remove from suffix index
    for i := 0; i < len(key); i++ {
        suffix := key[i:]
        if entry, exists := db.suffixIndex.Entries[suffix]; exists {
            entry.Keys = db.removeKeyFromSlice(entry.Keys, key)
            if len(entry.Keys) == 0 {
                delete(db.suffixIndex.Entries, suffix)
            }
        }
    }
}

// Converts a RedigoStorableValues to string for indexing
func (db *RedigoDB) valueToString(value types.RedigoStorableValues) string {
    switch v := value.(type) {
    case types.RedigoString:
        return string(v)
    case types.RedigoBool:
        if v {
            return "true"
        }
        return "false"
    case types.RedigoInt:
        return fmt.Sprintf("%d", int(v))
    default:
        return ""
    }
}

// Removes a key from a slice of keys
func (db *RedigoDB) removeKeyFromSlice(keys []string, keyToRemove string) []string {
    for i, k := range keys {
        if k == keyToRemove {
            return append(keys[:i], keys[i+1:]...)
        }
    }
    return keys
}

// Finds all keys that have the specified value
func (db *RedigoDB) SearchByValue(value string) []string {
    db.indexMutex.RLock()
    defer db.indexMutex.RUnlock()
    
    if entry, exists := db.valueIndex.Entries[value]; exists {
        // Return a copy to avoid concurrent modification
        result := make([]string, len(entry.Keys))
        copy(result, entry.Keys)
        return result
    }
    return []string{}
}

// Finds all keys that start with the specified prefix
func (db *RedigoDB) SearchByKeyPrefix(prefix string) []string {
    db.indexMutex.RLock()
    defer db.indexMutex.RUnlock()
    
    if entry, exists := db.prefixIndex.Entries[prefix]; exists {
        result := make([]string, len(entry.Keys))
        copy(result, entry.Keys)
        return result
    }
    return []string{}
}

// Finds all keys that end with the specified suffix
func (db *RedigoDB) SearchByKeySuffix(suffix string) []string {
    db.indexMutex.RLock()
    defer db.indexMutex.RUnlock()
    
    if entry, exists := db.suffixIndex.Entries[suffix]; exists {
        result := make([]string, len(entry.Keys))
        copy(result, entry.Keys)
        return result
    }
    return []string{}
}

// Finds all keys that contain the specified substring
func (db *RedigoDB) SearchByKeyContains(substring string) []string {
    db.indexMutex.RLock()
    defer db.indexMutex.RUnlock()
    
    var result []string
    
    // Search through all keys to find those containing the substring
    db.storeMutex.Lock()
    for key := range db.store {
        if strings.Contains(key, substring) {
            result = append(result, key)
        }
    }
    db.storeMutex.Unlock()
    
    return result
}

// Forces the database to save its state to a snapshot file
func (database *RedigoDB) ForceSave() error {
    if err := database.UpdateSnapshot(); err != nil {
        return fmt.Errorf("Error creating snapshot: %w", err)
    }
    
    return nil
}
