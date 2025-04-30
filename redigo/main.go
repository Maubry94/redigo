package redigo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"redigo/core/envs"
)

const (
    aofFilename      = "appendonly.aof"
    snapshotPrefix   = "snapshot-"
    snapshotSuffix   = ".json"
    timestampFormat  = "20060102-150405"
)

// Command represents a database operation to be persisted in the AOF
type Command struct {
    Name      string      // Operation type (e.g., "SET")
    Key       string      // Key on which the operation is performed
    ValueType string      // Data type of the value (string, int, bool)
    Value     interface{} // Actual value being stored
    TTL       int64       // Time-to-live in seconds (0 means no expiration)
    Timestamp int64       // Unix timestamp when the command was executed
}

// Variables globales pour stocker la configuration
var config envs.Envs

// RedigoDB is the main database structure that manages data storage and persistence
type RedigoDB struct {
    store      map[string]RedigoStorableValues // In-memory key-value store
    expiry     map[string]int64                // Map of key expiration timestamps
    mu         sync.Mutex                      // Mutex for thread-safe access to the store
    aof        *os.File                        // File descriptor for the AOF
    aofMu      sync.Mutex                      // Separate mutex for AOF operations
    cfg        envs.Envs                       // Configuration settings
}

// getDataDirectory returns the path to the directory where database files are stored
// Creates the directory if it doesn't exist
func getDataDirectory() (string, error) {
    if config.DataDirectory != "" {
        if err := os.MkdirAll(config.DataDirectory, 0755); err != nil {
            return "", err
        }
        return config.DataDirectory, nil
    }
    
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return "", err
    }
    
    dataDir := filepath.Join(homeDir, ".redigo")
    
    if err := os.MkdirAll(dataDir, 0755); err != nil {
        return "", err
    }
    
    return dataDir, nil
}

// getAOFPath returns the full path to the AOF file
func getAOFPath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    return filepath.Join(dataDir, aofFilename), nil
}

// getNewSnapshotPath generates a path for a new snapshot with current timestamp
func getNewSnapshotPath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    timestamp := time.Now().Format(timestampFormat)
    return filepath.Join(dataDir, fmt.Sprintf("%s%s%s", snapshotPrefix, timestamp, snapshotSuffix)), nil
}

// getLatestSnapshotPath returns the path to the most recent snapshot file
// Returns empty string if no snapshots exist
func getLatestSnapshotPath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    pattern := filepath.Join(dataDir, snapshotPrefix+"*"+snapshotSuffix)
    matches, err := filepath.Glob(pattern)
    if err != nil {
        return "", err
    }
    
    if len(matches) == 0 {
        return "", nil
    }
    
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })
    
    return matches[0], nil
}

// InitRedigo initializes the database, loads data from the most recent snapshot,
// replays the AOF, and starts background persistence processes
func InitRedigo() (*RedigoDB, error) {
    config = envs.Gets()

    db := &RedigoDB{
        store:  make(map[string]RedigoStorableValues),
        expiry: make(map[string]int64),
        cfg:    config,
    }
    
    if err := db.LoadFromSnapshot(); err != nil {
        return nil, fmt.Errorf("error loading snapshot: %w", err)
    }
    
    aofPath, err := getAOFPath()
    if err != nil {
        return nil, err
    }
    
    aof, err := os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        return nil, err
    }
    db.aof = aof
    
    if err := db.LoadFromAOF(); err != nil {
        return nil, fmt.Errorf("error loading AOF: %w", err)
    }
    
    go db.startAOFCompaction()
    go db.startSnapshotScheduler()
    go db.startExpiryChecker()
    go db.startSnapshotScheduler()
    go db.startAOFCompaction()
    
    return db, nil
}

// CloseAOF properly closes the AOF file
func (db *RedigoDB) CloseAOF() error {
    if db.aof != nil {
        return db.aof.Close()
    }
    return nil
}

// appendToAOF appends a command to the AOF file in JSON format
// This is called after every write operation to ensure durability
func (db *RedigoDB) appendToAOF(cmd Command) error {
    db.aofMu.Lock()
    defer db.aofMu.Unlock()
    
    jsonCmd, err := json.Marshal(cmd)
    if err != nil {
        return err
    }
    
    if _, err := db.aof.WriteString(string(jsonCmd) + "\n"); err != nil {
        return err
    }
    
    return db.aof.Sync()
}

// Set stores a value associated with the given key and persists the operation
// If ttl > 0, the key will expire after ttl seconds
func (db *RedigoDB) Set(key string, value RedigoStorableValues, ttl int64) error {
    db.mu.Lock()
    
    // Store the value
    db.store[key] = value
    
    // Set expiry if needed
    if ttl > 0 {
        expireTime := time.Now().Unix() + ttl
        db.expiry[key] = expireTime
    } else {
        // If ttl is 0, remove any existing expiration
        delete(db.expiry, key)
    }
    
    db.mu.Unlock()
    
    // Déterminer le type et la valeur à stocker dans l'AOF
    var valueType string
    var rawValue interface{}
    
    switch v := value.(type) {
    case RedigoString:
        valueType = "string"
        rawValue = string(v)
    case RedigoBool:
        valueType = "bool"
        rawValue = bool(v)
    case RedigoInt:
        valueType = "int"
        rawValue = int(v)
    }
    
    // Créer et enregistrer la commande
    cmd := Command{
        Name:      "SET",
        Key:       key,
        ValueType: valueType,
        Value:     rawValue,
        TTL:       ttl,
        Timestamp: time.Now().Unix(),
    }
    
    return db.appendToAOF(cmd)
}

// Get retrieves a value associated with the given key
// Returns the value and a boolean indicating if the key exists
func (db *RedigoDB) Get(key string) (RedigoStorableValues, bool) {
    db.mu.Lock()
    defer db.mu.Unlock()
    
    // Check if the key has expired
    if expireTime, exists := db.expiry[key]; exists && time.Now().Unix() > expireTime {
        delete(db.store, key)
        delete(db.expiry, key)
        return nil, false
    }
    
    val, ok := db.store[key]
    return val, ok
}

// GetTTL returns the remaining time-to-live for a key in seconds
// Returns:
// - remaining seconds (>0) if the key has a TTL
// - 0 if the key exists but has no TTL
// - -1 if the key doesn't exist
func (db *RedigoDB) GetTTL(key string) (int64, bool) {
    db.mu.Lock()
    defer db.mu.Unlock()
    
    // Check if key exists
    _, keyExists := db.store[key]
    if !keyExists {
        return -1, false
    }
    
    // Check if key has an expiration
    expireTime, hasExpiry := db.expiry[key]
    if !hasExpiry {
        return 0, true // Exists but has no expiration
    }
    
    // Calculate remaining time
    now := time.Now().Unix()
    remainingTime := expireTime - now
    
    // If expired, the key should have been cleaned up by expiryChecker
    // But just in case, handle it here
    if remainingTime <= 0 {
        delete(db.store, key)
        delete(db.expiry, key)
        return -1, false
    }
    
    return remainingTime, true
}

// SetExpiry sets an expiration time on a key
// Returns true if the timeout was set, false if the key doesn't exist
func (db *RedigoDB) SetExpiry(key string, seconds int64) bool {
    db.mu.Lock()
    defer db.mu.Unlock()
    
    // Check if key exists
    _, exists := db.store[key]
    if !exists {
        return false
    }
    
    // Set expiry
    if seconds > 0 {
        db.expiry[key] = time.Now().Unix() + seconds
    } else if seconds == 0 {
        // Remove expiration if seconds = 0
        delete(db.expiry, key)
    }
    
    // Persist this command to AOF
    cmd := Command{
        Name:      "EXPIRE",
        Key:       key,
        Value:     seconds,
        Timestamp: time.Now().Unix(),
    }
    
    db.appendToAOF(cmd)
    
    return true
}

// startExpiryChecker runs in the background and removes expired keys
func (db *RedigoDB) startExpiryChecker() {
    ticker := time.NewTicker(1 * time.Second)
    go func() {
        for range ticker.C {
            now := time.Now().Unix()
            
            // Create a list of keys to delete
            var expiredKeys []string
            
            db.mu.Lock()
            for key, expireTime := range db.expiry {
                if now > expireTime {
                    expiredKeys = append(expiredKeys, key)
                }
            }
            
            // Remove expired keys
            for _, key := range expiredKeys {
                delete(db.store, key)
                delete(db.expiry, key)
            }
            db.mu.Unlock()
        }
    }()
}

// LoadFromAOF replays all operations from the AOF file to rebuild the in-memory database state
// Called during startup to recover data since the last snapshot
func (db *RedigoDB) LoadFromAOF() error {
    aofPath, err := getAOFPath()
    if err != nil {
        return err
    }
    
    // Check if the AOF file exists
    if _, err := os.Stat(aofPath); os.IsNotExist(err) {
        return nil
    }
    
    file, err := os.Open(aofPath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    scanner := bufio.NewScanner(file)
    lineNum := 0
    now := time.Now().Unix()
    
    for scanner.Scan() {
        lineNum++
        line := scanner.Text()
        
        // Ignore empty lines
        if len(strings.TrimSpace(line)) == 0 {
            continue
        }
        
        var cmd Command
        if err := json.Unmarshal([]byte(line), &cmd); err != nil {
            fmt.Printf("Erreur de lecture à la ligne %d: %v\n", lineNum, err)
            continue
        }
        
        if cmd.Name == "SET" {
            var value RedigoStorableValues
            
            switch cmd.ValueType {
            case "string":
                if strVal, ok := cmd.Value.(string); ok {
                    value = RedigoString(strVal)
                }
            case "bool":
                if boolVal, ok := cmd.Value.(bool); ok {
                    value = RedigoBool(boolVal)
                }
            case "int":
                switch v := cmd.Value.(type) {
                case float64:
                    value = RedigoInt(int(v))
                case string:
                    if intVal, err := strconv.Atoi(v); err == nil {
                        value = RedigoInt(intVal)
                    }
                }
            }
            
            if value != nil {
                db.mu.Lock()
                db.store[cmd.Key] = value
                
                if cmd.TTL > 0 {
                    elapsedTime := now - cmd.Timestamp
                    remainingTTL := cmd.TTL - elapsedTime
                    
                    if remainingTTL > 0 {
                        db.expiry[cmd.Key] = now + remainingTTL
                    } else {
                        delete(db.store, cmd.Key)
                        delete(db.expiry, cmd.Key)
                    }
                } else if cmd.TTL == 0 {
                    delete(db.expiry, cmd.Key)
                }
                
                db.mu.Unlock()
            }
        } else if cmd.Name == "DEL" {
            db.mu.Lock()
            delete(db.store, cmd.Key)
            delete(db.expiry, cmd.Key)
            db.mu.Unlock()
        } else if cmd.Name == "EXPIRE" {
            db.mu.Lock() 
            if _, exists := db.store[cmd.Key]; exists {
                if seconds, ok := cmd.Value.(float64); ok {
                    elapsedTime := now - cmd.Timestamp
                    remainingTime := int64(seconds) - elapsedTime
                    
                    if remainingTime > 0 {
                        db.expiry[cmd.Key] = now + remainingTime
                    } else {
                        delete(db.store, cmd.Key)
                        delete(db.expiry, cmd.Key)
                    }
                }
            }
            db.mu.Unlock()
        }
    }
    
    if err := scanner.Err(); err != nil {
        return err
    }
    
    return nil
}

// startSnapshotScheduler runs in the background and creates periodic snapshots
// of the database state according to the autosaveInterval
func (db *RedigoDB) startSnapshotScheduler() {
    ticker := time.NewTicker(db.cfg.AutosaveInterval)
    for range ticker.C {
        if err := db.CreateSnapshot(); err != nil {
            fmt.Printf("Error creating snapshot: %v\n", err)
        } else {
            fmt.Println("Snapshot created successfully")
            db.cleanupOldSnapshots()
        }
    }
}

// CreateSnapshot creates a point-in-time snapshot of the current database state
// and saves it as a JSON file with timestamp in the filename
func (db *RedigoDB) CreateSnapshot() error {
    db.mu.Lock()
    defer db.mu.Unlock()
    
    snapshotPath, err := getNewSnapshotPath()
    if err != nil {
        return err
    }
    
    snapshot := make(map[string]map[string]interface{})
    
    for key, value := range db.store {
        var valueType string
        var rawValue interface{}
        
        switch v := value.(type) {
        case RedigoString:
            valueType = "string"
            rawValue = string(v)
        case RedigoBool:
            valueType = "bool"
            rawValue = bool(v)
        case RedigoInt:
            valueType = "int"
            rawValue = int(v)
        }
        
        snapshot[key] = map[string]interface{}{
            "type":  valueType,
            "value": rawValue,
        }
    }
    
    jsonData, err := json.MarshalIndent(snapshot, "", "  ")
    if err != nil {
        return err
    }
    
    return os.WriteFile(snapshotPath, jsonData, 0644)
}

// cleanupOldSnapshots removes old snapshot files, keeping only the most recent ones
// according to the maxSnapshots configuration
func (db *RedigoDB) cleanupOldSnapshots() error {
    dataDir, err := getDataDirectory()
    if err != nil {
        return err
    }
    
    pattern := filepath.Join(dataDir, snapshotPrefix+"*"+snapshotSuffix)
    matches, err := filepath.Glob(pattern)
    if err != nil {
        return err
    }
    
    if len(matches) <= db.cfg.MaxSnapshots {
        return nil
    }
    
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })
    
    for i := db.cfg.MaxSnapshots; i < len(matches); i++ {
        if err := os.Remove(matches[i]); err != nil {
            return err
        }
        fmt.Printf("Old snapshot deleted: %s\n", filepath.Base(matches[i]))
    }
    
    return nil
}

// LoadFromSnapshot loads the database state from the most recent snapshot file
// Called during startup before applying the AOF
func (db *RedigoDB) LoadFromSnapshot() error {
    snapshotPath, err := getLatestSnapshotPath()
    if err != nil {
        return err
    }
    
    if snapshotPath == "" {
        return nil
    }
    
    fileData, err := os.ReadFile(snapshotPath)
    if err != nil {
        return err
    }
    
    var snapshot map[string]map[string]interface{}
    if err := json.Unmarshal(fileData, &snapshot); err != nil {
        return err
    }
    
    db.mu.Lock()
    defer db.mu.Unlock()
    
    db.store = make(map[string]RedigoStorableValues)
    
    for key, item := range snapshot {
        typeStr, ok := item["type"].(string)
        if !ok {
            continue
        }
        
        switch typeStr {
        case "string":
            if strValue, ok := item["value"].(string); ok {
                db.store[key] = RedigoString(strValue)
            }
        case "bool":
            if boolValue, ok := item["value"].(bool); ok {
                db.store[key] = RedigoBool(boolValue)
            }
        case "int":
            if numValue, ok := item["value"].(float64); ok {
                db.store[key] = RedigoInt(int(numValue))
            }
        }
    }
    
    fmt.Printf("Database loaded from snapshot: %s\n", filepath.Base(snapshotPath))
    return nil
}

// startAOFCompaction runs in the background and periodically compacts the AOF file
// by rewriting it to only contain the commands needed for the current state
func (db *RedigoDB) startAOFCompaction() {
    ticker := time.NewTicker(db.cfg.AofCompactionInterval)
    for range ticker.C {
        if err := db.compactAOF(); err != nil {
            fmt.Printf("Error during AOF compaction: %v\n", err)
        } else {
            fmt.Println("AOF compaction successful")
        }
    }
}

// compactAOF rewrites the AOF file to contain only the current state of the database
// This prevents the AOF file from growing indefinitely
func (db *RedigoDB) compactAOF() error {
    db.mu.Lock()
    defer db.mu.Unlock()
    
    aofPath, err := getAOFPath()
    if err != nil {
        return err
    }
    
    tempPath := aofPath + ".temp"
    tempFile, err := os.Create(tempPath)
    if err != nil {
        return err
    }
    
    for key, value := range db.store {
        var valueType string
        var rawValue interface{}
        
        switch v := value.(type) {
        case RedigoString:
            valueType = "string"
            rawValue = string(v)
        case RedigoBool:
            valueType = "bool"
            rawValue = bool(v)
        case RedigoInt:
            valueType = "int"
            rawValue = int(v)
        }
        
        cmd := Command{
            Name:      "SET",
            Key:       key,
            ValueType: valueType,
            Value:     rawValue,
            Timestamp: time.Now().Unix(),
        }
        
        jsonCmd, err := json.Marshal(cmd)
        if err != nil {
            tempFile.Close()
            os.Remove(tempPath)
            return err
        }
        
        if _, err := tempFile.WriteString(string(jsonCmd) + "\n"); err != nil {
            tempFile.Close()
            os.Remove(tempPath)
            return err
        }
    }
    
    if err := tempFile.Close(); err != nil {
        os.Remove(tempPath)
        return err
    }
    
    db.aofMu.Lock()
    if err := db.aof.Close(); err != nil {
        db.aofMu.Unlock()
        os.Remove(tempPath)
        return err
    }
    
    if err := os.Rename(tempPath, aofPath); err != nil {
        db.aofMu.Unlock()
        return err
    }
    
    aof, err := os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
    if err != nil {
        db.aofMu.Unlock()
        return err
    }
    db.aof = aof
    db.aofMu.Unlock()
    
    return nil
}

// ForceSave creates a snapshot, compacts the AOF, and cleans up old snapshots
// Used for the SAVE command to manually trigger persistence
func (db *RedigoDB) ForceSave() error {
    if err := db.CreateSnapshot(); err != nil {
        return fmt.Errorf("error creating snapshot: %w", err)
    }
    
    if err := db.compactAOF(); err != nil {
        return fmt.Errorf("error during AOF compaction: %w", err)
    }
    
    if err := db.cleanupOldSnapshots(); err != nil {
        return fmt.Errorf("error cleaning up old snapshots: %w", err)
    }
    
    return nil
}

// RedigoStorableValues is an interface for all types that can be stored in the database
// It acts as a type marker to ensure type safety
type RedigoStorableValues interface {
    isRedigoValue()
}

// RedigoString represents a string value in the database
type RedigoString string
func (s RedigoString) isRedigoValue() {}

// RedigoBool represents a boolean value in the database
type RedigoBool bool
func (b RedigoBool) isRedigoValue() {}

// RedigoInt represents an integer value in the database
type RedigoInt int
func (i RedigoInt) isRedigoValue() {}