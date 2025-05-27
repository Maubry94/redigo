package redigo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
    Value     any         // Actual value being stored
    TTL       int64       // Time-to-live in seconds (0 means no expiration)
    Timestamp int64       // Unix timestamp when the command was executed
}

// Variables globales pour stocker la configuration
var config envs.Envs

// RedigoDB is the main database structure that manages data storage and persistence
type RedigoDB struct {
    store      map[string]RedigoStorableValues // In-memory key-value store
    expiry     map[string]int64                // Map of key expiration timestamps
    storeMutex sync.Mutex                      // Mutex for thread-safe access to the store
    aofFile    *os.File                        // File descriptor for the AOF
    aofMutex    sync.Mutex                      // Separate mutex for AOF operations
    envs        envs.Envs                       // Configuration settings
    aofCommandsBuffer  []string    // Buffer pour stocker les commandes avant écriture
    aofCommandsBufferMutex   sync.Mutex  // Mutex pour protéger le buffer
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
    envs := envs.Gets()

    db := &RedigoDB{
        store:  make(map[string]RedigoStorableValues),
        expiry: make(map[string]int64),
        envs:    envs,
        aofCommandsBuffer:  make([]string, 0),
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
    db.aofFile = aof
    
    if err := db.LoadFromAOF(); err != nil {
        return nil, fmt.Errorf("error loading AOF: %w", err)
    }
    
    go db.startWriteBufferInAOF()
    go db.startSnapshotScheduler()
    go db.startExpiryChecker()
    go db.startBufferFlusher()
    
    return db, nil
}

// CloseAOF properly closes the AOF file
func (db *RedigoDB) CloseAOF() error {
    if db.aofFile != nil {
        return db.aofFile.Close()
    }
    return nil
}

// addCommandsToAOFBuffer appends a command to the AOF file in JSON format
// This is called after every write operation to ensure durability
func (db *RedigoDB) addCommandsToAOFBuffer(cmd Command) error {
    jsonCmd, err := json.Marshal(cmd)
    if err != nil {
        return err
    }

    db.aofCommandsBufferMutex.Lock()
    db.aofCommandsBuffer = append(db.aofCommandsBuffer, string(jsonCmd))
    db.aofCommandsBufferMutex.Unlock()
    
    return nil
}

func (db *RedigoDB) flushBuffer() error {
    db.aofCommandsBufferMutex.Lock()
    if len(db.aofCommandsBuffer) == 0 {
        db.aofCommandsBufferMutex.Unlock()
        return nil
    }
    
    aofBufferCommands := make([]string, len(db.aofCommandsBuffer))
    copy(aofBufferCommands, db.aofCommandsBuffer)
    db.aofCommandsBuffer = db.aofCommandsBuffer[:0]
    db.aofCommandsBufferMutex.Unlock()

    db.aofMutex.Lock()
    defer db.aofMutex.Unlock()
    
    for _, cmd := range aofBufferCommands {
        if _, err := db.aofFile.WriteString(cmd + "\n"); err != nil {
            return err
        }
    }

    return nil
}

func (db *RedigoDB) startBufferFlusher() {
    ticker := time.NewTicker(1 * time.Second)
    go func() {
        for range ticker.C {
            if err := db.flushBuffer(); err != nil {
                fmt.Printf("Erreur lors de la vidange du buffer AOF: %v\n", err)
            }
        }
    }()
}

// Set stores a value associated with the given key and persists the operation
// If ttl > 0, the key will expire after ttl seconds
func (db *RedigoDB) Set(key string, value RedigoStorableValues, ttl int64) error {
    now := time.Now().Unix() // Capture le timestamp actuel une seule fois
    
    db.storeMutex.Lock()
    
    // Store the value
    db.store[key] = value
    
    // Set expiry if needed
    if ttl > 0 {
        expireTime := now + ttl  // Utilise le même timestamp que celui qui sera dans la commande
        db.expiry[key] = expireTime
    } else {
        delete(db.expiry, key)
    }
    
    db.storeMutex.Unlock()
    
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
    
    // Créer et enregistrer la commande avec le même timestamp
    cmd := Command{
        Name:      "SET",
        Key:       key,
        ValueType: valueType,
        Value:     rawValue,
        TTL:       ttl,        // Le TTL est préservé tel quel
        Timestamp: now,        // Utilise le même timestamp que pour l'expiration
    }
    
    return db.addCommandsToAOFBuffer(cmd)
}

// Get retrieves a value associated with the given key
// Returns the value and a boolean indicating if the key exists
func (db *RedigoDB) Get(key string) (RedigoStorableValues, bool) {
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
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
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
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
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
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
    
    db.addCommandsToAOFBuffer(cmd)
    
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
            
            db.storeMutex.Lock()
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
            db.storeMutex.Unlock()
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
                db.storeMutex.Lock()
                db.store[cmd.Key] = value
                
                if cmd.TTL > 0 {
                    // Calculer le temps d'expiration absolu basé sur le timestamp original
                    expiryTime := cmd.Timestamp + cmd.TTL
                    if expiryTime > now {
                        db.expiry[cmd.Key] = expiryTime
                    } else {
                        // Si la clé a déjà expiré, la supprimer
                        delete(db.store, cmd.Key)
                        delete(db.expiry, cmd.Key)
                    }
                }
                db.storeMutex.Unlock()
            }
        } else if cmd.Name == "DEL" {
            db.storeMutex.Lock()
            delete(db.store, cmd.Key)
            delete(db.expiry, cmd.Key)
            db.storeMutex.Unlock()
        } else if cmd.Name == "EXPIRE" {
            db.storeMutex.Lock() 
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
            db.storeMutex.Unlock()
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
    ticker := time.NewTicker(db.envs.AutosaveInterval)
    for range ticker.C {
        if err := db.CreateSnapshot(); err != nil {
            fmt.Printf("Error creating snapshot: %v\n", err)
        } else {
            fmt.Println("Snapshot created successfully")
            db.cleanupOldSnapshots()
        }
    }
}

//TODO: Créer un seul snapshot (pas plusieurs avec timestamp) toutes les 2 min via startSnapshotScheduler (envs configurable) recup tous le contenue de l'AOF le mettre dans le snapshot et vider l'AOF
// CreateSnapshot creates a point-in-time snapshot of the current database state
// and saves it as a JSON file with timestamp in the filename
func (db *RedigoDB) CreateSnapshot() error {
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
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
    
    if len(matches) <= db.envs.MaxSnapshots {
        return nil
    }
    
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })
    
    for i := db.envs.MaxSnapshots; i < len(matches); i++ {
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
    
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
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

// startWriteBufferInAOF runs in the background and periodically compacts the AOF file
// by rewriting it to only contain the commands needed for the current state
func (db *RedigoDB) startWriteBufferInAOF() {
    ticker := time.NewTicker(db.envs.AofCompactionInterval)
    for range ticker.C {
        if err := db.flushBuffer(); err != nil {
            fmt.Printf("Error during flushing AOF buffer: %v\n", err)
        }
    }
}

// ForceSave creates a snapshot, compacts the AOF, and cleans up old snapshots
// Used for the SAVE command to manually trigger persistence
func (db *RedigoDB) ForceSave() error {
    if err := db.CreateSnapshot(); err != nil {
        return fmt.Errorf("error creating snapshot: %w", err)
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