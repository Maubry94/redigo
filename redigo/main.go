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
)

const (
    aofFilename      = "appendonly.aof"
    snapshotPrefix   = "snapshot-"
    snapshotSuffix   = ".json"
    autosaveInterval = 5 * time.Minute
    maxSnapshots     = 5
)

// Format used for timestamps in snapshot filenames (YYYYMMDD-HHMMSS)
const timestampFormat = "20060102-150405"

// Command represents a database operation to be persisted in the AOF
type Command struct {
    Name      string      // Operation type (e.g., "SET")
    Key       string      // Key on which the operation is performed
    ValueType string      // Data type of the value (string, int, bool)
    Value     interface{} // Actual value being stored
    Timestamp int64       // Unix timestamp when the command was executed
}

// RedigoDB is the main database structure that manages data storage and persistence
type RedigoDB struct {
    store map[string]RedigoStorableValues // In-memory key-value store
    mu    sync.Mutex                      // Mutex for thread-safe access to the store
    aof   *os.File                        // File descriptor for the AOF
    aofMu sync.Mutex                      // Separate mutex for AOF operations
}

// getDataDirectory returns the path to the directory where database files are stored
// Creates the directory if it doesn't exist
func getDataDirectory() (string, error) {
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
    db := &RedigoDB{
        store: make(map[string]RedigoStorableValues),
    }
    
    if err := db.LoadFromSnapshot(); err != nil {
        return nil, fmt.Errorf("erreur lors du chargement du snapshot: %w", err)
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
        return nil, fmt.Errorf("erreur lors du chargement de l'AOF: %w", err)
    }
    
    go db.startAOFCompaction()
    go db.startSnapshotScheduler()
    
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
// Thread-safe through mutex locking
func (db *RedigoDB) Set(key string, value RedigoStorableValues) error {
    db.mu.Lock()
    db.store[key] = value
    db.mu.Unlock()
    
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
    
    return db.appendToAOF(cmd)
}

// Get retrieves a value associated with the given key
// Returns the value and a boolean indicating if the key exists
func (db *RedigoDB) Get(key string) (RedigoStorableValues, bool) {
    db.mu.Lock()
    defer db.mu.Unlock()
    val, ok := db.store[key]
    return val, ok
}

// LoadFromAOF replays all operations from the AOF file to rebuild the in-memory database state
// Called during startup to recover data since the last snapshot
func (db *RedigoDB) LoadFromAOF() error {
    aofPath, err := getAOFPath()
    if err != nil {
        return err
    }
    
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
    
    for scanner.Scan() {
        lineNum++
        line := scanner.Text()
        
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
                db.mu.Unlock()
            }
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
    ticker := time.NewTicker(autosaveInterval)
    for range ticker.C {
        if err := db.CreateSnapshot(); err != nil {
            fmt.Printf("Erreur lors de la création du snapshot: %v\n", err)
        } else {
            fmt.Println("Snapshot créé avec succès")
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
    
    if len(matches) <= maxSnapshots {
        return nil
    }
    
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })
    
    for i := maxSnapshots; i < len(matches); i++ {
        if err := os.Remove(matches[i]); err != nil {
            return err
        }
        fmt.Printf("Ancien snapshot supprimé: %s\n", filepath.Base(matches[i]))
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
    
    fmt.Printf("Base de données chargée depuis le snapshot: %s\n", filepath.Base(snapshotPath))
    return nil
}

// startAOFCompaction runs in the background and periodically compacts the AOF file
// by rewriting it to only contain the commands needed for the current state
func (db *RedigoDB) startAOFCompaction() {
    ticker := time.NewTicker(autosaveInterval * 2)
    for range ticker.C {
        if err := db.compactAOF(); err != nil {
            fmt.Printf("Erreur lors du compactage AOF: %v\n", err)
        } else {
            fmt.Println("Compactage AOF réussi")
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
        return fmt.Errorf("erreur lors de la création du snapshot: %w", err)
    }
    
    if err := db.compactAOF(); err != nil {
        return fmt.Errorf("erreur lors du compactage de l'AOF: %w", err)
    }
    
    if err := db.cleanupOldSnapshots(); err != nil {
        return fmt.Errorf("erreur lors du nettoyage des anciens snapshots: %w", err)
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