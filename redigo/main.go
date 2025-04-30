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

const timestampFormat = "20060102-150405" // YYYYMMDD-HHMMSS

type Command struct {
    Name      string
    Key       string
    ValueType string
    Value     interface{}
    Timestamp int64
}

type RedigoDB struct {
    store map[string]RedigoStorableValues
    mu    sync.Mutex
    aof   *os.File
    aofMu sync.Mutex
}

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

func getAOFPath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    return filepath.Join(dataDir, aofFilename), nil
}

func getNewSnapshotPath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    timestamp := time.Now().Format(timestampFormat)
    return filepath.Join(dataDir, fmt.Sprintf("%s%s%s", snapshotPrefix, timestamp, snapshotSuffix)), nil
}

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

func (db *RedigoDB) CloseAOF() error {
    if db.aof != nil {
        return db.aof.Close()
    }
    return nil
}

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

func (db *RedigoDB) Get(key string) (RedigoStorableValues, bool) {
    db.mu.Lock()
    defer db.mu.Unlock()
    val, ok := db.store[key]
    return val, ok
}

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

type RedigoStorableValues interface {
    isRedigoValue()
}

type RedigoString string
func (s RedigoString) isRedigoValue() {}

type RedigoBool bool
func (b RedigoBool) isRedigoValue() {}

type RedigoInt int
func (i RedigoInt) isRedigoValue() {}