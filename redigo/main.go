package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
    autosaveInterval = 5 * time.Minute
    timestampFormat = "20060102-150405" // YYYYMMDD-HHMMSS
    maxBackups = 10
)

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

func getLatestDataFile() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    pattern := filepath.Join(dataDir, "data-*.json")
    files, err := filepath.Glob(pattern)
    if err != nil {
        return "", err
    }
    
    if len(files) == 0 {
        return "", nil
    }
    
    sort.Slice(files, func(i, j int) bool {
        return files[i] > files[j]
    })
    
    return files[0], nil
}

func getNewDataFilePath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    
    timestamp := time.Now().Format(timestampFormat)
    return filepath.Join(dataDir, fmt.Sprintf("data-%s.json", timestamp)), nil
}

func cleanupOldBackups() error {
    dataDir, err := getDataDirectory()
    if err != nil {
        return err
    }
    
    pattern := filepath.Join(dataDir, "data-*.json")
    files, err := filepath.Glob(pattern)
    if err != nil {
        return err
    }
    
    if len(files) <= maxBackups {
        return nil
    }
    
    sort.Strings(files)
    
    for i := 0; i < len(files)-maxBackups; i++ {
        if err := os.Remove(files[i]); err != nil {
            return err
        }
    }
    
    return nil
}

func (db *RedigoDB) StartAutosave() {
    ticker := time.NewTicker(autosaveInterval)
    go func() {
        for range ticker.C {
            if err := db.SaveToJSON(""); err != nil {
                fmt.Printf("Autosave error: %v\n", err)
            } else {
                fmt.Println("Database autosaved successfully")
            }
        }
    }()
}

func (db *RedigoDB) SaveToJSON(_ string) error {
    db.mu.Lock()
    defer db.mu.Unlock()
    
    filePath, err := getNewDataFilePath()
    if err != nil {
        return err
    }
    
    data := make(map[string]interface{})
    
    for key, value := range db.store {
        switch v := value.(type) {
        case RedigoString:
            data[key] = map[string]interface{}{
                "type":  "string",
                "value": string(v),
            }
        case RedigoBool:
            data[key] = map[string]interface{}{
                "type":  "bool",
                "value": bool(v),
            }
        case RedigoInt:
            data[key] = map[string]interface{}{
                "type":  "int",
                "value": int(v),
            }
        }
    }
    
    jsonData, err := json.MarshalIndent(data, "", "  ")
    if err != nil {
        return err
    }
    
    if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
        return err
    }
    
    return cleanupOldBackups()
}

func (db *RedigoDB) LoadFromJSON(_ string) error {
    filePath, err := getLatestDataFile()
    if err != nil {
        return err
    }
    
    if filePath == "" {
        return nil
    }
    
    fileData, err := os.ReadFile(filePath)
    if err != nil {
        return err
    }
    
    var data map[string]map[string]interface{}
    if err := json.Unmarshal(fileData, &data); err != nil {
        return err
    }
    
    db.mu.Lock()
    defer db.mu.Unlock()
    
    for key, item := range data {
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
    
    return nil
}

type RedigoStorableValues interface {
    isRedigoValue()
}

type RedigoDB struct {
    store map[string]RedigoStorableValues
    mu    sync.Mutex
}

func InitRedigo() *RedigoDB {
    db := &RedigoDB{
        store: make(map[string]RedigoStorableValues),
    }
    db.StartAutosave()
    return db
}

func (db *RedigoDB) Set(key string, value RedigoStorableValues) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.store[key] = value
}

func (db *RedigoDB) Get(key string) (RedigoStorableValues, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.store[key]
	return val, ok
}

type RedigoString string

func (s RedigoString) isRedigoValue() {}

type RedigoBool bool

func (b RedigoBool) isRedigoValue() {}

type RedigoInt int

func (i RedigoInt) isRedigoValue() {}
