package redigo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
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

func getDataFilePath() (string, error) {
    dataDir, err := getDataDirectory()
    if err != nil {
        return "", err
    }
    return filepath.Join(dataDir, "data.json"), nil
}

func (db *RedigoDB) SaveToJSON(_ string) error {
    db.mu.Lock()
    defer db.mu.Unlock()

    filePath, err := getDataFilePath()
    if err != nil {
        return err
    }

    data := make(map[string]interface{})

    existingData := make(map[string]interface{})
    if _, err := os.Stat(filePath); err == nil {
        fileData, err := os.ReadFile(filePath)
        if err == nil {
            err = json.Unmarshal(fileData, &existingData)
            if err == nil {
                data = existingData
            }
        }
    }

    for key, value := range db.store {
        if _, exists := existingData[key]; !exists {
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
    }

    jsonData, err := json.MarshalIndent(data, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(filePath, jsonData, 0644)
}

func (db *RedigoDB) LoadFromJSON(_ string) error {
    filePath, err := getDataFilePath()
    if err != nil {
        return err
    }

    if _, err := os.Stat(filePath); os.IsNotExist(err) {
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
	return &RedigoDB{
		store: make(map[string]RedigoStorableValues),
	}
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
