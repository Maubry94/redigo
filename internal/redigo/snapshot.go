package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"time"
)

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

func (db *RedigoDB) UpdateSnapshot() error {
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
    snapshot := make(map[string]map[string]any)
    
    for key, value := range db.store {
        var valueType string
        var rawValue any
        
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
        
        snapshot[key] = map[string]any{
            "type":  valueType,
            "value": rawValue,
        }
    }
    
    jsonData, err := json.MarshalIndent(snapshot, "", "  ")

    if err != nil {
        return err
    }

    snapshotPath, err := utils.GetSnapshotFilePath()

    if err != nil {
        return err
    }

    tempSnapshotPath := snapshotPath + ".tmp"
    if err := os.WriteFile(tempSnapshotPath, jsonData, 0644); err != nil {
        return err
    }

    if err := os.Rename(tempSnapshotPath, snapshotPath); err != nil {
        return err
    }

    aofPath, err := utils.GetAOFPath()
    if err != nil {
        return err
    }

    if err := os.Truncate(aofPath, 0); err != nil {
        return fmt.Errorf("failed to truncate AOF file: %w", err)
    }
    
    return nil
}

func (db *RedigoDB) LoadFromSnapshot() error {
    snapshotPath, err := utils.GetSnapshotFilePath()
    
    if err != nil {
        return err
    }

    if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
        if err := os.WriteFile(snapshotPath, []byte("{}"), 0644); err != nil {
            return err
        }
    } else if err != nil {
        return err
    }
    
    snapshotFileContent, err := os.ReadFile(snapshotPath)

    if err != nil {
        return err
    }

    var snapshot map[string]map[string]interface{}

    if err := json.Unmarshal(snapshotFileContent, &snapshot); err != nil {
        return err
    }
    
    db.storeMutex.Lock()
    defer db.storeMutex.Unlock()
    
    db.store = make(map[string]types.RedigoStorableValues)
    
    for key, item := range snapshot {
        valueType, ok := item["type"].(string)

        if !ok {
            continue
        }
        
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
            if value, ok := item["value"].(float64); ok {
                db.store[key] = types.RedigoInt(int(value))
            }
        }
    }
    
    fmt.Printf("Database loaded from snapshot: %s\n", snapshotPath)

    return nil
}