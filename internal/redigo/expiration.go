package redigo

import (
	"redigo/internal/redigo/types"
	"time"
)

func (database *RedigoDB) GetTtl(key string) (int64, bool) {
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    _, keyExists := database.store[key]
    if !keyExists {
        return -1, false
    }
    
    expirationTime, hasExpiry := database.expirationKeys[key]
    if !hasExpiry {
        return 0, true
    }
    
    now := time.Now().Unix()
    remainingTime := expirationTime - now

    if remainingTime <= 0 {
        delete(database.store, key)
        delete(database.expirationKeys, key)
        return -1, false
    }
    
    return remainingTime, true
}

func (database *RedigoDB) SetExpiry(key string, seconds int64) bool {
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    _, exists := database.store[key]
    if !exists {
        return false
    }
    
    if seconds > 0 {
        database.expirationKeys[key] = time.Now().Unix() + seconds
    } else if seconds == 0 {
        delete(database.expirationKeys, key)
    }
    
    cmd := types.Command{
        Name:      "EXPIRE",
        Key:       key,
        Value:     types.CommandValue{
            Type:  "float64",
            Value: float64(seconds),
        },
        Timestamp: time.Now().Unix(),
    }
    
    database.AddCommandsToAofBuffer(cmd)
    
    return true
}

func (database *RedigoDB) StartDataExpirationListener() {
    ticker := time.NewTicker(database.envs.DataExpirationInterval)
    for range ticker.C {
        now := time.Now().Unix()
        
        var expiredKeys []string
        
        database.storeMutex.Lock()

        for key, expireTime := range database.expirationKeys {
            if now > expireTime {
                expiredKeys = append(expiredKeys, key)
            }
        }
        
        for _, key := range expiredKeys {
            delete(database.store, key)
            delete(database.expirationKeys, key)
        }
        database.storeMutex.Unlock()
    }
}