package redigo

import (
	"redigo/internal/redigo/errors"
	"redigo/internal/redigo/types"
	"time"
)

// Set stores a key-value pair in the database with optional TTL
// Returns an error if the key already exists
func (database *RedigoDB) Set(key string, value any, ttl int64) error {
    now := time.Now().Unix()

    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()

    if _, exists := database.store[key]; exists {
        return errors.ErrorKeyAlreadyExists
    }
    
    database.store[key] = value
    database.addToIndex(key, value)

    if ttl > 0 {
        database.expirationKeys[key] = now + ttl
    } else {
        delete(database.expirationKeys, key)
    }

    // Determine the type of value for command serialization
    var valueType string
    var rawValue any

    switch v := value.(type) {
    case string:
        valueType = "string"
        rawValue = v
    case int:
        valueType = "int"
        rawValue = v
    case bool:
        valueType = "bool"
        rawValue = v
    case float64:
        valueType = "float64"
        rawValue = v
    default:
        return errors.ErrorUnsupportedValueType
    }

    command := types.Command{
        Name: "SET",
        Key:  key,
        Value: types.CommandValue{
            Type:  valueType,
            Value: rawValue,
        },
        Ttl:       &ttl,
        Timestamp: now,
    }

    database.AddCommandsToAofBuffer(command)

    return nil
}


// Retrieves a value by key from the database
// Automatically removes expired keys and returns appropriate errors
func (database *RedigoDB) Get(key string) (any, error) {
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    // Check if key has expired and clean it up if so
    if expireTime, exists := database.expirationKeys[key]; exists && time.Now().Unix() > expireTime {
        database.unsafeRemoveKey(key)
        
        // Log the automatic deletion for consistency
        command := types.Command{
            Name:      "DELETE",
            Key:       key,
            Value:     types.CommandValue{},
            Timestamp: time.Now().Unix(),
        }
        database.AddCommandsToAofBuffer(command)
        
        return nil, errors.ErrorKeyExpired
    }
    
    // Retrieve value from store
    val, ok := database.store[key]
    if !ok {
        return nil, errors.ErrorKeyNotFound
    }
    return val, nil
}

// Delete removes a key-value pair from the store
// Returns true if the key was deleted, false if it didn't exist
func (database *RedigoDB) Delete(key string) bool {
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    // Check if key exists before attempting deletion
    value, exists := database.store[key]
    if !exists {
        return false
    }
    
    // Remove from reverse indexes before deleting
    database.removeFromIndex(key, value)
    
    // Remove from both store and expiration tracking
    database.unsafeRemoveKey(key)
    
    // Log deletion command for persistence
    command := types.Command{
        Name:      "DELETE",
        Key:       key,
        Value:     types.CommandValue{},
        Timestamp: time.Now().Unix(),
    }
    
    database.AddCommandsToAofBuffer(command)
    
    return true
}
