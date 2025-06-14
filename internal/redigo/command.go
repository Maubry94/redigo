package redigo

import (
	"redigo/internal/redigo/errors"
	"redigo/internal/redigo/types"
	"time"
)

// Set stores a key-value pair in the database with optional TTL
// Returns an error if the key already exists
func (database *RedigoDB) Set(key string, value types.RedigoStorableValues, ttl int64) error {
    now := time.Now().Unix()
    
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()

    // Prevent overwriting existing keys
    if _, exists := database.store[key]; exists {
        return errors.ErrorKeyAlreadyExists
    }

    // Store the key-value pair
    database.store[key] = value
    
    // Add to reverse indexes
    database.addToIndex(key, value)
    
    // Handle TTL setting
    if ttl > 0 {
        expirationTime := now + ttl
        database.expirationKeys[key] = expirationTime
    } else {
        // Remove any existing expiration if TTL is 0 (permanent key)
        delete(database.expirationKeys, key)
    }
    
    //TODO: a voir pour refacto
    var valueType string
    var rawValue any
    
    // Convert typed values to serializable format for AOF logging
    switch input := value.(type) {
        case types.RedigoString:
            valueType = "string"
            rawValue = string(input)
        case types.RedigoBool:
            valueType = "bool"
            rawValue = bool(input)
        case types.RedigoInt:
            valueType = "int"
            rawValue = int(input)
    }
    
    // Create command for AOF logging
    command := types.Command{
        Name:      "SET",
        Key:       key,
        Value:     types.CommandValue{
            Type:  valueType,
            Value: rawValue,
        },
        Ttl:       &ttl,
        Timestamp: now,
    }
    
    // Add command to buffer for eventual persistence
    database.AddCommandsToAofBuffer(command)

    return nil
}

// Retrieves a value by key from the database
// Automatically removes expired keys and returns appropriate errors
func (database *RedigoDB) Get(key string) (types.RedigoStorableValues, error) {
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    // Check if key has expired and clean it up if so
    if expireTime, exists := database.expirationKeys[key]; exists && time.Now().Unix() > expireTime {
        delete(database.store, key)
        delete(database.expirationKeys, key)
        
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
    delete(database.store, key)
    delete(database.expirationKeys, key)
    
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
