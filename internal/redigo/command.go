package redigo

import (
	"fmt"
	"redigo/internal/redigo/errors"
	"redigo/internal/redigo/types"
	"strconv"
	"strings"
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
		database.UnsafeRemoveKey(key)

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
	database.UnsafeRemoveKey(key)

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

// Sets an expiration time for an existing key
// Returns true if expiration was set successfully, false if key doesn't exist
func (database *RedigoDB) SetExpiry(key string, seconds int64) bool {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	// Verify key exists before setting expiration
	_, exists := database.store[key]
	if !exists {
		return false
	}

	if seconds > 0 {
		// Set expiration time (current time + seconds)
		database.expirationKeys[key] = time.Now().Unix() + seconds
	} else if seconds == 0 {
		// Remove expiration (make key permanent)
		delete(database.expirationKeys, key)
	}

	if seconds < 0 {
		return false // Invalid expiration
	}

	// Note: negative seconds are ignored (no action taken)

	// Log the EXPIRE command for persistence
	cmd := types.Command{
		Name: "EXPIRE",
		Key:  key,
		Value: types.CommandValue{
			Type:  "float64",
			Value: float64(seconds),
		},
		Timestamp: time.Now().Unix(),
	}

	database.AddCommandsToAofBuffer(cmd)

	return true
}

// Finds all keys that have the specified value
func (database *RedigoDB) SearchByValue(value string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	if entry, exists := database.valueIndex.Entries[value]; exists {
		result := make([]string, 0, len(entry.Keys))
		for key := range entry.Keys {
			result = append(result, key)
		}
		return result
	}
	return nil
}

// Finds all keys that start with the specified prefix
func (database *RedigoDB) SearchByKeyPrefix(prefix string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	if entry, exists := database.prefixIndex.Entries[prefix]; exists {
		result := make([]string, 0, len(entry.Keys))
		for key := range entry.Keys {
			result = append(result, key)
		}
		return result
	}
	return nil
}

// Finds all keys that end with the specified suffix
func (database *RedigoDB) SearchByKeySuffix(suffix string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	if entry, exists := database.suffixIndex.Entries[suffix]; exists {
		result := make([]string, 0, len(entry.Keys))
		for key := range entry.Keys {
			result = append(result, key)
		}
		return result
	}
	return nil
}

// Finds all keys that contain the specified substring
func (database *RedigoDB) SearchByKeyContains(substring string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	var result []string

	// Search through all keys to find those containing the substring
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	for key := range database.store {
		if strings.Contains(key, substring) {
			result = append(result, key)
		}
	}

	return result
}

func DeserializeCommandValue(commandValue types.CommandValue) (any, error) {
	switch commandValue.Type {
	case "string":
		if strVal, ok := commandValue.Value.(string); ok {
			return strVal, nil
		}
	case "bool":
		if boolVal, ok := commandValue.Value.(bool); ok {
			return boolVal, nil
		}
	case "int":
		switch v := commandValue.Value.(type) {
		case float64:
			return int(v), nil
		case string:
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			return i, nil
		}
	case "float64":
		if floatVal, ok := commandValue.Value.(float64); ok {
			return floatVal, nil
		}
	}
	return nil, fmt.Errorf("unsupported or invalid CommandValue type: %s", commandValue.Type)
}
