package redigo

import (
	"redigo/internal/redigo/types"
	"time"
)

// Returns the time-to-live for a key in seconds
func (database *RedigoDB) GetTtl(key string) (int64, bool) {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	// Check if key exists in the store
	_, keyExists := database.store[key]
	if !keyExists {
		return -1, false // Key doesn't exist
	}

	// Check if key has an expiration time set
	expirationTime, hasExpiry := database.expirationKeys[key]
	if !hasExpiry {
		return 0, true // Key exists but has no expiration
	}

	now := time.Now().Unix()
	remainingTime := expirationTime - now

	// If key has expired, clean it up immediately
	if remainingTime <= 0 {
		delete(database.store, key)
		delete(database.expirationKeys, key)
		return -1, false // Key has expired and been removed
	}

	return remainingTime, true // Return remaining seconds until expiration
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
	// Note: negative seconds are ignored (no action taken)

	// Log the EXPIRE command for persistence
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

// Runs a background goroutine that periodically
// scans for and removes expired keys from the database
func (database *RedigoDB) StartDataExpirationListener() {
	// Create ticker that triggers at configured expiration check interval
	ticker := time.NewTicker(database.envs.DataExpirationInterval)

	// Infinite loop for periodic expiration cleanup
	for range ticker.C {
		now := time.Now().Unix()

		var expiredKeys []string

		// Lock database and collect expired keys
		database.storeMutex.Lock()

		// Scan all keys with expiration times
		for key, expireTime := range database.expirationKeys {
			if now > expireTime {
				expiredKeys = append(expiredKeys, key)
			}
		}

		// Remove expired keys from both store and expiration tracking
		for _, key := range expiredKeys {
			delete(database.store, key)
			delete(database.expirationKeys, key)
		}
		database.storeMutex.Unlock()

		// Log deletion commands for each expired key (outside of lock)
		for _, key := range expiredKeys {
			command := types.Command{
				Name:      "DELETE",
				Key:       key,
				Value:     types.CommandValue{},
				Timestamp: now,
			}
			database.AddCommandsToAofBuffer(command)
		}
	}
}
