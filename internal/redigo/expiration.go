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
		database.SafeRemoveKey(key)
		return -1, false // Key has expired and been removed
	}

	return remainingTime, true // Return remaining seconds until expiration
}

// Handle TTL restoration for SET command
func (database *RedigoDB) handleTtlRestoration(command types.Command) {
	if command.Ttl == nil || *command.Ttl <= 0 {
		return
	}

	now := time.Now().Unix()
	expirationTime := command.Timestamp + *command.Ttl

	if expirationTime > now {
		// Key is still valid, restore expiration
		database.expirationKeys[command.Key] = expirationTime
	} else {
		// Key has expired, remove it immediately
		database.UnsafeRemoveKey(command.Key)
	}
}

// Apply expiration to a key considering elapsed time
func (database *RedigoDB) applyExpiration(key string, commandTimestamp, seconds int64) {
	now := time.Now().Unix()
	elapsedTime := now - commandTimestamp
	remainingTime := seconds - elapsedTime

	if remainingTime > 0 {
		// Key still has time left, set new expiration
		database.expirationKeys[key] = now + remainingTime
	} else {
		// Key has expired, remove it
		database.UnsafeRemoveKey(key)
	}
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
			database.SafeRemoveKey(key)
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
