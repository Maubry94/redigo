package redigo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"strconv"
	"time"
)

// Safely closes the AOF file handle
// Returns an error if the file close operation fails
func (database *RedigoDB) CloseAof() error {
    if database.aofFile != nil {
        return database.aofFile.Close()
    }
    return nil
}

// Adds a command to the AOF buffer in a thread-safe manner
// The buffer is used to batch commands before writing them to disk
// Returns the updated buffer slice
func (database *RedigoDB) AddCommandsToAofBuffer(command types.Command) []types.Command {
    database.aofCommandsBufferMutex.Lock()
    database.aofCommandsBuffer = append(database.aofCommandsBuffer, command)
    database.aofCommandsBufferMutex.Unlock()
    
    return database.aofCommandsBuffer
}

// Reads and replays commands from the AOF file to restore database state
// This is called during server startup to recover data from the previous session
func (database *RedigoDB) LoadFromAof() error {
    // Get the AOF file path from configuration
    aofPath, err := utils.GetAOFPath()
    if err != nil {
        return err
    }
    
    // Check if AOF file exists, if not, start with empty database
    if _, err := os.Stat(aofPath); os.IsNotExist(err) {
        return nil
    }
    
    // Open the AOF file for reading
    file, err := os.Open(aofPath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    // Scan the file line by line
    scanner := bufio.NewScanner(file)
    lineNumber := 0
    now := time.Now().Unix() // Current timestamp for TTL calculations
    
    for scanner.Scan() {
        lineNumber++
        row := scanner.Text()
        
        // Parse JSON command from each line
        var command types.Command
        if err := json.Unmarshal([]byte(row), &command); err != nil {
            fmt.Printf("Error when reading at %d: %v\n", lineNumber, err)
            continue
        }
        
        // Replay commands based on their type
        switch command.Name {
            case "SET":
                var value types.RedigoStorableValues

                // Convert the stored value back to its proper type
                switch command.Value.Type {
                case "string":
                    if strVal, ok := command.Value.Value.(string); ok {
                        value = types.RedigoString(strVal)
                    }
                case "bool":
                    if boolVal, ok := command.Value.Value.(bool); ok {
                        value = types.RedigoBool(boolVal)
                    }
                case "int":
                    // Handle different numeric representations from JSON
                    switch v := command.Value.Value.(type) {
                    case float64:
                        value = types.RedigoInt(int(v))
                    case string:
                        if intVal, err := strconv.Atoi(v); err == nil {
                            value = types.RedigoInt(intVal)
                        }
                    }
                }

                // Store the value if conversion was successful
                if value != nil {
                    database.storeMutex.Lock()
                    database.store[command.Key] = value

                    // Handle TTL restoration, checking if key has expired
                    if command.Ttl != nil && *command.Ttl > 0 {
                        expirationTime := command.Timestamp + *command.Ttl
                        if expirationTime > now {
                            // Key is still valid, restore expiration
                            database.expirationKeys[command.Key] = expirationTime
                        } else {
                            // Key has expired, remove it
                            delete(database.store, command.Key)
                            delete(database.expirationKeys, command.Key)
                        }
                    }
                    database.storeMutex.Unlock()
                }
                
            case "DELETE":
                // Replay DELETE command by removing key and its expiration
                database.storeMutex.Lock()
                delete(database.store, command.Key)
                delete(database.expirationKeys, command.Key)
                database.storeMutex.Unlock()
                
            case "EXPIRE":
                // Replay EXPIRE command with time adjustment
                database.storeMutex.Lock()
                if _, exists := database.store[command.Key]; exists {
                    var seconds int64
                    // Parse the expiration seconds from different formats
                    switch v := command.Value.Value.(type) {
                    case float64:
                        seconds = int64(v)
                    case string:
                        if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
                            seconds = intVal
                        }
                    }
                    
                    // Calculate remaining time considering elapsed time since command
                    elapsedTime := now - command.Timestamp
                    remainingTime := seconds - elapsedTime

                    if remainingTime > 0 {
                        // Key still has time left, set new expiration
                        database.expirationKeys[command.Key] = now + remainingTime
                    } else {
                        // Key has expired, remove it
                        delete(database.store, command.Key)
                        delete(database.expirationKeys, command.Key)
                    }
                }
                database.storeMutex.Unlock()
        }
    }
    
    // Check for scanner errors
    if err := scanner.Err(); err != nil {
        return err
    }
    
    return nil
}
