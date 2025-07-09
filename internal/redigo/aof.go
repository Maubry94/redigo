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
    aofPath, err := utils.GetAOFPath()
    if err != nil {
        return fmt.Errorf("failed to get AOF path: %w", err)
    }
    
    // Check if AOF file exists
    if !database.aofFileExists(aofPath) {
        return nil // No AOF file, start with empty database
    }
    
    file, err := database.openAofFile(aofPath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    return database.processAofCommands(file)
}

// Check if AOF file exists
func (database *RedigoDB) aofFileExists(aofPath string) bool {
    _, err := os.Stat(aofPath)
    return !os.IsNotExist(err)
}

// Open AOF file for reading
func (database *RedigoDB) openAofFile(aofPath string) (*os.File, error) {
    file, err := os.Open(aofPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open AOF file: %w", err)
    }
    return file, nil
}

// Process all commands from AOF file
func (database *RedigoDB) processAofCommands(file *os.File) error {
    scanner := bufio.NewScanner(file)
    lineNumber := 0
    
    for scanner.Scan() {
        lineNumber++
        if err := database.processAofLine(scanner.Text()); err != nil {
            // Log error but continue processing
            fmt.Printf("Error processing AOF line %d: %v\n", lineNumber, err)
        }
    }
    
    return scanner.Err()
}

// Process a single line from AOF file
func (database *RedigoDB) processAofLine(line string) error {
    var command types.Command
    if err := json.Unmarshal([]byte(line), &command); err != nil {
        return fmt.Errorf("failed to parse JSON: %w", err)
    }
    
    return database.replayCommand(command)
}

// Replay a single command based on its type
func (database *RedigoDB) replayCommand(command types.Command) error {
    switch command.Name {
    case types.SET:
        return database.replaySetCommand(command)
    case types.DELETE:
        return database.replayDeleteCommand(command)
    case types.EXPIRE:
        return database.replayExpireCommand(command)
    default:
        return fmt.Errorf("unknown command type: %s", command.Name)
    }
}

// Replay SET command
func (database *RedigoDB) replaySetCommand(command types.Command) error {
    value, err := DeserializeCommandValue(command.Value)
    if err != nil {
        return fmt.Errorf("failed to deserialize value: %w", err)
    }
    
    if value == nil {
        return fmt.Errorf("deserialized value is nil")
    }
    
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    database.store[command.Key] = value
    database.handleTtlRestoration(command)
    
    return nil
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
        database.unsafeRemoveKey(command.Key)
    }
}

// Replay DELETE command
func (database *RedigoDB) replayDeleteCommand(command types.Command) error {
    database.SafeRemoveKey(command.Key)
    return nil
}

// Replay EXPIRE command
func (database *RedigoDB) replayExpireCommand(command types.Command) error {
    seconds, err := database.parseExpirationSeconds(command.Value)
    if err != nil {
        return fmt.Errorf("failed to parse expiration seconds: %w", err)
    }
    
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    if _, exists := database.store[command.Key]; !exists {
        return nil // Key doesn't exist, nothing to do
    }
    
    database.applyExpiration(command.Key, command.Timestamp, seconds)
    return nil
}

// Parse expiration seconds from command value
func (database *RedigoDB) parseExpirationSeconds(value types.CommandValue) (int64, error) {
    switch v := value.Value.(type) {
    case float64:
        return int64(v), nil
    case string:
        seconds, err := strconv.ParseInt(v, 10, 64)
        if err != nil {
            return 0, fmt.Errorf("invalid string format: %w", err)
        }
        return seconds, nil
    default:
        return 0, fmt.Errorf("unsupported value type: %T", v)
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
        database.unsafeRemoveKey(key)
    }
}

// Remove key without mutex (assumes caller already has lock)
func (database *RedigoDB) unsafeRemoveKey(key string) {
    delete(database.store, key)
    delete(database.expirationKeys, key)
}

func DeserializeCommandValue(val types.CommandValue) (any, error) {
    switch val.Type {
    case "string":
        if strVal, ok := val.Value.(string); ok {
            return strVal, nil
        }
    case "bool":
        if boolVal, ok := val.Value.(bool); ok {
            return boolVal, nil
        }
    case "int":
        switch v := val.Value.(type) {
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
        if floatVal, ok := val.Value.(float64); ok {
            return floatVal, nil
        }
    }
    return nil, fmt.Errorf("unsupported or invalid CommandValue type: %s", val.Type)
}

