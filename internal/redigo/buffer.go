package redigo

import (
	"encoding/json"
	"fmt"
	"redigo/internal/redigo/types"
	"time"

	"github.com/samber/lo"
)

// Writes all buffered commands to the AOF file and clears the buffer
// This operation is thread-safe and ensures data persistence
func (database *RedigoDB) FlushBuffer() error {
	// Lock the buffer to prevent concurrent access during flush
	database.aofCommandsBufferMutex.Lock()

	// If buffer is empty, nothing to flush
	if len(database.aofCommandsBuffer) == 0 {
		database.aofCommandsBufferMutex.Unlock()
		return nil
	}

	// Create a copy of the buffer to avoid holding the lock during file I/O
	aofBufferCommands := make([]types.Command, len(database.aofCommandsBuffer))
	copy(aofBufferCommands, database.aofCommandsBuffer)
	// Clear the original buffer by resetting its length to 0
	database.aofCommandsBuffer = database.aofCommandsBuffer[:0]
	database.aofCommandsBufferMutex.Unlock()

	// Lock AOF file access to ensure atomic writes
	database.aofMutex.Lock()
	defer database.aofMutex.Unlock()

	var err error

	// Iterate through all buffered commands and write them to disk
	lo.ForEach(
		aofBufferCommands,
		func(command types.Command, _ int) {
			// Serialize command to JSON format
			jsonCommand, marshalError := json.Marshal(command)
			if marshalError != nil {
				err = marshalError
				return
			}

			// Write JSON command to AOF file with newline delimiter
			if _, writeError := database.aofFile.WriteString(string(jsonCommand) + "\n"); writeError != nil {
				err = writeError
				return
			}
		},
	)

	// Return any error that occurred during the flush process
	if err != nil {
		return err
	}

	return nil
}

// Runs a background goroutine that periodically flushes the AOF buffer
// This ensures commands are persisted to disk at regular intervals
func (database *RedigoDB) StartBufferListener() {
	// Create a ticker that triggers at the configured flush interval
	ticker := time.NewTicker(database.envs.FlushBufferInterval)

	// Infinite loop that processes ticker events
	for range ticker.C {
		// Attempt to flush the buffer
		if err := database.FlushBuffer(); err != nil {
			fmt.Printf("Error when flushing aof buffer: %v\n", err)
		} else {
			fmt.Println("AOF buffer flushed successfully")
		}
	}
}
