package redigo

import (
	"encoding/json"
	"fmt"
	"redigo/internal/redigo/types"
	"time"

	"github.com/samber/lo"
)

// Writes all buffered commands to the AOF file and clears the buffer
func (database *RedigoDB) FlushBuffer() error {
	// Lock the buffer to prevent concurrent access during flush
	database.aofCommandsBufferMutex.Lock()

	if len(database.aofCommandsBuffer) == 0 {
		database.aofCommandsBufferMutex.Unlock()
		return nil
	}

	aofBufferCommands := lo.Map(
		database.aofCommandsBuffer,
		func(cmd types.Command, _ int) types.Command {
			return cmd
		},
	)
	// Clear the original buffer by resetting its length to 0
	database.aofCommandsBuffer = database.aofCommandsBuffer[:0]
	database.aofCommandsBufferMutex.Unlock()

	// Lock AOF file access to ensure atomic writes
	database.aofMutex.Lock()
	defer database.aofMutex.Unlock()

	var accumulatedError error

	lo.ForEach(
		aofBufferCommands,
		func(command types.Command, _ int) {
			// Skip processing if we already have an error
			if accumulatedError != nil {
				return
			}

			// Serialize and write command
			if err := database.writeCommandToAof(command); err != nil {
				accumulatedError = err
			}
		},
	)

	return accumulatedError
}

// Helper function to write a single command to AOF file
func (database *RedigoDB) writeCommandToAof(command types.Command) error {
	// Serialize command to JSON format
	jsonCommand, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Write JSON command to AOF file with newline delimiter
	if _, err := database.aofFile.WriteString(string(jsonCommand) + "\n"); err != nil {
		return fmt.Errorf("failed to write to AOF file: %w", err)
	}

	return nil
}

// Runs a background goroutine that periodically flushes the AOF buffer
func (database *RedigoDB) StartBufferListener() {
	// Create a ticker that triggers at the configured flush interval
	ticker := time.NewTicker(database.envs.FlushBufferInterval)

	// Define flush handler as a function
	flushHandler := func() {
		err := database.FlushBuffer()
		message := lo.Ternary(
			err != nil,
			fmt.Sprintf("Error when flushing aof buffer: %v", err),
			"AOF buffer flushed successfully",
		)
		fmt.Println(message)
	}

	// Infinite loop that processes ticker events
	for range ticker.C {
		flushHandler()
	}
}
