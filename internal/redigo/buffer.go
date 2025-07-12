package redigo

import (
	"encoding/json"
	"fmt"
	"redigo/internal/redigo/types"
	"time"

	"github.com/samber/lo"
)

func (database *RedigoDB) FlushBuffer() error {
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
	database.aofCommandsBuffer = database.aofCommandsBuffer[:0]
	database.aofCommandsBufferMutex.Unlock()

	database.aofMutex.Lock()
	defer database.aofMutex.Unlock()

	var accumulatedError error

	lo.ForEach(
		aofBufferCommands,
		func(command types.Command, _ int) {
			if accumulatedError != nil {
				return
			}

			if err := database.writeCommandToAof(command); err != nil {
				accumulatedError = err
			}
		},
	)

	return accumulatedError
}

func (database *RedigoDB) writeCommandToAof(command types.Command) error {
	jsonCommand, err := json.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	if _, err := database.aofFile.WriteString(string(jsonCommand) + "\n"); err != nil {
		return fmt.Errorf("failed to write to AOF file: %w", err)
	}

	return nil
}

func (database *RedigoDB) StartBufferListener() {
	ticker := time.NewTicker(database.envs.FlushBufferInterval)

	flushHandler := func() {
		err := database.FlushBuffer()
		message := lo.Ternary(
			err != nil,
			fmt.Sprintf("Error when flushing aof buffer: %v", err),
			"AOF buffer flushed successfully",
		)
		fmt.Println(message)
	}

	for range ticker.C {
		flushHandler()
	}
}
