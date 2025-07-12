package redigo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"strconv"

	"github.com/samber/lo"
)

func (database *RedigoDB) CloseAof() error {
	if database.aofFile != nil {
		return database.aofFile.Close()
	}
	return nil
}

func (database *RedigoDB) AddCommandsToAofBuffer(command types.Command) []types.Command {
	database.aofCommandsBufferMutex.Lock()
	database.aofCommandsBuffer = append(database.aofCommandsBuffer, command)
	database.aofCommandsBufferMutex.Unlock()

	return database.aofCommandsBuffer
}

func (database *RedigoDB) LoadFromAof() error {
	aofPath, err := utils.GetAOFPath()
	if err != nil {
		return fmt.Errorf("failed to get AOF path: %w", err)
	}

	if !utils.FileExists(aofPath) {
		return nil
	}

	file, err := utils.OpenFile(aofPath)
	if err != nil {
		return err
	}
	defer file.Close()

	return database.processAofCommands(file)
}

func (database *RedigoDB) processAofCommands(aofFile *os.File) error {
	scanner := bufio.NewScanner(aofFile)
	lines := []string{}

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	lo.ForEach(
		lines,
		func(line string, index int) {
			if err := database.processAofLine(line); err != nil {
				fmt.Printf("Error processing AOF line %d: %v\n", index+1, err)
			}
		},
	)

	return nil
}

func (database *RedigoDB) processAofLine(aofLine string) error {
	var command types.Command
	if err := json.Unmarshal([]byte(aofLine), &command); err != nil {
		return fmt.Errorf("failed to parse aof line: %w", err)
	}

	return database.handleCommand(command)
}

func (database *RedigoDB) handleCommand(command types.Command) error {
	if !IsValidCommandType(command.Name) {
		return fmt.Errorf("unknown command type: %s", command.Name)
	}

	handlers := map[types.CommandName]func(types.Command) error{
		types.SET:    database.handleSetCommand,
		types.DELETE: database.handleDeleteCommand,
		types.EXPIRE: database.handleExpireCommand,
	}

	handler := handlers[command.Name]
	return handler(command)
}

func (database *RedigoDB) handleSetCommand(command types.Command) error {
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

func (database *RedigoDB) handleDeleteCommand(command types.Command) error {
	database.SafeRemoveKey(command.Key)
	return nil
}

func (database *RedigoDB) handleExpireCommand(command types.Command) error {
	seconds, err := database.parseExpirationSeconds(command.Value)
	if err != nil {
		return fmt.Errorf("failed to parse expiration seconds: %w", err)
	}

	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	if _, exists := database.store[command.Key]; !exists {
		return nil
	}

	database.applyExpiration(command.Key, command.Timestamp, seconds)
	return nil
}

func (database *RedigoDB) parseExpirationSeconds(value types.CommandValue) (int64, error) {
	parsers := map[string]func(any) (int64, error){
		"float64": func(value any) (int64, error) {
			if val, ok := value.(float64); ok {
				return int64(val), nil
			}
			return 0, fmt.Errorf("expected float64, got %T", value)
		},
		"string": func(value any) (int64, error) {
			if val, ok := value.(string); ok {
				seconds, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("invalid string format: %w", err)
				}
				return seconds, nil
			}
			return 0, fmt.Errorf("expected string, got %T", value)
		},
	}

	typeName := fmt.Sprintf("%T", value.Value)
	parser, exists := parsers[typeName]

	if !exists {
		return 0, fmt.Errorf("unsupported value type: %T", value.Value)
	}

	return parser(value.Value)
}
