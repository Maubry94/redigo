package redigo

import (
	"fmt"
	"redigo/internal/redigo/errors"
	"redigo/internal/redigo/types"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
)

type SetTypeResolution struct {
	valueType string
	rawValue  any
	found     bool
}

func (database *RedigoDB) Set(key string, value any, ttl int64) error {
	now := time.Now().Unix()

	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	if _, exists := database.store[key]; exists {
		return errors.ErrorKeyAlreadyExists
	}

	database.store[key] = value
	database.addToIndex(key, value)

	lo.Ternary(
		ttl > 0,
		func() { database.expirationKeys[key] = now + ttl },
		func() { delete(database.expirationKeys, key) },
	)()

	valueTypeMapping := map[string]func(any) (string, any, bool){
		"string": func(value any) (string, any, bool) {
			if val, ok := value.(string); ok {
				return "string", val, true
			}
			return "", nil, false
		},
		"int": func(value any) (string, any, bool) {
			if val, ok := value.(int); ok {
				return "int", val, true
			}
			return "", nil, false
		},
		"bool": func(value any) (string, any, bool) {
			if val, ok := value.(bool); ok {
				return "bool", val, true
			}
			return "", nil, false
		},
		"float64": func(value any) (string, any, bool) {
			if val, ok := value.(float64); ok {
				return "float64", val, true
			}
			return "", nil, false
		},
	}

	typeResult := lo.Reduce(
		lo.Keys(valueTypeMapping),
		func(
			acc SetTypeResolution,
			typeKey string,
			_ int,
		) SetTypeResolution {
			if acc.found {
				return acc
			}
			if valueType, rawValue, ok := valueTypeMapping[typeKey](value); ok {
				return SetTypeResolution{valueType, rawValue, true}
			}
			return acc
		},
		SetTypeResolution{"", nil, false},
	)

	if !typeResult.found {
		return errors.ErrorUnsupportedValueType
	}

	command := types.Command{
		Name: "SET",
		Key:  key,
		Value: types.CommandValue{
			Type:  typeResult.valueType,
			Value: typeResult.rawValue,
		},
		Ttl:       &ttl,
		Timestamp: now,
	}

	database.AddCommandsToAofBuffer(command)

	return nil
}

func (database *RedigoDB) Get(key string) (any, error) {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	if expireTime, exists := database.expirationKeys[key]; exists {
		isExpired := time.Now().Unix() > expireTime

		return lo.Ternary(
			isExpired,
			func() (any, error) {
				database.UnsafeRemoveKey(key)

				command := types.Command{
					Name:      "DELETE",
					Key:       key,
					Value:     types.CommandValue{},
					Timestamp: time.Now().Unix(),
				}
				database.AddCommandsToAofBuffer(command)

				return nil, errors.ErrorKeyExpired
			},
			func() (any, error) {
				if val, ok := database.store[key]; ok {
					return val, nil
				}
				return nil, errors.ErrorKeyNotFound
			},
		)()
	}

	value, ok := database.store[key]
	return lo.Ternary(
		ok,
		func() (any, error) {
			return value, nil
		},
		func() (any, error) {
			return nil, errors.ErrorKeyNotFound
		},
	)()
}

func (database *RedigoDB) Delete(key string) bool {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	if value, exists := database.store[key]; exists {
		database.removeFromIndex(key, value)

		database.UnsafeRemoveKey(key)

		command := types.Command{
			Name:      "DELETE",
			Key:       key,
			Value:     types.CommandValue{},
			Timestamp: time.Now().Unix(),
		}

		database.AddCommandsToAofBuffer(command)
		return true
	}

	return false
}

func (database *RedigoDB) SetExpiry(key string, seconds int64) bool {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	_, exists := database.store[key]
	if !exists {
		return false
	}

	expirationHandlers := map[string]func(int64) bool{
		"positive": func(s int64) bool {
			database.expirationKeys[key] = time.Now().Unix() + s
			return true
		},
		"zero": func(s int64) bool {
			delete(database.expirationKeys, key)
			return true
		},
		"negative": func(s int64) bool {
			return false
		},
	}

	expirationResult := lo.Reduce(
		[]string{"positive", "zero", "negative"},
		func(acc bool, handlerType string, _ int) bool {
			if acc {
				return acc
			}

			shouldHandle := lo.Switch[string, bool](handlerType).
				Case("positive", seconds > 0).
				Case("zero", seconds == 0).
				Case("negative", seconds < 0).
				Default(false)

			if shouldHandle {
				return expirationHandlers[handlerType](seconds)
			}
			return false
		},
		false,
	)

	if !expirationResult {
		return false
	}

	command := types.Command{
		Name: "EXPIRE",
		Key:  key,
		Value: types.CommandValue{
			Type:  "float64",
			Value: float64(seconds),
		},
		Timestamp: time.Now().Unix(),
	}

	database.AddCommandsToAofBuffer(command)
	return true
}

func (database *RedigoDB) SearchByValue(value string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	return lo.Ternary(
		lo.HasKey(database.valueIndex.Entries, value),
		func() []string {
			return lo.Keys(database.valueIndex.Entries[value].Keys)
		},
		func() []string {
			return nil
		},
	)()
}

func (database *RedigoDB) SearchByKeyPrefix(prefix string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	return lo.Ternary(
		lo.HasKey(database.prefixIndex.Entries, prefix),
		func() []string {
			return lo.Keys(database.prefixIndex.Entries[prefix].Keys)
		},
		func() []string {
			return nil
		},
	)()
}

func (database *RedigoDB) SearchByKeySuffix(suffix string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	return lo.Ternary(
		lo.HasKey(database.suffixIndex.Entries, suffix),
		func() []string {
			return lo.Keys(database.suffixIndex.Entries[suffix].Keys)
		},
		func() []string {
			return nil
		},
	)()
}

func (database *RedigoDB) SearchByKeyContains(substring string) []string {
	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	return lo.Filter(
		lo.Keys(database.store),
		func(key string, _ int) bool {
			return strings.Contains(key, substring)
		},
	)
}

func DeserializeCommandValue(commandValue types.CommandValue) (any, error) {
	deserializers := map[string]func(any) (any, error){
		"string": func(value any) (any, error) {
			if stringValue, ok := value.(string); ok {
				return stringValue, nil
			}
			return nil, fmt.Errorf("expected string, got %T", value)
		},
		"bool": func(value any) (any, error) {
			if boolValue, ok := value.(bool); ok {
				return boolValue, nil
			}
			return nil, fmt.Errorf("expected bool, got %T", value)
		},
		"int": func(value any) (any, error) {
			switch v := value.(type) {
			case float64:
				return int(v), nil
			case string:
				parsedInt, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("cannot convert string to int: %w", err)
				}
				return parsedInt, nil
			default:
				return nil, fmt.Errorf("cannot convert %T to int", v)
			}
		},
		"float64": func(value any) (any, error) {
			if floatValue, ok := value.(float64); ok {
				return floatValue, nil
			}
			return nil, fmt.Errorf("expected float64, got %T", value)
		},
	}

	deserializer, exists := deserializers[commandValue.Type]
	if !exists {
		return nil, fmt.Errorf("unsupported CommandValue type: %s", commandValue.Type)
	}

	return deserializer(commandValue.Value)
}

func IsValidCommandType(commandName types.CommandName) bool {
	validCommands := []types.CommandName{types.SET, types.DELETE, types.EXPIRE}
	return lo.Contains(validCommands, commandName)
}
