package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"redigo/pkg/utils"
	"time"

	"github.com/samber/lo"
)

func (database *RedigoDB) StartSnapshotListener() {
	ticker := time.NewTicker(database.envs.SnapshotSaveInterval)

	snapshotHandler := func() {
		message := lo.Ternary(
			database.UpdateSnapshot() != nil,
			fmt.Sprintf("Error when updating snapshot: %v", database.UpdateSnapshot()),
			"Snapshot updated successfully",
		)
		fmt.Println(message)
	}

	for range ticker.C {
		snapshotHandler()
	}
}

func (database *RedigoDB) UpdateSnapshot() error {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	now := time.Now().Unix()

	snapshot := lo.FilterMap(
		lo.Entries(database.store),
		func(entry lo.Entry[string, any], _ int) (lo.Entry[string, map[string]any], bool) {
			key, value := entry.Key, entry.Value

			if expireTime, exists := database.expirationKeys[key]; exists && now > expireTime {
				database.UnsafeRemoveKey(key)
				return lo.Entry[string, map[string]any]{}, false
			}

			return lo.Entry[string, map[string]any]{
				Key: key,
				Value: map[string]any{
					"value": value,
				},
			}, true
		},
	)

	snapshotMap := lo.FromEntries(snapshot)

	jsonData, err := json.MarshalIndent(snapshotMap, "", "  ")
	if err != nil {
		return err
	}

	fileOperations := []struct {
		name     string
		function func() error
	}{
		{
			"get_snapshot_path",
			func() error {
				_, err := utils.GetSnapshotFilePath()
				return err
			},
		},
		{
			"write_temp_file",
			func() error {
				snapshotPath, _ := utils.GetSnapshotFilePath()
				tempSnapshotPath := snapshotPath + ".tmp"
				return os.WriteFile(tempSnapshotPath, jsonData, 0644)
			},
		},
		{
			"atomic_rename",
			func() error {
				snapshotPath, _ := utils.GetSnapshotFilePath()
				tempSnapshotPath := snapshotPath + ".tmp"
				return os.Rename(tempSnapshotPath, snapshotPath)
			},
		},
		{
			"truncate_aof",
			func() error {
				aofPath, err := utils.GetAOFPath()
				if err != nil {
					return err
				}
				return os.Truncate(aofPath, 0)
			},
		},
		{
			"dump_indexes",
			func() error {
				return database.DumpIndexesToFile()
			},
		},
	}

	for _, operation := range fileOperations {
		if err := operation.function(); err != nil {
			return fmt.Errorf("failed to %s: %w", operation.name, err)
		}
	}

	return nil
}

func (database *RedigoDB) LoadFromSnapshot() error {
	snapshotPath, err := utils.GetSnapshotFilePath()
	if err != nil {
		return err
	}

	createEmptyFile := lo.Ternary(
		func() bool {
			_, err := os.Stat(snapshotPath)
			return os.IsNotExist(err)
		}(),
		func() error {
			return os.WriteFile(snapshotPath, []byte("{}"), 0600)
		},
		func() error {
			return nil
		},
	)

	if err := createEmptyFile(); err != nil {
		return err
	}

	snapshotFileContent, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	var snapshot map[string]map[string]any
	if err := json.Unmarshal(snapshotFileContent, &snapshot); err != nil {
		return err
	}

	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	database.store = make(map[string]any)

	lo.ForEach(
		lo.Entries(snapshot),
		func(entry lo.Entry[string, map[string]any], _ int) {
			key, item := entry.Key, entry.Value
			rawValue, exists := item["value"]

			if !exists {
				return
			}

			database.store[key] = lo.Switch[any, any](rawValue).
				Case(
					func(value any) bool {
						switch value.(type) {
						case string, bool, int, int64:
							return true
						}
						return false
					},
					func(value any) any {
						return value
					},
				).
				Case(
					func(value any) bool {
						_, ok := value.(float64)
						return ok
					},
					func(val any) any {
						value := val.(float64)
						return lo.Ternary(
							value == float64(int(value)),
							float64(int(value)),
							value,
						)
					},
				).
				Default(
					func(value any) any {
						fmt.Printf("Warning: unsupported value type %T for key '%s', skipping\n", value, key)
						return nil
					},
				)
		},
	)

	database.store = lo.PickBy(
		database.store,
		func(key string, value any) bool {
			return value != nil
		},
	)

	fmt.Printf("Database loaded from snapshot: %s (%d keys)\n", snapshotPath, len(database.store))
	return nil
}
