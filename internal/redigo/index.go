package redigo

import (
	"fmt"
	"os"
	"redigo/envs"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"sync"

	"github.com/samber/lo"
)


var envsConfig envs.Envs

type RedigoDB struct {
	store                  map[string]any   // Main key-value store
	expirationKeys         map[string]int64 // Maps keys to their expiration timestamps
	storeMutex             sync.Mutex       // Protects concurrent access to store and expirationKeys
	aofFile                *os.File         // Handle to the AOF for persistence
	aofMutex               sync.Mutex       // Protects concurrent writes to AOF file
	envs                   envs.Envs        // Configuration loaded from environment variables
	aofCommandsBuffer      []types.Command  // Buffer for AOF commands before flushing to disk
	aofCommandsBufferMutex sync.Mutex       // Protects concurrent access to the AOF buffer

	valueIndex  *types.ReverseIndex // Index for searching by exact value
	prefixIndex *types.ReverseIndex // Index for searching by key prefix
	suffixIndex *types.ReverseIndex // Index for searching by key suffix
	indexMutex  sync.RWMutex        // Protects concurrent access to indexes
}

func InitializeRedigo() (*RedigoDB, error) {
	envs := envs.Gets()

	database := &RedigoDB{
		store:             make(map[string]any),
		expirationKeys:    make(map[string]int64),
		envs:              envs,
		aofCommandsBuffer: make([]types.Command, 0),
	}

	indexTypes := []types.IndexType{
		types.VALUE_INDEX,
		types.PREFIX_INDEX,
		types.SUFFIX_INDEX,
	}
	indexes := lo.Map(
		indexTypes,
		func(indexType types.IndexType, _ int) *types.ReverseIndex {
			return &types.ReverseIndex{
				Type:    indexType,
				Entries: make(map[string]*types.IndexEntry),
			}
		})

	database.valueIndex = indexes[0]
	database.prefixIndex = indexes[1]
	database.suffixIndex = indexes[2]

	initSteps := []types.InitializationStep{
		{
			Name: "snapshot",
			Function: database.LoadFromSnapshot,
		},
		{
			Name: "aof_setup",
			Function: func() error {
				aofPath, err := utils.GetAOFPath()
				if err != nil {
					return err
				}

				loadedAOF, err := os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if err != nil {
					return err
				}

				database.aofFile = loadedAOF
				return nil
			},
		},
		{
			Name: "aof_load",
			Function: database.LoadFromAof,
		},
		{
			Name: "indexes_load",
			Function: func() error {
				if err := database.LoadIndexesFromFile(); err != nil {
					fmt.Printf("Failed to load indexes: %v\n", err)
				}
				return nil
			},
		},
	}

	for _, step := range initSteps {
		if err := step.Function(); err != nil {
			return nil, fmt.Errorf("error during %s initialization: %w", step.Name, err)
		}
	}

	backgroundProcesses := []func(){
		database.StartSnapshotListener,
		database.StartDataExpirationListener,
		database.StartBufferListener,
	}

	lo.ForEach(
		backgroundProcesses,
		func(process func(), _ int) {
			go process()
		},
	)

	return database, nil
}

func (database *RedigoDB) addToIndex(key string, value any) {
	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	valueStr := utils.ValueToString(value)

	indexOperations := []types.IndexOperation{
		{
			Name:  "value",
			Index: database.valueIndex,
			GetKeys: func(k string) []string {
				return []string{valueStr}
			},
		},
		{
			Name:  "prefix",
			Index: database.prefixIndex,
			GetKeys: func(k string) []string {
				return lo.Map(
					lo.Range(len(k)),
					func(i int, _ int) string {
						return k[:i+1]
					},
				)
			},
		},
		{
			Name:  "suffix",
			Index: database.suffixIndex,
			GetKeys: func(k string) []string {
				return lo.Map(lo.Range(len(k)), func(i int, _ int) string {
					return k[i:]
				})
			},
		},
	}

	lo.ForEach(
		indexOperations,
		func(operation types.IndexOperation, _ int) {
			keys := operation.GetKeys(key)

			lo.ForEach(
				keys,
				func(indexKey string, _ int) {
					lo.Ternary(
						lo.HasKey(operation.Index.Entries, indexKey),
						func() {
							operation.Index.Entries[indexKey].Keys[key] = true
						},
						func() {
							operation.Index.Entries[indexKey] = &types.IndexEntry{
								Keys: map[string]bool{key: true},
							}
						},
					)()
				},
			)
		},
	)
}

func (database *RedigoDB) removeFromIndex(key string, value any) {
	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	valueStr := utils.ValueToString(value)

	indexRemovalOps := []types.IndexOperation{
		{
			Name:  "value",
			Index: database.valueIndex,
			GetKeys: func(k string) []string {
				return []string{valueStr}
			},
		},
		{
			Name:  "prefix",
			Index: database.prefixIndex,
			GetKeys: func(k string) []string {
				return lo.Map(lo.Range(len(k)), func(i int, _ int) string {
					return k[:i+1]
				})
			},
		},
		{
			Name:  "suffix",
			Index: database.suffixIndex,
			GetKeys: func(k string) []string {
				return lo.Map(lo.Range(len(k)), func(i int, _ int) string {
					return k[i:]
				})
			},
		},
	}

	lo.ForEach(
		indexRemovalOps,
		func(operation types.IndexOperation, _ int) {
			keys := operation.GetKeys(key)

			lo.ForEach(
				keys,
				func(indexKey string, _ int) {
					if entry, exists := operation.Index.Entries[indexKey]; exists {
						delete(entry.Keys, key)

						lo.Ternary(
							len(entry.Keys) == 0,
							func() { delete(operation.Index.Entries, indexKey) },
							func() { /* nothing */ },
						)()
					}
				},
			)
		},
	)
}

func (database *RedigoDB) SafeRemoveKey(key string) {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	cleanupActions := []func(){
		func() { delete(database.store, key) },
		func() { delete(database.expirationKeys, key) },
	}

	lo.ForEach(
		cleanupActions,
		func(action func(), _ int) {
			action()
		},
	)
}

func (database *RedigoDB) UnsafeRemoveKey(key string) {
	cleanupActions := []func(){
		func() { delete(database.store, key) },
		func() { delete(database.expirationKeys, key) },
	}

	lo.ForEach(
		cleanupActions,
		func(action func(), _ int) {
			action()
		},
	)
}

func (database *RedigoDB) ForceSave() error {
	return lo.Ternary(
		func() error {
			return database.UpdateSnapshot()
		}() != nil,
		func() error {
			return fmt.Errorf("Error creating snapshot: %w", database.UpdateSnapshot())
		},
		func() error {
			return nil
		},
	)()
}
