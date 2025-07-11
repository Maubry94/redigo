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

	// Reverse indexes for efficient value-based searching
	valueIndex  *types.ReverseIndex // Index for searching by exact value
	prefixIndex *types.ReverseIndex // Index for searching by key prefix
	suffixIndex *types.ReverseIndex // Index for searching by key suffix
	indexMutex  sync.RWMutex        // Protects concurrent access to indexes
}

// Creates and initializes a new RedigoDB instance
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

	// Define initialization steps with their error handling
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
					// Non-critical error, continue
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

// Adds a key-value pair to the reverse indexes using functional approach
func (database *RedigoDB) addToIndex(key string, value any) {
	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	valueStr := utils.ValueToString(value)

	// Define index operations functionally
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

// Removes a key from all reverse indexes using functional approach
func (database *RedigoDB) removeFromIndex(key string, value any) {
	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	valueStr := utils.ValueToString(value)

	// Define index removal operations functionally
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

						// Remove entry if no keys left
						lo.Ternary(
							len(entry.Keys) == 0,
							func() { delete(operation.Index.Entries, indexKey) },
							func() { /* keep entry */ },
						)()
					}
				},
			)
		},
	)
}

// SafeRemoveKey removes a key from store and expiration keys with mutex protection
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

// UnsafeRemoveKey removes a key from store and expiration keys without mutex protection
func (database *RedigoDB) UnsafeRemoveKey(key string) {
	// Use functional approach to remove from multiple maps
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

// ForceSave forces the database to save its state to a snapshot file using functional approach
func (database *RedigoDB) ForceSave() error {
	// Use functional error handling with lo.Ternary
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
