package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
)

func (database *RedigoDB) DumpIndexesToFile() error {
	indexesPath, err := utils.GetIndexesFilePath()
	if err != nil {
		return fmt.Errorf("failed to get index file path: %w", err)
	}

	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	indexes := map[string]*types.ReverseIndex{
		"value":  database.valueIndex,
		"prefix": database.prefixIndex,
		"suffix": database.suffixIndex,
	}

	data, err := json.MarshalIndent(indexes, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal indexes: %w", err)
	}

	if err := os.WriteFile(indexesPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	return nil
}

func (database *RedigoDB) LoadIndexesFromFile() error {
	indexesPath, err := utils.GetIndexesFilePath()
	if err != nil {
		return fmt.Errorf("failed to get index file path: %w", err)
	}

	// Check if the indexes file exists
	if _, err := os.Stat(indexesPath); os.IsNotExist(err) {
		fmt.Println("No index file found, continuing with empty indexes")
		return nil
	}

	data, err := os.ReadFile(indexesPath)
	if err != nil {
		return err
	}

	indexes := map[string]*types.ReverseIndex{}
	if err := json.Unmarshal(data, &indexes); err != nil {
		return err
	}

	database.indexMutex.Lock()
	defer database.indexMutex.Unlock()

	// Recharge les index dans l’objet
	if value, ok := indexes["value"]; ok {
		database.valueIndex = value
	}
	if prefix, ok := indexes["prefix"]; ok {
		database.prefixIndex = prefix
	}
	if suffix, ok := indexes["suffix"]; ok {
		database.suffixIndex = suffix
	}

	fmt.Printf("Loaded %d value indexes, %d prefix indexes, %d suffix indexes\n",
		len(database.valueIndex.Entries),
		len(database.prefixIndex.Entries),
		len(database.suffixIndex.Entries),
	)

	return nil
}
