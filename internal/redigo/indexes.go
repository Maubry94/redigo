package redigo

import (
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"

	"github.com/samber/lo"
)

type IndexMapping struct {
	Key   string
	Field **types.ReverseIndex
}

func (database *RedigoDB) DumpIndexesToFile() error {
	indexesPath, err := utils.GetIndexesFilePath()
	if err != nil {
		return fmt.Errorf("failed to get index file path: %w", err)
	}

	database.indexMutex.RLock()
	defer database.indexMutex.RUnlock()

	indexEntries := []lo.Entry[string, *types.ReverseIndex]{
		{
			Key:   "value",
			Value: database.valueIndex,
		},
		{
			Key:   "prefix",
			Value: database.prefixIndex,
		},
		{
			Key:   "suffix",
			Value: database.suffixIndex,
		},
	}

	indexes := lo.FromEntries(indexEntries)

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

	indexMappings := []IndexMapping{
		{
			"value",
			&database.valueIndex,
		},
		{
			"prefix",
			&database.prefixIndex,
		},
		{
			"suffix",
			&database.suffixIndex,
		},
	}

	lo.ForEach(
		indexMappings,
		func(mapping IndexMapping, _ int) {
			if index, exists := indexes[mapping.Key]; exists {
				*mapping.Field = index
			}
		},
	)

	counts := lo.Map(
		indexMappings,
		func(mapping IndexMapping, _ int) int {
			return len((*mapping.Field).Entries)
		},
	)

	fmt.Printf(
		"Loaded %d value indexes, %d prefix indexes, %d suffix indexes\n",
		counts[0], counts[1], counts[2],
	)

	return nil
}
