package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"redigo/envs"
)

var envsConfig envs.Envs

const (
	AOF_FILENAME         = "appendonly.aof"
	SNAPSHOT_FILENAME    = "snapshot.redigo.json"
	INDEXES_FILENAME     = "indexes.redigo.json"
	REDIGO_ROOT_DIR_NAME = ".redigo"
)

// Returns the full path to Redigo's data directory
// Creates the directory if it doesn't exist
func GetRedigoFullPath() (string, error) {
	rootDirPath := envsConfig.RedigoRootDirPath

	// Use user's home directory if no custom path is configured
	if rootDirPath == "" {
		userHomeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("error getting user home directory: %w", err)
		}
		rootDirPath = userHomeDir
	}

	// Create full path by joining root directory with Redigo folder name
	redigoFullDirPath := filepath.Join(rootDirPath, REDIGO_ROOT_DIR_NAME)

	// Create directory with proper permissions if it doesn't exist
	if err := os.MkdirAll(redigoFullDirPath, 0755); err != nil {
		return "", err
	}

	return redigoFullDirPath, nil
}

// Returns the full path to the snapshot file
// Used for database state persistence
func GetSnapshotFilePath() (string, error) {
	redigoDirFullPath, err := GetRedigoFullPath()
	if err != nil {
		return "", err
	}

	return filepath.Join(redigoDirFullPath, SNAPSHOT_FILENAME), nil
}

func GetIndexesFilePath() (string, error) {
	redigoDirFullPath, err := GetRedigoFullPath()
	if err != nil {
		return "", err
	}

	return filepath.Join(redigoDirFullPath, INDEXES_FILENAME), nil
}

// Returns the full path to the Append Only File
// Used for command logging and replay functionality
func GetAOFPath() (string, error) {
	redigoDirFullPath, err := GetRedigoFullPath()
	if err != nil {
		return "", err
	}

	return filepath.Join(redigoDirFullPath, AOF_FILENAME), nil
}