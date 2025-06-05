package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"redigo/envs"
)

var envsConfig envs.Envs

const (
    AOF_FILENAME      = "appendonly.aof"
    SNAPSHOT_FILENAME  = "snapshot.redigo.json"
    REDIGO_ROOT_DIR_NAME = ".redigo"
)

func GetRedigoFullPath() (string, error) {
    rootDirPath := envsConfig.RedigoRootDirPath

    if rootDirPath == "" {
        userHomeDir, err := os.UserHomeDir(); if err != nil {
            return "", fmt.Errorf("error getting user home directory: %w", err)
        }
        rootDirPath = userHomeDir
    }
    
    redigoFullDirPath := filepath.Join(rootDirPath, REDIGO_ROOT_DIR_NAME)
    
    if err := os.MkdirAll(redigoFullDirPath, 0755); err != nil {
        return "", err
    }
    
    return redigoFullDirPath, nil
}

func GetSnapshotFilePath() (string, error) {
    redigoDirFullPath, err := GetRedigoFullPath()

    if err != nil {
        return "", err
    }
    
    return filepath.Join(redigoDirFullPath, SNAPSHOT_FILENAME), nil
}

func GetAOFPath() (string, error) {
    redigoDirFullPath, err := GetRedigoFullPath()

    if err != nil {
        return "", err
    }
    
    return filepath.Join(redigoDirFullPath, AOF_FILENAME), nil
}