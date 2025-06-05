package redigo

import (
	"fmt"
	"os"
	"redigo/envs"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"sync"
)

var envsConfig envs.Envs

type RedigoDB struct {
    store      map[string]types.RedigoStorableValues
    expirationKeys     map[string]int64
    storeMutex sync.Mutex
    aofFile    *os.File
    aofMutex   sync.Mutex
    envs       envs.Envs
    aofCommandsBuffer  []types.Command
    aofCommandsBufferMutex   sync.Mutex
}

func InitializeRedigo() (*RedigoDB, error) {
    envs := envs.Gets()

    database := &RedigoDB{
        store:  make(map[string]types.RedigoStorableValues),
        expirationKeys: make(map[string]int64),
        envs:   envs,
        aofCommandsBuffer:  make([]types.Command, 0),
    }
    
    if err := database.LoadFromSnapshot(); err != nil {
        return nil, fmt.Errorf("error loading snapshot: %w", err)
    }
    
    aofPath, err := utils.GetAOFPath()

    if err != nil {
        return nil, err
    }
    
    loadedAOF, err := os.OpenFile(aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

    if err != nil {
        return nil, err
    }

    database.aofFile = loadedAOF
    
    if err := database.LoadFromAof(); err != nil {
        return nil, fmt.Errorf("error loading AOF: %w", err)
    }
    
    go database.StartSnapshotListener()
    go database.StartDataExpirationListener()
    go database.StartBufferListener()
    
    return database, nil
}

func (database *RedigoDB) ForceSave() error {
    if err := database.UpdateSnapshot(); err != nil {
        return fmt.Errorf("Error creating snapshot: %w", err)
    }
    
    return nil
}