package redigo

import (
	"encoding/json"
	"fmt"
	"redigo/internal/redigo/types"
	"time"

	"github.com/samber/lo"
)

func (database *RedigoDB) FlushBuffer() error {
    database.aofCommandsBufferMutex.Lock()

    if len(database.aofCommandsBuffer) == 0 {
        database.aofCommandsBufferMutex.Unlock()
        return nil
    }
    
    aofBufferCommands := make([]types.Command, len(database.aofCommandsBuffer))
    copy(aofBufferCommands, database.aofCommandsBuffer)
    database.aofCommandsBuffer = database.aofCommandsBuffer[:0]
    database.aofCommandsBufferMutex.Unlock()

    database.aofMutex.Lock()
    defer database.aofMutex.Unlock()

    var err error

    lo.ForEach(
        aofBufferCommands,
        func(command types.Command, _ int) {
            jsonCommand, marshalError := json.Marshal(command)
            if marshalError != nil {
                err = marshalError
                return
            }
            
            if _, writeError := database.aofFile.WriteString(string(jsonCommand) + "\n"); writeError != nil {
                err = writeError
                return
            }
        },
    )

    if err != nil {
        return err
    }

    return nil
}

func (database *RedigoDB) StartBufferListener() {
    ticker := time.NewTicker(database.envs.FlushBufferInterval)
    for range ticker.C {
        if err := database.FlushBuffer(); err != nil {
            fmt.Printf("Error when flushing aof buffer: %v\n", err)
        } else {
            fmt.Println("AOF buffer flushed successfully")
        }
    }
}