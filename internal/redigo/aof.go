package redigo

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"redigo/internal/redigo/types"
	"redigo/pkg/utils"
	"strconv"
	"time"
)

func (database *RedigoDB) CloseAof() error {
    if database.aofFile != nil {
        return database.aofFile.Close()
    }
    return nil
}

func (database *RedigoDB) AddCommandsToAofBuffer(command types.Command) []types.Command {
    database.aofCommandsBufferMutex.Lock()
    database.aofCommandsBuffer = append(database.aofCommandsBuffer, command)
    database.aofCommandsBufferMutex.Unlock()
    
    return database.aofCommandsBuffer
}

func (database *RedigoDB) LoadFromAof() error {
    aofPath, err := utils.GetAOFPath()
    if err != nil {
        return err
    }
    
    if _, err := os.Stat(aofPath); os.IsNotExist(err) {
        return nil
    }
    
    file, err := os.Open(aofPath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    scanner := bufio.NewScanner(file)
    lineNumber := 0
    now := time.Now().Unix()
    
    for scanner.Scan() {
        lineNumber++
        row := scanner.Text()
        
        var command types.Command
        if err := json.Unmarshal([]byte(row), &command); err != nil {
            fmt.Printf("Error when reading at %d: %v\n", lineNumber, err)
            continue
        }
        
        switch command.Name {
            case "SET":
                var value types.RedigoStorableValues

            switch command.Value.Type {
            case "string":
                if strVal, ok := command.Value.Value.(string); ok {
                    value = types.RedigoString(strVal)
                }
            case "bool":
                if boolVal, ok := command.Value.Value.(bool); ok {
                    value = types.RedigoBool(boolVal)
                }
            case "int":
                switch v := command.Value.Value.(type) {
                case float64:
                    value = types.RedigoInt(int(v))
                case string:
                    if intVal, err := strconv.Atoi(v); err == nil {
                        value = types.RedigoInt(intVal)
                    }
                }
            }

            if value != nil {
                database.storeMutex.Lock()
                database.store[command.Key] = value

                if command.Ttl != nil && *command.Ttl > 0 {
                    expirationTime := command.Timestamp + *command.Ttl
                    if expirationTime > now {
                        database.expirationKeys[command.Key] = expirationTime
                    } else {
                        delete(database.store, command.Key)
                        delete(database.expirationKeys, command.Key)
                    }
                }
                database.storeMutex.Unlock()
            }
            case "DEL":
                database.storeMutex.Lock()
                delete(database.store, command.Key)
                delete(database.expirationKeys, command.Key)
                database.storeMutex.Unlock()
            case "EXPIRE":
                database.storeMutex.Lock()
                if _, exists := database.store[command.Key]; exists {
                    var seconds int64
                    switch v := command.Value.Value.(type) {
                    case float64:
                        seconds = int64(v)
                    case string:
                        if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
                            seconds = intVal
                        }
                    }
                    elapsedTime := now - command.Timestamp
                    remainingTime := seconds - elapsedTime

                    if remainingTime > 0 {
                        database.expirationKeys[command.Key] = now + remainingTime
                    } else {
                        delete(database.store, command.Key)
                        delete(database.expirationKeys, command.Key)
                    }
                }
                database.storeMutex.Unlock()
        }
    }
    
    if err := scanner.Err(); err != nil {
        return err
    }
    
    return nil
}