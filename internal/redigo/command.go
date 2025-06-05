package redigo

import (
	"redigo/internal/redigo/errors"
	"redigo/internal/redigo/types"
	"time"
)


func (database *RedigoDB) Set(key string, value types.RedigoStorableValues, ttl int64) error {
    now := time.Now().Unix()
    
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()

    if _, exists := database.store[key]; exists {
        return errors.ErrorKeyAlreadyExists
    }

    database.store[key] = value
    
    if ttl > 0 {
        expirationTime := now + ttl
        database.expirationKeys[key] = expirationTime
    } else {
        delete(database.expirationKeys, key)
    }
    
    //TODO: a voir pour refacto
    var valueType string
    var rawValue any
    
    switch input := value.(type) {
        case types.RedigoString:
            valueType = "string"
            rawValue = string(input)
        case types.RedigoBool:
            valueType = "bool"
            rawValue = bool(input)
        case types.RedigoInt:
            valueType = "int"
            rawValue = int(input)
    }
    
    command := types.Command{
        Name:      "SET",
        Key:       key,
        Value:     types.CommandValue{
            Type:  valueType,
            Value: rawValue,
        },
        Ttl:       &ttl,
        Timestamp: now,
    }
    
    database.AddCommandsToAofBuffer(command)

    return nil
}

func (database *RedigoDB) Get(key string) (types.RedigoStorableValues, error) {
    database.storeMutex.Lock()
    defer database.storeMutex.Unlock()
    
    if expireTime, exists := database.expirationKeys[key]; exists && time.Now().Unix() > expireTime {
        delete(database.store, key)
        delete(database.expirationKeys, key)
        return nil, errors.ErrorKeyExpired
    }
    
    val, ok := database.store[key]
    if !ok {
        return nil, errors.ErrorKeyNotFound
    }
    return val, nil
}