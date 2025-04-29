package redigo

import "sync"

type RedigoStorableValues interface {
	isRedigoValue()
}

type RedigoDB struct {
	store map[string]RedigoStorableValues
	mu    sync.Mutex
}

func InitRedigo() *RedigoDB {
	return &RedigoDB{
		store: make(map[string]RedigoStorableValues),
	}
}

func (db *RedigoDB) Set(key string, value RedigoStorableValues) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.store[key] = value
}

func (db *RedigoDB) Get(key string) (RedigoStorableValues, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.store[key]
	return val, ok
}

type RedigoString string

func (s RedigoString) isRedigoValue() {}

type RedigoBool bool

func (b RedigoBool) isRedigoValue() {}

type RedigoInt int

func (i RedigoInt) isRedigoValue() {}
