package types

type RedigoStorableValues interface {
    isRedigoValue()
}

type RedigoString string
func (s RedigoString) isRedigoValue() {}

type RedigoBool bool
func (b RedigoBool) isRedigoValue() {}

type RedigoInt int
func (i RedigoInt) isRedigoValue() {}
