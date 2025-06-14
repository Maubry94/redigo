package types

import (
	"strings"

	"github.com/samber/lo"
)

type RedigoQuery interface {
	Match(key string, value RedigoStorableValues) bool
}

type PrefixQuery struct {
	Prefix string
}

type ContainsValueQuery struct {
	Substring RedigoStorableValues
}

type AndQuery struct {
	Queries []RedigoQuery
}

func (q PrefixQuery) Match(key string, _ RedigoStorableValues) bool {
	return strings.HasPrefix(key, q.Prefix)
}

func (q ContainsValueQuery) Match(_ string, value RedigoStorableValues) bool {
	switch v := value.(type) {
	case RedigoString:
		if subStr, ok := q.Substring.(RedigoString); ok {
			return strings.Contains(string(v), string(subStr))
		}
		return false
	case RedigoBool:
		return v == q.Substring
	case RedigoInt:
		return v == q.Substring
	default:
		return false
	}
}

func (q AndQuery) Match(key string, value RedigoStorableValues) bool {
	return lo.EveryBy(q.Queries, func(subQuery RedigoQuery) bool {
        return subQuery.Match(key, value)
    })
}
