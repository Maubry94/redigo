package types

import (
	"strings"

	"github.com/samber/lo"
)

type RedigoQuery interface {
	Match(key string, value any) bool
}

type PrefixQuery struct {
	Prefix string
}

type ContainsValueQuery struct {
	Substring any
}

type AndQuery struct {
	Queries []RedigoQuery
}

func (q PrefixQuery) Match(key string, _ any) bool {
	return strings.HasPrefix(key, q.Prefix)
}

func (q ContainsValueQuery) Match(_ string, value any) bool {
	switch v := value.(type) {
	case string:
		if subStr, ok := q.Substring.(string); ok {
			return strings.Contains(v, subStr)
		}
		return false
	case bool, int, int64, float64:
		return v == q.Substring
	default:
		// Pour les autres types, on tente une comparaison directe
		return v == q.Substring
	}
}

func (q AndQuery) Match(key string, value any) bool {
	return lo.EveryBy(q.Queries, func(subQuery RedigoQuery) bool {
        return subQuery.Match(key, value)
    })
}
