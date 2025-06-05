package redigo

import "redigo/internal/redigo/types"

func ParseRedigoQuery(parts []string) types.RedigoQuery {
	var queries []types.RedigoQuery

	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case "PREFIX":
			if i+1 < len(parts) {
				queries = append(queries, types.PrefixQuery{Prefix: parts[i+1]})
				i++
			}
		case "CONTAINS":
			if i+1 < len(parts) {
				queries = append(queries, types.ContainsValueQuery{Substring: types.RedigoString(parts[i+1])})
				i++
			}
		}
	}

	if len(queries) == 1 {
		return queries[0]
	}
	return types.AndQuery{Queries: queries}
}

func (database *RedigoDB) GetByQuery(query types.RedigoQuery) map[string]types.RedigoStorableValues {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	queryResult := make(map[string]types.RedigoStorableValues)

	for key, value := range database.store {
		if query.Match(key, value) {
			queryResult[key] = value
		}
	}
	return queryResult
}