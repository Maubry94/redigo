package redigo

import "redigo/internal/redigo/types"

// Parses an array of string parts into a RedigoQuery object
// Supports combining multiple query conditions with AND logic
func ParseRedigoQuery(parts []string) types.RedigoQuery {
	var queries []types.RedigoQuery

	// Iterate through parts to build query conditions
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case "PREFIX":
			// Create a prefix query if next part exists
			if i+1 < len(parts) {
				queries = append(queries, types.PrefixQuery{Prefix: parts[i+1]})
				i++ // Skip the next part as it's the prefix value
			}
		case "CONTAINS":
			// Create a contains query if next part exists
			if i+1 < len(parts) {
				queries = append(queries, types.ContainsValueQuery{Substring: parts[i+1]})
				i++ // Skip the next part as it's the substring value
			}
		}
	}

	// Return single query if only one condition, otherwise combine with AND
	if len(queries) == 1 {
		return queries[0]
	}
	return types.AndQuery{Queries: queries}
}

// Executes a query against the database and returns matching key-value pairs
// This method is thread-safe and scans the entire database
func (database *RedigoDB) GetByQuery(query types.RedigoQuery) map[string]any {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	// Initialize result map to store matching entries
	queryResult := make(map[string]any)

	// Iterate through all key-value pairs in the database
	for key, value := range database.store {
		// Check if current key-value pair matches the query conditions
		if query.Match(key, value) {
			queryResult[key] = value
		}
	}

	return queryResult
}
