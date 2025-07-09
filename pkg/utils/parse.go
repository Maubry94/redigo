package utils

import (
	"fmt"
	"strconv"
)

// Converts string value to int64
func FromStringToInt64(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

// Converts a value to string for indexing
func ValueToString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case float32:
		return fmt.Sprintf("%g", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
