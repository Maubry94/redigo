package utils

import "strconv"

// Converts string value to int64
func FromStringToInt64(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}