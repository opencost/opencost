package env

import (
	"os"
	"strconv"
)

// Get parses an string from the environment variable key parameter. If the environment
// variable is empty, the defaultValue parameter is returned.
func Get(key string, defaultValue string) string {
	r := os.Getenv(key)
	if r == "" {
		return defaultValue
	}

	return r
}

// GetInt parses an int from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt(key string, defaultValue int) int {
	r := os.Getenv(key)
	i, err := strconv.Atoi(r)
	if err != nil {
		return defaultValue
	}

	return i
}

// GetInt8 parses an int8 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt8(key string, defaultValue int8) int8 {
	r := os.Getenv(key)
	i, err := strconv.ParseInt(r, 10, 8)
	if err != nil {
		return defaultValue
	}

	return int8(i)
}

// GetInt16 parses an int16 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt16(key string, defaultValue int16) int16 {
	r := os.Getenv(key)
	i, err := strconv.ParseInt(r, 10, 16)
	if err != nil {
		return defaultValue
	}

	return int16(i)
}

// GetInt32 parses an int32 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt32(key string, defaultValue int32) int32 {
	r := os.Getenv(key)
	i, err := strconv.ParseInt(r, 10, 32)
	if err != nil {
		return defaultValue
	}

	return int32(i)
}

// GetInt64 parses an int64 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetInt64(key string, defaultValue int64) int64 {
	r := os.Getenv(key)
	i, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		return defaultValue
	}

	return i
}

// GetUInt parses a uint from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt(key string, defaultValue uint) uint {
	r := os.Getenv(key)
	i, err := strconv.ParseUint(r, 10, 32)
	if err != nil {
		return defaultValue
	}

	return uint(i)
}

// GetUInt8 parses a uint8 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt8(key string, defaultValue uint8) uint8 {
	r := os.Getenv(key)
	i, err := strconv.ParseUint(r, 10, 8)
	if err != nil {
		return defaultValue
	}

	return uint8(i)
}

// GetUInt16 parses a uint16 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt16(key string, defaultValue uint16) uint16 {
	r := os.Getenv(key)
	i, err := strconv.ParseUint(r, 10, 16)
	if err != nil {
		return defaultValue
	}

	return uint16(i)
}

// GetUInt32 parses a uint32 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt32(key string, defaultValue uint32) uint32 {
	r := os.Getenv(key)
	i, err := strconv.ParseUint(r, 10, 32)
	if err != nil {
		return defaultValue
	}

	return uint32(i)
}

// GetUInt64 parses a uint64 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetUInt64(key string, defaultValue uint64) uint64 {
	r := os.Getenv(key)
	i, err := strconv.ParseUint(r, 10, 64)
	if err != nil {
		return defaultValue
	}

	return uint64(i)
}

// GetFloat32 parses a float32 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetFloat32(key string, defaultValue float32) float32 {
	r := os.Getenv(key)
	f, err := strconv.ParseFloat(r, 32)
	if err != nil {
		return defaultValue
	}

	return float32(f)
}

// GetFloat64 parses a float64 from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetFloat64(key string, defaultValue float64) float64 {
	r := os.Getenv(key)
	f, err := strconv.ParseFloat(r, 64)
	if err != nil {
		return defaultValue
	}

	return f
}

// GetBool parses a bool from the environment variable key parameter. If the environment
// variable is empty or fails to parse, the defaultValue parameter is returned.
func GetBool(key string, defaultValue bool) bool {
	r := os.Getenv(key)
	b, err := strconv.ParseBool(r)
	if err != nil {
		return defaultValue
	}

	return b
}

// Set sets the environment variable for the key provided using the value provided.
func Set(key string, value string) error {
	return os.Setenv(key, value)
}

// SetInt sets the environment variable to a string formatted int value
func SetInt(key string, value int) error {
	return os.Setenv(key, strconv.Itoa(value))
}

// SetInt8 sets the environment variable to a string formatted int8 value.
func SetInt8(key string, value int8) error {
	return os.Setenv(key, strconv.FormatInt(int64(value), 10))
}

// SetInt16 sets the environment variable to a string formatted int16 value.
func SetInt16(key string, value int16) error {
	return os.Setenv(key, strconv.FormatInt(int64(value), 10))
}

// SetInt32 sets the environment variable to a string formatted int32 value.
func SetInt32(key string, value int32) error {
	return os.Setenv(key, strconv.FormatInt(int64(value), 10))
}

// SetInt64 sets the environment variable to a string formatted int64 value.
func SetInt64(key string, value int64) error {
	return os.Setenv(key, strconv.FormatInt(value, 10))
}

// SetUInt sets the environment variable to a string formatted uint value
func SetUInt(key string, value uint) error {
	return os.Setenv(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt8 sets the environment variable to a string formatted uint8 value
func SetUInt8(key string, value uint8) error {
	return os.Setenv(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt16 sets the environment variable to a string formatted uint16 value
func SetUInt16(key string, value uint16) error {
	return os.Setenv(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt32 sets the environment variable to a string formatted uint32 value
func SetUInt32(key string, value uint32) error {
	return os.Setenv(key, strconv.FormatUint(uint64(value), 10))
}

// SetUInt64 sets the environment variable to a string formatted uint64 value
func SetUInt64(key string, value uint64) error {
	return os.Setenv(key, strconv.FormatUint(value, 10))
}

// SetBool sets the environment variable to a string formatted bool value.
func SetBool(key string, value bool) error {
	return os.Setenv(key, strconv.FormatBool(value))
}
