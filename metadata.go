package gossip

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// MetadataReader provides read-only access to metadata
type MetadataReader interface {
	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetInt32(key string) int32
	GetInt64(key string) int64
	GetUint(key string) uint
	GetUint32(key string) uint32
	GetUint64(key string) uint64
	GetFloat32(key string) float32
	GetFloat64(key string) float64
	GetTime(key string) time.Time
	GetTimestamp() int64
	GetLastModified() time.Time
	GetAll() map[string]interface{}
	GetAllKeys() []string
	GetAllAsString() map[string]string
	Exists(key string) bool
}

// Ensure Metadata implements MetadataReader
var _ MetadataReader = (*Metadata)(nil)

// Metadata holds node metadata with type-safe accessors
type Metadata struct {
	data    atomic.Value // Holds map[string]interface{}
	lastMod atomic.Int64 // Last modification timestamp in nanoseconds
}

// NewMetadata creates a new metadata container
func NewMetadata() *Metadata {
	md := &Metadata{}
	md.data.Store(make(map[string]interface{}))
	md.lastMod.Store(0)
	return md
}

// Get retrieves a raw value from metadata
func (md *Metadata) get(key string) (interface{}, bool) {
	currentData := md.data.Load().(map[string]interface{})
	val, ok := currentData[key]
	return val, ok
}

// Internal setter with timestamp update
func (md *Metadata) set(key string, value interface{}) {
	currentData := md.data.Load().(map[string]interface{})
	newData := make(map[string]interface{}, len(currentData)+1)

	for k, v := range currentData {
		newData[k] = v
	}
	newData[key] = value

	md.data.Store(newData)
	md.lastMod.Store(time.Now().UnixNano())
}

// GetString returns a string value
func (md *Metadata) GetString(key string) string {
	val, ok := md.get(key)
	if !ok {
		return ""
	}

	// Direct string case
	if str, ok := val.(string); ok {
		return str
	}

	// Convert from our supported types
	switch v := val.(type) {
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// GetBool returns a boolean value
func (md *Metadata) GetBool(key string) bool {
	val, ok := md.get(key)
	if !ok {
		return false
	}

	// Convert from our supported types
	switch v := val.(type) {
	case bool:
		return v
	case string:
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	case int, int32, int64, uint, uint32, uint64:
		return md.GetInt64(key) != 0
	case float32, float64:
		return md.GetFloat64(key) != 0
	}

	return false
}

// GetInt64 returns a 64-bit integer value (our base integer conversion method)
func (md *Metadata) GetInt64(key string) int64 {
	val, ok := md.get(key)
	if !ok {
		return 0
	}

	// Handle direct cases
	switch v := val.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i
		}
		// Try float parsing for strings like "123.45"
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return int64(f)
		}
	case bool:
		if v {
			return 1
		}
	}

	return 0
}

// GetInt returns an integer value
func (md *Metadata) GetInt(key string) int {
	return int(md.GetInt64(key))
}

// GetInt32 returns a 32-bit integer value
func (md *Metadata) GetInt32(key string) int32 {
	return int32(md.GetInt64(key))
}

// GetUint64 returns a 64-bit unsigned integer value (our base unsigned conversion method)
func (md *Metadata) GetUint64(key string) uint64 {
	val, ok := md.get(key)
	if !ok {
		return 0
	}

	// Handle direct cases
	switch v := val.(type) {
	case uint:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case int:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case int32:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case int64:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case float32:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case float64:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case string:
		u, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			return u
		}
	case bool:
		if v {
			return 1
		}
	}

	return 0
}

// GetUint returns an unsigned integer value
func (md *Metadata) GetUint(key string) uint {
	return uint(md.GetUint64(key))
}

// GetUint32 returns a 32-bit unsigned integer value
func (md *Metadata) GetUint32(key string) uint32 {
	return uint32(md.GetUint64(key))
}

// GetFloat64 returns a 64-bit float value (our base float conversion method)
func (md *Metadata) GetFloat64(key string) float64 {
	val, ok := md.get(key)
	if !ok {
		return 0
	}

	// Handle direct cases
	switch v := val.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f
		}
	case bool:
		if v {
			return 1
		}
	}

	return 0
}

// GetFloat32 returns a 32-bit float value
func (md *Metadata) GetFloat32(key string) float32 {
	return float32(md.GetFloat64(key))
}

// GetTime returns a time value
func (md *Metadata) GetTime(key string) time.Time {
	val, ok := md.get(key)
	if !ok {
		return time.Time{}
	}

	// Direct time case
	if t, ok := val.(time.Time); ok {
		return t
	}

	// Convert from our supported types
	switch v := val.(type) {
	case int64:
		return time.Unix(0, v)
	case string:
		// Try RFC3339 format first (recommended)
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t
		}

		// Try a few other common formats
		formats := []string{
			time.RFC1123,
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
	}

	return time.Time{}
}

// Type-specific setters

// SetString sets a string value
func (md *Metadata) SetString(key, value string) {
	md.set(key, value)
}

// SetBool sets a boolean value
func (md *Metadata) SetBool(key string, value bool) {
	md.set(key, value)
}

// SetInt sets an integer value
func (md *Metadata) SetInt(key string, value int) {
	md.set(key, value)
}

// SetInt32 sets a 32-bit integer value
func (md *Metadata) SetInt32(key string, value int32) {
	md.set(key, value)
}

// SetInt64 sets a 64-bit integer value
func (md *Metadata) SetInt64(key string, value int64) {
	md.set(key, value)
}

// SetUint sets an unsigned integer value
func (md *Metadata) SetUint(key string, value uint) {
	md.set(key, value)
}

// SetUint32 sets a 32-bit unsigned integer value
func (md *Metadata) SetUint32(key string, value uint32) {
	md.set(key, value)
}

// SetUint64 sets a 64-bit unsigned integer value
func (md *Metadata) SetUint64(key string, value uint64) {
	md.set(key, value)
}

// SetFloat32 sets a 32-bit float value
func (md *Metadata) SetFloat32(key string, value float32) {
	md.set(key, value)
}

// SetFloat64 sets a 64-bit float value
func (md *Metadata) SetFloat64(key string, value float64) {
	md.set(key, value)
}

// SetTime sets a time value
func (md *Metadata) SetTime(key string, value time.Time) {
	md.set(key, value)
}

// Delete removes a key from the metadata
func (md *Metadata) Delete(key string) {
	currentData := md.data.Load().(map[string]interface{})
	if _, exists := currentData[key]; !exists {
		return // Key doesn't exist, nothing to do
	}

	newData := make(map[string]interface{}, len(currentData)-1)

	// Copy existing data except the deleted key
	for k, v := range currentData {
		if k != key {
			newData[k] = v
		}
	}

	md.data.Store(newData)
	md.lastMod.Store(time.Now().UnixNano())
}

// Exists returns true if the key exists in the metadata
func (md *Metadata) Exists(key string) bool {
	currentData := md.data.Load().(map[string]interface{})
	_, exists := currentData[key]
	return exists
}

// GetTimestamp returns the last modification timestamp
func (md *Metadata) GetTimestamp() int64 {
	return md.lastMod.Load()
}

// GetLastModified returns when the metadata was last modified
func (md *Metadata) GetLastModified() time.Time {
	return time.Unix(0, md.lastMod.Load())
}

// GetAll returns a copy of all metadata
func (md *Metadata) GetAll() map[string]interface{} {
	currentData := md.data.Load().(map[string]interface{})

	// Create a copy to avoid exposing internal map
	result := make(map[string]interface{}, len(currentData))
	for k, v := range currentData {
		result[k] = v
	}

	return result
}

// GetAllKeys returns all keys in the metadata
func (md *Metadata) GetAllKeys() []string {
	currentData := md.data.Load().(map[string]interface{})

	keys := make([]string, 0, len(currentData))
	for k := range currentData {
		keys = append(keys, k)
	}

	return keys
}

// GetAllAsString returns all metadata as string values
func (md *Metadata) GetAllAsString() map[string]string {
	currentData := md.data.Load().(map[string]interface{})

	result := make(map[string]string, len(currentData))
	for k, _ := range currentData {
		result[k] = md.GetString(k)
	}

	return result
}

// update replaces all metadata if the timestamp is newer, if force is true then the timestamp check is ignored
func (md *Metadata) update(data map[string]interface{}, timestamp int64, force bool) bool {
	// Only replace if the incoming timestamp is newer
	currentTime := md.lastMod.Load()

	fmt.Println("Current time:", currentTime, "Incoming time:", timestamp, timestamp <= currentTime)

	if !force && timestamp <= currentTime {
		return false // Reject older data
	}

	// Create a copy to avoid exposing internal map
	dataCopy := make(map[string]interface{}, len(data))
	for k, v := range data {
		dataCopy[k] = v
	}

	md.data.Store(dataCopy)
	md.lastMod.Store(timestamp)
	return true
}
