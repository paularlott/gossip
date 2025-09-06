package gossip

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/paularlott/gossip/hlc"
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
	GetTimestamp() hlc.Timestamp
	GetAll() map[string]interface{}
	GetAllKeys() []string
	GetAllAsString() map[string]string
	Exists(key string) bool
}

// LocalMetadata provides read-write access for local node
type LocalMetadata interface {
	MetadataReader
	SetString(key, value string) LocalMetadata
	SetBool(key string, value bool) LocalMetadata
	SetInt(key string, value int) LocalMetadata
	SetInt32(key string, value int32) LocalMetadata
	SetInt64(key string, value int64) LocalMetadata
	SetUint(key string, value uint) LocalMetadata
	SetUint32(key string, value uint32) LocalMetadata
	SetUint64(key string, value uint64) LocalMetadata
	SetFloat32(key string, value float32) LocalMetadata
	SetFloat64(key string, value float64) LocalMetadata
	SetTime(key string, value time.Time) LocalMetadata
	Delete(key string)
}

// Metadata holds node metadata with type-safe accessors
type Metadata struct {
	data         atomic.Pointer[map[string]interface{}]
	lastModified atomic.Uint64 // Last modification timestamp in nanoseconds
}

// Ensure interfaces are implemented
var _ MetadataReader = (*Metadata)(nil)
var _ LocalMetadata = (*Metadata)(nil)

// NewMetadata creates a new metadata container
func NewMetadata() *Metadata {
	md := &Metadata{}
	initialMap := make(map[string]interface{})
	md.data.Store(&initialMap)
	md.lastModified.Store(0)
	return md
}

// Get retrieves a raw value from metadata
func (md *Metadata) get(key string) (interface{}, bool) {
	currentData := md.data.Load()
	if currentData == nil {
		return nil, false
	}
	val, ok := (*currentData)[key]
	return val, ok
}

// Internal setter with timestamp update
func (md *Metadata) set(key string, value interface{}) *Metadata {
	currentData := md.data.Load()
	if currentData == nil {
		currentData = &map[string]interface{}{}
	}

	// Create new map with optimized capacity
	newData := make(map[string]interface{}, len(*currentData)+1)
	for k, v := range *currentData {
		newData[k] = v
	}
	newData[key] = value

	ts := hlc.Now()
	md.data.Store(&newData)
	md.lastModified.Store(uint64(ts))

	return md
}

// GetString returns a string value
func (md *Metadata) GetString(key string) string {
	val, ok := md.get(key)
	if !ok {
		return ""
	}

	// Convert from our supported types
	switch v := val.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.Itoa(int(v))
	case int16:
		return strconv.Itoa(int(v))
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
	case int, int8, int16, int32, int64, uint, uint32, uint64:
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
	case int8:
		return int64(v)
	case int16:
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
	case uint8:
		return uint64(v)
	case uint16:
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
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
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
func (md *Metadata) SetString(key, value string) LocalMetadata {
	md.set(key, value)
	return md
}

// SetBool sets a boolean value
func (md *Metadata) SetBool(key string, value bool) LocalMetadata {
	md.set(key, value)
	return md
}

// SetInt sets an integer value
func (md *Metadata) SetInt(key string, value int) LocalMetadata {
	md.set(key, value)
	return md
}

// SetInt32 sets a 32-bit integer value
func (md *Metadata) SetInt32(key string, value int32) LocalMetadata {
	md.set(key, value)
	return md
}

// SetInt64 sets a 64-bit integer value
func (md *Metadata) SetInt64(key string, value int64) LocalMetadata {
	md.set(key, value)
	return md
}

// SetUint sets an unsigned integer value
func (md *Metadata) SetUint(key string, value uint) LocalMetadata {
	md.set(key, value)
	return md
}

// SetUint32 sets a 32-bit unsigned integer value
func (md *Metadata) SetUint32(key string, value uint32) LocalMetadata {
	md.set(key, value)
	return md
}

// SetUint64 sets a 64-bit unsigned integer value
func (md *Metadata) SetUint64(key string, value uint64) LocalMetadata {
	md.set(key, value)
	return md
}

// SetFloat32 sets a 32-bit float value
func (md *Metadata) SetFloat32(key string, value float32) LocalMetadata {
	md.set(key, value)
	return md
}

// SetFloat64 sets a 64-bit float value
func (md *Metadata) SetFloat64(key string, value float64) LocalMetadata {
	md.set(key, value)
	return md
}

// SetTime sets a time value
func (md *Metadata) SetTime(key string, value time.Time) LocalMetadata {
	md.set(key, value)
	return md
}

// Delete removes a key from the metadata
func (md *Metadata) Delete(key string) {
	currentData := md.data.Load()
	if currentData == nil {
		return
	}

	if _, exists := (*currentData)[key]; !exists {
		return // Key doesn't exist, nothing to do
	}

	// Create new map without the deleted key
	newData := make(map[string]interface{}, len(*currentData)-1)
	for k, v := range *currentData {
		if k != key {
			newData[k] = v
		}
	}

	ts := hlc.Now()
	md.data.Store(&newData)
	md.lastModified.Store(uint64(ts))
}

// Exists returns true if the key exists in the metadata
func (md *Metadata) Exists(key string) bool {
	currentData := md.data.Load()
	if currentData == nil {
		return false
	}
	_, exists := (*currentData)[key]
	return exists
}

// GetTimestamp returns the last modification timestamp
func (md *Metadata) GetTimestamp() hlc.Timestamp {
	return hlc.Timestamp(md.lastModified.Load())
}

// GetAll returns a copy of all metadata
func (md *Metadata) GetAll() map[string]interface{} {
	currentData := md.data.Load()
	if currentData == nil {
		return make(map[string]interface{})
	}

	// Create a copy to avoid exposing internal map
	result := make(map[string]interface{}, len(*currentData))
	for k, v := range *currentData {
		result[k] = v
	}
	return result
}

// GetAllKeys returns all keys in the metadata
func (md *Metadata) GetAllKeys() []string {
	currentData := md.data.Load()
	if currentData == nil {
		return nil
	}

	keys := make([]string, 0, len(*currentData))
	for k := range *currentData {
		keys = append(keys, k)
	}
	return keys
}

// GetAllAsString returns all metadata as string values
func (md *Metadata) GetAllAsString() map[string]string {
	currentData := md.data.Load()
	if currentData == nil {
		return nil
	}

	result := make(map[string]string, len(*currentData))
	for k := range *currentData {
		result[k] = md.GetString(k)
	}

	return result
}

// update replaces all metadata if the timestamp is newer, if force is true then the timestamp check is ignored
func (md *Metadata) update(data map[string]interface{}, timestamp hlc.Timestamp, force bool) bool {
	currentTime := md.lastModified.Load()
	currentTimestamp := hlc.Timestamp(currentTime)

	if !force && !timestamp.After(currentTimestamp) {
		return false // Reject older data
	}

	// Create a copy to avoid exposing internal map (optimized capacity)
	dataCopy := make(map[string]interface{}, len(data))
	for k, v := range data {
		dataCopy[k] = v
	}

	md.data.Store(&dataCopy)
	md.lastModified.Store(uint64(timestamp))
	return true
}
