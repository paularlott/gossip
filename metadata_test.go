package gossip

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ MetadataReader = (*Metadata)(nil)
	_ LocalMetadata  = (*Metadata)(nil)
)

// Each typed setter is read back by its getter, converts to the string form, and returns the metadata for chaining.
func TestMetadataTypedAccessors(t *testing.T) {
	md := NewMetadata()
	require.Zero(t, md.GetTimestamp(), "a new Metadata has no timestamp")
	require.Empty(t, md.GetAll())
	now := time.Now()

	result := md.SetString("s", "hello").
		SetBool("b", true).
		SetInt("i", 42).
		SetInt32("i32", 32).
		SetInt64("i64", 64).
		SetUint("u", 100).
		SetUint32("u32", 200).
		SetUint64("u64", 300).
		SetFloat32("f32", 3.14).
		SetFloat64("f64", 2.718).
		SetTime("t", now)

	assert.Same(t, md, result, "setters return the metadata")
	assert.Len(t, md.GetAllKeys(), 11)
	assert.NotZero(t, md.GetTimestamp(), "setting stamps the metadata")

	assert.Equal(t, "hello", md.GetString("s"))
	assert.True(t, md.GetBool("b"))
	assert.Equal(t, 42, md.GetInt("i"))
	assert.Equal(t, int32(32), md.GetInt32("i32"))
	assert.Equal(t, int64(64), md.GetInt64("i64"))
	assert.Equal(t, uint(100), md.GetUint("u"))
	assert.Equal(t, uint32(200), md.GetUint32("u32"))
	assert.Equal(t, uint64(300), md.GetUint64("u64"))
	assert.Equal(t, float32(3.14), md.GetFloat32("f32"))
	assert.Equal(t, 2.718, md.GetFloat64("f64"))
	assert.True(t, md.GetTime("t").Equal(now))

	assert.Equal(t, "true", md.GetString("b"))
	assert.Equal(t, "42", md.GetString("i"))
	assert.Equal(t, now.Format(time.RFC3339), md.GetString("t"))
	assert.Equal(t, float64(float32(3.14)), md.GetFloat64("f32"), "a float32 widens with its precision")
	assert.Equal(t, int64(42), md.GetInt64("i"), "ints convert")
}

// What a value converts to when read as another type.
func TestMetadataGetStringConversions(t *testing.T) {
	md := NewMetadata()

	tests := []struct {
		key      string
		value    interface{}
		expected string
	}{
		{"str", "hello", "hello"},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
		{"int", 42, "42"},
		{"int8", int8(8), "8"},
		{"int16", int16(16), "16"},
		{"int32", int32(32), "32"},
		{"int64", int64(64), "64"},
		{"uint", uint(100), "100"},
		{"uint32", uint32(200), "200"},
		{"uint64", uint64(300), "300"},
		{"float32", float32(3.14), "3.14"},
		{"float64", 2.718, "2.718"},
	}

	for _, tt := range tests {
		md.set(tt.key, tt.value)
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := md.GetString(tt.key)
			if result != tt.expected {
				t.Errorf("GetString(%s): expected %q, got %q", tt.key, tt.expected, result)
			}
		})
	}

	// time.Time conversion
	now := time.Now()
	md.set("time", now)
	result := md.GetString("time")
	expected := now.Format(time.RFC3339)
	if result != expected {
		t.Errorf("GetString(time): expected %q, got %q", expected, result)
	}

	// Unknown type
	md.set("custom", struct{ X int }{X: 1})
	result = md.GetString("custom")
	if result == "" {
		t.Error("GetString should format unknown types with fmt.Sprintf")
	}

	// Non-existent key
	if md.GetString("missing") != "" {
		t.Error("Expected empty string for missing key")
	}
}

func TestMetadataGetBoolConversions(t *testing.T) {
	md := NewMetadata()

	md.set("true_bool", true)
	md.set("false_bool", false)
	md.set("true_str", "true")
	md.set("false_str", "false")
	md.set("int_nonzero", 1)
	md.set("int_zero", 0)
	md.set("float_nonzero", 1.5)
	md.set("float_zero", 0.0)
	md.set("invalid_str", "notabool")

	tests := []struct {
		key      string
		expected bool
	}{
		{"true_bool", true},
		{"false_bool", false},
		{"true_str", true},
		{"false_str", false},
		{"int_nonzero", true},
		{"int_zero", false},
		{"float_nonzero", true},
		{"float_zero", false},
		{"invalid_str", false},
		{"missing", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := md.GetBool(tt.key)
			if result != tt.expected {
				t.Errorf("GetBool(%s): expected %v, got %v", tt.key, tt.expected, result)
			}
		})
	}
}

func TestMetadataGetInt64Conversions(t *testing.T) {
	md := NewMetadata()

	md.set("int", 42)
	md.set("int8", int8(8))
	md.set("int16", int16(16))
	md.set("int32", int32(32))
	md.set("int64", int64(64))
	md.set("uint", uint(100))
	md.set("uint32", uint32(200))
	md.set("uint64", uint64(300))
	md.set("float32", float32(3.14))
	md.set("float64", 2.718)
	md.set("str_int", "999")
	md.set("str_float", "123.45")
	md.set("str_invalid", "abc")
	md.set("bool_true", true)
	md.set("bool_false", false)

	tests := []struct {
		key      string
		expected int64
	}{
		{"int", 42},
		{"int8", 8},
		{"int16", 16},
		{"int32", 32},
		{"int64", 64},
		{"uint", 100},
		{"uint32", 200},
		{"uint64", 300},
		{"float32", 3},
		{"float64", 2},
		{"str_int", 999},
		{"str_float", 123}, // parsed as float then truncated
		{"str_invalid", 0},
		{"bool_true", 1},
		{"bool_false", 0},
		{"missing", 0},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := md.GetInt64(tt.key)
			if result != tt.expected {
				t.Errorf("GetInt64(%s): expected %d, got %d", tt.key, tt.expected, result)
			}
		})
	}
}

func TestMetadataGetUint64Conversions(t *testing.T) {
	md := NewMetadata()

	md.set("uint", uint(100))
	md.set("uint8", uint8(8))
	md.set("uint16", uint16(16))
	md.set("uint32", uint32(200))
	md.set("uint64", uint64(300))
	md.set("int_pos", 42)
	md.set("int_neg", -5)
	md.set("int32_pos", int32(32))
	md.set("int32_neg", int32(-1))
	md.set("int64_pos", int64(64))
	md.set("int64_neg", int64(-10))
	md.set("float32_pos", float32(3.14))
	md.set("float32_neg", float32(-1.5))
	md.set("float64_pos", 2.718)
	md.set("float64_neg", -2.718)
	md.set("str_uint", "999")
	md.set("str_invalid", "abc")
	md.set("bool_true", true)
	md.set("bool_false", false)

	tests := []struct {
		key      string
		expected uint64
	}{
		{"uint", 100},
		{"uint8", 8},
		{"uint16", 16},
		{"uint32", 200},
		{"uint64", 300},
		{"int_pos", 42},
		{"int_neg", 0},
		{"int32_pos", 32},
		{"int32_neg", 0},
		{"int64_pos", 64},
		{"int64_neg", 0},
		{"float32_pos", 3},
		{"float32_neg", 0},
		{"float64_pos", 2},
		{"float64_neg", 0},
		{"str_uint", 999},
		{"str_invalid", 0},
		{"bool_true", 1},
		{"bool_false", 0},
		{"missing", 0},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := md.GetUint64(tt.key)
			if result != tt.expected {
				t.Errorf("GetUint64(%s): expected %d, got %d", tt.key, tt.expected, result)
			}
		})
	}
}

func TestMetadataGetFloat64Conversions(t *testing.T) {
	md := NewMetadata()

	md.set("float32", float32(3.14))
	md.set("float64", 2.718)
	md.set("int", 42)
	md.set("int8", int8(8))
	md.set("int16", int16(16))
	md.set("int32", int32(32))
	md.set("int64", int64(64))
	md.set("uint", uint(100))
	md.set("uint8", uint8(8))
	md.set("uint16", uint16(16))
	md.set("uint32", uint32(200))
	md.set("uint64", uint64(300))
	md.set("str_float", "3.14159")
	md.set("str_invalid", "abc")
	md.set("bool_true", true)
	md.set("bool_false", false)

	tests := []struct {
		key      string
		expected float64
	}{
		{"float64", 2.718},
		{"int", 42},
		{"int8", 8},
		{"int16", 16},
		{"int32", 32},
		{"int64", 64},
		{"uint", 100},
		{"uint8", 8},
		{"uint16", 16},
		{"uint32", 200},
		{"uint64", 300},
		{"str_float", 3.14159},
		{"str_invalid", 0},
		{"bool_true", 1},
		{"bool_false", 0},
		{"missing", 0},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := md.GetFloat64(tt.key)
			if result != tt.expected {
				t.Errorf("GetFloat64(%s): expected %f, got %f", tt.key, tt.expected, result)
			}
		})
	}

	// float32 special handling (approximate comparison)
	result := md.GetFloat64("float32")
	if result < 3.13 || result > 3.15 {
		t.Errorf("GetFloat64(float32): expected ~3.14, got %f", result)
	}
}

func TestMetadataGetTimeConversions(t *testing.T) {
	md := NewMetadata()

	now := time.Now().Truncate(time.Second)

	// Direct time
	md.set("time", now)
	if got := md.GetTime("time"); !got.Equal(now) {
		t.Errorf("GetTime(time): expected %v, got %v", now, got)
	}

	// int64 (nanoseconds)
	md.set("nanos", now.UnixNano())
	if got := md.GetTime("nanos"); !got.Equal(now) {
		t.Errorf("GetTime(nanos): expected %v, got %v", now, got)
	}

	// RFC3339 string
	md.set("rfc3339", now.Format(time.RFC3339))
	if got := md.GetTime("rfc3339"); !got.Equal(now) {
		t.Errorf("GetTime(rfc3339): expected %v, got %v", now, got)
	}

	// RFC1123 string
	md.set("rfc1123", now.Format(time.RFC1123))
	gotRFC1123 := md.GetTime("rfc1123")
	if gotRFC1123.IsZero() {
		t.Error("GetTime(rfc1123): should parse RFC1123 format")
	}

	// Date string
	md.set("date", "2024-01-15")
	got := md.GetTime("date")
	if got.IsZero() {
		t.Error("GetTime(date): should parse date format")
	}

	// DateTime string
	md.set("datetime", "2024-01-15 10:30:45")
	got = md.GetTime("datetime")
	if got.IsZero() {
		t.Error("GetTime(datetime): should parse datetime format")
	}

	// Invalid string
	md.set("invalid", "notadate")
	got = md.GetTime("invalid")
	if !got.IsZero() {
		t.Error("GetTime(invalid): should return zero time")
	}

	// Missing key
	got = md.GetTime("missing")
	if !got.IsZero() {
		t.Error("GetTime(missing): should return zero time")
	}

	// Non-time, non-string, non-int64 type
	md.set("bool", true)
	got = md.GetTime("bool")
	if !got.IsZero() {
		t.Error("GetTime(bool): should return zero time")
	}
}

// The sized getters are the wide ones, converted.
func TestMetadataSizedGetters(t *testing.T) {
	md := NewMetadata()
	md.set("int", 42)
	md.set("uint", uint64(42))
	md.set("float", 3.5)

	assert.Equal(t, 42, md.GetInt("int"))
	assert.Equal(t, int32(42), md.GetInt32("int"))
	assert.Equal(t, uint(42), md.GetUint("uint"))
	assert.Equal(t, uint32(42), md.GetUint32("uint"))
	assert.Equal(t, float32(3.5), md.GetFloat32("float"))
}

// Setting, reading, listing and deleting; a deleted key is gone and stamps the metadata, and what is returned is a copy.
func TestMetadataLifecycle(t *testing.T) {
	md := NewMetadata()
	assert.False(t, md.Exists("key1"))
	assert.Empty(t, md.GetString("key1"))
	assert.Zero(t, md.GetInt("key1"))
	assert.False(t, md.GetBool("key1"))
	assert.True(t, md.GetTime("key1").IsZero())
	assert.Empty(t, md.GetAllKeys())
	assert.Empty(t, md.GetAllAsString())

	md.SetString("key1", "value1")
	md.SetInt("key2", 42)
	md.SetBool("key3", true)
	md.SetFloat64("key4", 3.14)
	md.SetString("empty", "")

	assert.True(t, md.Exists("key1"))
	assert.ElementsMatch(t, []string{"key1", "key2", "key3", "key4", "empty"}, md.GetAllKeys())
	assert.Equal(t, map[string]string{"key1": "value1", "key2": "42", "key3": "true", "key4": "3.14", "empty": ""}, md.GetAllAsString())
	assert.Zero(t, md.GetInt("empty"), "an empty string reads as 0")
	assert.False(t, md.GetBool("empty"), "and as false")

	all := md.GetAll()
	assert.Equal(t, 42, all["key2"])
	all["key1"] = "modified"
	assert.Equal(t, "value1", md.GetString("key1"), "GetAll returns a copy")

	before := md.GetTimestamp()
	md.Delete("key1")
	assert.False(t, md.Exists("key1"))
	assert.Empty(t, md.GetString("key1"))
	assert.True(t, md.Exists("key2"))
	assert.True(t, md.GetTimestamp().After(before), "deleting stamps the metadata")
	md.Delete("nonexistent")
}

func TestMetadataUpdate(t *testing.T) {
	md := NewMetadata()
	md.SetString("key1", "value1")
	initial := md.GetTimestamp()

	newer := hlc.Timestamp(uint64(initial) + 1000)
	require.True(t, md.update(map[string]interface{}{"key1": "updated_value1", "key2": "value2"}, newer, false), "a newer timestamp")
	assert.Equal(t, "updated_value1", md.GetString("key1"))
	assert.Equal(t, "value2", md.GetString("key2"))
	assert.Equal(t, newer, md.GetTimestamp())

	older := hlc.Timestamp(uint64(initial) - 1000)
	require.False(t, md.update(map[string]interface{}{"key1": "old_value"}, older, false), "an older timestamp")
	assert.Equal(t, "updated_value1", md.GetString("key1"))

	require.False(t, md.update(map[string]interface{}{"key1": "same"}, newer, false), "the same timestamp")
	assert.Equal(t, "updated_value1", md.GetString("key1"))

	require.True(t, md.update(map[string]interface{}{"key1": "old_value"}, older, true), "forced")
	assert.Equal(t, "old_value", md.GetString("key1"))
	assert.False(t, md.Exists("key2"), "an update replaces the whole map")
}

// The local-change callback runs for every change made through the setters and Delete.
func TestMetadataSetOnLocalChange(t *testing.T) {
	md := NewMetadata()
	var seen []map[string]interface{}
	md.SetOnLocalChange(func(ts hlc.Timestamp, data map[string]interface{}) { seen = append(seen, data) })

	md.SetString("key", "val")
	md.Delete("key")

	require.Len(t, seen, 2)
	assert.Equal(t, map[string]interface{}{"key": "val"}, seen[0])
	assert.Empty(t, seen[1])

	md.SetOnLocalChange(nil)
	md.SetString("key2", "val2")
	assert.Len(t, seen, 2, "no longer called")
}

// Readers run while a writer sets and deletes keys; every change calls back.
func TestMetadataConcurrentAccess(t *testing.T) {
	md := NewMetadata()
	var changes atomic.Int64
	md.SetOnLocalChange(func(hlc.Timestamp, map[string]interface{}) { changes.Add(1) })

	const readers, ops = 10, 1000
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ops {
				md.GetString("key_0")
				md.Exists("key_0")
				md.GetAll()
				md.GetAllKeys()
			}
		}()
	}
	for j := range ops {
		key := fmt.Sprintf("key_%d", j)
		md.SetString(key, "value")
		if j%2 == 0 {
			md.Delete(key)
		}
	}
	wg.Wait()

	assert.Len(t, md.GetAllKeys(), ops/2, "the keys that were not deleted")
	assert.EqualValues(t, ops+ops/2, changes.Load(), "every set and delete called back")
}

// Writers on separate goroutines must not lose each other's keys.
func TestMetadataConcurrentWritersLoseNoKeys(t *testing.T) {
	md := NewMetadata()
	const writers, keys = 10, 100
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keys {
				md.SetString(fmt.Sprintf("key_%d_%d", w, k), "value")
			}
		}()
	}
	wg.Wait()

	assert.Len(t, md.GetAllKeys(), writers*keys)
}
