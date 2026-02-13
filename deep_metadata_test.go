package gossip

import (
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
)

// ============================================================================
// Metadata: GetString type conversions
// ============================================================================

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

// ============================================================================
// Metadata: GetBool type conversions
// ============================================================================

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

// ============================================================================
// Metadata: GetInt64 type conversions
// ============================================================================

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

// ============================================================================
// Metadata: GetInt, GetInt32 (delegate to GetInt64)
// ============================================================================

func TestMetadataGetIntDelegates(t *testing.T) {
	md := NewMetadata()
	md.set("val", 42)

	if md.GetInt("val") != 42 {
		t.Error("GetInt should delegate to GetInt64")
	}
	if md.GetInt32("val") != 32 {
		// int32 truncation of 42 is still 42
	}
	if md.GetInt32("val") != 42 {
		t.Error("GetInt32 should delegate to GetInt64")
	}
}

// ============================================================================
// Metadata: GetUint64 type conversions
// ============================================================================

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

func TestMetadataGetUintDelegates(t *testing.T) {
	md := NewMetadata()
	md.set("val", uint64(42))

	if md.GetUint("val") != 42 {
		t.Error("GetUint should delegate")
	}
	if md.GetUint32("val") != 42 {
		t.Error("GetUint32 should delegate")
	}
}

// ============================================================================
// Metadata: GetFloat64 type conversions
// ============================================================================

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

func TestMetadataGetFloat32Delegates(t *testing.T) {
	md := NewMetadata()
	md.set("val", 3.14)

	result := md.GetFloat32("val")
	if result < 3.13 || result > 3.15 {
		t.Errorf("GetFloat32: expected ~3.14, got %f", result)
	}
}

// ============================================================================
// Metadata: GetTime type conversions
// ============================================================================

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

// ============================================================================
// Metadata: Set/Get/Delete lifecycle
// ============================================================================

func TestMetadataSetGetDeleteLifecycle(t *testing.T) {
	md := NewMetadata()

	// Set various types via typed setters
	md.SetString("s", "hello")
	md.SetBool("b", true)
	md.SetInt("i", 42)
	md.SetInt32("i32", 32)
	md.SetInt64("i64", 64)
	md.SetUint("u", 100)
	md.SetUint32("u32", 200)
	md.SetUint64("u64", 300)
	md.SetFloat32("f32", 3.14)
	md.SetFloat64("f64", 2.718)
	md.SetTime("t", time.Now())

	// All should exist
	keys := md.GetAllKeys()
	if len(keys) != 11 {
		t.Errorf("Expected 11 keys, got %d", len(keys))
	}

	// Exists
	if !md.Exists("s") {
		t.Error("Key 's' should exist")
	}
	if md.Exists("nonexistent") {
		t.Error("Key 'nonexistent' should not exist")
	}

	// Delete
	md.Delete("s")
	if md.Exists("s") {
		t.Error("Key 's' should be deleted")
	}

	// Delete non-existent (no-op)
	md.Delete("nonexistent")

	// GetAll
	all := md.GetAll()
	if len(all) != 10 { // 11 - 1 deleted
		t.Errorf("Expected 10 entries in GetAll, got %d", len(all))
	}

	// GetAllAsString
	allStr := md.GetAllAsString()
	if len(allStr) != 10 {
		t.Errorf("Expected 10 entries in GetAllAsString, got %d", len(allStr))
	}
}

// ============================================================================
// Metadata: chaining
// ============================================================================

func TestMetadataChainingDeep(t *testing.T) {
	md := NewMetadata()

	// All typed setters return LocalMetadata for chaining
	result := md.SetString("a", "1").
		SetBool("b", true).
		SetInt("c", 3).
		SetInt32("d", 4).
		SetInt64("e", 5).
		SetUint("f", 6).
		SetUint32("g", 7).
		SetUint64("h", 8).
		SetFloat32("i", 9.0).
		SetFloat64("j", 10.0).
		SetTime("k", time.Now())

	if result == nil {
		t.Error("Chaining should return non-nil")
	}

	if len(md.GetAllKeys()) != 11 {
		t.Error("All 11 keys should be set via chaining")
	}
}

// ============================================================================
// Metadata: update with timestamp comparison
// ============================================================================

func TestMetadataUpdateTimestampComparison(t *testing.T) {
	md := NewMetadata()

	// Initial update with force
	data1 := map[string]interface{}{"key": "v1"}
	ts1 := time.Now()
	md.set("key", "v1") // sets lastModified

	// Older timestamp should be rejected
	olderData := map[string]interface{}{"key": "old"}
	if md.update(olderData, 0, false) {
		t.Error("Should reject update with older timestamp")
	}

	// Force should bypass timestamp check
	if !md.update(data1, 0, true) {
		t.Error("Force update should always succeed")
	}

	_ = ts1
}

// ============================================================================
// Metadata: SetOnLocalChange
// ============================================================================

func TestMetadataSetOnLocalChange(t *testing.T) {
	md := NewMetadata()

	callCount := 0
	md.SetOnLocalChange(func(ts hlc.Timestamp, data map[string]interface{}) {
		callCount++
	})

	md.SetString("key", "val")
	if callCount != 1 {
		t.Errorf("Expected 1 callback, got %d", callCount)
	}

	md.Delete("key")
	if callCount != 2 {
		t.Errorf("Expected 2 callbacks, got %d", callCount)
	}

	// Set to nil should not panic
	md.SetOnLocalChange(nil)
	md.SetString("key2", "val2") // should not panic
}

// ============================================================================
// Metadata: GetAll returns copy
// ============================================================================

func TestMetadataGetAllReturnsCopy(t *testing.T) {
	md := NewMetadata()
	md.SetString("key", "value")

	all := md.GetAll()
	all["key"] = "modified"

	// Original should not be affected
	if md.GetString("key") != "value" {
		t.Error("GetAll should return a copy, not original")
	}
}

// ============================================================================
// Metadata: empty/nil edge cases
// ============================================================================

func TestMetadataEmptyEdgeCases(t *testing.T) {
	md := NewMetadata()

	// Operations on empty metadata
	if md.GetString("any") != "" {
		t.Error("Empty metadata GetString should return empty")
	}
	if md.GetInt("any") != 0 {
		t.Error("Empty metadata GetInt should return 0")
	}
	if md.GetBool("any") != false {
		t.Error("Empty metadata GetBool should return false")
	}
	if !md.GetTime("any").IsZero() {
		t.Error("Empty metadata GetTime should return zero time")
	}
	if md.Exists("any") {
		t.Error("Empty metadata Exists should return false")
	}

	keys := md.GetAllKeys()
	if len(keys) != 0 {
		t.Errorf("Empty metadata should have 0 keys, got %d", len(keys))
	}
}
