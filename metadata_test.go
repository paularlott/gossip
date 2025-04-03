package gossip

import (
	"fmt"
	"testing"
	"time"
)

func TestMetadataBasicOperations(t *testing.T) {
	md := NewMetadata()

	// Test empty state
	if val := md.GetString("nonexistent"); val != "" {
		t.Errorf("Expected empty string for nonexistent key, got %s", val)
	}

	// Set and get string
	md.SetString("name", "test-node")
	if val := md.GetString("name"); val != "test-node" {
		t.Errorf("Expected 'test-node', got %s", val)
	}

	// Test delete
	md.Delete("name")
	if val := md.GetString("name"); val != "" {
		t.Errorf("Expected empty string after delete, got %s", val)
	}
}

func TestMetadataStringConversions(t *testing.T) {
	md := NewMetadata()

	// Set various types and get as string
	md.SetBool("bool-true", true)
	md.SetBool("bool-false", false)
	md.SetInt("int", 123)
	md.SetInt64("int64", -9223372036854775807)
	md.SetUint64("uint64", 18446744073709551615)
	md.SetFloat64("float64", 123.456)
	testTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	md.SetTime("time", testTime)

	tests := []struct {
		key      string
		expected string
	}{
		{"bool-true", "true"},
		{"bool-false", "false"},
		{"int", "123"},
		{"int64", "-9223372036854775807"},
		{"uint64", "18446744073709551615"},
		{"float64", "123.456"},
		{"time", "2023-01-01T12:00:00Z"},
	}

	for _, test := range tests {
		got := md.GetString(test.key)
		if got != test.expected {
			t.Errorf("GetString(%s) = %s; want %s", test.key, got, test.expected)
		}
	}
}

func TestMetadataBoolConversions(t *testing.T) {
	md := NewMetadata()

	// Test direct bool values
	md.SetBool("true", true)
	md.SetBool("false", false)

	// Test numeric to bool conversions
	md.SetInt("int-one", 1)
	md.SetInt("int-zero", 0)
	md.SetInt64("int64-neg", -10)
	md.SetFloat64("float-nonzero", 0.1)
	md.SetFloat64("float-zero", 0.0)

	// Test string to bool conversions
	md.SetString("str-true", "true")
	md.SetString("str-false", "false")
	md.SetString("str-one", "1")
	md.SetString("str-zero", "0")
	md.SetString("str-invalid", "invalid")

	tests := []struct {
		key      string
		expected bool
	}{
		{"true", true},
		{"false", false},
		{"int-one", true},
		{"int-zero", false},
		{"int64-neg", true}, // Non-zero int is true
		{"float-nonzero", true},
		{"float-zero", false},
		{"str-true", true},
		{"str-false", false},
		{"str-one", true},
		{"str-zero", false},
		{"str-invalid", false}, // Invalid string should be false
		{"nonexistent", false}, // Nonexistent key should be false
	}

	for _, test := range tests {
		got := md.GetBool(test.key)
		if got != test.expected {
			t.Errorf("GetBool(%s) = %v; want %v", test.key, got, test.expected)
		}
	}
}

func TestMetadataIntConversions(t *testing.T) {
	md := NewMetadata()

	// Test various types that should convert to integers
	md.SetInt("int", 42)
	md.SetInt32("int32", 32)
	md.SetInt64("int64", 64)
	md.SetUint("uint", 100)
	md.SetUint32("uint32", 320)
	md.SetUint64("uint64", 640)
	md.SetFloat32("float32", 32.5)
	md.SetFloat64("float64", 64.5)
	md.SetString("str-int", "123")
	md.SetString("str-neg", "-456")
	md.SetString("str-float", "789.5")
	md.SetString("str-invalid", "not-a-number")
	md.SetBool("bool-true", true)
	md.SetBool("bool-false", false)

	// Test int conversions
	tests := []struct {
		key      string
		expected int
	}{
		{"int", 42},
		{"int32", 32},
		{"int64", 64},
		{"uint", 100},
		{"uint32", 320},
		{"uint64", 640},
		{"float32", 32}, // Truncated
		{"float64", 64}, // Truncated
		{"str-int", 123},
		{"str-neg", -456},
		{"str-float", 789}, // Truncated
		{"str-invalid", 0}, // Invalid string should be 0
		{"bool-true", 1},
		{"bool-false", 0},
		{"nonexistent", 0}, // Nonexistent key should be 0
	}

	for _, test := range tests {
		got := md.GetInt(test.key)
		if got != test.expected {
			t.Errorf("GetInt(%s) = %d; want %d", test.key, got, test.expected)
		}
	}

	// Test int32/int64 conversions - sample key cases
	if md.GetInt32("int") != 42 {
		t.Errorf("GetInt32(int) failed")
	}
	if md.GetInt64("str-neg") != -456 {
		t.Errorf("GetInt64(str-neg) failed")
	}
}

func TestMetadataUintConversions(t *testing.T) {
	md := NewMetadata()

	// Set various types
	md.SetUint("uint", 42)
	md.SetUint32("uint32", 32)
	md.SetUint64("uint64", 64)
	md.SetInt("int-pos", 100)
	md.SetInt("int-neg", -100) // Negative should convert to 0
	md.SetFloat64("float", 123.45)
	md.SetString("str-uint", "789")
	md.SetString("str-neg", "-123") // Should convert to 0
	md.SetBool("bool-true", true)
	md.SetBool("bool-false", false)

	tests := []struct {
		key      string
		expected uint
	}{
		{"uint", 42},
		{"uint32", 32},
		{"uint64", 64},
		{"int-pos", 100},
		{"int-neg", 0}, // Negative becomes 0
		{"float", 123}, // Truncated
		{"str-uint", 789},
		{"str-neg", 0}, // Negative string becomes 0
		{"bool-true", 1},
		{"bool-false", 0},
		{"nonexistent", 0},
	}

	for _, test := range tests {
		got := md.GetUint(test.key)
		if got != test.expected {
			t.Errorf("GetUint(%s) = %d; want %d", test.key, got, test.expected)
		}
	}

	// Test uint32/uint64 conversions - sample key cases
	if md.GetUint32("uint") != 42 {
		t.Errorf("GetUint32(uint) failed")
	}
	if md.GetUint64("str-uint") != 789 {
		t.Errorf("GetUint64(str-uint) failed")
	}
}

func TestMetadataFloatConversions(t *testing.T) {
	md := NewMetadata()

	// Set various types
	md.SetFloat32("float32", 32.5)
	md.SetFloat64("float64", 64.75)
	md.SetInt("int", 42)
	md.SetUint64("uint64", 100)
	md.SetString("str-float", "123.45")
	md.SetString("str-int", "789")
	md.SetString("str-invalid", "not-a-number")
	md.SetBool("bool-true", true)
	md.SetBool("bool-false", false)

	tests := []struct {
		key      string
		expected float64
	}{
		{"float32", 32.5},
		{"float64", 64.75},
		{"int", 42.0},
		{"uint64", 100.0},
		{"str-float", 123.45},
		{"str-int", 789.0},
		{"str-invalid", 0.0},
		{"bool-true", 1.0},
		{"bool-false", 0.0},
		{"nonexistent", 0.0},
	}

	for _, test := range tests {
		got := md.GetFloat64(test.key)
		if got != test.expected {
			t.Errorf("GetFloat64(%s) = %f; want %f", test.key, got, test.expected)
		}
	}

	// Test float32 conversion
	if md.GetFloat32("float64") != 64.75 {
		t.Errorf("GetFloat32(float64) failed")
	}
}

func TestMetadataTimeConversions(t *testing.T) {
	md := NewMetadata()

	testTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	md.SetTime("time", testTime)
	md.SetInt64("ts", testTime.UnixNano())
	md.SetString("str-rfc3339", testTime.Format(time.RFC3339))
	md.SetString("str-date", "2023-01-02") // Just date
	md.SetString("str-invalid", "not-a-time")

	// Direct time retrieval
	if !md.GetTime("time").Equal(testTime) {
		t.Errorf("GetTime(time) failed")
	}

	// Timestamp conversion
	if !md.GetTime("ts").Equal(testTime) {
		t.Errorf("GetTime(ts) failed")
	}

	// String conversion
	if !md.GetTime("str-rfc3339").Equal(testTime) {
		t.Errorf("GetTime(str-rfc3339) failed")
	}

	// Date-only string should work but have zero time
	expectedDate := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	if !md.GetTime("str-date").Equal(expectedDate) {
		t.Errorf("GetTime(str-date) failed, got %v, want %v",
			md.GetTime("str-date"), expectedDate)
	}

	// Invalid string should return zero time
	if !md.GetTime("str-invalid").Equal(time.Time{}) {
		t.Errorf("GetTime(str-invalid) should return zero time")
	}

	// Nonexistent key should return zero time
	if !md.GetTime("nonexistent").Equal(time.Time{}) {
		t.Errorf("GetTime(nonexistent) should return zero time")
	}
}

func TestMetadataGetAll(t *testing.T) {
	md := NewMetadata()

	// Add some values
	md.SetString("str", "string")
	md.SetInt("int", 42)
	md.SetBool("bool", true)

	// Get all values
	all := md.GetAll()

	// Check count
	if len(all) != 3 {
		t.Errorf("GetAll should return 3 items, got %d", len(all))
	}

	// Check values
	if all["str"] != "string" {
		t.Errorf("GetAll['str'] invalid")
	}
	if all["int"] != 42 {
		t.Errorf("GetAll['int'] invalid")
	}
	if all["bool"] != true {
		t.Errorf("GetAll['bool'] invalid")
	}

	// Modify the returned map should not affect original
	all["new"] = "value"
	if exists := md.Exists("new"); exists {
		t.Errorf("Modifying the map returned by GetAll should not affect the metadata")
	}
}

func TestMetadataConcurrentAccess(t *testing.T) {
	md := NewMetadata()

	// Number of goroutines to use for testing
	const numGoroutines = 10
	// Number of operations per goroutine
	const numOps = 100

	// Use a channel to signal completion
	done := make(chan bool, numGoroutines*2)

	// Start reader goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(n int) {
			for j := 0; j < numOps; j++ {
				// Read some values
				md.GetString(fmt.Sprintf("key-%d", j%10))
				md.GetInt(fmt.Sprintf("key-%d", j%10))
				md.GetBool(fmt.Sprintf("key-%d", j%10))
			}
			done <- true
		}(i)
	}

	// Start writer goroutines
	for i := 0; i < numGoroutines; i++ {
		go func(n int) {
			for j := 0; j < numOps; j++ {
				// Write different types of values
				key := fmt.Sprintf("key-%d", j%10)
				switch j % 3 {
				case 0:
					md.SetString(key, fmt.Sprintf("value-%d-%d", n, j))
				case 1:
					md.SetInt(key, j)
				case 2:
					md.SetBool(key, j%2 == 0)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines*2; i++ {
		<-done
	}

	// We mainly care that there are no race conditions or panics
	// but we should also have some data
	all := md.GetAll()
	if len(all) == 0 {
		t.Errorf("No metadata entries after concurrent operations")
	}
}
