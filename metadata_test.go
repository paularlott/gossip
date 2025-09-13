package gossip

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paularlott/gossip/hlc"
)

func TestNewMetadata(t *testing.T) {
	md := NewMetadata()
	
	if md == nil {
		t.Fatal("NewMetadata returned nil")
	}
	
	if md.data.Load() == nil {
		t.Fatal("data not initialized")
	}
	
	if len(*md.data.Load()) != 0 {
		t.Fatal("data should be empty initially")
	}
	
	if md.GetTimestamp() != 0 {
		t.Fatal("timestamp should be 0 initially")
	}
}

func TestMetadataInterfaceImplementation(t *testing.T) {
	md := NewMetadata()
	
	// Test MetadataReader interface
	var reader MetadataReader = md
	if reader == nil {
		t.Fatal("Metadata should implement MetadataReader")
	}
	
	// Test LocalMetadata interface
	var local LocalMetadata = md
	if local == nil {
		t.Fatal("Metadata should implement LocalMetadata")
	}
}

func TestMetadataSetString(t *testing.T) {
	md := NewMetadata()
	
	result := md.SetString("key1", "value1")
	if result != md {
		t.Fatal("SetString should return self")
	}
	
	if md.GetString("key1") != "value1" {
		t.Fatal("GetString should return set value")
	}
	
	if !md.Exists("key1") {
		t.Fatal("Key should exist after setting")
	}
	
	// Test timestamp update
	if md.GetTimestamp() == 0 {
		t.Fatal("Timestamp should be updated after setting")
	}
}

func TestMetadataSetBool(t *testing.T) {
	md := NewMetadata()
	
	md.SetBool("bool_true", true)
	md.SetBool("bool_false", false)
	
	if !md.GetBool("bool_true") {
		t.Fatal("GetBool should return true")
	}
	
	if md.GetBool("bool_false") {
		t.Fatal("GetBool should return false")
	}
	
	// Test string conversion
	if md.GetString("bool_true") != "true" {
		t.Fatal("Bool true should convert to string 'true'")
	}
	
	if md.GetString("bool_false") != "false" {
		t.Fatal("Bool false should convert to string 'false'")
	}
}

func TestMetadataSetInt(t *testing.T) {
	md := NewMetadata()
	
	md.SetInt("int", 42)
	md.SetInt32("int32", 32)
	md.SetInt64("int64", 64)
	
	if md.GetInt("int") != 42 {
		t.Fatal("GetInt should return 42")
	}
	
	if md.GetInt32("int32") != 32 {
		t.Fatal("GetInt32 should return 32")
	}
	
	if md.GetInt64("int64") != 64 {
		t.Fatal("GetInt64 should return 64")
	}
	
	// Test cross-type conversion
	if md.GetInt64("int") != 42 {
		t.Fatal("GetInt64 should convert int to int64")
	}
	
	if md.GetString("int") != "42" {
		t.Fatal("GetString should convert int to string")
	}
}

func TestMetadataSetUint(t *testing.T) {
	md := NewMetadata()
	
	md.SetUint("uint", 42)
	md.SetUint32("uint32", 32)
	md.SetUint64("uint64", 64)
	
	if md.GetUint("uint") != 42 {
		t.Fatal("GetUint should return 42")
	}
	
	if md.GetUint32("uint32") != 32 {
		t.Fatal("GetUint32 should return 32")
	}
	
	if md.GetUint64("uint64") != 64 {
		t.Fatal("GetUint64 should return 64")
	}
	
	// Test negative int conversion to uint
	md.SetInt("negative", -5)
	if md.GetUint64("negative") != 0 {
		t.Fatal("Negative int should convert to 0 for uint")
	}
}

func TestMetadataSetFloat(t *testing.T) {
	md := NewMetadata()
	
	md.SetFloat32("float32", 3.14)
	md.SetFloat64("float64", 2.718)
	
	if md.GetFloat32("float32") != 3.14 {
		t.Fatal("GetFloat32 should return 3.14")
	}
	
	if md.GetFloat64("float64") != 2.718 {
		t.Fatal("GetFloat64 should return 2.718")
	}
	
	// Test cross-type conversion
	if md.GetFloat64("float32") != 3.140000104904175 { // float32 precision
		t.Fatalf("GetFloat64 should convert float32, got %f", md.GetFloat64("float32"))
	}
}

func TestMetadataSetTime(t *testing.T) {
	md := NewMetadata()
	
	now := time.Now()
	md.SetTime("time", now)
	
	retrieved := md.GetTime("time")
	if !retrieved.Equal(now) {
		t.Fatal("GetTime should return the same time")
	}
	
	// Test string conversion
	expected := now.Format(time.RFC3339)
	if md.GetString("time") != expected {
		t.Fatal("Time should convert to RFC3339 string")
	}
}

func TestMetadataDelete(t *testing.T) {
	md := NewMetadata()
	
	md.SetString("key1", "value1")
	md.SetString("key2", "value2")
	
	if !md.Exists("key1") {
		t.Fatal("key1 should exist")
	}
	
	initialTimestamp := md.GetTimestamp()
	time.Sleep(time.Microsecond) // Ensure timestamp difference
	
	md.Delete("key1")
	
	if md.Exists("key1") {
		t.Fatal("key1 should not exist after deletion")
	}
	
	if md.GetString("key1") != "" {
		t.Fatal("Deleted key should return empty string")
	}
	
	if md.Exists("key2") != true {
		t.Fatal("key2 should still exist")
	}
	
	// Test timestamp update on delete
	if md.GetTimestamp() <= initialTimestamp {
		t.Fatal("Timestamp should be updated after deletion")
	}
	
	// Test deleting non-existent key
	md.Delete("nonexistent")
	// Should not panic
}

func TestMetadataGetAll(t *testing.T) {
	md := NewMetadata()
	
	md.SetString("str", "value")
	md.SetInt("int", 42)
	md.SetBool("bool", true)
	
	all := md.GetAll()
	
	if len(all) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(all))
	}
	
	if all["str"] != "value" {
		t.Fatal("str value incorrect")
	}
	
	if all["int"] != 42 {
		t.Fatal("int value incorrect")
	}
	
	if all["bool"] != true {
		t.Fatal("bool value incorrect")
	}
	
	// Test that returned map is a copy
	all["str"] = "modified"
	if md.GetString("str") == "modified" {
		t.Fatal("GetAll should return a copy, not reference")
	}
}

func TestMetadataGetAllKeys(t *testing.T) {
	md := NewMetadata()
	
	md.SetString("key1", "value1")
	md.SetString("key2", "value2")
	md.SetString("key3", "value3")
	
	keys := md.GetAllKeys()
	
	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}
	
	keyMap := make(map[string]bool)
	for _, key := range keys {
		keyMap[key] = true
	}
	
	if !keyMap["key1"] || !keyMap["key2"] || !keyMap["key3"] {
		t.Fatal("Missing expected keys")
	}
}

func TestMetadataGetAllAsString(t *testing.T) {
	md := NewMetadata()
	
	md.SetString("str", "value")
	md.SetInt("int", 42)
	md.SetBool("bool", true)
	md.SetFloat64("float", 3.14)
	
	allStr := md.GetAllAsString()
	
	if len(allStr) != 4 {
		t.Fatalf("Expected 4 items, got %d", len(allStr))
	}
	
	if allStr["str"] != "value" {
		t.Fatal("str conversion incorrect")
	}
	
	if allStr["int"] != "42" {
		t.Fatal("int conversion incorrect")
	}
	
	if allStr["bool"] != "true" {
		t.Fatal("bool conversion incorrect")
	}
	
	if allStr["float"] != "3.14" {
		t.Fatal("float conversion incorrect")
	}
}

func TestMetadataTypeConversions(t *testing.T) {
	md := NewMetadata()
	
	// Test string to various types
	md.SetString("str_int", "123")
	md.SetString("str_float", "45.67")
	md.SetString("str_bool", "true")
	md.SetString("str_time", "2023-01-01T12:00:00Z")
	
	if md.GetInt64("str_int") != 123 {
		t.Fatal("String to int conversion failed")
	}
	
	if md.GetFloat64("str_float") != 45.67 {
		t.Fatal("String to float conversion failed")
	}
	
	if !md.GetBool("str_bool") {
		t.Fatal("String to bool conversion failed")
	}
	
	expectedTime, _ := time.Parse(time.RFC3339, "2023-01-01T12:00:00Z")
	if !md.GetTime("str_time").Equal(expectedTime) {
		t.Fatal("String to time conversion failed")
	}
	
	// Test bool to numeric
	md.SetBool("bool_true", true)
	md.SetBool("bool_false", false)
	
	if md.GetInt64("bool_true") != 1 {
		t.Fatal("Bool true should convert to 1")
	}
	
	if md.GetInt64("bool_false") != 0 {
		t.Fatal("Bool false should convert to 0")
	}
	
	// Test numeric to bool
	md.SetInt("zero", 0)
	md.SetInt("nonzero", 5)
	
	if md.GetBool("zero") {
		t.Fatal("Zero should convert to false")
	}
	
	if !md.GetBool("nonzero") {
		t.Fatal("Non-zero should convert to true")
	}
}

func TestMetadataUpdate(t *testing.T) {
	md := NewMetadata()
	
	// Set initial data
	md.SetString("key1", "value1")
	initialTimestamp := md.GetTimestamp()
	
	// Test update with newer timestamp
	newData := map[string]interface{}{
		"key1": "updated_value1",
		"key2": "value2",
	}
	newerTimestamp := hlc.Timestamp(uint64(initialTimestamp) + 1000)
	
	if !md.update(newData, newerTimestamp, false) {
		t.Fatal("Update with newer timestamp should succeed")
	}
	
	if md.GetString("key1") != "updated_value1" {
		t.Fatal("Data should be updated")
	}
	
	if md.GetString("key2") != "value2" {
		t.Fatal("New key should be added")
	}
	
	if md.GetTimestamp() != newerTimestamp {
		t.Fatal("Timestamp should be updated")
	}
	
	// Test update with older timestamp (should fail)
	olderData := map[string]interface{}{
		"key1": "old_value",
	}
	olderTimestamp := hlc.Timestamp(uint64(initialTimestamp) - 1000)
	
	if md.update(olderData, olderTimestamp, false) {
		t.Fatal("Update with older timestamp should fail")
	}
	
	if md.GetString("key1") == "old_value" {
		t.Fatal("Data should not be updated with older timestamp")
	}
	
	// Test forced update with older timestamp
	if !md.update(olderData, olderTimestamp, true) {
		t.Fatal("Forced update should succeed")
	}
	
	if md.GetString("key1") != "old_value" {
		t.Fatal("Forced update should change data")
	}
}

func TestMetadataExists(t *testing.T) {
	md := NewMetadata()
	
	if md.Exists("nonexistent") {
		t.Fatal("Non-existent key should return false")
	}
	
	md.SetString("key", "value")
	
	if !md.Exists("key") {
		t.Fatal("Existing key should return true")
	}
	
	md.Delete("key")
	
	if md.Exists("key") {
		t.Fatal("Deleted key should return false")
	}
}

func TestMetadataEmptyValues(t *testing.T) {
	md := NewMetadata()
	
	// Test getting non-existent keys
	if md.GetString("missing") != "" {
		t.Fatal("Missing string should return empty")
	}
	
	if md.GetInt("missing") != 0 {
		t.Fatal("Missing int should return 0")
	}
	
	if md.GetBool("missing") {
		t.Fatal("Missing bool should return false")
	}
	
	if !md.GetTime("missing").IsZero() {
		t.Fatal("Missing time should return zero time")
	}
	
	// Test empty string conversions
	md.SetString("empty", "")
	
	if md.GetInt("empty") != 0 {
		t.Fatal("Empty string should convert to 0")
	}
	
	if md.GetBool("empty") {
		t.Fatal("Empty string should convert to false")
	}
}

func TestMetadataConcurrentAccess(t *testing.T) {
	md := NewMetadata()
	
	const numGoroutines = 10
	const numOperations = 100
	
	var wg sync.WaitGroup
	var setCount atomic.Int64
	
	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "key_" + string(rune(goroutineID)) + "_" + string(rune(j))
				md.SetString(key, "value")
				setCount.Add(1)
			}
		}(i)
	}
	
	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				md.GetString("key_0_0")
				md.Exists("key_0_0")
				md.GetAll()
			}
		}()
	}
	
	wg.Wait()
	
	// Verify some data was written
	if setCount.Load() != int64(numGoroutines*numOperations) {
		t.Fatalf("Expected %d sets, got %d", numGoroutines*numOperations, setCount.Load())
	}
}

func TestMetadataChaining(t *testing.T) {
	md := NewMetadata()
	
	// Test method chaining
	result := md.SetString("str", "value").
		SetInt("int", 42).
		SetBool("bool", true).
		SetFloat64("float", 3.14)
	
	if result != md {
		t.Fatal("Chaining should return same instance")
	}
	
	if md.GetString("str") != "value" {
		t.Fatal("Chained SetString failed")
	}
	
	if md.GetInt("int") != 42 {
		t.Fatal("Chained SetInt failed")
	}
	
	if !md.GetBool("bool") {
		t.Fatal("Chained SetBool failed")
	}
	
	if md.GetFloat64("float") != 3.14 {
		t.Fatal("Chained SetFloat64 failed")
	}
}

// Benchmarks

func BenchmarkMetadataSetString(b *testing.B) {
	md := NewMetadata()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.SetString("key", "value")
	}
}

func BenchmarkMetadataGetString(b *testing.B) {
	md := NewMetadata()
	md.SetString("key", "value")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.GetString("key")
	}
}

func BenchmarkMetadataSetInt(b *testing.B) {
	md := NewMetadata()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.SetInt("key", i)
	}
}

func BenchmarkMetadataGetInt(b *testing.B) {
	md := NewMetadata()
	md.SetInt("key", 42)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.GetInt("key")
	}
}

func BenchmarkMetadataExists(b *testing.B) {
	md := NewMetadata()
	md.SetString("key", "value")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.Exists("key")
	}
}

func BenchmarkMetadataGetAll(b *testing.B) {
	md := NewMetadata()
	for i := 0; i < 100; i++ {
		md.SetString("key"+string(rune(i)), "value")
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		md.GetAll()
	}
}

func BenchmarkMetadataConcurrentRead(b *testing.B) {
	md := NewMetadata()
	md.SetString("key", "value")
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			md.GetString("key")
		}
	})
}

func BenchmarkMetadataConcurrentWrite(b *testing.B) {
	md := NewMetadata()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			md.SetString("key", "value"+string(rune(i)))
			i++
		}
	})
}

func BenchmarkMetadataMixed(b *testing.B) {
	md := NewMetadata()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			md.SetString("key", "value")
		case 1:
			md.GetString("key")
		case 2:
			md.Exists("key")
		case 3:
			md.Delete("key")
		}
	}
}