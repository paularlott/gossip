package gossip

import "testing"

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
