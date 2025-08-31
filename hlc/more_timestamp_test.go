package hlc

import "testing"

func TestTimestampComparisonsAndCounter(t *testing.T) {
	c := NewClock()
	a := c.Now()
	b := c.Now()

	if !a.Before(b) {
		t.Fatalf("expected a < b")
	}
	if !b.After(a) {
		t.Fatalf("expected b > a")
	}
	if a.Equal(b) {
		t.Fatalf("expected a != b")
	}

	// Counter should be small and extraction should not panic
	_ = a.Counter()

	// Default clock function
	d := Now()
	if d == 0 {
		t.Fatalf("expected Now() to produce non-zero timestamp")
	}
}
