package crypto

import "testing"

func TestRandomBytesReturnsCorrectLengthSlice(t *testing.T) {
	b, err := randomBytes(16)
	if err != nil {
		t.Error("Unexpected error:", err)
	}
	if len(b) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(b))
	}
}

func TestRandomBytesReturnsDifferentValuesOnSubsequentCalls(t *testing.T) {
	b1, err1 := randomBytes(8)
	b2, err2 := randomBytes(8)
	if err1 != nil || err2 != nil {
		t.Error("Unexpected error on randomBytes call")
	}
	if string(b1) == string(b2) {
		t.Error("Expected different random bytes on subsequent calls")
	}
}
