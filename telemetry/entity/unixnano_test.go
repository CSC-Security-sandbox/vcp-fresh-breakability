package entity

import (
	"testing"
	"time"
)

func validUnixNano() UnixNano {
	return UnixNano(time.Date(2023, 10, 1, 12, 34, 56, 789000000, time.UTC).UnixNano())
}

func validUnixNanoJSON() []byte {
	return []byte("\"2023-10-01T12:34:56.789Z\"")
}

func invalidUnixNanoJSON() []byte {
	return []byte("\"invalid-timestamp\"")
}

func marshalUnixNanoToJSON() ([]byte, error) {
	return validUnixNano().MarshalJSON()
}

func unmarshalUnixNanoFromJSON(data []byte) error {
	var unixNano UnixNano
	return unixNano.UnmarshalJSON(data)
}

func unmarshalUnixNanoFromJSONWithInvalidData(data []byte) error {
	var unixNano UnixNano
	return unixNano.UnmarshalJSON(data)
}

func TestMarshalUnixNanoToJSON(t *testing.T) {
	data, err := marshalUnixNanoToJSON()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if string(data) != string(validUnixNanoJSON()) {
		t.Fatalf("Expected %s, got %s", validUnixNanoJSON(), data)
	}
}

func TestUnmarshalUnixNanoFromJSON(t *testing.T) {
	err := unmarshalUnixNanoFromJSON(validUnixNanoJSON())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestUnmarshalUnixNanoFromJSONWithInvalidData(t *testing.T) {
	err := unmarshalUnixNanoFromJSONWithInvalidData(invalidUnixNanoJSON())
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
}
