package utils

import (
	"regexp"
	"testing"
)

func TestGenerateRandomAlphanumeric(t *testing.T) {
	if r := GenerateRandomAlphanumeric(0); len(r) != 0 {
		t.Fail()
	}
	if r := GenerateRandomAlphanumeric(1); len(r) == 0 {
		t.Fail()
	}
	if r := GenerateRandomAlphanumeric(2); len(r) == 0 {
		t.Fail()
	}
	if r := GenerateRandomAlphanumeric(10); len(r) == 0 {
		t.Fail()
	}
}

func TestGenerateRandomHex(t *testing.T) {
	if r := GenerateRandomHex(0); len(r) != 0 {
		t.Fail()
	}
	if r := GenerateRandomHex(1); len(r) == 0 {
		t.Fail()
	}
	if r := GenerateRandomHex(2); len(r) == 0 {
		t.Fail()
	}
	if r := GenerateRandomHex(10); len(r) == 0 {
		t.Fail()
	}
}

func TestGenerateRandomInRange(t *testing.T) {
	if r := GenerateRandomInRange(1); r < 0 {
		t.Fail()
	}
	if r := GenerateRandomInRange(2); r < 0 {
		t.Fail()
	}
	if r := GenerateRandomInRange(10); r < 0 {
		t.Fail()
	}
	if r := GenerateRandomInRange(10000); r < 0 {
		t.Fail()
	}
}

const (
	uuidLength = 36
	uuidFormat = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

func TestRandomUUID(t *testing.T) {

	re := regexp.MustCompile(uuidFormat)
	if re == nil {
		t.Error("Regular expression for UUID format failed to compile")
	}

	uuid := RandomUUID()
	if len(uuid) != uuidLength {
		t.Errorf("Length %v of random UUID does not match expected length of %v", len(uuid), uuidLength)
	}
	if !re.MatchString(uuid) {
		t.Error("Random UUID does not match expected UUID format")
	}

	uuids := make(map[string]bool)
	uuids[uuid] = true
	numUniquenessChecks := 1000000 // XXX: has been tested with this set 100 times higher, i.e. 100000000
	for i := 1; i < numUniquenessChecks; i++ {
		uuids[RandomUUID()] = true
	}
	if len(uuids) != numUniquenessChecks {
		t.Errorf("Insufficient uniqueness for RandomUUID() - %v unique UUIDs out of %v calls", len(uuids), numUniquenessChecks)
	}
}

func TestIsValidUUID(t *testing.T) {

	t.Run("WithInvalidUUIDString", func(tt *testing.T) {
		testString := "sOmE-rANdom-string123"
		if IsValidUUID(testString) {
			tt.Errorf("IsValidUUID giving wrong response: true for invalid uuid string %s", testString)
		}
	})
	t.Run("WithValidUUIDString", func(tt *testing.T) {
		testString := "54bD70b1-Bc09-45A0-9A38-1f929EC352c3"
		if !IsValidUUID(testString) {
			tt.Errorf("IsValidUUID giving wrong response: false for valid uuid string %s", testString)
		}
	})
}
