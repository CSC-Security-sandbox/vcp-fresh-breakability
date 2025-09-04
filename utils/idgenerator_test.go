package utils

import (
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
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

func TestGetOperationUUID(t *testing.T) {
	t.Run("TestGetOperationUUIDReturnsInputWhenNoSlashesPresent", func(tt *testing.T) {
		operationID := "no-slash-uuid"
		if got := GetOperationUUID(operationID); got != operationID {
			t.Errorf("expected %s, got %s", operationID, got)
		}
	})
	t.Run("TestGetOperationUUIDReturnsEmptyStringForEmptyInput", func(tt *testing.T) {
		if got := GetOperationUUID(""); got != "" {
			t.Errorf("expected empty string, got %s", got)
		}
	})
	t.Run("TestGetOperationUUIDReturnsLastSegmentForNonStandardOperationID", func(tt *testing.T) {
		operationID := "foo/bar/baz/last-segment"
		expected := "last-segment"
		if got := GetOperationUUID(operationID); got != expected {
			t.Errorf("expected %s, got %s", expected, got)
		}
	})

	t.Run("TestGetOperationUUIDReturnsUUIDFromValidOperationID", func(tt *testing.T) {
		operationID := "projects/123456789/locations/us-central1/operations/12345678-1234-5678-9012-123456789012"
		expectedUUID := "12345678-1234-5678-9012-123456789012"
		if got := GetOperationUUID(operationID); got != expectedUUID {
			t.Errorf("expected %s, got %s", expectedUUID, got)
		}
	})
}

func TestGenerateDeterministicDeploymentName(t *testing.T) {
	accountID := int64(12345)
	poolID := "test-pool"
	region := "us-central1"
	name := GenerateDeterministicDeploymentName(accountID, poolID, region)

	// Check length
	if len(name) != 20 {
		t.Errorf("Expected length 20, got %d", len(name))
	}

	// Check prefix
	if name[:5] != "gcnv-" {
		t.Errorf("Expected prefix 'gcnv-', got %s", name[:5])
	}

	// Check hex part
	hexPart := name[5:]
	if len(hexPart) != 15 {
		t.Errorf("Expected hex part length 15, got %d", len(hexPart))
	}
	hexRegex := regexp.MustCompile("^[0-9a-f]{15}$")
	if !hexRegex.MatchString(hexPart) {
		t.Errorf("Hex part contains invalid characters: %s", hexPart)
	}

	// Check determinism
	name2 := GenerateDeterministicDeploymentName(accountID, poolID, region)
	assert.Equal(t, name, name2, "Expected deterministic output to be the same for the same inputs")

	// Check uniqueness for different inputs
	name3 := GenerateDeterministicDeploymentName(accountID+1, poolID, region)
	assert.NotEqual(t, name, name3, "Expected different output for different accountID")

	name4 := GenerateDeterministicDeploymentName(accountID, poolID+"-diff", region)
	assert.NotEqual(t, name, name4, "Expected different output for different poolID")

	name5 := GenerateDeterministicDeploymentName(accountID, poolID, region+"-diff")
	assert.NotEqual(t, name, name5, "Expected different output for different region")
}

func TestGenerateDeterministicDeploymentNameForIntegrationTests(t *testing.T) {
	accountID := int64(12345)
	poolID := "test-pool"
	region := "us-central1"

	originalEnv := strconv.FormatBool(env.GetBool("INTEGRATION_TEST", false))
	// Restore the original value after the test
	err := os.Setenv("INTEGRATION_TEST", "true")
	if err != nil {
		return
	}
	defer func() {
		err := os.Setenv("INTEGRATION_TEST", originalEnv)
		if err != nil {
			return
		}
	}()

	name := GenerateDeterministicDeploymentName(accountID, poolID, region)
	assert.Equal(t, name, "integration-test", "Expected deterministic output to be 'integration-test'")
}
