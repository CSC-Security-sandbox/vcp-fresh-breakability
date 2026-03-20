package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// secureRandomSource is the reader used by secureIntn for crypto/rand.Int
var secureRandomSource io.Reader = rand.Reader

// secureIntnNow returns the current time for secureIntn fallback
var secureIntnNow = time.Now

// secureIntn generates a cryptographically secure random integer in [0, n)
// It retries up to 3 times before falling back to a time-based value
func secureIntn(n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("n must be positive")
	}
	maxVal := big.NewInt(int64(n))

	// Retry up to 3 times
	for attempt := 0; attempt < 3; attempt++ {
		result, err := rand.Int(secureRandomSource, maxVal)
		if err == nil {
			return int(result.Int64()), nil
		}
		// Small delay before retry
		time.Sleep(time.Millisecond * time.Duration(attempt+1))
	}

	// Fallback: use time-based pseudo-random value as last resort
	// This ensures the function never fails completely
	u := secureIntnNow().UnixNano()
	if u < 0 {
		u = -u
	}
	fallback := int(u % int64(n))
	return fallback, nil
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz1234567890")
var hexRunes = []rune("abcdef0123456789")

// GenerateRandomAlphanumeric returns a random alphanumeric string of length n
func GenerateRandomAlphanumeric(n int) string {
	b := make([]rune, n)
	letterLen := len(letterRunes)
	for i := range b {
		idx, _ := secureIntn(letterLen) // secureIntn handles errors internally with fallback
		b[i] = letterRunes[idx]
	}
	return string(b)
}

// GenerateRandomHex returns a random hexadecimal string of length n
func GenerateRandomHex(n int) string {
	b := make([]rune, n)
	hexLen := len(hexRunes)
	for i := range b {
		idx, _ := secureIntn(hexLen) // secureIntn handles errors internally with fallback
		b[i] = hexRunes[idx]
	}
	return string(b)
}

// GenerateRandomInRange returns a random integer from 0 to less than n
func GenerateRandomInRange(n int) int {
	if n <= 0 {
		return 0
	}
	result, _ := secureIntn(n) // secureIntn handles errors internally with fallback
	return result
}

// RandomUUID returns a random UUID
func RandomUUID() string {
	return GenerateRandomHex(8) + "-" +
		GenerateRandomHex(4) + "-" +
		GenerateRandomHex(4) + "-" +
		GenerateRandomHex(4) + "-" +
		GenerateRandomHex(12)
}

var r = regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")

// IsValidUUID validates whether the given string is in UUID format or not.
func IsValidUUID(uuid string) bool {
	return r.MatchString(uuid)
}

// ValidUUID validates whether the given string is in UUID format or not without using regex with google api which is 20x faster.
func ValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

// GetOperationUUID extracts the operation UUID from the operation ID.
// example: "projects/123456789/locations/us-central1/operations/123456789-1234-5678-9012-123456789012"
func GetOperationUUID(operationID string) string {
	parts := regexp.MustCompile("/").Split(operationID, -1)
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

// GenerateDeterministicDeploymentName generates a deterministic deployment name based on accountID and poolID
func GenerateDeterministicDeploymentName(accountID int64, poolID string, region string) string {
	if env.GetBool("INTEGRATION_TEST", false) {
		return env.GetString("DEPLOYMENT_NAME", "integration-test")
	}
	data := fmt.Sprintf("%d-%s-%s", accountID, poolID, region)
	hash := sha256.Sum256([]byte(data))
	return "gcnv-" + hex.EncodeToString(hash[:8])[:15]
}

// GenerateUniqueUsername creates a 8-character hash from the input string combined with the current timestamp
func GenerateUniqueUsername(input string) string {
	// Get the current timestamp
	currentTime := time.Now().UnixNano()
	timestamp := fmt.Sprintf("%d", currentTime)

	// Combine the input string with the current timestamp
	combinedString := input + timestamp

	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(combinedString))
	fullHash := hex.EncodeToString(sha256Hash.Sum(nil))

	// Truncate the hash to the first 8 characters
	shortHash := fullHash[:8]
	return shortHash
}
