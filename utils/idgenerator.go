package utils

import (
	"math/rand"
	"regexp"
	"time"

	"github.com/google/uuid"
)

func init() {
	seedInit()
}

var seedInit = _seedInit

func _seedInit() {
	source := rand.NewSource(time.Now().UnixNano())
	rand.New(source)
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz1234567890")
var hexRunes = []rune("abcdef0123456789")

// GenerateRandomAlphanumeric returns a random alphanumeric string of length n
func GenerateRandomAlphanumeric(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// GenerateRandomHex returns a random hexadecimal string of length n
func GenerateRandomHex(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexRunes[rand.Intn(len(hexRunes))]
	}
	return string(b)
}

// GenerateRandomInRange returns a random integer from 0 to less than n
func GenerateRandomInRange(n int) int {
	return rand.Intn(n)
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
