package utils

import (
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"regexp"
	"strings"
)

// Constants for error messages, regex patterns, and expected parts
const (
	ErrInvalidResourceFormat      = "invalid resource format"
	ErrInvalidServiceAccountEmail = "invalid service account email format"
	ServiceAccountEmailPattern    = `^([a-zA-Z0-9-]+)-([a-zA-Z0-9-]+)@(\d+)\.iam\.gserviceaccount\.com$`
	ExpectedResourceParts         = 8
	ExpectedServiceAccountParts   = 4
)

// ParsedKeyFullPathResource represents the parsed components of the input resource string.
type ParsedKeyFullPathResource struct {
	ProjectID string
	Location  string
	KeyRing   string
	CryptoKey string
}

// ParseKeyFullPathResource parses the input string and returns a ParsedKeyFullPathResource struct.
// example projects/123/locations/australia-southeast1/keyRings/name/cryptoKeys/name2
func ParseKeyFullPathResource(resource string) (*ParsedKeyFullPathResource, error) {
	parts := strings.Split(resource, "/")

	if len(parts) != ExpectedResourceParts {
		return nil, errors.New(ErrInvalidResourceFormat)
	}

	return &ParsedKeyFullPathResource{
		ProjectID: parts[1],
		Location:  parts[3],
		KeyRing:   parts[5],
		CryptoKey: parts[7],
	}, nil
}

// String reconstructs the full path from the ParsedKeyFullPathResource struct.
func (p ParsedKeyFullPathResource) String() string {
	return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", p.ProjectID, p.Location, p.KeyRing, p.CryptoKey)
}

// ParsedServiceAccount represents the parsed components of a GCP service account email.
type ParsedServiceAccount struct {
	Prefix            string
	CustomerProjectID string
	GlobalProjectID   string
}

// ParseServiceAccountEmail parses a GCP service account email and returns its components.
func ParseServiceAccountEmail(email string) (*ParsedServiceAccount, error) {
	re := regexp.MustCompile(ServiceAccountEmailPattern)

	matches := re.FindStringSubmatch(email)
	if len(matches) != ExpectedServiceAccountParts {
		return nil, errors.New(ErrInvalidServiceAccountEmail)
	}

	return &ParsedServiceAccount{
		Prefix:            matches[1],
		CustomerProjectID: matches[2],
		GlobalProjectID:   matches[3],
	}, nil
}
