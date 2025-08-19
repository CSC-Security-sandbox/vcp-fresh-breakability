package utils

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// Constants for error messages, regex patterns, and expected parts
const (
	ErrInvalidResourceFormat      = "invalid key full path format"
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
		return nil, errors.NewBadRequestErr(ErrInvalidResourceFormat)
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

// DetermineStartToCloseTimeoutBasedOnUsedSize returns the maxtimeout for polling for pool or volume based on its used size
func DetermineStartToCloseTimeoutBasedOnUsedSize(volumesForMigration []*datamodel.Volume) int64 {
	var usedSpaceInGB float64

	if len(volumesForMigration) == 1 {
		usedSpaceInGB = usedSpaceInGB + float64(volumesForMigration[0].UsedBytes/1024/1024/1024)
	} else {
		for _, volume := range volumesForMigration {
			if volume != nil {
				usedSpaceInGB = usedSpaceInGB + float64(volume.UsedBytes/1024/1024/1024)
			}
		}
	}

	// Define constants for StartToCloseTimeout (in minutes)
	// Our experiments have shown roughly a 6-9x increase in encryption times for 10x increase in occupied size
	const (
		TimeoutLowOccupied     int64 = 15
		TimeoutLessThan100GB   int64 = 30
		TimeoutLessThan500GB   int64 = 150
		TimeoutLessThan1000GB  int64 = 300
		TimeoutLessThan5000GB  int64 = 1500
		TimeoutLessThan10000GB int64 = 3000
		MaximumTimeout         int64 = 10000
	)

	var startToCloseTimeout int64
	switch {
	case usedSpaceInGB < 10.0:
		startToCloseTimeout = TimeoutLowOccupied
	case usedSpaceInGB < 100.0:
		startToCloseTimeout = TimeoutLessThan100GB
	case usedSpaceInGB < 500.0:
		startToCloseTimeout = TimeoutLessThan500GB
	case usedSpaceInGB < 1000.0:
		startToCloseTimeout = TimeoutLessThan1000GB
	case usedSpaceInGB < 5000.0:
		startToCloseTimeout = TimeoutLessThan5000GB
	case usedSpaceInGB < 10000.0:
		startToCloseTimeout = TimeoutLessThan10000GB
	case usedSpaceInGB >= 10000.0:
		startToCloseTimeout = MaximumTimeout
	default:
		return MaximumTimeout
	}
	return startToCloseTimeout
}
