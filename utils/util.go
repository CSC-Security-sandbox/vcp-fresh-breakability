package utils

import (
	"context"
	"net"
	"regexp"
	"strconv"
	"strings"

	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	localRegion                   = env.GetString("LOCAL_REGION", "local")
	parseRegionAndZone            = _parseRegionAndZone
	ParseAndValidateRegionAndZone = _parseAndValidateRegionAndZone
	GetLoggerFromContext          = getLoggerFromContext
)

func ValidateIPv4Address(ipAddr string) bool {
	parsedIP := net.ParseIP(ipAddr)
	return parsedIP != nil && parsedIP.To4() != nil
}

// ItemsInSliceUnique checks if items in the slice are unique, returns false if not
func ItemsInSliceUnique(in []string) bool {
	seen := make(map[string]bool, len(in))
	for _, elem := range in {
		if !seen[strings.ToLower(elem)] {
			seen[strings.ToLower(elem)] = true
		} else {
			return false
		}
	}
	return true
}

// ContainsString checks if items in the slice match the inputted string, returns false if not
func ContainsString(arr []string, elem string) bool {
	for _, obj := range arr {
		if obj == elem {
			return true
		}
	}
	return false
}

func ContainsInt(arr []int, elem int) bool {
	for _, obj := range arr {
		if obj == elem {
			return true
		}
	}
	return false
}

func ContainsFloat64(arr []float64, elem float64) bool {
	for _, obj := range arr {
		if obj == elem {
			return true
		}
	}
	return false
}

// IsDuplicateUUID checks if uuid exists in the map.
func IsDuplicateUUID(keys map[string]bool, uuid string) bool {
	if _, value := keys[uuid]; !value {
		return false
	}
	return true
}

// EnvToInt32Conversion int to int32 Incorrect conversion for CodeQL
func EnvToInt32Conversion(envVal string, def int32) int32 {
	parseVal, err := strconv.ParseInt(envVal, 10, 32)
	if err != nil {
		return def
	} else {
		return int32(parseVal)
	}
}

func CheckForRetriableError(errorMessage string, retriableErrors []string) bool {
	// Iterate through each error message and check if it is retriableError
	if errorMessage == "" {
		return false
	}
	for _, retriableError := range retriableErrors {
		if strings.Contains(errorMessage, retriableError) {
			return true
		}
	}
	return false
}

func _parseAndValidateRegionAndZone(locationID string) (string, string, *gcpgenserver.Error) {
	region, zone, err := parseRegionAndZone(locationID)
	if err != nil {
		code := float64(400)
		return "", "", &gcpgenserver.Error{Code: code, Message: err.Error()}
	}
	if region != localRegion {
		code := float64(400)
		msg := "Invalid region. Region can only be " + localRegion
		return "", "", &gcpgenserver.Error{Code: code, Message: msg}
	}
	return region, zone, nil
}

func _parseRegionAndZone(locationID string) (string, string, error) {
	var region string
	var zone string
	pattern := regexp.MustCompile(`^([a-z]+-[a-z]+\d+)(-[a-z])?$`)
	if pattern.MatchString(locationID) {
		if pattern.FindStringSubmatch(locationID)[2] == "" {
			// locationID represents a region
			region = locationID
			zone = ""
		} else {
			// locationID represents a zone
			region = pattern.FindStringSubmatch(locationID)[1]
			zone = pattern.FindStringSubmatch(locationID)[0]
		}
	} else {
		// locationID represents neither region nor zone.
		msg := "LocationID represents neither a region nor a zone"
		return "", "", errors.New(msg)
	}
	return region, zone, nil
}

// getLoggerFromContext extracts the logger from the provided context.
func getLoggerFromContext(ctx context.Context) log.Logger {
	logger, _ := ctx.Value(middleware.ContextSLoggerKey).(log.Logger)
	return logger
}
