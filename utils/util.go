package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	localRegion                   = env.GetString("LOCAL_REGION", "local")
	PairedRegions                 = env.GetString("VCP_PAIRED_REGIONS", "")
	parseRegionAndZone            = _parseRegionAndZone
	ParseAndValidateRegionAndZone = _parseAndValidateRegionAndZone
	GetPairedRegionURI            = _getPairedRegionURI
	ConvertStringToMap            = _convertStringToMap
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

// GetJWTTokenFromContext gets the JWT token from the context
func GetJWTTokenFromContext(ctx context.Context) string {
	if header, ok := ctx.Value(middleware.HeaderContextKey).(http.Header); ok {
		return header.Get("Authorization")
	}
	return ""
}

// ParseProjectId parses the remoteAccount id and returns project number and network name
func ParseProjectId(network string) (string, string, error) {
	tmp := strings.Split(network, "/")
	if len(tmp) != 5 {
		return "", "", vsaerrors.NewVCPError(vsaerrors.ErrBadRequest, errors.New(fmt.Sprintf("parseProjectId failed for network : %s", network)))
	}
	return tmp[1], tmp[4], nil
}

// BytesToGigabytes converts bytes to gigabytes
func BytesToGigabytes(sizeInBytes uint64) int {
	return int(sizeInBytes / 1024 / 1024 / 1024)
}

type Unit int

const (
	B Unit = 1 << (10 * iota)
	KiB
	MiB
	GiB
	TiB
	PiB
)

func ConvertToBytes(size float64, unit Unit) (int64, error) {
	switch unit {
	case B, KiB, MiB, GiB, TiB, PiB:
		return int64(size * float64(unit)), nil
	default:
		return 0, fmt.Errorf("invalid unit: %v", unit)
	}
}

// GetOptString safely converts a pointer to a string into an OptString.
func GetOptString(value *string) gcpgenserver.OptString {
	if value != nil {
		return gcpgenserver.NewOptString(*value)
	}
	return gcpgenserver.OptString{}
}

// GetOptInt64 safely converts a pointer to an int64 into an OptInt64.
func GetOptInt64(value *int64) gcpgenserver.OptInt64 {
	if value != nil {
		return gcpgenserver.NewOptInt64(*value)
	}
	return gcpgenserver.OptInt64{}
}

// GetOptBool safely converts a pointer to a bool into an OptBool.
func GetOptBool(value *bool) gcpgenserver.OptBool {
	if value != nil {
		return gcpgenserver.NewOptBool(*value)
	}
	return gcpgenserver.OptBool{}
}

// GetOptDateTime safely converts a pointer to a strfmt.DateTime into an OptDateTime.
func GetOptDateTime(value *strfmt.DateTime) gcpgenserver.OptDateTime {
	if value != nil {
		return gcpgenserver.NewOptDateTime(time.Time(*value))
	}
	return gcpgenserver.OptDateTime{}
}

func SafeString(value *string) gcpgenserver.OptNilString {
	if value == nil {
		return gcpgenserver.OptNilString{}
	}
	return gcpgenserver.NewOptNilString(*value)
}

func SafeFloat64(value *float64) gcpgenserver.OptNilFloat64 {
	if value == nil {
		return gcpgenserver.OptNilFloat64{}
	}
	return gcpgenserver.NewOptNilFloat64(*value)
}

func SafeInt64(value *int64) gcpgenserver.OptNilInt64 {
	if value == nil {
		return gcpgenserver.OptNilInt64{}
	}
	return gcpgenserver.NewOptNilInt64(*value)
}

func SafeInt64ToInt32(value *int64) gcpgenserver.OptNilInt32 {
	if value == nil {
		return gcpgenserver.OptNilInt32{}
	}
	return gcpgenserver.NewOptNilInt32(int32(*value))
}

func SafeBool(value *bool) gcpgenserver.OptNilBool {
	if value == nil {
		return gcpgenserver.NewOptNilBool(false)
	}
	return gcpgenserver.NewOptNilBool(*value)
}

func SafeOptFloat64(value *float64) gcpgenserver.OptFloat64 {
	if value == nil {
		return gcpgenserver.OptFloat64{}
	}
	return gcpgenserver.NewOptFloat64(*value)
}

// _getPairedRegionURI retrieves the URI of the paired region for the given region from a predefined in the configmap.
// Returns an error if the paired regions are not defined or the region is not found in the mapping.
func _getPairedRegionURI(region string) (string, error) {
	if PairedRegions == "" {
		return "", errors.New("paired regions not defined for this region")
	}
	sMap, err := ConvertStringToMap(PairedRegions)
	if err != nil {
		return "", err
	}

	uri, ok := sMap[region]
	if !ok {
		return "", errors.New("region not found in paired regions list")
	}
	return uri, nil
}

// _convertStringToMap converts a JSON-formatted string into a map[string]string.
// It expects the input string to be a valid JSON object with string keys and values.
// Returns an error if the JSON is invalid or cannot be unmarshalled into the map.
func _convertStringToMap(s string) (map[string]string, error) {
	var mapSlice map[string]string
	sMapBytes := []byte(s)
	err := json.Unmarshal(sMapBytes, &mapSlice)
	if err != nil {
		return nil, errors.New("error when unmarshalling response")
	}
	return mapSlice, nil
}

func RemovePrefix(str string, prefix string) string {
	if strings.HasPrefix(str, prefix) {
		return strings.TrimPrefix(str, prefix)
	}
	return str
}

func GetTimeNow() time.Time {
	return time.Now()
}

func GetCoRelationIDFromContext(ctx context.Context) string {
	if header, ok := ctx.Value(middleware.CorrelationContextKey).(http.Header); ok {
		return header.Get(string(middleware.CorrelationIDName))
	} else if fields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if _, ok := fields[string(middleware.RequestCorrelationID)]; !ok {
			// If the correlation ID is not present in the fields, generate a new one
			correlationID := RandomUUID()
			fields[string(middleware.RequestCorrelationID)] = correlationID
			return correlationID
		}

		return fields[string(middleware.RequestCorrelationID)].(string)
	}
	return ""
}
