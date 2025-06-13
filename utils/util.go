package utils

import (
	"context"
	"crypto/rand"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-openapi/strfmt"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	localRegion                   = env.GetString("LOCAL_REGION", "local")
	PairedRegions                 = env.GetString("VCP_PAIRED_REGIONS", "")
	ParseRegionAndZone            = _parseRegionAndZone
	ParseAndValidateRegionAndZone = _parseAndValidateRegionAndZone
	GetPairedRegionURI            = _getPairedRegionURI
	ConvertStringToMap            = _convertStringToMap
	GetSignedCallbackToken        = _getSignedCallbackToken
	ConvertBytesToGib             = _convertBytesToGib
	authGetSignedAccessToken      = auth.GetSignedAccessToken
	sleep                         = _sleep
	exponentialBackOffErrors      = []int{429}
	maxExpBackOffDelay            = time.Duration(80) * time.Second
	jitterBase                    = time.Millisecond
	generateRandomString          = _generateRandomString
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
	region, zone, err := ParseRegionAndZone(locationID)
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
		return "", "", errors.New(fmt.Sprintf("parseProjectId failed for network : %s", network))
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

func generateJitter() time.Duration {
	return jitterBase * time.Duration(GenerateRandomInRange(100)+100) // [100, 200] ms jitter
}

func _getSignedCallbackToken() (string, error) {
	return authGetSignedAccessToken()
}

// _sleep is a helper function to sleep for a given duration according to the error type
func _sleep(retryDelay time.Duration, err error, attempt int) {
	// Retry in Exponential backoff way for error codes defined in exponentialBackOffErrors
	_, httpcode := err.(*errs.CustomError).GetHttpCode()
	if ContainsInt(exponentialBackOffErrors, httpcode) {
		var nextRetry int64 = 1 << attempt
		retryDelay = min(maxExpBackOffDelay, time.Duration(nextRetry)*retryDelay+generateJitter())
	}
	time.Sleep(retryDelay)
}

// RetrierOnCodes retries the function fn on specific HTTP error codes.
func RetrierOnCodes(logger log.Logger, fn func() (bool, error), retryCodes []int, maxRetries int, retryDelay time.Duration) {
	shouldSleep := false
	var err error
	for i := 0; i < maxRetries; i++ {
		if shouldSleep {
			sleep(retryDelay, err, i)
			logger.Debug("Retrying function", "attempt", i+1)
		}
		shouldSleep = true
		var stopRetry bool
		stopRetry, err = fn()
		if err != nil && !stopRetry {
			_, httpcode := err.(*errs.CustomError).GetHttpCode()
			if ContainsInt(retryCodes, httpcode) {
				logger.Errorf("Got an retryable error code while calling server %v: attemp %d", err, i+1)
				continue
			}
			innerErr := err.(*errs.CustomError).Unwrap()
			if innerErr != nil {
				if goerrors.Is(innerErr, syscall.ECONNREFUSED) || goerrors.Is(innerErr, syscall.ETIMEDOUT) {
					logger.Warnf("Got an error while calling server %v: attemp %d", err, i+1)
					continue
				}
				if neterror, ok := innerErr.(net.Error); ok && neterror.Timeout() {
					logger.Warnf("Got an timeout while calling server %v: attemp %d", err, i+1)
					continue
				}
			}
			break
		}
		return
	}
}

func _convertBytesToGib(bytes float64) int64 {
	gib := bytes / 1024 / 1024 / 1024

	return int64(gib)
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

func ConvertJsonToModel(jsonb []byte, model any) error {
	err := json.Unmarshal(jsonb, &model)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to unmarshal json: %v", err))
	}

	return nil
}

func GetRequestIDFromContext(ctx context.Context) string {
	if fields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if requestID, ok := fields[string(middleware.RequestID)]; ok {
			return requestID.(string)
		}
	}
	return ""
}

// GenerateResourceNames generates unique service account name, email, and bucket name
func GetResourcesNameForBackup(gcpRegion, tenantProjectRegion, tenantProjectNumber, backupVaultUUID string) (email, bucketName, serviceAccountId string, err error) {
	const maxServiceAccountLength = 30
	const maxBucketNameLength = 60

	// Generate a random string for uniqueness
	randCode, err := generateRandomString(6)
	if err != nil {
		return "", "", "", err
	}

	// Generate service account ID
	baseServiceAccountId := "vsa-backup-" + sliceRegionForServiceAccount(gcpRegion)
	if len(baseServiceAccountId)+len(randCode) > maxServiceAccountLength {
		baseServiceAccountId = baseServiceAccountId[:maxServiceAccountLength-len(randCode)]
	}
	serviceAccountId = baseServiceAccountId + randCode

	// Generate email
	email = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", serviceAccountId, tenantProjectNumber)

	// Generate bucket name
	baseBucketName := fmt.Sprintf("vsa-backup-%s", backupVaultUUID)
	if len(baseBucketName)+len(randCode) > maxBucketNameLength {
		baseBucketName = baseBucketName[:maxBucketNameLength-len(randCode)]
	}
	bucketName = baseBucketName + "-" + randCode

	return email, bucketName, serviceAccountId, nil
}

// sliceRegionForServiceAccount ensures the region part of the service account name is within limits
func sliceRegionForServiceAccount(region string) string {
	const maxRegionLength = 25
	if len(region) > maxRegionLength {
		return region[:maxRegionLength]
	}
	return region
}

// generateRandomString generates a random alphanumeric string of the given length
func _generateRandomString(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		result[i] = charset[n.Int64()]
	}
	return string(result), nil
}

func GetLunName(volumeName string) string {
	return "lun_" + volumeName
}

func IsTransitionalState(state string) bool {
	transitionalStates := map[string]struct{}{
		models.LifeCycleStateCreating: {},
		models.LifeCycleStateUpdating: {},
		models.LifeCycleStateDeleting: {},
	}
	_, exists := transitionalStates[state]
	return exists
}
