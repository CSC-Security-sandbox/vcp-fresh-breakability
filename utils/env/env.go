package env

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/errors"
	"github.com/goccy/go-yaml"
)

const (
	Admin    = "admin"
	LocalEnv = "local"
)

func init() {
	if GetBool("LOCAL_ENV", false) {
		volumeEnvPath = "./config/config.yaml"
	}
	LogLevel = GetString("LOGGER_LEVEL", "info")
	LoggerType = GetString("LOGGER_TYPE", "slog")
	SlogHandlerType = GetString("SLOG_HANDLER_TYPE", "json")
	ExporterType = GetString("EXPORTER_TYPE", "stdout")
	AddSource = GetBool("ADD_LOG_SOURCE_FILE", false)
	ServiceName = GetString("OTEL_SERVICE_NAME", "VCP-VSA")
	OtelGoogleProjectID = GetString("OTEL_GOOGLE_PROJECT_ID", "")
}

func getFromVolumeOrEnv(key string) (string, error) {
	// 1. Try from localConfig map if useVolumeEnv is true
	if useVolumeEnv {
		if val, ok := localConfig[key]; ok && val != "" {
			return val, nil
		}
		// 2. If not already loaded, try to load from volume file (YAML)
		data, err := os.ReadFile(volumeEnvPath)
		if err == nil {
			tempConfig := map[string]string{}
			if yamlErr := yaml.Unmarshal(data, &tempConfig); yamlErr == nil {
				// Cache loaded config
				localConfig = tempConfig
				if val, ok := localConfig[key]; ok && val != "" {
					return val, nil
				}
			}
		}
	}
	// 3. Fallback to os.Getenv
	if val, found := os.LookupEnv(key); found {
		return val, nil
	}
	// 4. Not found anywhere
	return "", fmt.Errorf("environment variable or config key '%s' not found", key)
}

// IsEnvSet returns true if the specified environment variable is set, false otherwise.
func IsEnvSet(key string) bool {
	_, found := os.LookupEnv(key)
	return found
}

// GetStringFromEnvIfNil returns the specified string if non-nil.
// If nil, returns the specified environment variable if set.
// If the environment variable is not set, returns the specified default.
func GetStringFromEnvIfNil(val *string, key, def string) *string {
	if val == nil {
		v := GetString(key, def)
		val = &v
	}
	return val
}

// GetStrings returns all the environment variables that match the pattern given by regex.
func GetStrings(regex string) (variables map[string]string, err error) {
	variables = map[string]string{}
	re, err := regexp.Compile(regex)
	if err != nil {
		return variables, err
	}
	for _, element := range os.Environ() {
		variable := strings.SplitN(element, "=", 2) // Split only on the first "="
		varName := variable[0]
		if re.MatchString(varName) {
			variables[varName] = variable[1]
		}
	}
	return variables, nil
}

// GetString returns the specified environment variable.
// If the environment variable is not set, returns the specified default.
func GetString(key, def string) string {
	if env, err := getFromVolumeOrEnv(key); err == nil {
		return env
	}
	return def
}

// GetIntFromEnvIfNil returns the specified integer if non-nil.
// If nil, returns the integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetIntFromEnvIfNil(val *int, key string, def int) *int {
	if val == nil {
		v := GetInt(key, def)
		val = &v
	}
	return val
}

// GetInt returns an integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetInt(key string, def int) int {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.Atoi(val); err1 == nil {
			return intVal
		}
	}
	return def
}

// GetIntNotNegative returns an integer representation of the specified environment variable.
// If the environment variable is not set, is not valid, or is negative, returns the specified default.
func GetIntNotNegative(key string, def int) int {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.Atoi(val); err1 == nil && intVal >= 0 {
			return intVal
		}
	}
	return def
}

// GetUintFromEnvIfNil returns the specified unsigned integer if non-nil.
// If nil, returns the unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUintFromEnvIfNil(val *uint, key string, def uint) *uint {
	if val == nil {
		v := GetUint(key, def)
		val = &v
	}
	return val
}

// GetUint returns an unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint(key string, def uint) uint {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.ParseUint(val, 10, 32); err1 == nil {
			return uint(intVal)
		}
	}
	return def
}

// GetUint16 returns a 16-bit unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint16(key string, def uint16) uint16 {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.ParseUint(val, 10, 16); err1 == nil {
			return uint16(intVal)
		}
	}
	return def
}

// GetInt64FromEnvIfNil returns the specified 64-bit integer if non-nil.
// If nil, returns the 64-bit integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetInt64FromEnvIfNil(val *int64, key string, def int64) *int64 {
	if val == nil {
		v := GetInt64(key, def)
		val = &v
	}
	return val
}

// GetInt64 returns a 64-bit integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetInt64(key string, def int64) int64 {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.ParseInt(val, 10, 64); err1 == nil {
			return intVal
		}
	}
	return def
}

// GetUint64FromEnvIfNil returns the specified 64-bit unsigned integer if non-nil.
// If nil, returns the 64-bit unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint64FromEnvIfNil(val *uint64, key string, def uint64) *uint64 {
	if val == nil {
		v := GetUint64(key, def)
		val = &v
	}
	return val
}

// GetUint64 returns a 64-bit unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint64(key string, def uint64) uint64 {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.ParseUint(val, 10, 64); err1 == nil {
			return intVal
		}
	}
	return def
}

// GetFloat64FromEnvIfNil returns the specified 64-bit floating-point number if non-nil.
// If nil, returns the 64-bit floating-point number representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetFloat64FromEnvIfNil(val *float64, key string, def float64) *float64 {
	if val == nil {
		v := GetFloat64(key, def)
		val = &v
	}
	return val
}

// GetFloat64 returns a 64-bit floating-point number representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetFloat64(key string, def float64) float64 {
	if val, err := getFromVolumeOrEnv(key); err == nil {
		if intVal, err1 := strconv.ParseFloat(val, 64); err1 == nil {
			return intVal
		}
	}
	return def
}

// GetBoolFromEnvIfNil returns the specified boolean if non-nil.
// If nil, returns the boolean representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetBoolFromEnvIfNil(val *bool, key string, def bool) *bool {
	if val == nil {
		v := GetBool(key, def)
		val = &v
	}
	return val
}

// GetBool returns a boolean representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetBool(key string, def bool) bool {
	val, err := getFromVolumeOrEnv(key)
	if err != nil {
		return def
	}
	env := strings.ToLower(val)
	if env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			return val != 0
		}
		if strings.HasPrefix(env, "true") || strings.HasPrefix(env, "yes") {
			return true
		}
		if strings.HasPrefix(env, "false") || strings.HasPrefix(env, "no") {
			return false
		}
	}
	return def
}

func GetDuration(key string, def time.Duration) time.Duration {
	val, err := getFromVolumeOrEnv(key)
	if err != nil {
		return def
	}
	if val != "" {
		if durationVal, parseErr := time.ParseDuration(val); parseErr == nil {
			return durationVal
		}
	}

	return def
}

const (
	USERNAME_PWD         = 0 // Username/Password authentication
	USERNAME_PWD_SEC_MGR = 1 // Username/Password authentication with secret manager
	USER_CERTIFICATE     = 2 // Certificate authentication
	VCP_ADMIN            = "vcp_admin"
)

var (
	Env                     = GetString("ENV", "")
	AuthType                = GetInt("VSA_AUTH_TYPE", USERNAME_PWD) // 0 for username/password, 1 for username/password in secret manager and 2 for certificate authentication
	Region                  = GetString("LOCAL_REGION", "")
	CaName                  = GetString("CA_NAME", "")
	CaPoolName              = GetString("CA_POOL_NAME", "")
	CaPoolDeployedProjectID = GetString("CA_POOL_DEPLOYED_PROJECT_ID", "")
	SecretManagerProjectID  = GetString("SECRET_MANAGER_PROJECT_ID", "")
	VsaDeployedDnsName      = GetString("VSA_DEPLOYED_DNS_NAME", "")
	VsaManagedZone          = GetString("VSA_MANAGED_ZONE", "")
	CertificateLifetime     = GetString("CERTIFICATE_LIFETIME", "94608000s") // Default to 3 years
	NodePassword            = GetString("VSA_NODE_PASSWORD", "")
	CloudDNSCacheTTL        = GetInt64("CLOUD_DNS_CACHE_TTL", 300) // Default to 300 seconds
	PrivateKeyBits          = GetInt("PRIVATE_KEY_BITS", 4096)     // Default to 4096 bits for RSA keys

	MgmtFirewallSourceRanges = GetString("MGMT_FIREWALL_SOURCE_RANGES", "")
	RsmFirewallSourceRanges  = GetString("RSM_FIREWALL_SOURCE_RANGES", "")
	IcFirewallSourceRanges   = GetString("IC_FIREWALL_SOURCE_RANGES", "")
	DataFirewallSourceRanges = GetString("DATA_FIREWALL_SOURCE_RANGES", "")

	MgmtRegionalNatIP = GetString("MGMT_REGIONAL_NAT_IP", "")

	MgmtNetworkIpRange = GetString("MGMT_NETWORK_IP_RANGE", "198.18.0.0/20")
	RsmNetworkIpRange  = GetString("RSM_NETWORK_IP_RANGE", "198.18.16.0/20")
	IcNetworkIpRange   = GetString("IC_NETWORK_IP_RANGE", "198.18.32.0/20")

	WorkerTaskQueue = GetString("WORKER_TASK_QUEUE", "customer-workflows")

	NLFLicenseSecretPath = GetString("NLF_LICENSE_SECRET_PATH", "")
	// Get current VCP version from environment
	CurrentOntapVersionDetails = GetString("ONTAP_VERSION_DETAILS", "9.17.1P1")
	// ONTAP Image Version Match Configuration
	SkipOntapImageVersionMatch = GetBool("SKIP_ONTAP_IMAGE_VERSION_MATCH", false)

	ExpertModeUser = GetString("EXPERT_MODE_USER", "gcnvadmin")
)

// networkEnvVariables holds the environment variables related to firewall of network configuration for source ranges
var NetworkSourceRanges = map[string]string{
	"MGMT_FIREWALL_SOURCE_RANGES": MgmtFirewallSourceRanges,
	"RSM_FIREWALL_SOURCE_RANGES":  RsmFirewallSourceRanges,
	"IC_FIREWALL_SOURCE_RANGES":   IcFirewallSourceRanges,
	"DATA_FIREWALL_SOURCE_RANGES": DataFirewallSourceRanges,
	"MGMT_REGIONAL_NAT_IP":        MgmtRegionalNatIP,
}

// networkEnvVariables holds the environment variables related to subnet in network configuration for ip ranges
var NetworkIpRanges = map[string]string{
	"MGMT_NETWORK_IP_RANGE": MgmtNetworkIpRange,
	"RSM_NETWORK_IP_RANGE":  RsmNetworkIpRange,
	"IC_NETWORK_IP_RANGE":   IcNetworkIpRange,
}

func ValidateEnvironmentVariables() error {
	if Region == "" {
		return errors.New(500, "LOCAL_REGION must be set for authentication")
	}
	if CaName == "" {
		return errors.New(500, "CA_NAME must be set for authentication")
	}
	if CaPoolName == "" {
		return errors.New(500, "CA_POOL_NAME must be set for authentication")
	}
	if CaPoolDeployedProjectID == "" {
		return errors.New(500, "CA_POOL_DEPLOYED_PROJECT_ID must be set for authentication")
	}
	if SecretManagerProjectID == "" {
		return errors.New(500, "SECRET_MANAGER_PROJECT_ID must be set for authentication")
	}
	if VsaDeployedDnsName == "" {
		return errors.New(500, "VSA_DEPLOYED_DNS_NAME must be set for authentication")
	}
	if VsaManagedZone == "" {
		return errors.New(500, "VSA_MANAGED_ZONE must be set for authentication")
	}
	if CertificateLifetime == "" {
		return errors.New(500, "CERTIFICATE_LIFETIME must be set for authentication")
	}
	if CloudDNSCacheTTL == 0 {
		return errors.New(500, "CLOUD_DNS_CACHE_TTL must be set for authentication")
	}
	if NodePassword == "" {
		return errors.New(500, "VSA_NODE_PASSWORD must be set for authentication")
	}
	if PrivateKeyBits == 0 {
		return errors.New(500, "PRIVATE_KEY_BITS must be set for authentication")
	}
	if WorkerTaskQueue == "" {
		return errors.New(500, "WORKER_TASK_QUEUE must be set for worker configuration")
	}
	return validateNetworkEnvVariables()
}

func validateNetworkEnvVariables() error {
	for envVariableName, envVariableValue := range NetworkSourceRanges {
		if err := validateSourceRanges(envVariableName, envVariableValue); err != nil {
			return err
		}
	}
	for envVariableName, envVariableValue := range NetworkIpRanges {
		basicErrorString := " must be set for subnet for VSA deployment"
		if err := validateIpRange(envVariableValue, basicErrorString, envVariableName); err != nil {
			return err
		}
	}
	return nil
}

func validateSourceRanges(envVariableName, sourceRanges string) error {
	basicErrorString := " must be set for firewall for VSA deployment"
	if sourceRanges == "" {
		return errors.New(500, "%s"+basicErrorString, envVariableName)
	}
	ranges := strings.Split(sourceRanges, ",")
	for _, rangeStr := range ranges {
		if err := validateIpRange(rangeStr, basicErrorString, envVariableName); err != nil {
			return err
		}
	}
	return nil
}

func validateIpRange(ipRange, basicErrorString, envVariableName string) error {
	if ipRange == "" {
		return errors.New(500, basicErrorString, envVariableName)
	}
	if strings.Contains(ipRange, " ") {
		return errors.New(500, "%s%s. Can't contain empty spaces in: %s", envVariableName, basicErrorString, ipRange)
	}
	// Validate CIDR format using net.ParseCIDR
	if _, _, err := net.ParseCIDR(ipRange); err != nil {
		return errors.New(500, "%s%s. Invalid CIDR format in: %s", envVariableName, basicErrorString, ipRange)
	}
	return nil
}

var IsLocalEnv = isLocalEnv

func isLocalEnv() bool {
	return Env == LocalEnv
}
