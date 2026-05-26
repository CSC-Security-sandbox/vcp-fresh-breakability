package oci

import "fmt"

// ConfigParseError is returned when the OCI VMRS YAML cannot be read or
// fails YAML decoding.
type ConfigParseError struct {
	Message string
	Path    string
}

func (e *ConfigParseError) Error() string {
	return fmt.Sprintf("[vmrs/oci] ConfigParseError: %s (path: %s)", e.Message, e.Path)
}

// InvalidConfigError is returned when the parsed config is structurally
// valid YAML but doesn't meet semantic requirements (malformed keys,
// non-positive capacities, empty flex tiers, etc.).
type InvalidConfigError struct {
	Message string
}

func (e *InvalidConfigError) Error() string {
	return fmt.Sprintf("[vmrs/oci] InvalidConfigError: %s", e.Message)
}

// NoFeasibleSelectionError is returned when the customer request cannot be
// satisfied by any (flex, VPU) combination — either because the requested
// throughput exceeds the catalogue or the requested capacity is below
// every VPU floor in the chosen flex. DesiredIOPS is intentionally NOT a
// failure cause today: the selector ignores it for filtering (see
// SingleVMSelector's algorithm doc); when IOPS-based filtering is
// re-introduced, "explicit IOPS target can't be met" will become a third
// cause and this comment must be updated alongside the selector.
type NoFeasibleSelectionError struct {
	Message string
	Request CustomerRequest
}

func (e *NoFeasibleSelectionError) Error() string {
	return fmt.Sprintf("[vmrs/oci] NoFeasibleSelectionError: %s (request: %+v)", e.Message, e.Request)
}
