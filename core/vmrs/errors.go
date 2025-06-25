// This file defines the error types used in the VMRS package.

package vmrs

import (
	"fmt"
)

// This error is returned when we are unable to parse the provided configuration, or a required configuration value is missing.
type ConfigParseError struct {
	// The error message.
	Message string
	// The path to the configuration file that caused the error.
	Path string
}

func (e *ConfigParseError) Error() string {
	return fmt.Sprintf("[vmrs] ConfigParseError: %s (path: %s)", e.Message, e.Path)
}

// This error is returned when we are unable to find a VM that satisfies the customer requested performance.
type NoSuitableVMError struct {
	// The error message.
	Message string
	// The customer requested performance that could not be satisfied.
	CustomerRequest CustomerRequestedPerformance
}
