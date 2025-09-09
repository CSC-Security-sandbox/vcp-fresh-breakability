package common

import (
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func Helper() string {
	return "Hello"
}

// GetBoolOrDefault safely extracts a boolean value from an OptBool, returning the default if not set
func GetBoolOrDefault(opt gcpgenserver.OptBool, defaultValue bool) bool {
	if opt.Set {
		return opt.Value
	}
	return defaultValue
}
