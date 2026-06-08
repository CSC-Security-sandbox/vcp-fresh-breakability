package datamodel

import (
	"fmt"
	"strings"
)

const (
	ExternalClusterProtocolHTTP          = "HTTP"
	ExternalClusterProtocolHTTPS         = "HTTPS"
	ExternalClusterProtocolInsecureHTTPS = "INSECURE_HTTPS"
	ExternalClusterDefaultProtocol       = ExternalClusterProtocolInsecureHTTPS
	ExternalClusterDefaultPortHTTP       = 80
	ExternalClusterDefaultPortHTTPS      = 443
)

var externalClusterHostDefaultPorts = map[string]int{
	ExternalClusterProtocolHTTP:          ExternalClusterDefaultPortHTTP,
	ExternalClusterProtocolHTTPS:         ExternalClusterDefaultPortHTTPS,
	ExternalClusterProtocolInsecureHTTPS: ExternalClusterDefaultPortHTTPS,
}

// NormalizeExternalClusterProtocolAndPort validates protocol, applies defaults, and returns values to persist.
// When port is unset (<= 0), the default port for the resolved protocol is applied; only an invalid protocol returns an error.
func NormalizeExternalClusterProtocolAndPort(protocol string, port int) (string, int, error) {
	p := strings.TrimSpace(strings.ToUpper(protocol))
	if p == "" {
		p = ExternalClusterDefaultProtocol
	}
	defaultPort, ok := externalClusterHostDefaultPorts[p]
	if !ok {
		return "", 0, fmt.Errorf("invalid protocol %q: must be HTTP, HTTPS, or INSECURE_HTTPS", protocol)
	}
	if port <= 0 {
		port = defaultPort
	}
	return p, port, nil
}
