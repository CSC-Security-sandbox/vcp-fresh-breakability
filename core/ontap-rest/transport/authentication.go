package transport

import (
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// NewAuthenticationRoundTripper creates a new AuthenticationRoundTripper
func NewAuthenticationRoundTripper(roundTripper http.RoundTripper, username string, password log.Secret, useCert bool) *AuthenticationRoundTripper {
	return &AuthenticationRoundTripper{
		roundTripper: roundTripper,
		username:     username,
		password:     password,
		useCert:      useCert,
	}
}

// AuthenticationRoundTripper adds authentication to a request
type AuthenticationRoundTripper struct {
	username     string
	password     log.Secret
	useCert      bool
	roundTripper http.RoundTripper
}

var (
	ontapRestOAuthEnabled = env.GetBool("ONTAP_REST_OAUTH_ENABLED", false)
	token                 log.Secret
)

// RoundTrip performs the round trip for this RoundTripper
func (prt *AuthenticationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if prt.useCert {
		req.Header.Add("Accept", "application/json")
		return prt.roundTripper.RoundTrip(req)
	}

	if ontapRestOAuthEnabled {
		req.Header.Add("Authorization", "Bearer "+string(token))
		return prt.roundTripper.RoundTrip(req)
	}

	req.SetBasicAuth(prt.username, string(prt.password))
	return prt.roundTripper.RoundTrip(req)
}
