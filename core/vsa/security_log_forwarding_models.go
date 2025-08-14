package vsa

type CreateSecurityLogForwardingParams struct {
	Address      *string
	Port         *int64
	Protocol     *string
	Facility     *string
	VerifyServer *bool
}

type GetSecurityLogForwardingParams struct {
	Address string
	Port    int64
}

type SecurityLogForwardingResponse struct {
	Address  *string
	Port     *int64
	Protocol *string
}
type CreateSecurityLogForwardingResponse struct {
	ProviderResponse
}

type GetSecurityLogForwardingResponse struct {
	ProviderResponse
}
