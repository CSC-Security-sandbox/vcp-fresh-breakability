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

type CreateEMSEventForwardingParams struct {
	DestinationName      string
	DestinationIP        string
	DestinationPort      int64
	Transport            string // "tcp-unencrypted", "udp_unencrypted", "tcp_encrypted"
	TimestampFormat      string // "rfc-3164", "rfc-5424", "no-override"
	MessageFormat        string // "legacy-netapp", "rfc-5424"
	FilterName           string
	Severities           []string // "EMERGENCY", "ALERT", "ERROR", "NOTICE", "INFORMATIONAL"
}

type EMSEventDestination struct {
	Name    string
	Type    string
	Syslog  *EMSEventDestinationSyslog
}

type EMSEventDestinationSyslog struct {
	Host              string
	Port              int64
	Transport         string
	TimestampFormat   string
	MessageFormat     string
}
