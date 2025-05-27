package hyperscaler

type ComputeOperation struct {
	Name string

	// for compute operation
	Status   string
	Progress int64

	// for service networking operation
	Done          bool
	Response      []byte
	ErrorResponse string
}

type VPCNetwork struct {
	ProjectName string
	Name        string
	Subnetworks []string
}

type Subnet struct {
	ProjectName    string
	Name           string
	Region         *string
	Network        string
	IpCidrRange    string
	GatewayAddress string
}

type Firewall struct {
	ProjectName      string
	Name             string
	AllowedPortRules []string
	SourceRanges     []string
	VPCNetworkName   string
	Description      string
	Direction        string
	Priority         int64
}
