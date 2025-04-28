package vsa

type ProviderDetails struct {
	IPAddress          string
	UserName           string
	Password           string
	Port               *int
	UseHTTPS           bool
	Protocol           string
	InsecureSkipVerify bool
}

type CreateSvmParams struct {
	Name      string
	Protocols Protocols
}

type Protocols struct {
	EnableIscsi bool
}

type Svm struct {
	Name         string
	ExternalUUID string
}

type CreateLifParams struct {
	Name      string
	SvmName   string
	IpAddress string
	NodeName  string
	HomePort  string
}

type Lif struct {
	Name         string
	ExternalUUID string
	IPAddress    string
	SubnetMask   string
}

type CreateNetworkIPRouteParams struct {
	SvmName string
	Gateway string
}

type Node struct {
	Name         string
	State        string
	ExternalUUID string
}

type Aggregate struct {
	Name  string
	State string
}
