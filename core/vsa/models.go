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

type ProviderResponse struct {
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

type CreateVolumeParams struct {
	VolumeName    string
	SvmName       string
	AggregateName string
	Size          int64
	VolumeType    string
}

type IgroupCreateParams struct {
	IgroupName string
	SvmName    string
	OsType     string
	Initiator  []string
}

type LunCreateParams struct {
	LunName    string
	SvmName    string
	OsType     string
	VolumeName string
	Size       int64
}

type LunMapCreateParams struct {
	LunName    string
	SvmName    string
	IGroupName []string
	LunNumber  int
}
