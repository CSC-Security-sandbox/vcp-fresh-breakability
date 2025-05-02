package common

// CreatePoolParams describes parameters supplied to CreatePool
type CreatePoolParams struct {
	AccountName             string
	Region                  string
	Name                    string
	Description             string
	VendorID                string
	ServiceLevel            string
	QosType                 string
	Tags                    string
	SizeInBytes             uint64
	CoolAccess              bool
	CurrentZone             string
	VendorSubNetID          string
	Zones                   []string
	CustomThroughputMibps   uint64
	HostUUID                string
	CustomPerformanceParams *CustomPerformanceParams
}

// CustomPerformanceParams is used to specify the custom performance parameters for a pool
type CustomPerformanceParams struct {
	Enabled    bool
	Throughput float64
	Iops       int64
}

type TenancyInfo struct {
	RegionalTenantProject string
	Network               string
	SubnetworkName        string
	SnHostProject         string
}
