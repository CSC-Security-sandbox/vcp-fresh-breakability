package common

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"

// CreatePoolParams describes parameters supplied to CreatingPool
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
	Gateway               string
}

// HostParams FixMe: remove this once HostGroup table is created
type HostParams struct {
	HostName string
	HostIQNs []string
	OsType   string
}

// CreateVolumeParams describes parameters supplied to CreatePool
type CreateVolumeParams struct {
	AccountName     string
	Region          string
	Name            string
	Description     string
	Network         string
	PoolID          string
	VendorID        string
	CreationToken   string
	DisplayName     string
	QuotaInBytes    uint64
	Protocols       []string
	BlockProperties *models.BlockProperties
}

type CreateLunMapParams struct {
	LunName   string
	SvmName   string
	HostNames []string
}
