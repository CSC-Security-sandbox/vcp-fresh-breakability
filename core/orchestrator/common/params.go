package common

import (
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

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
	AllowAutoTiering        bool
	HotTierSizeInBytes      uint64
	EnableHotTierAutoResize bool
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

// DeletePoolParams describes parameters supplied to DeletePool
type DeletePoolParams struct {
	AccountName string
	PoolID      string
}

type SnapshotBaseParams struct {
	AccountName string
	VolumeID    string
}

type CreateSnapshotParams struct {
	SnapshotBaseParams
	Name            string
	Description     string
	IsAppConsistent bool
}

type GetSnapshotParams struct {
	SnapshotBaseParams
	SnapshotUUID string
}

type ClusterPeerParams struct {
	PeerAddresses       []string
	PeerName            string
	AccountName         string
	InterclusterLifList []string
	ExpiryTime          *time.Time
	GeneratePassphrase  bool
	Passphrase          *string
	UUID                string
}

type ClusterPeer struct {
	UUID                string
	PeerAddresses       []string
	PeerClusterName     string
	Availability        string
	AuthenticationState string
	Passphrase          *log.Secret
	IPSpace             string
	ExternalUUID        string
	HostUUID            string
	AccountUUID         string
	AccountName         string
	ExpiryTime          *strfmt.DateTime
}
