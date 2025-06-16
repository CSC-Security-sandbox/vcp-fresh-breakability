package models

// Pool describes a pool in the cloud volume model
type Pool struct {
	BaseModel
	Name                    string
	Description             string
	State                   string
	StateDetails            string
	ServiceLevel            string
	SizeInBytes             uint64
	AccountName             string
	VendorID                string
	Region                  string
	Zone                    string
	TotalThroughputMibps    float64
	UtilizedThroughputMibps float64
	Tags                    string
	AllowAutoTiering        bool
	HotTierSizeInBytes      uint64
	EnableHotTierAutoResize bool
	VendorSubNetID          string
	QosType                 string
	PoolAttributes          *PoolAttributes
	ClusterAttributes       *ClusterAttributes
	CustomPerformanceParams *CustomPerformanceParams
	AutoTierBucketName      string
	SaAccountID             string
}

// PoolAttributes describes the attributes of a pool model
type PoolAttributes struct {
	Events          string
	Features        string
	PrimaryZone     string
	SecondaryZone   string
	AllocatedBytes  float64
	NumberOfVolumes int64
}

// ClusterAttributes describes the attributes of a cluster model
type ClusterAttributes struct {
	ExternalName      string
	OntapVersion      string
	InstanceType      string
	ExternalIpAddress string
	InternalIpAddress string
	InterClusterLifs  []string
}

// CustomPerformanceParams is used to specify the custom performance parameters for a pool
type CustomPerformanceParams struct {
	Enabled    bool
	Throughput float64
	Iops       int64
}
