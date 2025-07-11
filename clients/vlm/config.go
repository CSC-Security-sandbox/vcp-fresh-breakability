// IMPORTANT: This is the VLM workflow datamodel file.
// We shouldn't edit this from the VCP side unless a newer version is shared by the VLM team.

package vlm

const (
	GCPCloud string = "gcp"

	DeploymentTypeSharedHA    string = "shared_ha"
	DeploymentTypeNonSharedHA string = "non_shared_ha"
)

// VLMWorkflowName is the name of the workflow
const (
	CreateVSAClusterDeploymentWorkflowName = "vlm.CreateVSAClusterDeploymentWorkflow"
	CreateVSASVMWorkflowName               = "vlm.CreateVSASVMWorkflow"
	DeleteVSAClusterDeploymentWorkflowName = "vlm.DeleteVSAClusterDeploymentWorkflow"
	UpdateVSAClusterDeploymentWorkflowName = "vlm.UpdateVSAClusterDeploymentWorkflow"
)

type VLMConfig struct {
	Cloud      CloudConfig          `json:"cloud"`
	Deployment DeploymentConfig     `json:"deployment"`
	VsaCluster VsaClusterConfig     `json:"vsa_cluster"`
	DataAggr   []DataAggrConfig     `json:"data_aggr"`
	Svm        map[string]SvmConfig `json:"svm"`
}

type CloudConfig struct {
	HAPairs []HAPair `json:"ha_pair"` // sde need not fill this
}

type DeploymentConfig struct {
	Provider           string      `json:"provider"`             // (gcp/aws/azure)
	DeploymentID       string      `json:"deployment_id"`        // Added
	SerialNumberPrefix string      `json:"serial_number_prefix"` // used to generate serial number for all the VMs
	Region             string      `json:"region" `              // Added
	Zone               ZoneInfo    `json:"zone"`                 // Added
	Images             ImageConfig `json:"images"`               // Added

	Tags         string            `json:"tags"`           // Comma separated list of tags to be attached for the VMs created by the deployment
	Labels       map[string]string `json:"labels"`         // List of labels to attach to resources
	UserBootargs string            `json:"user_boot_args"` // The input is a list of key-value pairs with semicolons as delimiters.

	DeploymentType       string `json:"deployment_type"`        // SingeNode or ShareHA or NonShareHA
	NumHAPair            int    `json:"num_ha_pair"`            // Number of HA pairs to be created
	VSAInstanceType      string `json:"vsa_instance_type"`      // rename to VSAInstanceType
	MediatorInstanceType string `json:"mediator_instance_type"` // rename to MediatorInstanceType
	DataDiskType         string `json:"data_disk_type"`         // Move to GCP config ?
	SystemDiskType       string `json:"system_disk_type"`       // Move to GCP config ?
	DataDiskCount        int    `json:"data_disk_count"`        // Number of data disks to be created

	// TODO: check if zone wise netconfig is required
	NetConfig map[VSALIFType]NetworkConfig `json:"net_config"` // Network configuration for the deployment
	GCPConfig GCPConfig                    `json:"gcpconfig"`  // GCP specific configuration
	SPConfig  SPConfig                     `json:"spconfig"`   // Storagepool specific configuration
	DevFlags  DevFlags                     `json:"dev_flags"`  // Development flags
}

type DevFlags struct {
	ExtIPForNodeMgmt      bool `json:"ext_ip_for_node_mgmt"`     // External IP for node management
	DisableDataNicTier1   bool `json:"disable_data_nic_tier1"`   // Disable Tier 1 for data NIC
	EnablePremiumTierData bool `json:"enable_premium_tier_data"` // Enable Premium Tier for data NIC
	DisableGVNIC          bool
	EnableNfsV3Support    bool `json:"enable_nfs_v3_support"` // Enable NFS v3 support
}

type GCPConfig struct {
	ProjectID              string `json:"project_id"`                // GCP project ID
	ImageProjectID         string `json:"image_project_id"`          // Image project ID for GCP        `json:"gcp_image_config"`      // GCP image configuration
	MediatorImageProjectID string `json:"mediator_image_project_id"` // Mediator image project ID for GCP
	ServiceAccountEmail    string `json:"service_account_email"`     // Service account email for GCP
	BucketName             string `json:"bucket_name"`               // GCP bucket name for storing data
}

type GCPNetworkConfig struct {
	SubnetProjectID string `json:"subnet_project_id"` // Project ID for the subnet
}

type ZoneInfo struct {
	Zone1        string `json:"zone1"`
	Zone2        string `json:"zone2"`
	MediatorZone string `json:"mediator_zone"`
}

type ImageConfig struct {
	VSAImageName      string `json:"vsa_image_name"`      // Image name for VSA
	MediatorImageName string `json:"mediator_image_name"` // Image name for Mediator
}

type SPConfig struct {
	Size       string `json:"size"` // Size of the storage pool in GB
	IOps       int64  `json:"iops"` // IOPS for the storage pool
	Throughput int64  `json:"tput"` // Throughput for the storage pool
}

type OntapCertificate struct {
	Certificate             string   `json:"certificate"`              // Certificate for ONTAP
	PrivateKey              string   `json:"private_key"`              // Private key for ONTAP
	InterMediateCertificate []string `json:"intermediate_certificate"` // Intermediate certificate for ONTAP
	CommonName              string   `json:"common_name"`              // Common name for ONTAP
	CaCertificate           string   `json:"ca_certificate"`           // CA certificate for ONTAP
}

// OntapCredentials holds the credentials for ONTAP, including the certificate and username/password.
type OntapCredentials struct {
	Certificate   OntapCertificate `json:"certificate"`    // Certificate for ONTAP
	AdminPassword string           `json:"admin_password"` // Password for ONTAP
}

type HAPair struct {
	VM1      VMConfig `json:"vm1"`
	VM2      VMConfig `json:"vm2"`
	Mediator VMConfig `json:"mediator"` // Added
}

type VMConfig struct {
	Region       string                   `json:"region"`    // Added
	Zone         string                   `json:"zone"`      // Added
	Name         string                   `json:"name"`      // Name of the VM
	HostName     string                   `json:"host_name"` // Available during cluster create.
	SerialNumber string                   `json:"serial_number"`
	NodeIndex    int                      `json:"node_index"` // Added
	IsMediator   bool                     `json:"is_mediator"`
	SystemLIFs   map[VSALIFType]LIFConfig `json:"lifs"`         // List of IPs for the VM
	SystemDisks  []DiskConfig             `json:"system_disks"` // List of system disks for the VM
	DataDisks    []DiskConfig             `json:"data_disks"`   // List of data disks for the VM
	// TODO: Add resource status
}

type OntapDiskType string

const (
	OntapDiskBoot     OntapDiskType = "boot"
	OntapDiskNvram    OntapDiskType = "nvram"
	OntapDiskCore     OntapDiskType = "core"
	OntapDiskRoot     OntapDiskType = "root"
	OntapDiskRootCopy OntapDiskType = "rootcopy"
	OntapDiskData     OntapDiskType = "data"
)

type VSALIFType string

const (
	LIFTypeNodeMgmt     VSALIFType = "nodemgmt"
	LIFTypeIC           VSALIFType = "ic"
	LIFTypeCluster      VSALIFType = "clus"
	LIFTypeInterCluster VSALIFType = "intercluster"
	LIFTypeRSM          VSALIFType = "rsm"
	LIFTypeIscsi        VSALIFType = "iscsi"
	LIFTypeNfs          VSALIFType = "nfs"
	LIFTypeMediator     VSALIFType = "mediator"
	// TODO: Remove this workaround once the VLM worker image is updated to use the correct LIF type ("iscsi").
	// Currently, the received data uses "default-data-iscsi" instead of the expected "iscsi" as per the data model.
	DefaultLIFTypeIscsi VSALIFType = "default-data-iscsi"
)

type LIFConfig struct {
	Name          string        `json:"lif_name"`       // Name of the LIF
	VSALIFType    VSALIFType    `json:"vsa_ip_type"`    // Type of VSA LIF
	IP            string        `json:"ip"`             // IP for the LIF
	Uuid          string        `json:"lif_uuid"`       // UUID of the LIF
	NetworkConfig NetworkConfig `json:"network_config"` // Network configuration for the LIF
	Region        string        `json:"region"`         // Region for the LIF
	HomeNode      string        `json:"home_node"`      // Home node for the LIF
}

type NetworkConfig struct {
	Subnet           string           `json:"subnet"`  // Subnet for the NIC
	VPC              string           `json:"vpc"`     // VPC for the NIC
	Gateway          string           `json:"gateway"` // Gateway for the subnet
	GCPNetworkConfig GCPNetworkConfig `json:"gcp_network_config"`
}

type VsaClusterConfig struct {
	ClusterMgmtNetmask  string `json:"cluster_mgmt_netmask"`
	ClusterMgmtGateway  string `json:"cluster_mgmt_gateway"`
	CustBroadcastDomain string `json:"cust_broadcast_domain"`
	CustIPSpace         string `json:"cust_ip_space"`
	ObjectStoreName     string `json:"object_store_name"`
}

type SvmLIFConfigs map[VSALIFType][]LIFConfig

type SvmConfig struct {
	Svmname string `json:"svm_name"`
	Svmuuid string `json:"svm_uuid"`
	// Map of Lifs for SVM. Can be either nas or iscsi.
	// SVM can have multiple iSCSI lifs, hence it is maintained as a slice of LIFConfigs
	SVMLIFs SvmLIFConfigs `json:"svm_lifs"`
}

type DataAggrConfig struct {
	Name     string `json:"name"`
	Aggruuid string `json:"uuid"`
	Size     uint64 `json:"size"`      // in GB ?
	HomeNode string `json:"home_node"` // Home node for the aggregate
}

type DiskConfig struct {
	Name           string            `json:"name"`
	Size           uint64            `json:"size"`        // in GB
	AccessMode     string            `json:"access_mode"` // READ_WRITE or READ_WRITE_MANY
	Type           string            `json:"type"`        // Disk type (e.g., pd-standard, pd-ssd)
	DiskIops       int64             `json:"disk_iops"`
	DiskThroughput int64             `json:"disk_throughput"`
	ResourceStatus string            `json:"resource_status"` // Status of the resource
	Zone           string            `json:"zone"`            // Zone for the disk
	Labels         map[string]string `json:"labels"`
	// TODO: Add resource status
}

type GCPDiskConfig struct {
	DeviceName string `json:"device_name,omitempty"` // Device name for the disk (only when attached)
	// Add other GCP-specific fields here if needed
}

type CreateSVMRequest struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`
	Name             string           `json:"name"` // SVM name
	DnsDomains       string           `json:"dns_ip"`
	NameServers      string           `json:"servers"`           // List of servers
	OntapCredentials OntapCredentials `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
}

type CreateSVMResponse struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}

type UpdateVSAClusterDeploymentRequest struct {
	VLMConfig        VLMConfig          `json:"vlm_config"`        // VLM configuration
	NumHAPair        int                `json:"num_ha_pair"`       // Number of HA pairs to be created
	SPConfig         SPConfig           `json:"spconfig"`          // Storagepool specific configuration
	OntapCredentials OntapCredentials   `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
	NewInstanceType  string             `json:"new_instance_type"` // Instance type for the storage pool
	OntapUpgrade     OntapUpgradeConfig `json:"ontap_upgrade"`     // ONTAP upgrade configuration
}

type OntapUpgradeConfig struct {
	OntapImageVersion string `json:"otap_image_version"`
}

type DeploymentUpdateStatus struct {
	DetachFail   bool `json:"detach_fail"`
	SPUpdateFail bool `json:"sp_update_fail"`
	AttachFail   bool `json:"attach_fail"`
	LifDownFail  bool `json:"lif_down_fail"`
	AggrDownFail bool `json:"aggr_down_fail"`
	AggrUpFail   bool `json:"aggr_up_fail"`
	LifUpFail    bool `json:"lif_up_fail"`
}

type UpdateVSAClusterDeploymentResponse struct {
	VLMConfig    VLMConfig
	UpdateStatus DeploymentUpdateStatus
}

type DeleteVSAClusterDeploymentRequest struct {
	CloudProvider string
	DeploymentID  string
	ProjectID     string
}

// DeployVSACluster deploys a VSA cluster using the provided deployment configuration.
type CreateVSAClusterDeploymentRequest struct {
	VLMConfig        VLMConfig
	OntapCredentials OntapCredentials `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
}

type CreateVSAClusterDeploymentResponse struct {
	VLMConfig VLMConfig
}

type ProvisionCloudResourcesForDataAggrWorkflowRequest struct {
	Cloud      CloudConfig
	Deployment DeploymentConfig
}

type ProvisionCloudResourcesForDataAggrWorkflowResponse struct {
	Cloud      CloudConfig
	Deployment DeploymentConfig
}

type CreateAggregatesForHAPairsWorkflowRequest struct {
	Cloud            CloudConfig
	Deployment       DeploymentConfig
	DataAggr         []DataAggrConfig
	VsaCluster       VsaClusterConfig
	OntapCredentials OntapCredentials
}

type CreateAggregatesForHAPairsWorkflowResponse struct {
	Cloud      CloudConfig
	Deployment DeploymentConfig
	DataAggr   []DataAggrConfig
	VsaCluster VsaClusterConfig
}
