// IMPORTANT: This is the VLM workflow datamodel file.
// We shouldn't edit this from the VCP side unless a newer version is shared by the VLM team.
package vlm

import (
	"time"
)

const (
	GCPCloud   string = "gcp"
	OCICloud   string = "oci"
	AzureCloud string = "azure"

	DeploymentTypeSharedHA    string = "shared_ha"
	DeploymentTypeNonSharedHA string = "non_shared_ha"

	CorrelationIDKey   string = "x-correlation-id"
	DeploymentIDKey    string = "x-deployment-id"
	DeploymentIDTagKey string = "deployment_id"
)

// VLMWorkflowName is the name of the workflow
const (
	CreateVSAClusterDeploymentWorkflowName     = "vlm.CreateVSAClusterDeploymentWorkflow"
	CreateVSASVMWorkflowName                   = "vlm.CreateVSASVMWorkflow"
	ModifyVSASVMWorkflowName                   = "vlm.ModifyVSASVMWorkflow"
	DeleteVSASVMWorkflowName                   = "vlm.DeleteVSASVMWorkflow"
	DeleteVSAClusterDeploymentWorkflowName     = "vlm.DeleteVSAClusterDeploymentWorkflow"
	UpdateVSAClusterDeploymentWorkflowName     = "vlm.UpdateVSAClusterDeploymentWorkflow"
	UpgradeVSAClusterDeploymentWorkflowName    = "vlm.UpgradeVSAClusterDeploymentWorkflow"
	PreUpgradeVSAClusterDeploymentWorkflowName = "vlm.PreUpgradeVSAClusterDeploymentWorkflow"
	VSASvmUpgradeWorkflowName                  = "vlm.VSASvmUpgradeWorkflow"
	ClusterPowerCycleWorkflowName              = "vlm.ClusterPowerCycle"
	ClusterHealthCheckWorkflowName             = "vlm.ClusterHealthCheck"
	GetClusterZiZsDetailsWorkflowName          = "vlm.GetClusterZiZsDetails"
	UpdateVSAMediatorWorkflowName              = "vlm.UpdateVSAMediatorWorkflow"
	CreateVSAExpertModeUserWorkflowName        = "vlm.CreateVSAExpertModeUserWorkflow"
	AddExpertModeUserWorkflowName              = "vlm.AddExpertModeUserWorkflow"
	UpdateLicenseWorkflowName                  = "vlm.UpdateLicenseWorkflow"
	ASUPTriggerWaitWorkflowName                = "vlm.ASUPTriggerWaitWorkflow"

	GCP_DISK_PD_SSD              = "pd-ssd"
	GCP_DISK_HDB                 = "hyperdisk-balanced"
	ONTAP_CREDENTIAL_ENCRYPT_KEY = "ONTAP_CREDENTIAL_ENCRYPT_KEY"

	ErrorTypeVLMError       string = "VLMError"
	ErrorTypeVLMClientError string = "VLMClientError"

	ClusterPowerOn    string = "start"
	ClusterPowerOff   string = "stop"
	ClusterPowerReset string = "reset"

	ZiZsComputeInstanceKey string = "compute_instance"
	ZiZsComputeDiskKey     string = "compute_disk"

	// Create a single placement policy for SPREAD with a separate AD active, passive and mediator VM sets
	GCPPlacementPolicySpreadSingle string = "spread_single"
	// Zonal Cluster only: Create a single placement policy for all VMs, to be implemented later
	GCPPlacementPolicyCompactSingle string = "compact_single"
	// Create a separate placement policy for active, passive and mediator VM sets and assign VMs to respective policies
	GCPPlacementPolicySpreadMulti string = "spread_multi"
	// Create a separate placement policy for active, passive and mediator VM sets and assign VMs to respective policies
	GCPPlacementPolicyCompactMulti string = "compact_multi"
	// No placement policy created/applied
	GCPPlacementPolicyNone string = "none"
)

// TODO: Need to revisit these values for Multi HA configurations
var WorkflowExecutionTimeoutMap map[string]time.Duration = map[string]time.Duration{
	"DefaultWorkflowExecutionTimeout":          10 * time.Minute,
	CreateVSAClusterDeploymentWorkflowName:     30 * time.Minute,
	CreateVSASVMWorkflowName:                   15 * time.Minute,
	ModifyVSASVMWorkflowName:                   15 * time.Minute,
	DeleteVSASVMWorkflowName:                   15 * time.Minute,
	DeleteVSAClusterDeploymentWorkflowName:     20 * time.Minute,
	UpdateVSAClusterDeploymentWorkflowName:     120 * time.Minute,
	UpgradeVSAClusterDeploymentWorkflowName:    300 * time.Minute,
	PreUpgradeVSAClusterDeploymentWorkflowName: 120 * time.Minute,
	ClusterPowerCycleWorkflowName:              40 * time.Minute,
	ClusterHealthCheckWorkflowName:             15 * time.Minute,
	GetClusterZiZsDetailsWorkflowName:          10 * time.Minute,
	UpdateVSAMediatorWorkflowName:              30 * time.Minute,
	UpdateLicenseWorkflowName:                  10 * time.Minute,
	CreateVSAExpertModeUserWorkflowName:        30 * time.Minute,
}

type VLMConfig struct {
	Cloud      CloudConfig          `json:"cloud"`
	Deployment DeploymentConfig     `json:"deployment"`
	Upgrade    OntapUpgradeConfig   `json:"upgrade"`
	VsaCluster VsaClusterConfig     `json:"vsa_cluster"`
	DataAggr   []DataAggrConfig     `json:"data_aggr"`
	Svm        map[string]SvmConfig `json:"svm"`
}

type CloudConfig struct {
	HAPairs []HAPair `json:"ha_pair"` // sde need not fill this
}

type DeploymentConfigFlags struct {
	EnableAASupportSvm         bool   `json:"enable_aa_support_svm"`          // Enable AA support for svm
	EnableAAConfig             bool   `json:"enable_aa_config"`               // Enable AA Config for active-active deployments
	EnableIlbSupport           bool   `json:"enable_ilb_support"`             // Enable ILB support
	EnableNfsV364BitIdentifier string `json:"enable_nfs_v3_64bit_identifier"` // Enable NFS v3 64-bit identifier support
	EnableNonLssdInstanceType  bool   `json:"enable_non_lssd_instance_type"`  // Enable Non LSSD instance type support
	EnableTcpUdpBasedIlb       bool   `json:"enable_tcp_udp_based_ilb"`       // Enable TCP/UDP based ILB support
	EnableFlashCache           bool   `json:"enable_flash_cache"`             // Enable Flash Cache support
}

type DeploymentConfig struct {
	Provider     string `json:"provider"`      // (gcp/aws/azure)
	DeploymentID string `json:"deployment_id"` // Added
	// If the Serial number Prefix is provided then it will be used to generate serial numbers for the VMs.
	SerialNumberPrefix string      `json:"serial_number_prefix"` // used to generate serial number for all the VMs
	VMSerialNumbers    []string    `json:"vm_serial_numbers"`    // List of serial numbers for the VMs
	Region             string      `json:"region" `              // Added
	Zone               ZoneInfo    `json:"zone"`                 // Added
	Images             ImageConfig `json:"images"`               // Added

	Tags           string            `json:"tags"`             // Comma separated list of tags to be attached for the VMs created by the deployment
	Labels         map[string]string `json:"labels"`           // List of labels to attach to resources
	UserBootargs   string            `json:"user_boot_args"`   // The input is a list of key-value pairs with semicolons as delimiters.
	UserCustomdata map[string]string `json:"user_custom_data"` // Additional Custom data to be passed to the VMs by user

	DeploymentType       string                       `json:"deployment_type"`        // SingeNode or ShareHA or NonShareHA
	NumHAPair            int                          `json:"num_ha_pair"`            // Number of HA pairs to be created
	VSAInstanceType      string                       `json:"vsa_instance_type"`      // rename to VSAInstanceType
	MediatorInstanceType string                       `json:"mediator_instance_type"` // rename to MediatorInstanceType
	DataDiskType         string                       `json:"data_disk_type"`         // Move to GCP config ?
	SystemDiskType       string                       `json:"system_disk_type"`       // Move to GCP config ?
	MediatorDiskType     string                       `json:"mediator_disk_type"`     // Move to GCP config ?
	DataDiskCount        int                          `json:"data_disk_count"`        // Number of data disks to be created
	VSASystemDiskConfig  map[OntapDiskType]DiskConfig `json:"vsa_system_disk_config"` // System disk configuration for VSA

	// TODO: check if zone wise netconfig is required
	NetConfig   map[VSALIFType]NetworkConfig `json:"net_config"`  // Network configuration for the deployment
	GCPConfig   GCPConfig                    `json:"gcpconfig"`   // GCP specific configuration
	OCIConfig   OCIConfig                    `json:"ociconfig"`   // OCI specific configuration
	AzureConfig AzureConfig                  `json:"azureconfig"` // Azure specific configuration
	SPConfig    SPConfig                     `json:"spconfig"`    // Storagepool specific configuration
	DevFlags    DevFlags                     `json:"dev_flags"`   // Development flags
	NTPServers  []string                     `json:"ntp_servers"` // NTP servers for time synchronization
	DNSServers  []string                     `json:"dns_servers"` // DNS servers for name resolution
	// DeploymentConfigFlags added for future flags
	DeploymentConfigFlags DeploymentConfigFlags `json:"additional_deployment_config_flags"`
	PlacementPolicyConfig PlacementPolicyConfig `json:"placement_policy_config"`
}

type DevFlags struct {
	ExtIPForNodeMgmt              bool `json:"ext_ip_for_node_mgmt"`          // External IP for node management
	AllowNonDenseShapeForVsa      bool `json:"allow_non_dense_shape_for_vsa"` // Allow using non DenseIO shapes for VSA Node VMs
	DisableDataNicTier1           bool `json:"disable_data_nic_tier1"`        // Disable Tier 1 for data NIC
	EnablePremiumTierData         bool `json:"enable_premium_tier_data"`      // Enable Premium Tier for data NIC
	DisableGVNIC                  bool
	EnableNfsV3Support            bool `json:"enable_nfs_v3_support"`             // Enable NFS v3 support
	EnableIlbSupport              bool `json:"enable_ilb_support"`                // Enable ILB support
	DisableBootDiskSnapshotPolicy bool `json:"disable_boot_disk_snapshot_policy"` // Disable boot disk snapshot policy creation and attachment (default: false/enabled)
	DisableAzureVNetCreation      bool `json:"disable_azure_vnet_creation"`       // Skip Azure VNet/subnet/NSG creation and use existing network
}

type SnapshotConfig struct {
	PolicyName    string `json:"policy_name,omitempty"`    // Name of the snapshot policy
	RetentionDays int32  `json:"retention_days,omitempty"` // Number of days to retain snapshots (default: 7)
}

type SnapshotConfigList struct {
	BootDiskConfig SnapshotConfig `json:"boot_disk_config,omitempty"` // Snapshot configuration for boot disks
	// MrootDiskConfig SnapshotConfig `json:"mroot_disk_config,omitempty"` // TODO: Snapshot configuration for mroot disks (future use)
}

type GCPConfig struct {
	ProjectID              string             `json:"project_id"`                // GCP project ID
	ImageProjectID         string             `json:"image_project_id"`          // Image project ID for GCP        `json:"gcp_image_config"`      // GCP image configuration
	MediatorImageProjectID string             `json:"mediator_image_project_id"` // Mediator image project ID for GCP
	ServiceAccountEmail    string             `json:"service_account_email"`     // Service account email for GCP
	BucketName             string             `json:"bucket_name"`               // GCP bucket name for storing data
	SnapshotConfigList     SnapshotConfigList `json:"snapshot_config_list"`      // Snapshot configuration container for different disk types
}

type OCIConfig struct {
	CompartmentID      string                            `json:"compartment_id"` // OCI Compartment ID
	SubnetID           string                            `json:"subnet_id"`
	DataNICSubnetID    string                            `json:"data_nic_subnet_id"`
	AvailabilityDomain AvailabilityDomainInfo            `json:"availability_domain_info"`         // OCI Availability Domain Info
	VSAInstanceShape   string                            `json:"vsa_instance_shape"`               // Instance shape for VSA
	VSAFlexOcpus       float32                           `json:"vsa_flex_ocpus,omitempty"`         // OCPUs for VSA flex (non-mediator); 0 = default 4
	VSAFlexMemoryInGBs float32                           `json:"vsa_flex_memory_in_gbs,omitempty"` // Memory in GB for VSA flex (non-mediator); 0 = default 32
	Creator            string                            `json:"creator"`                          // Creator for OCI mandatory tags (netapp_tags); overridable by CLI --creator
	FreeFormTags       map[string]string                 `json:"freeform_tags"`                    // Free form tags for OCI resources
	DefinedTags        map[string]map[string]interface{} `json:"defined_tags"`                     // Defined tags for OCI resources
	SubnetDomainName   string                            `json:"subnet_domain_name"`               // Domain name of subnet
}

type AzureConfig struct {
	ResourceGroup        string     `json:"resource_group"`
	VNetName             string     `json:"vnet_name"`
	SubnetName           string     `json:"subnet_name"`
	AddressSpace         []string   `json:"address_space"`
	SubnetCIDR           string     `json:"subnet_cidr"`
	NetworkSecurityGroup string     `json:"network_security_group"`
	AdminUsername        string     `json:"admin_username"`
	AdminPassword        string     `json:"admin_password"`
	SSHPublicKey         string     `json:"ssh_public_key"`
	SharedHAType         string     `json:"shared_ha_type"`    // Valid values: lrs, zrs, pageblob
	DataStorageType      string     `json:"data_storage_type"` // "esan", "pv2", or "both" (default: "both")
	RootStorageType      string     `json:"root_storage_type"` // "esan", "pv2", or "both" (default: "both")
	ESANConfig           ESANConfig `json:"esan_config"`
}

// ESANConfig holds Azure Elastic SAN configuration.
// Names are derived from deployment_id if not provided.
type ESANConfig struct {
	ESANName        string           `json:"esan_name"`         // derived: {deployment_id}-esan
	VolumeGroupName string           `json:"volume_group_name"` // derived: {deployment_id}-vg
	Volumes         []ESANVolumeInfo `json:"volumes"`           // populated after creation
}

// ESANVolumeInfo holds iSCSI target info for a single ESAN volume.
type ESANVolumeInfo struct {
	VolumeName       string `json:"volume_name"`
	TargetIQN        string `json:"target_iqn"`
	TargetPortalHost string `json:"target_portal_host"`
	TargetPortalPort int32  `json:"target_portal_port"`
	SizeGiB          int64  `json:"size_gib"`
	VolumeType       string `json:"volume_type"` // "data" or "root"
}

// Storage type constants for Azure data/root disk configuration.
const (
	StorageTypeESAN = "esan"
	StorageTypePV2  = "pv2"
	StorageTypeBoth = "both"
)

type GCPNetworkConfig struct {
	SubnetProjectID string `json:"subnet_project_id"` // Project ID for the subnet
}

type OCINetworkConfig struct {
	SubnetOCID string `json:"subnet_ocid"` // OCID for the subnet
}

type AzureNetworkConfig struct {
	SubnetID   string `json:"subnet_id"`
	VNetID     string `json:"vnet_id"`
	NICID      string `json:"nic_id"`
	PublicIPID string `json:"public_ip_id"`
	NSGID      string `json:"nsg_id"`
}

type ZoneInfo struct {
	Zone1        string `json:"zone1"`
	Zone2        string `json:"zone2"`
	MediatorZone string `json:"mediator_zone"`
}

type AvailabilityDomainInfo struct {
	AvailabilityDomain1        string `json:"availability_domain1"`
	AvailabilityDomain2        string `json:"availability_domain2"`
	MediatorAvailabilityDomain string `json:"mediator_availability_domain"`
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
	DNSName                 string   `json:"cert_dns,omitempty"`       // DNS name for the certificate
}

// OntapCredentials holds the credentials for ONTAP, including the certificate and username/password.
type OntapCredentials struct {
	Certificate   OntapCertificate `json:"certificate"`    // Certificate for ONTAP
	AdminPassword string           `json:"admin_password"` // Password for ONTAP
}

// Will be revisted during multi svm support
type GCPILBHaResources struct {
	ForwardingRules  string `json:"forwarding_rules"`   // Added for backward compatibility with vcp, deprecated
	BackendServices  string `json:"backend_services"`   // Added for backward compatibility with vcp, deprecated
	HealthChecks     string `json:"health_checks"`      // Added for backward compatibility with vcp, deprecated
	HealthCheckPorts int32  `json:"health_check_ports"` // Added for backward compatibility with vcp, deprecated
}

type OntapExpertModeUserConfig struct {
	VLMConfig                 VLMConfig        `json:"vlm_config"`                    // VLM configuration for expert mode
	OntapCredentials          OntapCredentials `json:"ontap_credentials"`             // ONTAP credentials for expert mode
	ExpertModeUserCredentials OntapCredentials `json:"expert_mode_credentials"`       // expert mode credentials
	Username                  string           `json:"username,omitempty"`            // expert mode username
	AuthenticationType        string           `json:"authentication_type,omitempty"` // "password" or "certificate", default is password
	RbacFileURL               string           `json:"rbac_file_url,omitempty"`       // URL for the RBAC file
	RbacFileChecksum          string           `json:"rbac_file_checksum,omitempty"`  // Checksum of the RBAC file
}

type AddExpertModeUserRequest struct {
	OntapCredential      OntapCredentials `json:"ontap_credential"`       // ONTAP admin credentials
	ExpertModeCredential OntapCredentials `json:"expert_mode_credential"` // expert mode user credentials
	Username             string           `json:"username"`               // expert mode username
	Vserver              string           `json:"vserver"`                // cluster name / vserver
	NodeMgmtIP           string           `json:"node_mgmt_ip"`           // cluster management IP
	AuthenticationType   string           `json:"authentication_type"`    // "password" or "certificate"
	RoleName             string           `json:"role_name"`              // RBAC role name to assign
	Provider             string           `json:"provider"`               // cloud provider (gcp, oci, etc.)
}

type OntapExpertModeUserResponse struct {
	RbacFileChecksum string `json:"rbac_checksum,omitempty"` // Checksum of the applied RBAC file
}

type GCPILBVmResources struct {
	Negs string `json:"negs"` // Added for backward compatibility with vcp, deprecated
}

type AdditionalVmResources struct {
	GCPILBVmResources GCPILBVmResources `json:"gcp_ilb_vm_resources"` // Stores gcp ilb vm resources
}

type AdditionalHaResources struct {
	GCPILBHaResources GCPILBHaResources `json:"gcp_ilb_ha_resources"` // Stores gcp ilb resources
}

type HAPair struct {
	VM1                   VMConfig              `json:"vm1"`
	VM2                   VMConfig              `json:"vm2"`
	Mediator              VMConfig              `json:"mediator"`                // Added
	AdditionalHaResources AdditionalHaResources `json:"additional_ha_resources"` // Added
}

type VMConfig struct {
	Region                string                   `json:"region"`    // Added
	Zone                  string                   `json:"zone"`      // Added
	Name                  string                   `json:"name"`      // Name of the VM
	HostName              string                   `json:"host_name"` // Available during cluster create.
	SerialNumber          string                   `json:"serial_number"`
	NodeIndex             int                      `json:"node_index"` // Added
	IsMediator            bool                     `json:"is_mediator"`
	SystemLIFs            map[VSALIFType]LIFConfig `json:"lifs"`                    // List of IPs for the VM
	SystemDisks           []DiskConfig             `json:"system_disks"`            // List of system disks for the VM
	DataDisks             []DiskConfig             `json:"data_disks"`              // List of data disks for the VM
	VSAManagementIP       string                   `json:"vsa_management_ip"`       // VSA management IP for the VM
	AdditionalVmResources AdditionalVmResources    `json:"additional_vm_resources"` // additional resources
	OCIVMInstanceID       string                   `json:"oci_vm_instance_id"`      // OCI VM instance ID
}

// GCP Only: Used for ZiZs workflow
type ResourceInformation struct {
	GCPRI map[string][]GCPResourceInformation `json:"gcp_resource_information"`
}

// GCP Only: Used for ZiZs workflow
type GCPResourceInformation struct {
	SatisfiesPzi bool   `json:"satisfies_pzi"`
	SatisfiesPzs bool   `json:"satisfies_pzs"`
	AssetType    string `json:"asset_type"`
	AssetLink    string `json:"asset_link"`
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
	LIFTypeNodeMgmt         VSALIFType = "nodemgmt"
	LIFTypeNodeMgmtInternal VSALIFType = "nodemgmtinternal"
	LIFTypeIC               VSALIFType = "ic"
	LIFTypeCluster          VSALIFType = "clus"
	LIFTypeInterCluster     VSALIFType = "intercluster"
	LIFTypeRSM              VSALIFType = "rsm"
	LIFTypeSan              VSALIFType = "san"
	LIFTypeNas              VSALIFType = "nas"
	LIFTypeMediator         VSALIFType = "mediator"
	LIFTypeIlbNas           VSALIFType = "ilbnas"
	LIFTypeRbac             VSALIFType = "rbac"
)

type LIFConfig struct {
	Name          string        `json:"lif_name"`       // Name of the LIF
	VSALIFType    VSALIFType    `json:"vsa_ip_type"`    // Type of VSA LIF
	IP            string        `json:"ip"`             // IP for the LIF
	Uuid          string        `json:"lif_uuid"`       // UUID of the LIF
	NetworkConfig NetworkConfig `json:"network_config"` // Network configuration for the LIF
	Region        string        `json:"region"`         // Region for the LIF
	HomeNode      string        `json:"home_node"`      // Home node for the LIF
	ProbePort     int32         `json:"probe_port"`     // NFS probe port for the LIF only used for nas lif
}

type NetworkConfig struct {
	Subnet             string             `json:"subnet"`  //Subnet for the NIC
	VPC                string             `json:"vpc"`     //VPC for the NIC
	Gateway            string             `json:"gateway"` //Gateway for the subnet
	Netmask            string             `json:"netmask"` //Netmask for the subnet
	GCPNetworkConfig   GCPNetworkConfig   `json:"gcp_network_config"`
	OCINetworkConfig   OCINetworkConfig   `json:"oci_network_config"`
	AzureNetworkConfig AzureNetworkConfig `json:"azure_network_config"`
}

type VsaClusterConfig struct {
	ClusterMgmtNetmask  string `json:"cluster_mgmt_netmask"`
	ClusterMgmtGateway  string `json:"cluster_mgmt_gateway"`
	CustBroadcastDomain string `json:"cust_broadcast_domain"`
	CustIPSpace         string `json:"cust_ip_space"`
	ObjectStoreName     string `json:"object_store_name"`
	ClusterName         string `json:"cluster_name"` // Name of the VSA cluster
	AutoTierThreshold   int64  `json:"auto_tier_threshold"`
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
	Name           string        `json:"name"`
	Size           uint64        `json:"size"`        // in GB
	AccessMode     string        `json:"access_mode"` // READ_WRITE or READ_WRITE_MANY
	Type           string        `json:"type"`        // Disk type (e.g., pd-standard, pd-ssd)
	DiskIops       int64         `json:"disk_iops"`
	DiskThroughput int64         `json:"disk_throughput"`
	ResourceStatus string        `json:"resource_status"` // Status of the resource
	Zone           string        `json:"zone"`            // Zone for the disk
	GCPDiskConfig  GCPDiskConfig `json:"gcp_disk_config"` // GCP specific disk configuration
	OCIDiskConfig  OCIDiskConfig `json:"oci_disk_config"` // OCI specific disk configuration
	// TODO: Add resource status
}

type GCPDiskConfig struct {
	DeviceName string `json:"device_name,omitempty"` // Device name for the disk (only when attached)
	// Add other GCP-specific fields here if needed
}

type OCIDiskConfig struct {
	DeviceName          string                            `json:"device_name,omitempty"` // Device name for the disk (only when attached)
	AvailabilityDomain  string                            `json:"availability_domain"`   // Availability Domain for the disk
	Vpus                int64                             `json:"vpus"`                  // Vpus for the disk
	DiskMinVPUMultiPath int64                             `json:"disk_min_vpu_multi_path"`
	DevicePath          string                            `json:"device_path"`
	DiskOciID           string                            `json:"disk_oci_id"`    // OCID for the disk
	CompartmentID       string                            `json:"compartment_id"` // OCI Compartment ID
	FreeFormTags        map[string]string                 `json:"freeform_tags"`  // Free form tags for OCI resources
	DefinedTags         map[string]map[string]interface{} `json:"defined_tags"`   // Defined tags for OCI resources
	// Add other OCI-specific fields here if needed
}

type CreateSVMRequest struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`
	Name             string           `json:"name"` // SVM name
	DnsDomains       string           `json:"dns_ip"`
	NameServers      string           `json:"servers"`                      // List of servers
	OntapCredentials OntapCredentials `json:"ontap_credentials"`            // ONTAP credentials for the VSA cluster
	EnableNasLif     bool             `json:"enable_nas_lif"`               // When true, VLM creates NAS LIF + ILB during SVM creation
	SvmAdminPassword string           `json:"svm_admin_password,omitempty"` // SVM Admin Password
}

type CreateSVMResponse struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}

type DeleteSVMRequest struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`
	Name             string           `json:"name"`              // SVM name
	OntapCredentials OntapCredentials `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
}

type DeleteSVMResponse struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}
type ModifySVMRequest struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`
	Name             string           `json:"name"`              // SVM name
	OntapCredentials OntapCredentials `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
}

type ModifySVMResponse struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}

type UpdateVSAClusterDeploymentRequest struct {
	VLMConfig                VLMConfig          `json:"vlm_config"`                   // VLM configuration
	NumHAPair                int                `json:"num_ha_pair"`                  // Number of HA pairs to be created
	SPConfig                 SPConfig           `json:"spconfig"`                     // Storagepool specific configuration
	OntapCredentials         OntapCredentials   `json:"ontap_credentials"`            // ONTAP credentials for the VSA cluster
	NewInstanceType          string             `json:"new_instance_type"`            // Instance type for the storage pool
	OntapUpgrade             OntapUpgradeConfig `json:"ontap_upgrade"`                // ONTAP upgrade configuration
	HAPairIndices            []int              `json:"ha_pair_indices"`              // Selected HA pair indices for targeted operations
	ITCRecovery              bool               `json:"itc_recovery"`                 // Flag to indicate if this is a recovery operation (ITC)
	BucketName               string             `json:"bucket_name"`                  // GCP Bucket Name
	AutoTierThreshold        int64              `json:"auto_tier_threshold"`          // Auto tiering threshold percentage (0-100)
	AllowHAPairLimitOverride bool               `json:"allow_ha_pair_limit_override"` // Allow selected callers (e.g. CLI) to bypass HA pair selection limit
}

type UpdateMediatorRequest struct {
	VLMConfig        VLMConfig            `json:"vlm_config"`        // VLM configuration
	MediatorUpdate   MediatorUpdateConfig `json:"mediator_update"`   // Mediator update configuration
	OntapCredentials OntapCredentials     `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
	HAPairIndices    []int                `json:"ha_pair_indices"`   // Indices of the HA pairs to update (empty or nil to update all)
}

type UpdateMediatorResponse struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}

type OntapUpgradeConfig struct {
	SkipOntapImageVersionMatch     bool   `json:"skip_ontap_image_version_match"`     // Skip Image version match for upgrade
	OntapUpgradeTargetImageVersion string `json:"ontap_upgrade_target_image_version"` // Image version for upgrade
	OntapUpgradeImagePath          string `json:"ontap_upgrade_image_path"`           // Image path for upgrade
	RunPreUpgrade                  bool   `json:"run_preupgrade"`                     // Run pre-upgrade workflow before upgrade
}

type MediatorUpdateConfig struct {
	MediatorImageName      string `json:"mediator_image_name"`
	MediatorImageProjectId string `json:"mediator_image_project_id"`
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

// Used for error propagation to VCP
type VLMClientError struct {
	HttpCode       int      `json:"vlmclient_http_code"`
	Code           string   `json:"vlmclient_code"`
	Message        string   `json:"vlmclient_message"`
	OntapErrorCode string   `json:"vlmclient_ontap_error_code,omitempty"`
	Component      string   `json:"vlmclient_component"`
	Retryable      bool     `json:"vlmclient_retryable"`
	External       bool     `json:"vlmclient_external"`
	Cause          []string `json:"vlmclient_error_string"`
}

type UpdateVSAClusterDeploymentResponse struct {
	VLMConfig    VLMConfig              `json:"vlm_config"`
	UpdateStatus DeploymentUpdateStatus `json:"update_status"`
}

type UpgradeVSAClusterDeploymentResponse struct {
	VLMConfig     VLMConfig              `json:"vlm_config"`
	UpgradeStatus DeploymentUpdateStatus `json:"upgrade_status"`
	OntapVersion  string                 `json:"ontap_version"`
}

type DeleteVSAClusterDeploymentRequest struct {
	CloudProvider     string             `json:"cloud_provider"`
	DeploymentID      string             `json:"deployment_id"`
	ProjectID         string             `json:"project_id"`
	HyperScalerConfig *HyperScalerConfig `json:"hyper_scaler_config"`
}

type HyperScalerConfig struct {
	GCPConfig GCPConfig `json:"gcp_config"`
	OCIConfig OCIConfig `json:"oci_config"`
}

// DeployVSACluster deploys a VSA cluster using the provided deployment configuration.
type CreateVSAClusterDeploymentRequest struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`        // VLM configuration
	OntapCredentials OntapCredentials `json:"ontap_credentials"` // ONTAP credentials for the VSA cluster
	OntapLicense     OntapLicense     `json:"ontap_license"`
}

type CreateVSAClusterDeploymentResponse struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}

type AsupReq struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`
	OntapCredentials OntapCredentials `json:"ontap_credentials"`
	Message          string           `json:"message"`
	VmConfig         VMConfig         `json:"vm_config"`
}

type ValidateClusterHealthRequest struct {
	VLMConfig            VLMConfig        `json:"vlm_config"`
	OntapCredentials     OntapCredentials `json:"ontap_credentials"`
	TriggerASUPOnFailure bool             `json:"trigger_asup_on_failure"`
}

type ClusterPowerOpReq struct {
	VLMConfig        VLMConfig        `json:"vlm_config"`
	OntapCredentials OntapCredentials `json:"ontap_credentials"`
	Operation        string           `json:"operation"`
}

type ClusterPowerOpResp struct {
	VLMConfig VLMConfig `json:"vlm_config"`
}

// GCP only
type GetResourceInfoReq struct {
	ProjectID    string `json:"project_id"`
	DeploymentID string `json:"deployment_id"`
}

// GCP only
type GetResourceInfoResp struct {
	ProjectID    string              `json:"project_id"`
	DeploymentID string              `json:"deployment_id"`
	ResourceInfo ResourceInformation `json:"resource_info"`
}

type OntapLicense struct {
	SecretUri []string `json:"secret_uri"`
}

type UpdateLicenseRequest struct {
	OntapLicense     OntapLicense     `json:"ontap_license"`
	OntapCredentials OntapCredentials `json:"ontap_credentials"`
	VSAManagementIP  string           `json:"vsa_management_ip"` // VSA management IP for the VM
	Provider         string           `json:"provider"`          // Provider for the license
}

type PlacementPolicyConfig struct {
	GCPPlacementPolicyConfig GCPPlacementPolicyConfig `json:"gcp_placement_policy_config"`
}

type GCPPlacementPolicyConfig struct {
	PolicyConfig       string `json:"policy_config"`         // SPREAD_SINGLE, COMPACT_SINGLE, SPREAD_MULTI, COMPACT_MULTI
	CompactMaxDistance int32  `json:"compact_max_distance"`  // Compact configs only: Max distance for COMPACT configs
	SpreadMultiADCount int32  `json:"spread_multi_ad_count"` // SPREAD_MULTI config only: Number of availability domains for SPREAD_MULTI config
}
