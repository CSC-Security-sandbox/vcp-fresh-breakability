package ontap_rest

import (
	"strconv"
	"strings"
	"time"

	cr "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cloud"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/networking"
	san "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/s_a_n"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/security"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/snapmirror"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/svm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	priv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

var (
	returnTimeout = strconv.FormatInt(int64(utils.GetConstraintInteger(env.GetUint("ONTAP_REST_SYNC_RETURN_TIMEOUT_SECONDS", 15), 0, 15, 15)), 10)
	// MD: returnTimeoutNoJob signals that we are not interested in getting a job and the entire operation should instead time out
	// this is useful for resources that in most cases take very little time to delete but may sometimes take longer.
	returnTimeoutNoJob         = nillable.ToPointer(strconv.Itoa(utils.GetConstraintInteger(int(cr.DefaultTimeout), 15, 120, 30)))
	objStoreProviderType       = env.GetString("OBJECT_STORE_PROVIDER", "googlecloud")
	objStoreServer             = env.GetString("OBJECT_STORES_SERVER", "storage.googleapis.com")
	objStoreAuthenticationType = env.GetString("OBJECT_STORE_AUTH_TYPE", "GCP_SA")
	objStoreSnapmirrorUse      = "data"
	objStoreOwner              = "snapmirror"
)

// BaseParams contains all the common parameters that ONTAP REST supports
type BaseParams struct {
	Fields        []string
	ReturnRecords *bool
	MaxRecords    *int64
}

// JobAccepted contains the async job information from ONTAP
type JobAccepted struct {
	JobUUID      string
	ResourceUUID string
}

// CloudTargetCollectionGetParams is the input param struct for cloudClient.CloudTargetsGet
type CloudTargetCollectionGetParams struct {
	BaseParams
	Owner *string
	Name  *string
}

// CloudTargetCreateParams is the input param struct for cloudClient.CloudTargetCreate
type CloudTargetCreateParams struct {
	BaseParams
	Name           *string
	ProviderType   *string
	Server         *string
	Container      *string
	IpspaceName    *string
	Owner          *string
	AccessKey      *string
	SecretPassword *strfmt.Password
	SslEnabled     bool
}

// CloudTargetModifyParams is the input param struct for cloudClient.CloudTargetModify
type CloudTargetModifyParams struct {
	BaseParams
	Name           *string
	ProviderType   *string
	Server         *string
	Container      *string
	AccessKey      *string
	SecretPassword *strfmt.Password
	Owner          *string
	UUID           string
}

// ClusterGetParams is the input param struct for clusterClient.ClusterGet
type ClusterGetParams struct {
	BaseParams
}

// Cluster is a simple wrapper of models.Cluster
type Cluster struct {
	models.Cluster
}

// JobGetParams is the input param struct for clusterClient.JobGet
type JobGetParams struct {
	BaseParams
	UUID string
}

// Job is a simple wrapper of models.Job
type Job struct {
	models.Job
}

// JobCollectionGetParams is the input param struct for clusterClient.JobGet
type JobCollectionGetParams struct {
	BaseParams
	Fields      []string
	SvmUUID     string
	Description string
}

// NodesGetParams is the input param struct for clusterClient.NodesGet
type NodesGetParams struct {
	BaseParams
}

func nodesGetParamsToONTAP(params *NodesGetParams) *cluster.NodesGetParams {
	otParams := cluster.NewNodesGetParams()
	if params == nil {
		return otParams
	}

	otParams.Fields = params.Fields
	return otParams
}

// Node is a simple wrapper of models.NodeResponseInlineRecordsInlineArrayItem
type Node struct {
	models.NodeResponseInlineRecordsInlineArrayItem
}

// ScheduleCollectionGetParams is the input param struct for clusterClient.ScheduleCollectionGet
type ScheduleCollectionGetParams struct {
	BaseParams
	Name string
}

// Schedule is a simple wrapper of models.Schedule
type Schedule struct {
	models.Schedule
}

// DNSGetParams is the input param struct for nameServicesClient.DNSGet
type DNSGetParams struct {
	BaseParams
	SvmUUID string
}

// DNSCreateParams is the input param struct for nameServicesClient.DNSCreate
type DNSCreateParams struct {
	SvmUUID    string
	Domains    []string
	DNSServers []string
}

// DNS is a simple wrapper of models.DNS
type DNS struct {
	models.DNS
}

// DNSModifyParams is the input param struct for nameServicesClient.DNSModify
type DNSModifyParams struct {
	BaseParams
	SvmUUID          string
	Domains          []string
	NameServers      []string
	DDNSModifyParams DDNSModifyParams
}

// DDNSModifyParams is the input param struct for nameServicesClient.DNSModify.DynamicDNS
type DDNSModifyParams struct {
	UseSecure *bool
	Fqdn      *string
	Enabled   *bool
}

// CifsServiceCollectionGetGroupsParams is the input param struct for fetching cifs groups and users
type CifsServiceCollectionGetGroupsParams struct {
	BaseParams
	Sid     *string
	SvmUUID string
}

// CifsGroup is a CIFS group
type CifsGroup struct {
	Name    string
	Sid     string
	Members []string
}

// CifsServiceCollectionGetPrivilegedMembersParams is the input param struct for fetching privileged members
type CifsServiceCollectionGetPrivilegedMembersParams struct {
	BaseParams
	SvmUUID string
}

// CifsServiceModifyGroupMembersParams is the input param struct to add or remove members to CIFS groups
type CifsServiceModifyGroupMembersParams struct {
	Sid     string
	Members []string
	SvmUUID string
}

// CifsServiceModifySecurityPrivilegeParams is the input param struct to modify CIFS user privileges
type CifsServiceModifySecurityPrivilegeParams struct {
	Member  string
	SvmUUID string
}

// HostRecordGetParams is the input param struct for nameServicesClient.HostRecordGet
type HostRecordGetParams struct {
	BaseParams
	Host     string
	SvmUUID  string
	UseCache *bool
}

// CifsDomainPreferredDCDeleteParams is the input param struct for nasClient.CifsDomainCifsDomainPreferredDCDelete
type CifsDomainPreferredDCDeleteParams struct {
	BaseParams
	Fqdn     *string
	ServerIP *string
	SvmUUID  string
}

// CifsDomainPreferredDCCreateParams is the input param struct for nasClient.CifsDomainCifsDomainPreferredDCCreate
type CifsDomainPreferredDCCreateParams struct {
	BaseParams
	CifsDomainPreferredDC *CifsDomainPreferredDC
	SkipConfigValidation  *bool
	SvmUUID               string
}

// CifsDomainPreferredDC in the input param for model.CifsDomainPreferredDCCreateParams
type CifsDomainPreferredDC struct {
	Fqdn     *string
	ServerIP *string
	Status   *CifsDomainPreferredDcInlineStatus
}

// CifsDomainPreferredDcInlineStatus is the input param for model.CifsDomainPreferredDC
type CifsDomainPreferredDcInlineStatus struct {
	Details   *string
	Reachable *bool
}

// SrvLookupParams is the input param struct for nasClient.DomainControllersSrvLookupGet
type SrvLookupParams struct {
	BaseParams
	LookupString string
	LookupType   *string
	NameServers  []string
	Node         string
	SVMName      string
}

// HostRecord is a simple wrapper of models.HostRecord
type HostRecord struct {
	models.HostRecord
}

// LdapCollectionGetParams is the input param struct for nameServicesClient.LdapCollectionGet
type LdapCollectionGetParams struct {
	BaseParams
	SvmUUID *string
}

// LdapService is a simple wrapper of models.LdapService
type LdapService struct {
	models.LdapService
}

// LdapGetParams is the input params struct for nameServicesClient.LdapGet
type LdapGetParams struct {
	BaseParams
	SvmUUID string
}

// LdapCreateParams is the input params struct for nameServicesClient.LdapCreate
type LdapCreateParams struct {
	BaseParams
	BindAsCifsServer              *bool
	DomainName                    *string
	BaseDN                        *string
	UserDn                        *string
	GroupDn                       *string
	GroupMembershipFilter         *string
	PreferredServersForLdapClient []*string
	TLSEnabled                    *bool
	Schema                        *string
	SessionSecurity               *string
	SvmUUID                       string
	LdapPort                      *int64
	LdapServers                   []*string
}

// LdapModifyParams is the input params struct for nameServicesClient.LdapModify
type LdapModifyParams struct {
	BaseParams
	UserDn                        *string
	GroupDn                       *string
	BaseDN                        *string
	GroupMembershipFilter         *string
	PreferredServersForLdapClient []*string
	TLSEnabled                    *bool
	Schema                        *string
	SvmUUID                       string
	Domain                        *string
	LdapServers                   []*string
}

// LdapDeleteParams is the input params struct for nameServicesClient.LdapDelete
type LdapDeleteParams struct {
	BaseParams
	SvmUUID string
}

// LdapSchemaCreateParams is the input params struct for nameServicesClient.LdapSchemaCreate
type LdapSchemaCreateParams struct {
	BaseParams
	Name     *string
	Template *string
	SvmUUID  *string
}

// LdapSchemaModifyParams is the input params struct for nameServicesClient.LdapSchemaModify
type LdapSchemaModifyParams struct {
	BaseParams
	MaximumGroups *int64
	SvmUUID       string
	SchemaName    string
}

// LocalHostCreateParams is the input param struct for nameServicesClient.LocalHostCreate
type LocalHostCreateParams struct {
	BaseParams
	Address  *string
	Hostname *string
	Owner    *string
	Timeout  time.Duration
}

// LocalHostDeleteParams is the input param struct for nameServicesClient.LocalHostDelete
type LocalHostDeleteParams struct {
	BaseParams
	Address string
	SvmUUID string
	Timeout time.Duration
}

// UnixGroupCollectionGetParams is the input param struct for nameServicesClient.UnixGroupCollectionGet
type UnixGroupCollectionGetParams struct {
	BaseParams
	SvmName   *string
	ID        *int64
	Name      *string
	UsersName *string
}

// UnixGroup is a simple wrapper of models.UnixGroup
type UnixGroup struct {
	models.UnixGroup
}

// UnixGroupCreateParams is the input param struct for nameServicesClient.UnixGroupCreate
type UnixGroupCreateParams struct {
	SvmName string
	SvmUUID string
	Name    string
	GID     uint32
}

// UnixGroupDeleteParams is the input param struct for nameServicesClient.UnixGroupDelete
type UnixGroupDeleteParams struct {
	SvmName string // FIXME: unused - remove?
	SvmUUID string
	Name    string
}

// UnixUserCollectionGetParams is the input param struct for nameServicesClient.UnixUserCollectionGet
type UnixUserCollectionGetParams struct {
	BaseParams
	SvmName    string
	SvmUUID    string
	Name       *string
	FullName   *string
	UID        *uint32
	PrimaryGID *uint32
}

// UnixUser is a simple wrapper of models.UnixUser
type UnixUser struct {
	models.UnixUser
}

// UnixUserCreateParams is the input param struct for nameServicesClient.UnixUserCreate
type UnixUserCreateParams struct {
	SvmName    string
	SvmUUID    string
	Name       string
	FullName   *string
	UID        uint32
	PrimaryGID uint32
}

// UnixUserDeleteParams is the input param struct for nameServicesClient.UnixUserDelete
type UnixUserDeleteParams struct {
	SvmName string // FIXME: unused - remove?
	SvmUUID string
	Name    string
}

// GetGroupIDsListParams is the input param struct for nameServicesClient.GetGroupIDsList
type GetGroupIDsListParams struct {
	BaseParams
	Username string
	SvmName  string
	Node     string
	UseCache string
}

// CifsDomainGetParams is the input param struct for nasClient.CifsDomainGet
type CifsDomainGetParams struct {
	BaseParams
	RediscoverTrusts       *bool
	ResetDiscoveredServers *bool
	SvmUUID                string
}

// CifsDomain is a simple wrapper of models.CifsDomain
type CifsDomain struct {
	models.CifsDomain
}

// NfsGetParams is the input params struct for nasClient.NfsGet
type NfsGetParams struct {
	BaseParams
	SvmUUID string
}

// NfsCreateParams is the input param struct for nasClient.NfsModify
type NfsCreateParams struct {
	SvmUUID               string
	NFSv41Enabled         bool
	NFSv364BitIdentifiers bool
	ShowmountEnabled      *bool
	NFSv4IDDomain         *string
	VstorageEnabled       bool
}

// Nfs is a simple wrapper of models.NfsService
type Nfs struct {
	models.NfsService
}

// NfsModifyParams is the input param struct for nasClient.NfsModify
type NfsModifyParams struct {
	SvmUUID                    string
	V4IDDomain                 *string
	ShowmountEnabled           *bool
	RquotaEnabled              *bool
	AllowLocalNFSUsersWithLdap *bool
	ExtendedGroupsLimit        *int64
	Enabled                    *bool
	V3Enabled                  *bool
	V40Enabled                 *bool
	V41Enabled                 *bool
	VstorageEnabled            *bool
	FileSessionIoGroupingCount *int64
}

// NfsClientsGetParams is the input param struct for nasClient.NfsClientsGet
type NfsClientsGetParams struct {
	BaseParams
	VolumeUUID *string
	SvmName    *string
	Protocol   *string
}

// NfsClients is a simple wrapper of models.NfsClients
type NfsClients struct {
	models.NfsClients
}

// AuditCreateParams is the input param struct for nasClient.AuditCreate
type AuditCreateParams struct {
	BaseParams
	SvmName             *string
	Enabled             *bool
	AuthorizationPolicy *bool
	CapStaging          *bool
	CifsLogonLogoff     *bool
	FileOperations      *bool
	FileShare           *bool
	SecurityGroup       *bool
	UserAccount         *bool
	Format              *string
	LogSize             *int64
	LogPath             *string
	LogRotation         []*int64
	LogRetentionCount   *int64
	Guarantee           *bool
	ChargeQos           *bool
}

// AuditLogRedirectCreateParams is the input param struct for securityClient.AuditLogRedirectCreate
type AuditLogRedirectCreateParams struct {
	BaseParams
	SvmUUID *string
}

// AuditLogRedirectGetParams is the input param struct for securityClient.AuditLogRedirectGet
type AuditLogRedirectGetParams struct {
	BaseParams
}

// AuditLogRedirect is a simple wrapper of models.AuditLogRedirect
type AuditLogRedirect struct {
	models.AuditLogRedirect
}

// AuditLogRedirectDeleteParams is the input param struct for securityClient.AuditLogRedirectDelete
type AuditLogRedirectDeleteParams struct {
	BaseParams
}

// CifsLocalGroupMembersCreateParams is the input param struct for nasClient.CifsLocalGroupMembersCreate
type CifsLocalGroupMembersCreateParams struct {
	BaseParams
	SvmUUID string
	SID     string
	Users   []string
}

// CifsLocalGroupMembersBulkDeleteParams is the input param struct for nasClient.CifsLocalGroupMembersBulkDelete
type CifsLocalGroupMembersBulkDeleteParams struct {
	BaseParams
	SID     string
	SvmUUID string
	Users   []string
}

// CifsLocalGroupMember is a simple wrapper of models.LocalCifsGroupMembers
type CifsLocalGroupMember struct {
	models.LocalCifsGroupMembers
}

// CifsLocalGroupMembersCollectionGetParams is the input param struct for nasClient.CifsLocalGroupMembersCollectionGet
type CifsLocalGroupMembersCollectionGetParams struct {
	BaseParams
	SvmUUID string
	SID     string
}

// CifsUserGroupPrivilegesCreateParams is the input param struct for nasClient.CifsUserGroupPrivilegesCreate
type CifsUserGroupPrivilegesCreateParams struct {
	BaseParams
	Name       *string
	Privileges []*string
	SvmUUID    *string
}

// CifsUserGroupPrivileges is a simple wrapper of models.UserGroupPrivileges
type CifsUserGroupPrivileges struct {
	models.UserGroupPrivileges
}

// CifsUserGroupPrivilegesCollectionGetParams is the input param struct for nasClient.CifsUserGroupPrivilegesCollectionGet
type CifsUserGroupPrivilegesCollectionGetParams struct {
	BaseParams
	Privileges *string
	SvmUUID    *string
}

// CifsUserGroupPrivilegesModifyParams is the input param struct for nasClient.CifsUserGroupPrivilegesModify
type CifsUserGroupPrivilegesModifyParams struct {
	BaseParams
	Name       string
	Privileges []*string
	SvmUUID    string
}

// CifsSessionCollectionGetParams is the input param struct for nasClient.CifsSessionCollectionGet
type CifsSessionCollectionGetParams struct {
	BaseParams
	VolumeName *string
}

// CifsSession is a simple wrapper of models.CifsSession
type CifsSession struct {
	models.CifsSession
}

// BreakFileLocksParams is the input param struct for nasClient.BreakFileLocks
type BreakFileLocksParams struct {
	BaseParams
	VolumeUUID string
	SvmName    string
	ClientIP   *string
}

// NetworkIPInterfacesGetParams is the input param struct for networkingClient.NetworkIPInterfacesGet
type NetworkIPInterfacesGetParams struct {
	BaseParams
	SvmUUID           *string
	SvmName           *string
	Name              *string
	IPAddress         *string
	ServicePolicyName *string
}

func networkIPInterfacesGetParamsToONTAP(params *NetworkIPInterfacesGetParams) *networking.NetworkIPInterfacesGetParams {
	otParams := networking.NewNetworkIPInterfacesGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	otParams.SetFields(params.Fields)
	if params.SvmName != nil {
		otParams.SetSvmName(params.SvmName)
	}
	if params.Name != nil {
		otParams.SetName(params.Name)
	}
	if params.SvmUUID != nil {
		otParams.SetSvmUUID(params.SvmUUID)
	}
	if params.IPAddress != nil {
		otParams.SetIPAddress(params.IPAddress)
	}
	if params.ServicePolicyName != nil {
		otParams.SetServicePolicyName(params.ServicePolicyName)
	}
	return otParams
}

// NetworkIPInterfacesCreateParams is the input param struct for networkingClient.NetworkIPInterfacesGet
type NetworkIPInterfacesCreateParams struct {
	Name          string
	IPAddress     string
	Netmask       string
	SvmName       string
	HomePort      string
	HomeNode      string
	ServicePolicy string
}

// IPInterface is a simple wrapper of models.IPInterface
type IPInterface struct {
	models.IPInterface
}

// IPServicePolicy is a simple wrapper of models.IPServicePolicy
type IPServicePolicy struct {
	models.IPServicePolicy
}

// SecurityKeyManagerCollectionGetParams is the input param struct for securityClient.SecurityKeyManagerCollectionGet
type SecurityKeyManagerCollectionGetParams struct {
	BaseParams
	Timeout time.Duration
}

// SecurityKeyManager is a simple wrapper of models.SecurityKeyManager
type SecurityKeyManager struct {
	models.SecurityKeyManager
}

// SecurityKeyManagerMigrateParams is the input param struct for securityClient.SecurityKeyManagerMigrate
type SecurityKeyManagerMigrateParams struct {
	BaseParams
	SourceUUID      string
	DestinationUUID string
	Timeout         time.Duration
}

// SecurityKeystoreModifyParams is the input param struct for securityClient.SecurityKeystoreModify
type SecurityKeystoreModifyParams struct {
	BaseParams
	KeystoreUUID string
	Enabled      bool
	Timeout      time.Duration
}

// SecurityKeystoreDeleteParams is the input param struct for securityClient.SecurityKeystoreDelete
type SecurityKeystoreDeleteParams struct {
	BaseParams
	KeystoreUUID string
	Timeout      time.Duration
}

// IpsecPolicyEndpoint describes an endpoint for IpsecPolicy parameters
type IpsecPolicyEndpoint struct {
	Address string
	Netmask string
	Port    string
}

// IpsecPolicyCreateParams is the input param struct for securityClient.IpsecPolicyCreate
type IpsecPolicyCreateParams struct {
	BaseParams
	Action         *string
	Enabled        *bool
	LocalEndpoint  *IpsecPolicyEndpoint
	Name           *string
	Protocol       *string
	RemoteEndpoint *IpsecPolicyEndpoint
	SecretKey      *string
	LocalIdentity  *string
	RemoteIdentity *string
	SvmName        *string
}

// IpsecPolicyModifyParams is the input param struct for securityClient.IpsecPolicyModify
type IpsecPolicyModifyParams struct {
	BaseParams
	UUID           string
	RemoteEndpoint *IpsecPolicyEndpoint
	LocalIdentity  *string
	RemoteIdentity *string
}

// IpsecPolicyDeleteParams is the input param struct for securityClient.IpsecPolicyDelete
type IpsecPolicyDeleteParams struct {
	BaseParams
	UUID string
}

// IpsecPolicyCollectionGetParams is the input param struct for securityClient.IpsecPolicyCollectionGet
type IpsecPolicyCollectionGetParams struct {
	BaseParams
	Name    *string
	SvmName *string
}

// IpsecPolicy is a simple wrapper of models.IpsecPolicyResponseInlineRecordsInlineArrayItem
type IpsecPolicy struct {
	models.IpsecPolicyResponseInlineRecordsInlineArrayItem
}

// GcpKmsCreateParams is the input param struct for securityClient.GcpKmsCreate
type GcpKmsCreateParams struct {
	BaseParams
	KeyName                *string
	KeyRingLocation        *string
	KeyRingName            *string
	ProjectID              *string
	ApplicationCredentials *strfmt.Password
	SvmName                *string
	PrivilegedAccount      *string
}

func gcpKmsCreateParamsToONTAP(params *GcpKmsCreateParams) *security.GcpKmsCreateParams {
	otParams := security.NewGcpKmsCreateParams()
	if params == nil {
		return otParams
	}

	rr := "true"
	otParams.SetReturnRecords(&rr)
	otParams.SetInfo(
		&models.GcpKms{
			KeyName:                params.KeyName,
			KeyRingLocation:        params.KeyRingLocation,
			KeyRingName:            params.KeyRingName,
			ProjectID:              params.ProjectID,
			ApplicationCredentials: params.ApplicationCredentials,
			Svm:                    &models.GcpKmsInlineSvm{Name: params.SvmName},
			PrivilegedAccount:      params.PrivilegedAccount,
		})
	return otParams
}

// GcpKms is a simple wrapper of models.GcpKms
type GcpKms struct {
	models.GcpKms
}

// GcpKmsPriv is a simple wrapper of models.GcpKms
type GcpKmsPriv struct {
	privmodels.GcpKms
}

// GcpKmsDeleteParams is the input param struct for securityClient.GcpKmsDelete
type GcpKmsDeleteParams struct {
	BaseParams
	UUID string
}

// GcpKmsModifyParams is the input param struct for securityClient.GcpKmsModify
type GcpKmsModifyParams struct {
	BaseParams
	UUID                   string
	ApplicationCredentials *log.Secret
}

// GcpKmsGetParams is the input param struct for securityClient.GcpKmsGet
type GcpKmsGetParams struct {
	BaseParams
	UUID string
}

func gcpKmsGetParamsToONTAP(params *GcpKmsGetParams) *security.GcpKmsGetParams {
	otParams := security.NewGcpKmsGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.UUID)
	otParams.SetFields(params.Fields)
	return otParams
}

// AggregateCollectionGetParams is the input param struct for storageClient.AggregateCollectionGet
type AggregateCollectionGetParams struct {
	BaseParams
	Name *string
}

func aggregateCollectionGetParamsToONTAP(params *AggregateCollectionGetParams) *storage.AggregateCollectionGetParams {
	otParams := storage.NewAggregateCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetName(params.Name)
	otParams.SetFields(params.Fields)
	return otParams
}

// Aggregate is a simple wrapper of models.Aggregate
type Aggregate struct {
	models.Aggregate
}

// IsHome return true if the aggregate is currently on its home node
func (a *Aggregate) IsHome() bool {
	if a.HomeNode == nil || a.HomeNode.UUID == nil || a.Node == nil || a.Node.UUID == nil {
		return false
	}

	return *a.HomeNode.UUID == *a.Node.UUID
}

// IsOnline returns true if an aggregate is online.
func (a *Aggregate) IsOnline() bool {
	if a.State == nil || *a.State != models.AggregateStateOnline {
		return false
	}

	return true
}

// AggregateModifyParams is the input param struct for storageClient.AggregateModify
type AggregateModifyParams struct {
	BaseParams
	UUID                     string
	TieringFullnessThreshold *int64
}

func aggregateModifyParamsToONTAP(params *AggregateModifyParams) *storage.AggregateModifyParams {
	otParams := storage.NewAggregateModifyParams()
	if params == nil {
		return otParams
	}
	otParams.SetUUID(params.UUID)
	otParams.SetInfo(&models.Aggregate{
		CloudStorage: &models.AggregateInlineCloudStorage{
			TieringFullnessThreshold: params.TieringFullnessThreshold,
		},
	})
	return otParams
}

func qosPolicyGroupCollectionModifyParamsToONTAP(qosPolicyGroupParams []*QosPolicyGroupModifyCollectionParams) *storage.QosPolicyModifyCollectionParams {
	otParams := storage.NewQosPolicyModifyCollectionParams()

	var qosPolicyList []*models.QosPolicy
	for _, qosPolicy := range qosPolicyGroupParams {
		if qosPolicy.UUID == "" {
			continue
		}
		throughput := int64(qosPolicy.Throughput)
		qosPolicyGroup := &models.QosPolicy{
			UUID: &qosPolicy.UUID,
			Fixed: &models.QosPolicyInlineFixed{
				MaxThroughputMbps: &throughput,
			},
		}

		qosPolicyList = append(qosPolicyList, qosPolicyGroup)
	}

	if len(qosPolicyList) == 0 {
		return otParams
	}
	otParams.SetInfo(storage.QosPolicyModifyCollectionBody{
		QosPolicyResponseInlineRecords: qosPolicyList,
	})
	continueOnFailure := "true"
	otParams.SetContinueOnFailure(&continueOnFailure)
	return otParams
}

// AggregateSimulate is a simple wrapper of models.Aggregate
type AggregateSimulate struct {
	models.AggregateSimulate
}

// QosPolicyModifyCollection is a simple wrapper of models.QosPolicyModifyCollection
type QosPolicyModifyCollection struct {
	models.QosPolicyJobLinkResponse
}

// QosPolicyGroupModifyCollectionParams is the input param struct for storageClient.VolumeModifyCollectionParams
type QosPolicyGroupModifyCollectionParams struct {
	BaseParams
	Throughput float64
	UUID       string
}

// VolumeModifyParams is the input param struct for storageClient.VolumeModify
type VolumeModifyParams struct {
	BaseParams
	UUID                           string
	QuotaEnabled                   *bool
	ReKey                          *bool
	SplitInitiated                 *bool
	MatchParentStorageTier         bool
	RestoreToSnapshotUUID          *string
	State                          *string
	Path                           *string
	SnapshotPolicyName             *string
	Movement                       *VolumeMovementParams
	Comment                        *string
	SecurityStyle                  *string
	UnixPermissions                *string
	SnapReserve                    *int
	MaxFiles                       *uint64
	MaxAutoSize                    *uint64
	Size                           *uint64
	LogicalSpaceEnforcement        *bool
	SnapshotDirectoryAccessEnabled *bool
	SetAtTimeEnabled               *bool
	TieringPolicy                  *string
	TieringMinimumCoolingDays      *int32
	CloudRetrievalPolicy           *string
	ExportPolicy                   *string
	QosPolicy                      *string
	AntiRansomwareState            *string
}

// FlexcacheModifyParams is the input param struct for storageClient.FlexcacheModify
type FlexcacheModifyParams struct {
	BaseParams
	UUID                       string
	PrepopulateDirPaths        []*string
	PrepopulateExcludeDirPaths []*string
	PrepopulateRecurse         *bool
	WritebackEnabled           *bool
	RelativeSizeEnabled        *bool
	RelativeSizePercentage     *int16
	AtimeScrubEnabled          *bool
	AtimeScrubPeriod           *int16
	CifsChangeNotifyEnabled    *bool
}

// VolumeMovementParams is the param struct which is a part of VolumeModifyParams
type VolumeMovementParams struct {
	VolumeMovementDestinationAggregate *VolumeMovementDestinationAggregate
	TieringPolicy                      *string
	State                              *string
}

// VolumeMovementDestinationAggregate is the param struct which is a part of VolumeMovementParams
type VolumeMovementDestinationAggregate struct {
	DestinationAggregateUUID *string
	DestinationAggregateName *string
}

func volumeModifyParamsToONTAP(params *VolumeModifyParams) *storage.VolumeModifyParams {
	otParams := storage.NewVolumeModifyParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.UUID)
	info := &models.Volume{}
	if params.QuotaEnabled != nil {
		info.Quota = &models.VolumeInlineQuota{Enabled: params.QuotaEnabled}
	}
	if params.ReKey != nil {
		info.Encryption = &models.VolumeInlineEncryption{Rekey: params.ReKey}
	}

	useReturnTimeout := true
	if params.SplitInitiated != nil {
		useReturnTimeout = false
		info.Clone = &models.VolumeInlineClone{
			SplitInitiated: params.SplitInitiated,
		}
		if params.MatchParentStorageTier {
			mpst := "true"
			otParams.CloneMatchParentStorageTier = &mpst
		}
	}
	if useReturnTimeout {
		otParams.SetReturnTimeout(&returnTimeout)
	}
	if params.State != nil {
		info.State = params.State
	}
	if params.SnapshotPolicyName != nil {
		info.SnapshotPolicy = &models.VolumeInlineSnapshotPolicy{Name: params.SnapshotPolicyName}
	}
	if params.Movement != nil {
		info.Movement = &models.VolumeInlineMovement{
			TieringPolicy: params.Movement.TieringPolicy,
			State:         params.Movement.State,
		}
		if params.Movement.VolumeMovementDestinationAggregate != nil {
			info.Movement.DestinationAggregate = &models.VolumeInlineMovementInlineDestinationAggregate{
				UUID: params.Movement.VolumeMovementDestinationAggregate.DestinationAggregateUUID,
				Name: params.Movement.VolumeMovementDestinationAggregate.DestinationAggregateName,
			}
		}
	}
	if params.Comment != nil {
		info.Comment = params.Comment
	}

	if params.Size != nil || params.LogicalSpaceEnforcement != nil || params.SnapReserve != nil || params.MaxAutoSize != nil {
		info.Space = &models.VolumeInlineSpace{}
		if params.Size != nil {
			info.Space.Size = nillable.ToPointer(utils.ConstrainedCastUint64(*params.Size))
		}
		if params.MaxAutoSize != nil {
			info.Autosize = &models.VolumeInlineAutosize{Maximum: nillable.ToPointer(utils.ConstrainedCastUint64(*params.MaxAutoSize))}
		}
		if params.LogicalSpaceEnforcement != nil {
			info.Space.LogicalSpace = &models.VolumeInlineSpaceInlineLogicalSpace{Enforcement: params.LogicalSpaceEnforcement}
		}
		if params.SnapReserve != nil {
			info.Space.Snapshot = &models.VolumeInlineSpaceInlineSnapshot{ReservePercent: nillable.ToPointer(int64(*params.SnapReserve))}
		}
	}

	if params.SnapshotDirectoryAccessEnabled != nil {
		info.SnapshotDirectoryAccessEnabled = params.SnapshotDirectoryAccessEnabled
	}

	if params.SetAtTimeEnabled != nil {
		info.AccessTimeEnabled = params.SetAtTimeEnabled
	}

	if params.TieringPolicy != nil || params.TieringMinimumCoolingDays != nil {
		var cool *int64

		if params.TieringMinimumCoolingDays != nil && *params.TieringMinimumCoolingDays != 0 {
			cool = nillable.ToPointer(int64(*params.TieringMinimumCoolingDays))
		}
		// skip assigning the cooling days if the policy is none
		if params.TieringPolicy != nil && *params.TieringPolicy == "none" {
			cool = nil
		}

		info.Tiering = &models.VolumeInlineTiering{
			Policy:         params.TieringPolicy,
			MinCoolingDays: cool,
		}
	}

	if params.CloudRetrievalPolicy != nil {
		info.CloudRetrievalPolicy = params.CloudRetrievalPolicy
	}

	if params.QosPolicy != nil {
		info.Qos = &models.VolumeInlineQos{
			Policy: &models.VolumeInlineQosInlinePolicy{
				Name: params.QosPolicy,
			},
		}
	}

	if params.RestoreToSnapshotUUID != nil {
		otParams.SetRestoreToSnapshotUUID(params.RestoreToSnapshotUUID)
	}
	if params.AntiRansomwareState != nil {
		info.AntiRansomware = &models.VolumeInlineAntiRansomware{State: params.AntiRansomwareState}
	}

	otParams.SetInfo(info)
	return otParams
}

// VolumeRevertParams is the input param struct for tunnelledStorageClient.VolumeRevert
type VolumeRevertParams struct {
	UUID                  string
	RestoreToSnapshotUUID string
}

// VolumeUnmountParams is the input params struct for tunnelledStorageClient.VolumeUnmount
type VolumeUnmountParams struct {
	UUID string
}

// VolumeMountParams is the input params struct for tunnelledStorageClient.VolumeMount
type VolumeMountParams struct {
	UUID         string
	JunctionPath string
}

// SnapshotCollectionGetParams is the input param struct for storageClient.SnapshotCollectionGet
type SnapshotCollectionGetParams struct {
	BaseParams
	VolumeUUID      string
	SvmName         string
	UUID            *string
	Name            *string
	SnapmirrorLabel *string
}

func snapshotCollectionGetParamsToONTAP(params *SnapshotCollectionGetParams) *storage.SnapshotCollectionGetParams {
	otParams := storage.NewSnapshotCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetFields(params.Fields)
	otParams.SetUUID(params.UUID)
	otParams.SetMaxRecords(nillable.ToStringPtr(params.MaxRecords))
	otParams.SetSnapmirrorLabel(params.SnapmirrorLabel)
	otParams.SetVolumeUUID(params.VolumeUUID)
	return otParams
}

// Snapshot is a simple wrapper of models.Snapshot
type Snapshot struct {
	models.Snapshot
}

// SnapshotCreateParams is the input param struct for storageClient.SnapshotCreate
type SnapshotCreateParams struct {
	VolumeUUID string
	Name       string
	Comment    *string
}

// SnapshotPolicyGetParams is the input param struct for storageClient.SnapshotPolicyGet
type SnapshotPolicyGetParams struct {
	BaseParams
	UUID string
}

func snapshotPolicyGetParamsToONTAP(params *SnapshotPolicyGetParams) *storage.SnapshotPolicyGetParams {
	otParams := storage.NewSnapshotPolicyGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetFields(params.Fields)
	otParams.SetUUID(params.UUID)
	return otParams
}

// SnapshotPolicyDeleteParams is the input param struct for storageClient.SnapshotPolicyDelete
type SnapshotPolicyDeleteParams struct {
	BaseParams
	Name string
}

// snapshotPolicyDeleteParamsToONTAPCollectionDelete converts SnapshotPolicyDeleteParams to ONTAP storage.SnapshotPolicyDeleteCollectionParams
func snapshotPolicyDeleteParamsToONTAPCollectionDelete(params *SnapshotPolicyDeleteParams) *storage.SnapshotPolicyDeleteCollectionParams {
	otParams := storage.NewSnapshotPolicyDeleteCollectionParams()
	if params == nil {
		return otParams
	}

	otParams.SetName(&params.Name)
	return otParams
}

// SnapshotPolicy is a simple wrapper of models.SnapshotPolicy
type SnapshotPolicy struct {
	models.SnapshotPolicy
}

// SnapshotPolicySchedule describes the schedules in SnapshotPolicyCreateParams
type SnapshotPolicySchedule struct {
	Prefix          string
	Count           int64
	SnapmirrorLabel string
	Name            string
	Months          []int
	DaysOfMonth     []int
	DaysOfWeek      []int
	Hours           []int
	Minutes         []int
}

// SnapshotPolicyCreateParams is the input param struct SnapshotPolicyCreate
type SnapshotPolicyCreateParams struct {
	BaseParams
	Name      *string
	Comment   *string
	Enabled   *bool
	Schedules []*SnapshotPolicySchedule
}

// SnapshotPolicyModifyParams is the input param struct SnapshotPolicyModify
type SnapshotPolicyModifyParams struct {
	BaseParams
	UUID    string
	Comment *string
	Enabled *bool
}

// convertSnapshotPolicyModifyParamsToOntap converts SnapshotPolicyCreateParams to ONTAP storage.SnapshotPolicyCreateParams
func convertSnapshotPolicyModifyParamsToOntap(params *SnapshotPolicyModifyParams) *storage.SnapshotPolicyModifyParams {
	otParams := storage.NewSnapshotPolicyModifyParams()
	if params == nil {
		return otParams
	}

	otParams.UUID = params.UUID
	otParams.Info = &models.SnapshotPolicy{
		Comment: params.Comment,
		Enabled: params.Enabled,
	}
	return otParams
}

// SnapshotPolicyScheduleCreateParams is the input param struct SnapshotPolicyScheduleCreate
type SnapshotPolicyScheduleCreateParams struct {
	SnapshotPolicyUUID string
	ScheduleName       string
	Count              int64
	SnapmirrorLabel    string
}

// convertSnapshotPolicyScheduleCreateParamsToONTAP converts SnapshotPolicyScheduleCreateParams to ONTAP storage.SnapshotPolicyScheduleCreateParams
func convertSnapshotPolicyScheduleCreateParamsToONTAP(params *SnapshotPolicyScheduleCreateParams) *storage.SnapshotPolicyScheduleCreateParams {
	otParams := storage.NewSnapshotPolicyScheduleCreateParams()
	if params == nil {
		return otParams
	}

	otParams.SnapshotPolicyUUID = params.SnapshotPolicyUUID
	count := params.Count
	otParams.Info = &models.SnapshotPolicySchedule{
		Count:           &count,
		SnapmirrorLabel: &params.SnapmirrorLabel,
		Prefix:          &params.SnapmirrorLabel,
		Schedule:        &models.SnapshotPolicyScheduleInlineSchedule{Name: &params.ScheduleName},
	}
	return otParams
}

// SnapshotPolicyScheduleModifyParams is the input param struct SnapshotPolicyScheduleModify
type SnapshotPolicyScheduleModifyParams struct {
	ScheduleUUID       string
	SnapshotPolicyUUID string
	Count              int
	SnapmirrorLabel    string
}

// convertSnapshotPolicyScheduleModifyParamsToONTAP converts SnapshotPolicyScheduleCreateParams to ONTAP storage.SnapshotPolicyScheduleCreateParams
func convertSnapshotPolicyScheduleModifyParamsToONTAP(params *SnapshotPolicyScheduleModifyParams) *storage.SnapshotPolicyScheduleModifyParams {
	otParams := storage.NewSnapshotPolicyScheduleModifyParams()
	if params == nil {
		return otParams
	}

	otParams.SnapshotPolicyUUID = params.SnapshotPolicyUUID
	otParams.ScheduleUUID = params.ScheduleUUID

	count := int64(params.Count)
	otParams.Info = &models.SnapshotPolicySchedule{
		Count:           &count,
		SnapmirrorLabel: &params.SnapmirrorLabel,
	}
	return otParams
}

// SnapshotPolicyScheduleDeleteParams is the input param struct SnapshotPolicyScheduleDelete
type SnapshotPolicyScheduleDeleteParams struct {
	ScheduleUUID       string
	SnapshotPolicyUUID string
}

// convertSnapshotPolicyScheduleDeleteParamsToONTAP converts SnapshotPolicyScheduleDeleteParams to ONTAP storage.SnapshotPolicyScheduleDeleteParams
func convertSnapshotPolicyScheduleDeleteParamsToONTAP(params *SnapshotPolicyScheduleDeleteParams) *storage.SnapshotPolicyScheduleDeleteParams {
	otParams := storage.NewSnapshotPolicyScheduleDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.ScheduleUUID = params.ScheduleUUID
	otParams.SnapshotPolicyUUID = params.SnapshotPolicyUUID
	return otParams
}

type SnapshotPolicyFindParams struct {
	Name   string
	Fields []string
}

// ScheduleCreateParams is the input param struct ScheduleCreate
type ScheduleCreateParams struct {
	Name        string
	Months      []int
	DaysOfMonth []int
	DaysOfWeek  []int
	Hours       []int
	Minutes     []int
}

// VolumeCollectionGetParams is the input param struct for storageClient.VolumeCollectionGet
type VolumeCollectionGetParams struct {
	BaseParams
	UUID    *string
	Name    *string
	SvmName *string
}

func volumeCollectionGetParamsToONTAP(params *VolumeCollectionGetParams) *storage.VolumeCollectionGetParams {
	otParams := storage.NewVolumeCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetFields(params.Fields)
	otParams.SetMaxRecords(nillable.ToStringPtr(params.MaxRecords))
	otParams.SetUUID(params.UUID)
	otParams.SetName(params.Name)
	otParams.SetSvmName(params.SvmName)
	return otParams
}

// Volume is a simple wrapper of models.Volume
type Volume struct {
	models.Volume
}

// Flexcache is a simple wrapper of models.Flexcache
type Flexcache struct {
	models.Flexcache
}

// VolumeGetParams is the input param struct for storageClient.VolumeGet
type VolumeGetParams struct {
	BaseParams
	UUID    string
	Name    string
	SvmName *string
}

func volumeGetParamsToONTAP(params *VolumeGetParams) *storage.VolumeGetParams {
	otParams := storage.NewVolumeGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetFields(params.Fields)
	otParams.SetUUID(params.UUID)
	return otParams
}

// SnapshotModifyParams is the input param struct for storageClient.SnapshotModify
type SnapshotModifyParams struct {
	UUID       string
	VolumeUUID string
	Name       string
}

// SnapshotGetParams is the input param struct for storageClient.SnapshotGet
type SnapshotGetParams struct {
	BaseParams
	UUID       string
	Name       string
	VolumeUUID string
}

// SnapshotDeleteParams is the input param struct for storageClient.SnapshotDelete
type SnapshotDeleteParams struct {
	UUID       string
	VolumeUUID string
}

// Svm is a simple wrapper of models.Svm
type Svm struct {
	models.Svm
}

// SvmGetParams is the input params struct for svm_client.SvmGet
type SvmGetParams struct {
	BaseParams
	SvmName string
}

func svmGetParamsToONTAP(params *SvmGetParams) *svm.SvmCollectionGetParams {
	otParams := svm.NewSvmCollectionGetParams()
	otParams.SetName(&params.SvmName)
	otParams.SetFields(params.Fields)
	return otParams
}

// SvmGetCollectionParams is the input params struct for svm_client.SvmCollectionGet
type SvmGetCollectionParams struct {
	BaseParams
	SvmName     *string
	IpspaceName *string
}

// SvmPeer represents an svm peer
type SvmPeer struct {
	models.SvmPeer
}

// NsSwitchSource contains slice of nsSwitchSource db values
type NsSwitchSource struct {
	NsSwitchSourceGroup    []*models.NsswitchSource
	NsSwitchSourcePasswd   []*models.NsswitchSource
	NsSwitchSourceNetgroup []*models.NsswitchSource
	NsSwitchSourceNamemap  []*models.NsswitchSource
}

// SvmModifyParams is the input params struct for svm_client.SvmModify
type SvmModifyParams struct {
	BaseParams
	SvmUUID              string
	NsSwitch             *NsSwitchSource
	NfsAllowed           *bool
	CifsAllowed          *bool
	IscsiAllowed         *bool
	RetentionPeriodHours *int64
}

func svmModifyParamsToOntap(params *SvmModifyParams) *svm.SvmModifyParams {
	otParams := svm.NewSvmModifyParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.SvmUUID)
	svmParam := &models.Svm{
		RetentionPeriod: params.RetentionPeriodHours,
	}

	if params.NsSwitch != nil {
		nsSwitchParams := &models.SvmInlineNsswitch{}
		if params.NsSwitch.NsSwitchSourceGroup != nil {
			nsSwitchParams.Group = params.NsSwitch.NsSwitchSourceGroup
		}
		if params.NsSwitch.NsSwitchSourcePasswd != nil {
			nsSwitchParams.Passwd = params.NsSwitch.NsSwitchSourcePasswd
		}
		if params.NsSwitch.NsSwitchSourceNamemap != nil {
			nsSwitchParams.Namemap = params.NsSwitch.NsSwitchSourceNamemap
		}
		if params.NsSwitch.NsSwitchSourceNetgroup != nil {
			nsSwitchParams.Netgroup = params.NsSwitch.NsSwitchSourceNetgroup
		}
		svmParam.Nsswitch = nsSwitchParams
	}
	if params.CifsAllowed != nil {
		svmParam.Cifs = &models.SvmInlineCifs{Allowed: params.CifsAllowed}
	}
	if params.NfsAllowed != nil {
		svmParam.Nfs = &models.SvmInlineNfs{Allowed: params.NfsAllowed}
	}

	otParams.SetInfo(svmParam)
	otParams.SetReturnTimeout(&returnTimeout)
	return otParams
}

// ClusterPeerCreateParams is the input parameter for cluster peer create
type ClusterPeerCreateParams struct {
	Name               string
	IPAddresses        []string
	GeneratePassphrase bool
	IPSpace            string
	ExpiryTime         *string
	Passphrase         *string
}

// ClusterPeer is a simple wrapper of models.ClusterPeer
type ClusterPeer struct {
	models.ClusterPeer
}

// ClusterPeerResponse will represent the response from ListClusterPeer endpoint
type ClusterPeerResponse struct {
	IPAddresses         []string
	PeerClusterName     string
	AuthenticationState string
	Availability        string
	UUID                string
	ExpiryTime          string
}

// ClusterPeerCreateResponse will represent the response from cluster peer creation
type ClusterPeerCreateResponse struct {
	GeneratedPassphrase *string
	ClusterPeerUUID     string
	ExpiryTime          *strfmt.DateTime
}

// VolumeDeleteParams describes the params to invoke volume Delete
type VolumeDeleteParams struct {
	UUID string
	Name string
}

func volumeDeleteParamsToONTAP(params *VolumeDeleteParams) *storage.VolumeDeleteParams {
	otParams := storage.NewVolumeDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.UUID)
	force := "true"
	otParams.SetForce(&force)
	otParams.SetReturnTimeout(&returnTimeout)
	return otParams
}

func volumeDeleteParamsToONTAPCollectionDelete(params *VolumeDeleteParams) *storage.VolumeDeleteCollectionParams {
	otParams := storage.NewVolumeDeleteCollectionParams()
	if params == nil {
		return otParams
	}

	otParams.SetName(&params.Name)
	otParams.SetForce(nillable.ToPointer("true"))
	otParams.SetReturnTimeout(&returnTimeout)
	return otParams
}

// ServerRootCACertificate is a simple wrapper of models.SecurityCertificate
type ServerRootCACertificate struct {
	models.SecurityCertificate
}

// ServerRootCAGetParams is the input param struct for securityClient.SecurityCertificateCollectionGet
type ServerRootCAGetParams struct {
	BaseParams
	Name            *string
	CertificateType *string
	SvmName         *string
}

// ServerRootCAGetCollectionParams is the input param struct for securityClient.SecurityCertificateCollectionGet
type ServerRootCAGetCollectionParams struct {
	BaseParams
	CertificateType *string
	SvmName         *string
	Name            *string
}

// ServerRootCAGenerateParams is the input param struct for securityClient.ServerRootCAGenerateParams
type ServerRootCAGenerateParams struct {
	BaseParams
	SvmName         *string
	CertificateType *string
	CommonName      *string
	Name            *string
	KeySize         *int64
}

// ServerRootCAInstallParams is the input param struct for securityClient.ServerRootCAInstallParams
type ServerRootCAInstallParams struct {
	BaseParams
	PrivateKey      *string
	Certificate     *string
	SvmName         *string
	CertificateType *string
	CommonName      *string
	Name            *string
}

// ServerRootCADeleteParams is the input param struct for securityClient.ServerRootCADeleteParams
type ServerRootCADeleteParams struct {
	UUID                 *string
	SvmName              *string
	SerialNumber         *string
	CommonName           *string
	CertificateAuthority *string
}

// SnapmirrorRelationshipCreateParams describes the params to invoke snapmirror relationship create
type SnapmirrorRelationshipCreateParams struct {
	DestinationPath string
	SourcePath      string
	Policy          string
	Schedule        *string
	AccessToken     *string
}

// SnapmirrorRelationshipDeleteParams describes the params to invoke snapmirror relationship delete
type SnapmirrorRelationshipDeleteParams struct {
	DestinationOnly *bool
	SourceOnly      *bool
	UUID            string
}

// SnapmirrorRelationshipReleaseParams describes the params to invoke snapmirror relationship release
type SnapmirrorRelationshipReleaseParams struct {
	SourceInfoOnly *bool
	UUID           string
}

// SnapmirrorRelationshipModifyParams represents snapmirror relationship modify parameters
type SnapmirrorRelationshipModifyParams struct {
	UUID             string
	TransferSchedule *string
}

// SnapmirrorRelationship represents a snapmirror relationship object
type SnapmirrorRelationship struct {
	models.SnapmirrorRelationship
}

// SnapmirrorRelationshipListParams represents snapmirror relationship list parameters
type SnapmirrorRelationshipListParams struct {
	DestinationPath string
	SourcePath      string
}

// SnapmirrorRelationshipListDestinationsParams represents snapmirror relationship list destination parameters
type SnapmirrorRelationshipListDestinationsParams struct {
	DestinationPath    *string
	SourcePath         *string
	DestinationSVMName *string
	SourceSVMName      *string
}

// SnapmirrorRelationshipGetParams represents snapmirror relationship get parameters
type SnapmirrorRelationshipGetParams struct {
	UUID            string
	DestinationPath *string
	SourcePath      *string
}

// SnapmirrorPolicyDeleteCollectionParams is the input param struct for storageClient.
type SnapmirrorPolicyDeleteCollectionParams struct {
	BaseParams
	Name    string
	SvmUUID string
}

// SnapmirrorRelationshipResyncParams describes the params to invoke snapmirror relationship resync
type SnapmirrorRelationshipTransferCreateParams struct {
	UUID         string
	SnapshotName string
	AccessToken  *string
}

// SnapmirrorRelationshipTransferGetParams describes the params to invoke snapmirror relationship transfer get
type SnapmirrorRelationshipTransferGetParams struct {
	SnapmirrorUUID string
	SnapshotName   string
}

// SnapmirrorCloudEndpointDeleteParams describes the params to invoke Snapmirror Cloud Endpoint Delete
type SnapmirrorCloudEndpointDeleteParams struct {
	ObjectStoreUUID string
	EndpointUUID    string
}

// SnapmirrorCloudSnapshotDeleteParams describes the params to invoke Snapmirror Cloud Snapshot Delete
type SnapmirrorCloudSnapshotDeleteParams struct {
	ObjectStoreUUID string
	EndpointUUID    string
	SnapshotUUID    string
}

// NetworkIPDefaultRouteCreateParams describes the params to invoke Network Route Creation
type NetworkIPDefaultRouteCreateParams struct {
	IPSpace string
	SvmName string
	Gateway string
	Timeout *time.Duration
}

func networkIPRouteCreateParamsToONTAP(params *NetworkIPDefaultRouteCreateParams) *networking.NetworkIPRoutesCreateParams {
	otParams := networking.NewNetworkIPRoutesCreateParams()

	info := &models.NetworkRoute{
		Destination: &models.IPInfo{
			Address: nillable.ToPointer(models.IPAddress("0.0.0.0")),
			Netmask: nillable.ToPointer(models.IPNetmask("0")),
		},
		Gateway: nillable.ToPointer(params.Gateway),
		Metric:  nillable.ToPointer(int64(20)),
	}

	if params.IPSpace != "" {
		info.Ipspace = &models.NetworkRouteInlineIpspace{
			Name: nillable.ToPointer(params.IPSpace),
		}
	}

	if params.SvmName != "" {
		info.Svm = &models.NetworkRouteInlineSvm{
			Name: nillable.ToPointer(params.SvmName),
		}
	}

	otParams.SetInfo(info)
	if params.Timeout != nil {
		otParams.SetTimeout(*params.Timeout)
	}
	return otParams
}

// NetworkEthernetBroadcastDomainsGetParams describes the params to invoke network ethernet port get
type NetworkEthernetBroadcastDomainsGetParams struct {
	BaseParams
	Name    string
	IPSpace string
}

func networkEthernetBroadcastDomainsGetParamsToONTAP(params *NetworkEthernetBroadcastDomainsGetParams) *networking.NetworkEthernetBroadcastDomainsGetParams {
	otParams := networking.NewNetworkEthernetBroadcastDomainsGetParams()
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	otParams.SetFields(params.Fields)
	if params.IPSpace != "" {
		otParams.SetIpspaceName(&params.IPSpace)
	}
	if params.Name != "" {
		otParams.SetName(&params.Name)
	}
	return otParams
}

// BroadcastDomain is a simple wrapper of models.BroadcastDomain
type BroadcastDomain struct {
	models.BroadcastDomain
}

// NetworkEthernetBroadcastDomainCreateParams is the params for NetworkEthernetBroadcastDomainCreate call
type NetworkEthernetBroadcastDomainCreateParams struct {
	Name    string
	IPSpace string
}

// NetworkEthernetBroadcastDomainDeleteParams describes the params to invoke network ethernet Broadcast Domain delete
type NetworkEthernetBroadcastDomainDeleteParams struct {
	Name string
}

func networkEthernetBroadcastDomainDeleteParamsToONTAP(params *NetworkEthernetBroadcastDomainDeleteParams) *networking.NetworkEthernetBroadcastDomainDeleteCollectionParams {
	otParams := networking.NewNetworkEthernetBroadcastDomainDeleteCollectionParams()
	otParams.WithName(&params.Name)
	return otParams
}

// IpspaceDeleteParams describes the params to invoke ipspace delete
type IpspaceDeleteParams struct {
	Name string
}

func ipspaceDeleteParamsToONTAP(params *IpspaceDeleteParams) *networking.IpspaceDeleteCollectionParams {
	otParams := networking.NewIpspaceDeleteCollectionParams()
	otParams.WithName(&params.Name)
	return otParams
}

// Role is a simple wrapper of models.Role
type Role struct {
	models.Role
}

// RolePrivilege describes the privilege level of a Role
type RolePrivilege struct {
	Path   string
	Access string
	Query  string
}

// RoleGetParams is the input param struct for securityClient.RoleGet
type RoleGetParams struct {
	BaseParams
	Name      string
	OwnerUUID *string
}

// RoleCreateParams is the input param struct for securityClient.RoleCreate
type RoleCreateParams struct {
	Name       string
	Privileges []*RolePrivilege
}

// RolePrivilegeModifyParams is the input param struct for securityClient.RoleModify
type RolePrivilegeModifyParams struct {
	OwnerID string
	Name    string
	Access  string
	Query   string
	Path    string
}

// QosPolicy is the model for QosPolicy
type QosPolicy struct {
	models.QosPolicy
}

// QosPolicyDeleteCollectionParams is the input params for storageClient.QosPolicyDeleteCollection
type QosPolicyDeleteCollectionParams struct {
	Name *string
}

// QosPolicyCreateParams is the input params for storageClient.QosPolicyCreate
type QosPolicyCreateParams struct {
	CapacityShared    *bool
	MaxThroughputMbps *int64
	MinThroughputMbps *int64
	Name              *string
	SvmUUID           *string
}

// QosPolicyGroupCollectionGetParams is the input params for storageClient.QosPolicyGroupCollectionGet
type QosPolicyGroupCollectionGetParams struct {
	BaseParams
	Name string
}

func qosPolicyGroupCollectionGetParamsToONTAPCollectionGet(params *QosPolicyGroupCollectionGetParams) *storage.QosPolicyCollectionGetParams {
	otParams := storage.NewQosPolicyCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetFields(params.Fields)
	otParams.SetName(&params.Name)
	return otParams
}

// SvmCreateParams is the params to create a svm
type SvmCreateParams struct {
	Name      string
	IPSpace   string
	Protocols Protocols
}

type Protocols struct {
	EnableIscsi bool
}

// RestoreFromSnapshotParams contains parameters for restoring a volume from a snapshot
type RestoreFromSnapshotParams struct {
	ParentVolumeExternalUUID string // External UUID of the source/parent volume
	ParentVolumeName         string // Name of the Volume
	SnapshotUUID             string // UUID of the snapshot to restore from
	SnapshotName             string // Name of the snapshot to restore from
	ParentVolumeSvmName      string // Name of the SVM where the parent volume resides
	// Add more fields as needed
}

// VolumeCreateParams is the params to create a volume
type VolumeCreateParams struct {
	Aggregates                     []string
	ConstituentsPerAggregate       *int64
	Name                           string
	Comment                        string
	Type                           string
	Size                           int64
	QosPolicy                      string
	SnapshotPolicy                 string
	ExportPolicy                   string
	SecurityStyle                  string
	SnapshotReservePercent         int64
	TieringPolicy                  string
	MinCoolingDays                 int64
	CloudRetrievalPolicy           string
	JunctionPath                   string
	SnapshotDirectoryAccessEnabled bool
	Encrypt                        bool
	UnixPermissions                *string
	Language                       *string
	Svm                            string
	RestoreFromSnapshot            *RestoreFromSnapshotParams
}

const (
	VolumeStateOnline = "online"
	GuaranteeTypeNone = "none"
)

func volumeCreateFromSnapshotParamsToONTAP(params *VolumeCreateParams) *storage.VolumeCreateParams {
	otParams := storage.NewVolumeCreateParams()

	otParams.SetInfo(&models.Volume{
		Name: &params.Name,
		Type: &params.Type,
		Svm: &models.VolumeInlineSvm{
			Name: &params.Svm,
		},
		Guarantee: &models.VolumeInlineGuarantee{
			Type: nillable.ToPointer(GuaranteeTypeNone),
		},
		Space: &models.VolumeInlineSpace{
			Snapshot: &models.VolumeInlineSpaceInlineSnapshot{
				ReservePercent: &params.SnapshotReservePercent,
			},
			LogicalSpace: &models.VolumeInlineSpaceInlineLogicalSpace{
				Enforcement: nillable.ToPointer(true),
				Reporting:   nillable.ToPointer(true),
			},
		},
		Autosize: &models.VolumeInlineAutosize{
			Mode: nillable.ToPointer("off"),
		},
		Clone: &models.VolumeInlineClone{
			ParentSnapshot: &models.SnapshotReference{
				Name: &params.RestoreFromSnapshot.SnapshotName,
			},
			ParentSvm: &models.VolumeInlineCloneInlineParentSvm{
				Name: &params.RestoreFromSnapshot.ParentVolumeSvmName,
			},
			ParentVolume: &models.VolumeInlineCloneInlineParentVolume{
				Name: &params.RestoreFromSnapshot.ParentVolumeName,
			},
			IsFlexclone: nillable.ToPointer(true),
		},
	})

	otParams.SetReturnTimeout(&returnTimeout)
	otParams.SetReturnRecords(nillable.ToPointer("true"))

	return otParams
}

func volumeCreateParamsToONTAP(params *VolumeCreateParams) *storage.VolumeCreateParams {
	if params.RestoreFromSnapshot != nil {
		return volumeCreateFromSnapshotParamsToONTAP(params)
	}

	otParams := storage.NewVolumeCreateParams()
	otParams.SetInfo(&models.Volume{
		Name:  &params.Name,
		Type:  &params.Type,
		State: nillable.ToPointer(VolumeStateOnline),
		Size:  &params.Size,
		Svm: &models.VolumeInlineSvm{
			Name: &params.Svm,
		},
		Guarantee: &models.VolumeInlineGuarantee{
			Type: nillable.ToPointer(GuaranteeTypeNone),
		},
		Space: &models.VolumeInlineSpace{
			Snapshot: &models.VolumeInlineSpaceInlineSnapshot{
				ReservePercent: &params.SnapshotReservePercent,
			},
			LogicalSpace: &models.VolumeInlineSpaceInlineLogicalSpace{
				Enforcement: nillable.ToPointer(true),
				Reporting:   nillable.ToPointer(true),
			},
		},
		Autosize: &models.VolumeInlineAutosize{
			Mode: nillable.ToPointer("off"),
		},
		SnapshotPolicy: &models.VolumeInlineSnapshotPolicy{
			Name: &params.SnapshotPolicy,
		},
	})

	for _, aggregate := range params.Aggregates {
		otParams.Info.VolumeInlineAggregates = append(otParams.Info.VolumeInlineAggregates,
			&models.VolumeInlineAggregatesInlineArrayItem{
				Name: nillable.ToPointer(aggregate),
			})
	}

	otParams.SetReturnTimeout(&returnTimeout)
	otParams.SetReturnRecords(nillable.ToPointer("true"))
	return otParams
}

// NetworkIPServicePoliciesGetParams is the input parameter for getting ip service policies
type NetworkIPServicePoliciesGetParams struct {
	SvmName *string
	Name    *string
}

// IPServicePolicyCreateParams is the input parameter for ip service policy creation
type IPServicePolicyCreateParams struct {
	IPServicePolicyInlineServices []*string
	Name                          *string
	Scope                         *string
	SvmName                       *string
}

// IPServicePolicyModifyParams is the input parameter for modifying an IP service policy
type IPServicePolicyModifyParams struct {
	UUID     string
	Services []string
}

// NetworkIPInterfaceModifyParams is the input parameter for modifying the network ip interface
type NetworkIPInterfaceModifyParams struct {
	ServicePolicyName *string
	UUID              *string
}

// NetworkIPInterfacesDeleteParams is the input parameter for deleting network ip interfaces
type NetworkIPInterfacesDeleteParams struct {
	SvmName *string
	Name    *string
}

// NameMapping is a simple wrapper of models.NameMapping
type NameMapping struct {
	models.NameMapping
}

// NameMappingDeleteParams is the input params for name_services.NameMappingDeleteParams
type NameMappingDeleteParams struct {
	Direction string
	Index     int64
	SvmUUID   string
}

// NameMappingCreateParams is the input params for name_services.NameMappingCreateParams
type NameMappingCreateParams struct {
	BaseParams
	SvmUUID     *string
	Pattern     *string
	Replacement *string
	Direction   *string
	Index       int64
	SvmName     *string
}

// NameMappingModifyParams is the input params for name_services.NameMappingUpdateParams
type NameMappingModifyParams struct {
	BaseParams
	Direction string
	Index     int64
	SwapIndex *int64
	SvmUUID   string
	Body      *NameMappingModifyBodyParam
}

// NameMappingModifyBodyParam is the info param for name_services.NameMappingUpdateParams
type NameMappingModifyBodyParam struct {
	Direction   *string
	Index       *int64
	Pattern     *string
	Replacement *string
}

// NameMappingCollectionGetParams is the input params for name_services.NameMappingCollectionGetParams
type NameMappingCollectionGetParams struct {
	BaseParams
	SvmUUID     *string
	Pattern     *string
	Replacement *string
	Direction   *string
	SvmName     *string
}

// Iscsi is the iscsi service
type Iscsi struct {
	models.IscsiService
}

// IscsiGetParams is the input parameter for getting the iscsi service
type IscsiGetParams struct {
	BaseParams
	SvmUUID string
}

func iscsiServiceGetParamsToONTAP(params *IscsiGetParams) *san.IscsiServiceCollectionGetParams {
	otParams := san.NewIscsiServiceCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetSvmUUID(&params.SvmUUID)
	otParams.SetFields(params.Fields)

	// MD: It's a GET call why is there return_timeout ??
	otParams.SetReturnTimeout(returnTimeoutNoJob)
	return otParams
}

// IscsiCreateParams is the input parameter for creating the iscsi service
type IscsiCreateParams struct {
	BaseParams
	SvmUUID string
	// TODO models.IscsiServiceInlineSvm needs to support target-alias
	// TargetAlias string
}

func iscsiServiceCreateParamsToONTAP(params *IscsiCreateParams) *san.IscsiServiceCreateParams {
	otParams := san.NewIscsiServiceCreateParams()
	if params == nil {
		return otParams
	}

	otParams.SetInfo(&models.IscsiService{
		Svm: &models.IscsiServiceInlineSvm{
			UUID: &params.SvmUUID,
		},
	})
	return otParams
}

func convertListClusterPeerFromREST(resp *cluster.ClusterPeerCollectionGetOK) []*ClusterPeerResponse {
	var clusterPeers []*ClusterPeerResponse
	for _, peer := range resp.Payload.ClusterPeerResponseInlineRecords {
		var ipAddresses []string
		for _, ipAddress := range peer.Remote.IPAddresses {
			ipAddresses = append(ipAddresses, string(*ipAddress))
		}
		clusterPeer := ClusterPeerResponse{
			UUID:                nillable.FromPointer(peer.UUID),
			PeerClusterName:     nillable.FromPointer(peer.Remote.Name),
			AuthenticationState: nillable.FromPointer(peer.Authentication.State),
			Availability:        nillable.FromPointer(peer.Status.State),
			IPAddresses:         ipAddresses,
			ExpiryTime:          nillable.FromPointer(peer.Authentication.ExpiryTime),
		}
		clusterPeers = append(clusterPeers, &clusterPeer)
	}
	return clusterPeers
}

func convertClusterPeerCreateFromREST(created *priv.ClusterPeerCreateCreated) *ClusterPeerCreateResponse {
	uuid := ""
	if created.Payload.ClusterPeerResponseInlineRecords[0].Links.Self != nil {
		theLink := created.Payload.ClusterPeerResponseInlineRecords[0].Links.Self.Href
		parts := strings.Split(*theLink, "/")

		uuid = parts[len(parts)-1]
	}

	clusterPeerResponse := &ClusterPeerCreateResponse{
		ClusterPeerUUID: uuid,
	}
	if created.Payload.ClusterPeerResponseInlineRecords[0].Authentication != nil && created.Payload.ClusterPeerResponseInlineRecords[0].Authentication.Passphrase != nil {
		clusterPeerResponse.GeneratedPassphrase = created.Payload.ClusterPeerResponseInlineRecords[0].Authentication.Passphrase
	}
	if created.Payload.ClusterPeerResponseInlineRecords[0].Authentication != nil && created.Payload.ClusterPeerResponseInlineRecords[0].Authentication.ExpiryTime != nil {
		clusterPeerResponse.ExpiryTime = created.Payload.ClusterPeerResponseInlineRecords[0].Authentication.ExpiryTime
	}
	return clusterPeerResponse
}

func clusterPeerIDToONTAPDelete(clusterPeerID string, timeout time.Duration) *cluster.ClusterPeerDeleteParams {
	otDeleteParams := cluster.ClusterPeerDeleteParams{}
	otDeleteParams.SetTimeout(timeout)
	return otDeleteParams.WithUUID(clusterPeerID)
}

func clusterPeerIDToONTAPGet(clusterPeerID string) *cluster.ClusterPeerGetParams {
	otGetParams := cluster.NewClusterPeerGetParams()
	otGetParams.SetUUID(clusterPeerID)
	return otGetParams
}

func convertClusterPeerFromREST(resp *cluster.ClusterPeerGetOK) *ClusterPeerResponse {
	if resp == nil {
		return nil
	}
	peer := resp.Payload
	if peer.Remote == nil {
		return nil
	}
	var ipAddresses []string
	for _, ipAddress := range peer.Remote.IPAddresses {
		ipAddresses = append(ipAddresses, string(*ipAddress))
	}
	clusterPeer := ClusterPeerResponse{
		UUID:                nillable.FromPointer(peer.UUID),
		PeerClusterName:     nillable.FromPointer(peer.Remote.Name),
		AuthenticationState: nillable.FromPointer(peer.Authentication.State),
		Availability:        nillable.FromPointer(peer.Status.State),
		IPAddresses:         ipAddresses,
		ExpiryTime:          nillable.FromPointer(peer.Authentication.ExpiryTime),
	}
	return &clusterPeer
}

func clusterPeerToONTAPCreate(params ClusterPeerCreateParams) *priv.ClusterPeerCreateParams {
	otParams := priv.NewClusterPeerCreateParams()
	var ipAddresses []*privmodels.IPAddress
	for _, address := range params.IPAddresses {
		ipAddresses = append(ipAddresses, nillable.ToPointer(privmodels.IPAddress(address)))
	}

	clusterPeer := &privmodels.ClusterPeer{
		Name: &params.Name,
		Remote: &privmodels.ClusterPeerInlineRemote{
			IPAddresses: ipAddresses,
		},
		Authentication: &privmodels.ClusterPeerInlineAuthentication{
			GeneratePassphrase: &params.GeneratePassphrase,
			ExpiryTime:         params.ExpiryTime,
			Passphrase:         params.Passphrase,
		},
		Ipspace: &privmodels.ClusterPeerInlineIpspace{
			Name: &params.IPSpace,
		},
	}

	otParams.SetReturnRecords(nillable.ToPointer(true))
	otParams.SetInfo(clusterPeer)
	return otParams
}

func clusterPeerToONTAPAccept(params ClusterPeerCreateParams) *priv.ClusterPeerCreateParams {
	otParams := priv.NewClusterPeerCreateParams()
	var ipAddresses []*privmodels.IPAddress
	for _, address := range params.IPAddresses {
		ipAddresses = append(ipAddresses, nillable.ToPointer(privmodels.IPAddress(address)))
	}

	clusterPeer := &privmodels.ClusterPeer{
		Name: &params.Name,
		Remote: &privmodels.ClusterPeerInlineRemote{
			IPAddresses: ipAddresses,
		},
		Authentication: &privmodels.ClusterPeerInlineAuthentication{
			ExpiryTime: params.ExpiryTime,
			Passphrase: params.Passphrase,
		},
		Ipspace: &privmodels.ClusterPeerInlineIpspace{
			Name: &params.IPSpace,
		},
	}

	otParams.SetReturnRecords(nillable.ToPointer(true))
	otParams.SetInfo(clusterPeer)
	return otParams
}

// Lun is the lun
type Lun struct {
	models.Lun
}

// LunCreateParams is the input parameter for creating a Lun
type LunCreateParams struct {
	SvmName                        string
	Name                           string
	OsType                         string
	VolumeName                     string
	Size                           int64
	ThinProvisioningSupportEnabled *bool
}

const lunNamePrefix = "/vol/"

// lunCreateParamsToONTAP converts LunCreateParams to ONTAP API parameters.
func lunCreateParamsToONTAP(params *LunCreateParams) *san.LunCreateParams {
	otParams := san.NewLunCreateParams()
	if params == nil {
		return otParams
	}

	otParams.SetInfo(&models.Lun{
		Svm:    &models.LunInlineSvm{Name: &params.SvmName},
		Name:   constructLunName(&params.VolumeName, &params.Name),
		OsType: &params.OsType,
		Location: &models.LunInlineLocation{
			Volume: &models.LunInlineLocationInlineVolume{Name: &params.VolumeName},
		},
		Space: &models.LunInlineSpace{
			Size:                               &params.Size,
			ScsiThinProvisioningSupportEnabled: params.ThinProvisioningSupportEnabled,
		},
	})
	otParams.SetReturnTimeout(&returnTimeout)
	otParams.SetReturnRecords(nillable.ToPointer("true"))
	return otParams
}

// LunUpdateParams is the input parameter for updating a Lun
type LunUpdateParams struct {
	UUID       string
	SvmName    string
	Name       string
	VolumeName string
	Size       int64
}

// lunModifyParamsToONTAP converts LunModifyParams to ONTAP API parameters.
func lunModifyParamsToONTAP(params *LunUpdateParams) *san.LunModifyParams {
	otParams := san.NewLunModifyParams()
	if params == nil {
		return otParams
	}
	otParams.SetInfo(&models.Lun{
		Name: constructLunName(&params.VolumeName, &params.Name),
		Space: &models.LunInlineSpace{
			Size: &params.Size,
		},
	})
	otParams.UUID = params.UUID
	otParams.SetReturnTimeout(&returnTimeout)
	return otParams
}

type LunMap struct {
	models.LunMap
}

// LunMapCreateParams is the input parameter for creating a LunMap
type LunMapCreateParams struct {
	IGroupName string
	LunName    string
	SvmName    string
}

// LunMapDeleteParams is the input parameter for deleting a LunMap
type LunMapDeleteParams struct {
	IGroupUUID string
	LunUUID    string
}

// lunMapCreateParamsToONTAP converts LunMapCreateParams to ONTAP API parameters.
func lunMapCreateParamsToONTAP(params *LunMapCreateParams) *san.LunMapCreateParams {
	otParams := san.NewLunMapCreateParams()
	if params == nil {
		return otParams
	}

	otParams.SetInfo(&models.LunMap{
		Igroup: &models.LunMapInlineIgroup{
			Name: &params.IGroupName,
		},
		Lun: &models.LunMapInlineLun{
			Name: &params.LunName,
		},
		Svm: &models.LunMapInlineSvm{
			Name: &params.SvmName,
		},
	})
	return otParams
}

// lunMapDeleteParamsToONTAP converts LunMapDeleteParams to ONTAP API parameters.
func lunMapDeleteParamsToONTAP(params *LunMapDeleteParams) *san.LunMapDeleteParams {
	otParams := san.NewLunMapDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.SetLunUUID(params.LunUUID)
	otParams.SetIgroupUUID(params.IGroupUUID)

	return otParams
}

// LunMapGetParams is the input parameter for getting a LunMap
type LunMapGetParams struct {
	BaseParams
	LunUUID string
}

type LunGetParams struct {
	BaseParams
	SvmName    *string
	VolumeName *string
	LunName    *string
}

// lunGetParamsToONTAP converts LunGetParams to ONTAP API parameters.
func lunGetParamsToONTAP(params *LunGetParams) *san.LunCollectionGetParams {
	otParams := san.NewLunCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetSvmName(params.SvmName)
	otParams.SetLocationVolumeName(params.VolumeName)
	otParams.SetName(constructLunName(params.VolumeName, params.LunName))
	otParams.SetFields(params.Fields)
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	return otParams
}

func constructLunName(volumeName, lunName *string) *string {
	if volumeName == nil || lunName == nil {
		return nil
	}
	return nillable.ToPointer(lunNamePrefix + *volumeName + "/" + *lunName)
}

// Igroup is the igroup
type Igroup struct {
	models.Igroup
}

// IgroupCreateParams is the input parameter for creating an Igroup
type IgroupCreateParams struct {
	SvmName    string
	Name       string
	OsType     string
	Initiators []string
	JobID      string
}

// igroupCreateParamsToONTAP converts IgroupCreateParams to ONTAP API parameters.
func igroupCreateParamsToONTAP(params *IgroupCreateParams) *san.IgroupCreateParams {
	otParams := san.NewIgroupCreateParams()
	if params == nil {
		return otParams
	}

	initiators := make([]*models.IgroupInlineInitiatorsInlineArrayItem, len(params.Initiators))
	for i := range params.Initiators {
		initiators[i] = &models.IgroupInlineInitiatorsInlineArrayItem{
			Name: &params.Initiators[i],
		}
	}

	otParams.SetInfo(&models.Igroup{
		Comment:                &params.JobID,
		IgroupInlineInitiators: initiators,
		Name:                   &params.Name,
		OsType:                 &params.OsType,
		Protocol:               nillable.ToPointer(models.IgroupProtocolIscsi),
		Svm:                    &models.IgroupInlineSvm{Name: &params.SvmName},
	})

	otParams.SetReturnRecords(nillable.ToPointer("true"))
	return otParams
}

// IgroupAddInitiatorParams is the input parameter for modifying an IgroupInitiators
type IgroupAddInitiatorParams struct {
	Name         string
	InitiatorQNs []string
	IgroupUUID   string
}

// IgroupDeleteInitiatorParams is the input parameter for deleting an IgroupInitiator
type IgroupDeleteInitiatorParams struct {
	InitiatorIQNName string
	IgroupUUID       string
}

// igroupAddInitiatorParamsToONTAP converts IgroupAddInitiatorParams to ONTAP API parameters.
func igroupAddInitiatorParamsToONTAP(params *IgroupAddInitiatorParams) *san.IgroupInitiatorCreateParams {
	otParams := san.NewIgroupInitiatorCreateParams()
	if params == nil {
		return otParams
	}

	initiators := make([]*models.IgroupInitiatorInlineRecordsInlineArrayItem, len(params.InitiatorQNs))
	for i := range params.InitiatorQNs {
		initiators[i] = &models.IgroupInitiatorInlineRecordsInlineArrayItem{
			Name: &params.InitiatorQNs[i],
		}
	}

	otParams.SetInfo(&models.IgroupInitiator{
		IgroupInitiatorInlineRecords: initiators,
	})
	otParams.SetIgroupUUID(params.IgroupUUID)
	return otParams
}

// igroupDeleteInitiatorParamsToONTAP converts IgroupAddInitiatorParams to ONTAP API parameters.
func igroupDeleteInitiatorParamsToONTAP(params *IgroupDeleteInitiatorParams) *san.IgroupInitiatorDeleteParams {
	otParams := san.NewIgroupInitiatorDeleteParams()
	if params == nil {
		return otParams
	}
	otParams.SetAllowDeleteWhileMapped(nillable.GetStringPtr("true"))
	otParams.SetIgroupUUID(params.IgroupUUID)
	otParams.SetName(params.InitiatorIQNName)

	otParams.SetIgroupUUID(params.IgroupUUID)
	return otParams
}

// IgroupGetParams is the input parameter for getting Igroups
type IgroupGetParams struct {
	BaseParams
	SvmName *string
	Name    *string
}

// igroupGetParamsToONTAP converts IgroupGetParams to ONTAP API parameters.
func igroupGetParamsToONTAP(params *IgroupGetParams) *san.IgroupCollectionGetParams {
	otParams := san.NewIgroupCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.SetName(params.Name)
	otParams.SetSvmName(params.SvmName)
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	otParams.SetFields(params.Fields)
	// MD: It's a GET call why is there return_timeout ??
	otParams.SetReturnTimeout(returnTimeoutNoJob)
	return otParams
}

// scheduleCreateParamsToONTAP converts ScheduleCreateParams to ONTAP API parameters.
func scheduleCreateParamsToONTAP(params *ScheduleCreateParams) *cluster.ScheduleCreateParams {
	otParams := cluster.NewScheduleCreateParams()
	if params == nil {
		return otParams
	}

	hours := make([]*int64, len(params.Hours))
	for i, val := range params.Hours {
		int64Val := int64(val)
		hours[i] = &int64Val
	}
	minutes := make([]*int64, len(params.Minutes))
	for i, val := range params.Minutes {
		int64Val := int64(val)
		minutes[i] = &int64Val
	}
	months := make([]*int64, len(params.Months))
	for i, val := range params.Months {
		int64Val := int64(val)
		months[i] = &int64Val
	}
	weekdays := make([]*int64, len(params.DaysOfWeek))
	for i, val := range params.DaysOfWeek {
		int64Val := int64(val)
		weekdays[i] = &int64Val
	}
	days := make([]*int64, len(params.DaysOfMonth))
	for i, val := range params.DaysOfMonth {
		int64Val := int64(val)
		days[i] = &int64Val
	}

	otParams.Info = &models.Schedule{
		Name: &params.Name,
		Cron: &models.ScheduleInlineCron{
			Hours:    hours,
			Months:   months,
			Days:     days,
			Weekdays: weekdays,
			Minutes:  minutes,
		},
	}
	return otParams
}

// scheduleCollectionGetParamsToONTAP converts ScheduleCollectionGetParams to ONTAP API parameters.
func scheduleCollectionGetParamsToONTAP(params *ScheduleCollectionGetParams) *cluster.ScheduleCollectionGetParams {
	otParams := cluster.NewScheduleCollectionGetParams()
	if params == nil {
		return otParams
	}

	otParams.Fields = params.Fields
	otParams.Name = &params.Name
	return otParams
}

// InterclusterLif is a simple wrapper for models.IPInterface
type InterclusterLif struct {
	models.IPInterface
}

// SvmPeerGetCollectionParams is the input params struct for svm_client.SvmPeerCollectionGet
type SvmPeerGetCollectionParams struct {
	BaseParams
	SvmName     *string
	PeerSvmName *string
}

func svmPeerGetCollectionParamsToONTAP(params *SvmPeerGetCollectionParams) *svm.SvmPeerCollectionGetParams {
	otParams := svm.NewSvmPeerCollectionGetParams()
	if params == nil {
		return otParams
	}
	otParams.SetSvmName(params.SvmName)
	otParams.SetPeerSvmName(params.PeerSvmName)
	otParams.SetFields(params.Fields)

	return otParams
}

// SvmPeerCreateParams is the input params struct for svm_client.SvmPeerCreate
type SvmPeerCreateParams struct {
	BaseParams
	models.SvmPeer
}

func svmPeerCreateParamsToONTAP(params *SvmPeerCreateParams) *svm.SvmPeerCreateParams {
	otParams := svm.NewSvmPeerCreateParams()
	if params == nil {
		return otParams
	}

	otParams.SetInfo(&params.SvmPeer)
	otParams.SetReturnTimeout(&returnTimeout)
	return otParams
}

// SvmPeerModifyParams is the input params struct for svm_client.SvmPeerModify
type SvmPeerModifyParams struct {
	BaseParams
	UUID    string
	SvmPeer models.SvmPeer
}

func svmPeerModifyParamsToONTAP(params *SvmPeerModifyParams) *svm.SvmPeerModifyParams {
	otParams := svm.NewSvmPeerModifyParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.UUID)
	otParams.SetInfo(&params.SvmPeer)
	otParams.SetReturnTimeout(&returnTimeout)
	return otParams
}

// SvmPeerDeleteParams is the input params struct for svm_client.SvmPeerDelete
type SvmPeerDeleteParams struct {
	BaseParams
	SvmPeerUUID string
	Force       bool
}

func svmPeerDeleteParamsToONTAP(params *SvmPeerDeleteParams) *svm.SvmPeerDeleteParams {
	otParams := svm.NewSvmPeerDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.SvmPeerUUID)
	otParams.SetReturnTimeout(&returnTimeout)

	if params.Force {
		otParams.SetForce(nillable.ToPointer("true"))
	}
	return otParams
}

func snapmirrorRelationshipCreateParamsToONTAP(params *SnapmirrorRelationshipCreateParams) *snapmirror.SnapmirrorRelationshipCreateParams {
	otParams := snapmirror.NewSnapmirrorRelationshipCreateParams()
	if params == nil {
		return otParams
	}

	sm := &models.SnapmirrorRelationship{
		Destination: &models.SnapmirrorEndpoint{
			Path: &params.DestinationPath,
		},
		Source: &models.SnapmirrorSourceEndpoint{
			Path: &params.SourcePath,
		},
	}
	if params.Policy != "" {
		sm.Policy = &models.SnapmirrorRelationshipInlinePolicy{
			Name: &params.Policy,
		}
	}
	if params.Schedule != nil {
		sm.TransferSchedule = &models.SnapmirrorRelationshipInlineTransferSchedule{
			Name: params.Schedule,
		}
	}
	if params.AccessToken != nil && *params.AccessToken != "" {
		xNetappAuthorization := "Bearer " + *params.AccessToken
		otParams.WithXNetappAuthorization(&xNetappAuthorization)
	}
	otParams.SetInfo(sm)
	returnRecords := "true"
	otParams.SetReturnRecords(&returnRecords)
	return otParams
}

func snapmirrorRelationshipSetStateParamsToONTAP(snapmirrorUUID string, state string) *snapmirror.SnapmirrorRelationshipModifyParams {
	otParams := snapmirror.NewSnapmirrorRelationshipModifyParams()
	if snapmirrorUUID == "" {
		return otParams
	}

	sm := &models.SnapmirrorRelationship{
		State: &state,
	}

	otParams.SetUUID(snapmirrorUUID)
	otParams.SetInfo(sm)
	return otParams
}

func snapmirrorRelationshipListParamsToONTAP(params *SnapmirrorRelationshipListParams) *snapmirror.SnapmirrorRelationshipsGetParams {
	otParams := snapmirror.NewSnapmirrorRelationshipsGetParams()
	if params == nil {
		return otParams
	}
	otParams.SetDestinationPath(&params.DestinationPath)
	otParams.SetSourcePath(&params.SourcePath)
	// This checks if the DestinationPath is a cloud object store path.
	if strings.Contains(params.DestinationPath, ":/objstore/") {
		otParams.WithFields([]string{"destination.uuid"})
	}
	return otParams
}

func snapmirrorRelationshipModifyParamsToONTAP(params *SnapmirrorRelationshipModifyParams) *snapmirror.SnapmirrorRelationshipModifyParams {
	otParams := snapmirror.NewSnapmirrorRelationshipModifyParams()
	if params == nil {
		return otParams
	}

	info := &models.SnapmirrorRelationship{}
	if params.TransferSchedule != nil {
		info.TransferSchedule = &models.SnapmirrorRelationshipInlineTransferSchedule{
			Name: params.TransferSchedule,
		}
	}

	otParams.SetUUID(params.UUID)
	otParams.SetInfo(info)
	return otParams
}

func snapmirrorRelationshipListDestinationsParamsToONTAP(params *SnapmirrorRelationshipListDestinationsParams) *snapmirror.SnapmirrorRelationshipsGetParams {
	otParams := snapmirror.NewSnapmirrorRelationshipsGetParams()
	otParams.SetListDestinationsOnly(nillable.ToPointer("true"))
	if params == nil {
		return otParams
	}
	if params.DestinationPath != nil {
		otParams.SetDestinationPath(params.DestinationPath)
	}
	if params.SourcePath != nil {
		otParams.SetSourcePath(params.SourcePath)
	}
	if params.DestinationSVMName != nil {
		otParams.SetDestinationSvmName(params.DestinationSVMName)
	}
	if params.SourceSVMName != nil {
		otParams.SetSourceSvmName(params.SourceSVMName)
	}
	return otParams
}

func convertSnapmirrorRelationshipListFromREST(response *snapmirror.SnapmirrorRelationshipsGetOK) []*SnapmirrorRelationship {
	var snapmirrorRelationships []*SnapmirrorRelationship
	if response != nil && response.Payload != nil {
		for _, record := range response.Payload.SnapmirrorRelationshipResponseInlineRecords {
			snapmirrorRelationship := SnapmirrorRelationship{
				*record,
			}
			snapmirrorRelationships = append(snapmirrorRelationships, &snapmirrorRelationship)
		}
	}
	return snapmirrorRelationships
}

func convertSnapmirrorRelationshipGetFromREST(response *snapmirror.SnapmirrorRelationshipGetOK) *SnapmirrorRelationship {
	if response != nil && response.Payload != nil {
		snapmirrorRelationship := SnapmirrorRelationship{
			*response.Payload,
		}
		return &snapmirrorRelationship
	}
	return nil
}

func snapmirrorRelationshipDeleteParamsToONTAP(params *SnapmirrorRelationshipDeleteParams) *snapmirror.SnapmirrorRelationshipDeleteParams {
	otParams := snapmirror.NewSnapmirrorRelationshipDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.UUID)
	otParams.SetDestinationOnly(nillable.ToStringPtr(params.DestinationOnly))
	otParams.SetSourceOnly(nillable.ToStringPtr(params.SourceOnly))
	return otParams
}

func snapmirrorRelationshipReleaseParamsToONTAP(params *SnapmirrorRelationshipReleaseParams) *snapmirror.SnapmirrorRelationshipDeleteParams {
	otParams := snapmirror.NewSnapmirrorRelationshipDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.SetUUID(params.UUID)
	if params.SourceInfoOnly != nil {
		otParams.SetSourceInfoOnly(nillable.ToStringPtr(params.SourceInfoOnly))
	} else {
		otParams.SetSourceOnly(nillable.ToPointer("true"))
	}
	return otParams
}

func snapmirrorRelationshipGetParamsToONTAP(params *SnapmirrorRelationshipGetParams) *snapmirror.SnapmirrorRelationshipGetParams {
	otParams := snapmirror.NewSnapmirrorRelationshipGetParams()
	if params == nil {
		return otParams
	}
	otParams.SetUUID(params.UUID)
	return otParams
}

type CloudTarget struct {
	models.CloudTarget
}

func cloudTargetCreateParamsToONTAP(params *CloudTargetCreateParams) *cloud.CloudTargetCreateParams {
	otParams := cloud.NewCloudTargetCreateParams()
	if params == nil {
		return otParams
	}
	otParams.Info = &models.CloudTarget{
		ProviderType:       &objStoreProviderType,
		AuthenticationType: &objStoreAuthenticationType,
		Server:             &objStoreServer,
		Owner:              &objStoreOwner,
		SnapmirrorUse:      &objStoreSnapmirrorUse,
		Name:               params.Name,
		Container:          params.Container,
	}
	return otParams
}

func cloudTargetCollectionGetParamsToONTAP(params *CloudTargetCollectionGetParams) *cloud.CloudTargetCollectionGetParams {
	otParams := cloud.NewCloudTargetCollectionGetParams()
	if params == nil {
		return otParams
	}
	otParams.SetName(params.Name)
	return otParams
}

// SnapmirrorTransfer is a simple wrapper of models.SnapmirrorTransfer
type SnapmirrorTransfer struct {
	models.SnapmirrorTransfer
}

func snapmirrorRelationshipTransferCreateParamsToONTAP(params *SnapmirrorRelationshipTransferCreateParams) *snapmirror.SnapmirrorRelationshipTransferCreateParams {
	otParams := snapmirror.NewSnapmirrorRelationshipTransferCreateParams()
	if params == nil {
		return otParams
	}
	otParams.SetRelationshipUUID(params.UUID)
	otParams.SetInfo(&models.SnapmirrorTransfer{SourceSnapshot: &params.SnapshotName})
	if params.AccessToken != nil && *params.AccessToken != "" {
		xNetappAuthorization := "Bearer " + *params.AccessToken
		otParams.WithXNetappAuthorization(&xNetappAuthorization)
	}
	return otParams
}

func snapmirrorRelationshipTransferGetParamsToONTAP(params *SnapmirrorRelationshipTransferGetParams) *snapmirror.SnapmirrorRelationshipTransfersGetParams {
	otParams := snapmirror.NewSnapmirrorRelationshipTransfersGetParams()
	if params == nil {
		return otParams
	}
	otParams.SetRelationshipUUID(params.SnapmirrorUUID)
	otParams.SetSnapshot(&params.SnapshotName)
	return otParams
}

func snapmirrorCloudEndpointDeleteParamsToONTAP(params *SnapmirrorCloudEndpointDeleteParams) *snapmirror.SnapmirrorObjstoreEpDeleteParams {
	otParams := snapmirror.NewSnapmirrorObjstoreEpDeleteParams()
	if params.ObjectStoreUUID != "" {
		otParams.SetObjectStoreUUID(params.ObjectStoreUUID)
	}
	if params.EndpointUUID != "" {
		otParams.SetUUID(params.EndpointUUID)
	}
	return otParams
}

func snapmirrorCloudSnapshotDeleteParamsToONTAP(params *SnapmirrorCloudSnapshotDeleteParams) *snapmirror.SnapmirrorObjstoreEpSnapshotDeleteParams {
	otParams := snapmirror.NewSnapmirrorObjstoreEpSnapshotDeleteParams()
	if params.ObjectStoreUUID != "" {
		otParams.SetObjectStoreUUID(params.ObjectStoreUUID)
	}
	if params.EndpointUUID != "" {
		otParams.SetEndpointUUID(params.EndpointUUID)
	}
	if params.SnapshotUUID != "" {
		otParams.SetUUID(params.SnapshotUUID)
	}
	return otParams
}
