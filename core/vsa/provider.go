package vsa

import (
	"context"

	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	expectedNodeCount  = 2
	ipSpaceName        = "Default"
	DefaultNetmask     = "255.255.255.255"
	iscsiServicePolicy = "default-data-iscsi"
)

type Provider interface {
	GetONTAPVersion() (*string, error)
	JobGet(jobUUID string) (*OntapJob, error)
	AreAllNodeUpAndRunning() (bool, error)
	IsAggregateOnline(aggregateName string) (bool, error)
	GetNodes() ([]*Node, error)
	GetNodeByName(name string) (*Node, error)
	CreateSVM(params CreateSvmParams) (*ProviderResponse, error)
	CreateDataLIF(params CreateLifParams) (*Lif, error)
	CreateNetworkIpRoute(params CreateNetworkIPRouteParams) error
	CreateVolume(params CreateVolumeParams) (*VolumeResponse, error)
	DeleteVolume(volumeUUID, volumeName string) error
	GetVolume(params GetVolumeParams) (*VolumeResponse, error)
	GetVolumeEncryptionStatus(params GetVolumeParams) (*VolumeResponse, error)
	GetVolumes() ([]*Volume, error)
	UpdateVolume(params UpdateVolumeParams) error
	UpdateVolumeEnableEncryption(params UpdateVolumeParams) error
	IgroupCreate(params IgroupCreateParams) (string, error)
	IgroupGet(name, svm *string) (*ontapRest.Igroup, error)
	IgroupExists(name string, svm *string) (bool, *ontapRest.Igroup, error)
	LunCreate(params LunCreateParams) (*LunResponse, error)
	LunGet(params LunGetParams) (*LunResponse, error)
	LunUpdate(params LunUpdateParams) error
	IgroupAddInitiator(params IgroupAddInitiator) error
	IgroupDeleteInitiator(params IgroupDeleteInitiator) error
	LunMapCreate(params LunMapCreateParams) error
	LunMapDelete(params LunMapDeleteParams) error
	IscsiServiceCreate(svmUUID string) error
	CreateClusterPeer(params CreateClusterPeerParams) (*ClusterPeer, error)
	AcceptClusterPeer(params CreateClusterPeerParams) (*ClusterPeer, error)
	DeleteClusterPeer(clusterPeerID string) error
	GetClusterPeer(clusterPeerID string) (*ClusterPeer, error)
	ListClusterPeers() ([]*ClusterPeer, error)
	GetInterclusterLIFs(servicePolicyName string) ([]*InterclusterLif, error)
	CreateSvmPeering(srcClusterName, srcSVMName, dstSVMName string, snapmirrorApplication ontaprestmodels.SvmPeerApplications) error
	AcceptSvmPeering(srcSVMName, dstSVMName string) error
	GetSVMPeer(localSVMName, remoteSVMName *string) (*SvmPeer, error)
	DeleteSVMPeer(svmPeerUUID string, force bool) error
	CreateVolumeReplication(params *CreateVolumeReplicationParams) (*VolumeReplication, error)
	AuthorizeVolumeReplication(params *CreateVolumeReplicationParams) (*VolumeReplication, error)
	DeleteVolumeReplication(params *DeleteVolumeReplicationParams) (*VolumeReplication, error)
	UpdateVolumeReplication(volRep *VolumeReplication) (*VolumeReplication, error)
	ReleaseVolumeReplication(params *CreateVolumeReplicationParams) (*VolumeReplication, error)
	ResyncVolumeReplication(volRep *VolumeReplication) (*VolumeReplication, error)
	BreakVolumeReplication(volRep *VolumeReplication) (*VolumeReplication, error)
	AbortVolumeReplication(volRep *VolumeReplication) (*VolumeReplication, error)
	GetReplicationDetails(ctx context.Context, volRep *VolumeReplication) (*VolumeReplication, error)
	GetVolumeReplication(replication *VolumeReplication) (*VolumeReplication, error)
	CreateSnapshot(params CreateSnapshotParams) (*SnapshotProviderResponse, error)
	DeleteSnapshot(snapshotUUID string, volumeUUID string) error
	GetSnapshots(volumeUUID string) ([]*Snapshot, error)
	ListSnapmirrorSnapshots(volumeUUID string) ([]*SnapshotListResponse, error)
	UpdateSnapshotPolicy(ctx context.Context, params *UpdateSnapshotPolicyParams) error
	CloudTargetGet(name *string) (*ontapRest.CloudTarget, error)
	CloudTargetCreate(name, containerName string) (*ontapRest.CloudTarget, error)
	SnapmirrorRelationshipCreate(params *commonparams.SnapmirrorRelationshipParams, smcToken *string) (*ontapRest.SnapmirrorRelationship, error)
	SnapmirrorRelationshipGet(destinationPath, sourcePath string) (*ontapRest.SnapmirrorRelationship, error)
	SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName string, smcToken *string) error
	SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName string) (*ontapRest.SnapmirrorTransfer, error)
	SnapmirrorRelationshipDelete(UUID string) (*OntapAsyncResponse, error)
	SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, EndpointUUID string) (*OntapAsyncResponse, error)
	SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, EndpointUUID, snapshotUUID string) (*OntapAsyncResponse, error)
	CreateSnapshotPolicy(sp *SnapshotPolicy) error
	DeleteSnapshotPolicy(snapshotPolicyName string) error
	CreateKmsConfig(params CreateKmsConfigParams) (*CreateKmsConfigResponse, error)
	IsGcpKmsReachable(params GetKmsConfigParams) (bool, error)
	PostClusterLicenseAccessToken(ctx context.Context, clientSecret string) (*string, error)
	CreateDns(params CreateDnsParams) error
	CreateQoSGroupPolicy(params CreateQoSGroupPolicyParams) (*QoSGroupPolicyResponse, error)
	ModifySVMWithQoSPolicy(params ModifySVMWithQoSPolicyParams) error
	CreateExportPolicy(params *ExportPolicy) error
}

type OntapRestProvider struct {
	Provider           ProviderDetails            `json:"Provider"`
	ClientParams       ontapRest.RESTClientParams `json:"ClientParams"`
	InsecureSkipVerify bool                       `json:"insecureSkipVerify"`
	Logger             *log.Slogger               `json:"-"`
}

func NewProvider(ctx context.Context, provider ProviderDetails) *OntapRestProvider {
	logger := util.GetLogger(ctx)
	ontapRestProvider := &OntapRestProvider{
		Provider: provider,
		ClientParams: ontapRest.RESTClientParams{
			Hosts:              provider.Hosts,
			Host:               provider.IPAddress,
			InsecureSkipVerify: provider.InsecureSkipVerify,
			Trace:              logger.(*log.Slogger),
		},
		Logger: logger.(*log.Slogger),
	}
	if provider.Certificate != nil {
		ontapRestProvider.ClientParams.Certificate = &models.Certificate{
			SignedCertificate:        provider.Certificate.SignedCertificate,
			PrivateKey:               provider.Certificate.PrivateKey,
			InterMediateCertificates: provider.Certificate.InterMediateCertificates,
			CommonName:               provider.Certificate.CommonName,
			RootCaCertificate:        provider.Certificate.RootCaCertificate,
		}
		ontapRestProvider.ClientParams.CertificateBasedAuthEnabled = true
	} else {
		ontapRestProvider.ClientParams.Password = log.Secret(provider.Password)
		ontapRestProvider.ClientParams.CertificateBasedAuthEnabled = false
	}
	return ontapRestProvider
}
