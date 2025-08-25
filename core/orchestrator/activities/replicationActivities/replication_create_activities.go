package replicationActivities

import (
	"context"
	"strings"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	convertReplicationScheduleToInternalReplicationSchedule = _convertReplicationScheduleToInternalReplicationSchedule
	convertVolumeReplicationCreateParams                    = _convertVolumeReplicationCreateParams
	hydrateVolume                                           = HydrateVolume
	describeVolume                                          = DescribeVolume
)

const (
	snapmirrorFirewallName      = "ingress-snapmirror"
	snapmirrorFirewallPriority  = int64(1000)
	snapmirrorFirewallDirection = "INGRESS"
)

var (
	snapmirrorFirewallSourceRanges = env.GetString("DATA_FIREWALL_SOURCE_RANGES", "")
	snapmirrorFirewallPortRules    = env.GetString("SNAPMIRROR_FIREWALL_PORT_RULES", "tcp,10566,11104,11105")
)

type VolumeReplicationCreateActivity struct {
	SE database.Storage
}

func (a *VolumeReplicationCreateActivity) GetSourceInterclusterLifs(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetSourcePoolDetails for pool: %s", result.Event.SourcePool.Name)
	provider, err := hyperscaler.GetProviderByNode(ctx, result.SrcNode)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	interClusterLifs, err := provider.GetInterclusterLIFs("default-intercluster")
	if err != nil {
		logger.Error("Failed to get interCluster lifs", "error", err)
		return nil, err
	}
	var icLifs []string
	for _, icLif := range interClusterLifs {
		icLifs = append(icLifs, string(icLif.Address))
	}
	result.SrcIps = icLifs
	return result, nil
}

func (a *VolumeReplicationCreateActivity) GetDestinationPoolDetails(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	describePoolParams := &googleproxyclient.V1betaInternalDescribePoolParams{
		PoolName:      result.Event.DestinationPoolName,
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.DestinationLocationID,
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDescribePool(ctx, *describePoolParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInternalDescribePoolAPI, err)
	}
	pool, ok := res.(*googleproxyclient.PoolInternalV1beta)
	if ok {
		result.DstPool = pool
		result.DstIps = pool.InterclusterLifs
		return result, nil
	}
	return nil, vsaerrors.NewVCPError(vsaerrors.ErrInternalDescribePoolNotFound, vsaerrors.New("Pool not found"))
}

func (j *VolumeReplicationCreateActivity) CreateClusterPeering(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("CreateClusterPeer called")

	node := result.SrcNode
	expiryTime := time.Now().Add(time.Minute * 10) // Default expiry time of 10 mins
	clustePeerParams := &commonparams.ClusterPeerParams{
		PeerName:      result.DstPool.ClusterName.Value,
		PeerAddresses: result.DstIps,
		ExpiryTime:    &expiryTime,
	}
	resp, err := activities.CreateClusterPeer(ctx, clustePeerParams, node)
	if err != nil {
		return nil, err
	}
	result.ClusterPeerUUID = &resp.UUID
	result.Passphrase = (*string)(resp.Passphrase)
	return result, nil
}

func (a *VolumeReplicationCreateActivity) AcceptClusterPeering(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	if result.Passphrase == nil {
		return result, nil
	}
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	accpetClusterPeerParams := &googleproxyclient.V1betaInternalAcceptClusterPeerParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.DestinationLocationID,
	}

	expiryTime := time.Now().Add(time.Minute * 10)
	body := &googleproxyclient.ClusterPeerV1{
		PeerAddresses:   result.SrcIps,
		PeerClusterName: result.Event.SourcePool.ClusterDetails.ExternalName,
		Passphrase:      *result.Passphrase,
		PoolUUID:        result.DstPool.PoolId.Value,
		ExpiryTime:      googleproxyclient.NewOptNilDateTime(expiryTime),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalAcceptClusterPeer(ctx, body, *accpetClusterPeerParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInternalAcceptClusterPeerAPI, err)
	}
	clusterPeer, ok := res.(*googleproxyclient.ClusterPeerV1)
	if ok {
		result.JobId = &clusterPeer.Jobs[0].JobId.Value
		return result, nil
	}
	return nil, vsaerrors.NewVCPError(vsaerrors.ErrInternalAcceptClusterPeerNotFound, vsaerrors.New("Cluster peer not found"))
}

func (a *VolumeReplicationCreateActivity) CreateDestinationVolume(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	createVolumeParams := &googleproxyclient.V1betaCreateVolumeParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.DestinationLocationID,
	}

	body := &googleproxyclient.VolumeCreateV1beta{
		Volume:     convertSourceVolumeToDestinationVolume(result),
		VolumeType: googleproxyclient.OptVolumeCreateV1betaVolumeType{Value: googleproxyclient.VolumeCreateV1betaVolumeTypeSECONDARY, Set: true},
	}

	res, err := googleProxyClient.Invoker.V1betaCreateVolume(ctx, body, *createVolumeParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrCreatingDestinationVolume, err)
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		volume := gcpserver.VolumeV1beta{}
		err := replication.JsonUnMarshal(operation.Response, &volume)
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrorFailedToUnmarshal, err)
		}
		result.JobId = &strings.Split(operation.Name.Value, "/")[7]
		result.DstVolume = &volume
		return result, nil
	}
	return nil, nil
}

func DescribeVolume(ctx context.Context, result *replication.CreateReplicationResult) (*googleproxyclient.InternalVolumeV1beta, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	createVolumeParams := &googleproxyclient.V1betaInternalDescribeVolumeParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.DestinationLocationID,
		VolumeId:      result.DstVolume.VolumeId.Value,
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDescribeVolume(ctx, *createVolumeParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDescribingVolume, err)
	}
	volumeV1Beta, ok := res.(*googleproxyclient.InternalVolumeV1beta)
	if ok {
		return volumeV1Beta, nil
	}
	return nil, nil
}

func (a *VolumeReplicationCreateActivity) HydrateDestinationVolume(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	if hydrationEnabled {
		err := hydrateVolume(ctx, convertVolumeV1BetaToVolumeModel(*result.DstVolume, result.Event.DestinationLocationID), *result.DstProjectNumber, result.DstPool.ResourceId)
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrHydrateVolumeCreate, err)
		}
	}
	return result, nil
}

func convertVolumeV1BetaToVolumeModel(vol gcpserver.VolumeV1beta, dstLocation string) models.Volume {
	protocols := make([]string, 0)
	for _, protocol := range vol.Protocols {
		protocolStr, err := protocol.MarshalText()
		if err != nil {
			return models.Volume{}
		}
		protocols = append(protocols, string(protocolStr))
	}
	return models.Volume{
		BaseModel: models.BaseModel{
			UUID: vol.VolumeId.Value,
		},
		DisplayName:    vol.ResourceId,
		QuotaInBytes:   uint64(vol.QuotaInBytes.Value),
		LifeCycleState: string(vol.VolumeState.Value),
		ProtocolTypes:  protocols,
		Region:         dstLocation,
	}
}

func (a *VolumeReplicationCreateActivity) CreateReplicationOnDestination(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	createVolumeParams := &googleproxyclient.V1betaInternalCreateVolumeReplicationParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.DestinationLocationID,
	}

	body := convertVolumeReplicationCreateParams(*result)

	res, err := googleProxyClient.Invoker.V1betaInternalCreateVolumeReplication(ctx, &body, *createVolumeParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrCreatingDestinationVolume, err)
	}
	response, ok := res.(*googleproxyclient.VolumeReplicationInternalV1beta)
	if ok {
		result.DstReplication = response
		result.JobId = &response.Jobs[0].JobId.Value
		return result, nil
	}
	return nil, nil
}

func (a *VolumeReplicationCreateActivity) UpdateReplicationState(ctx context.Context, volumeRep datamodel.VolumeReplication) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	err := se.UpdateVolumeReplicationStates(ctx, &volumeRep)
	if err != nil {
		return err
	}
	logger.Debug("Volume Replication state:%s update successfully in the db", volumeRep.Name)

	return nil
}

func (a *VolumeReplicationCreateActivity) UpdateDestinationVolumeDetails(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	volumeRep := result.DbVolReplication
	volumeRep.ReplicationAttributes.DestinationVolumeUUID = result.DstVolume.VolumeId.Value
	volumeRep.ReplicationAttributes.DestinationVolumeName = result.DstVolume.ResourceId
	err := se.UpdateVolumeReplication(ctx, volumeRep)
	if err != nil {
		return nil, err
	}
	result.DbVolReplication = volumeRep

	logger.Debug("Volume Replication state:%s update successfully in the db", volumeRep.Name)

	return result, nil
}

func (a *VolumeReplicationCreateActivity) UpdateDestinationVolumeReplicationDetails(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	volumeRep := result.DbVolReplication
	volumeRep.ReplicationAttributes.DestinationPoolUUID = result.DstPool.PoolId.Value
	volumeRep.ReplicationAttributes.DestinationHostName = result.DstPool.ClusterName.Value
	volumeRep.ReplicationAttributes.DestinationReplicationUUID = result.DstReplication.VolumeReplicationUuid.Value
	err := se.UpdateVolumeReplication(ctx, volumeRep)
	if err != nil {
		return nil, err
	}
	result.DbVolReplication = volumeRep

	logger.Debug("Volume Replication state:%s update successfully in the db", volumeRep.Name)

	return result, nil
}

func (a *VolumeReplicationCreateActivity) UpdateReplicationDetails(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	volumeRep := result.DbVolReplication
	volumeRep.State = models.LifeCycleStateCreated
	volumeRep.StateDetails = models.LifeCycleStateCreatedDetails
	volumeRep.ReplicationAttributes.SourceSvmName = *result.SrcSvm
	volumeRep.ReplicationAttributes.DestinationSvmName = *result.DstSvm
	volumeRep.ReplicationAttributes.SourceHostName = result.Event.SourcePool.ClusterDetails.ExternalName
	volumeRep.ReplicationAttributes.SourceReplicationUUID = volumeRep.UUID
	volumeRep.ReplicationAttributes.ReplicationType = string(result.DstReplication.ReplicationType.Value)

	err := se.UpdateVolumeReplication(ctx, volumeRep)
	if err != nil {
		return nil, err
	}
	result.DbVolReplication = volumeRep
	logger.Debug("Volume Replication state:%s update successfully in the db", volumeRep.Name)

	return result, nil
}

func (a *VolumeReplicationCreateActivity) AcceptSvmPeer(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, result.SrcNode)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	svmPeer, err := provider.GetSVMPeer(result.SrcSvm, result.DstSvm)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGettingSvmPeer, err)
	}
	if svmPeer.State == "peered" && svmPeer.PeerSvmName == *result.DstSvm {
		// SVMs are already peered
		logger.Infof("SVMs already peered")
		return result, nil
	}
	err = provider.AcceptSvmPeering(*result.SrcSvm, *result.DstSvm)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrAcceptSvmPeer, err)
	}

	return result, nil
}

func (a *VolumeReplicationCreateActivity) GetVolumeSVMNames(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	srcVol, err := se.DescribeVolume(ctx, result.Event.SourceVolume.UUID)
	if err != nil {
		logger.Error("Failed to describe source volume", "error", err)
		return nil, err
	}
	if srcVol.Svm == nil || srcVol.Svm.Name == "" {
		logger.Error("Source volume SVM name not found")
		return nil, vsaerrors.New("Source volume SVM name not found")
	}

	dstVol, err := describeVolume(ctx, result)
	if err != nil {
		logger.Error("Failed to describe destination volume", "error", err)
		return nil, err
	}
	if !dstVol.SvmName.IsSet() {
		logger.Error("Destination volume SVM name not found")
		return nil, vsaerrors.New("Destination volume SVM name not found")
	}

	dstSvm := dstVol.SvmName.Value
	result.SrcSvm = &srcVol.Svm.Name
	result.DstSvm = &dstSvm
	return result, nil
}

func (a *VolumeReplicationCreateActivity) GetSrcBasePath(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.LocationID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *VolumeReplicationCreateActivity) GetDstBasePath(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	dstBasePath, err := GetBasePath(ctx, result.Event.DestinationLocationID)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *VolumeReplicationCreateActivity) GetSignedSrcToken(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, result.Event.SourceProjectNumber)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *VolumeReplicationCreateActivity) GetSignedDstToken(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	dstJwt, err := GetSignedToken(ctx, *result.DstProjectNumber)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err)
	}
	result.DstJwtToken = dstJwt
	return result, nil
}

func (a *VolumeReplicationCreateActivity) MountReplication(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	createVolumeParams := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.DestinationLocationID,
		VolumeReplicationId: result.DstReplication.VolumeReplicationUuid.Value,
	}

	res, err := googleProxyClient.Invoker.V1betaInternalMountVolumeReplication(ctx, *createVolumeParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrMountingVolumeReplication, err)
	}
	response, ok := res.(*googleproxyclient.InternalJobV1beta)
	if ok {
		result.JobId = &response.JobUuid.Value
		return result, nil
	}
	return nil, nil
}

// DescribeRemoteJob gives the status of a remote job
func (a *VolumeReplicationCreateActivity) DescribeRemoteJob(ctx context.Context, result *replication.CreateReplicationResult) error {
	err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.DestinationLocationID)
	if err != nil {
		return err
	}
	return nil
}

func convertSourceVolumeToDestinationVolume(result *replication.CreateReplicationResult) googleproxyclient.VolumeV1beta {
	srcVol := result.Event.SourceVolume
	protocols := make([]googleproxyclient.ProtocolsV1beta, 0)
	for _, value := range srcVol.VolumeAttributes.Protocols {
		var protocolsV1beta googleproxyclient.ProtocolsV1beta
		_ = protocolsV1beta.UnmarshalText([]byte(value))
		protocols = append(protocols, protocolsV1beta)
	}

	// Convert BlockDevices
	blockDevices := make([]googleproxyclient.BlockDeviceV1beta, 0)
	if srcVol.VolumeAttributes.BlockDevices != nil {
		for _, blockDevice := range *srcVol.VolumeAttributes.BlockDevices {
			blockDeviceV1beta := googleproxyclient.BlockDeviceV1beta{
				OsType: googleproxyclient.NewOptBlockDeviceV1betaOsType(convertBlockDeviceOsType(blockDevice.OSType)),
			}
			blockDevices = append(blockDevices, blockDeviceV1beta)
		}
	}

	var creationToken *string
	if creationToken = &result.Event.CreateReplicationParams.DestinationVolumeParameters.ShareName; result.Event.CreateReplicationParams.DestinationVolumeParameters.ShareName == "" {
		creationToken = &result.Event.SourceVolume.VolumeAttributes.CreationToken
	}

	var resourceId *string
	if resourceId = &result.Event.CreateReplicationParams.DestinationVolumeParameters.VolumeID; result.Event.CreateReplicationParams.DestinationVolumeParameters.VolumeID == "" {
		resourceId = &result.Event.SourceVolume.Name
	}

	volume := googleproxyclient.VolumeV1beta{
		ResourceId:    *resourceId,
		CreationToken: googleproxyclient.NewOptString(*creationToken),
		PoolId:        googleproxyclient.NewNilString(result.DstPool.PoolId.Value),
		QuotaInBytes:  googleproxyclient.NewOptFloat64(float64(srcVol.SizeInBytes)),
		Network:       googleproxyclient.NewOptString(result.DstPool.Network),
		Description:   googleproxyclient.NewOptNilString(nillable.GetString(result.Event.CreateReplicationParams.DestinationVolumeParameters.Description, "")),
		Protocols:     protocols,
		BlockDevices:  blockDevices,
	}
	return volume
}

func _convertVolumeReplicationCreateParams(result replication.CreateReplicationResult) googleproxyclient.VolumeReplicationCreateInternalV1beta {
	createReplicationParams := googleproxyclient.VolumeReplicationCreateInternalV1beta{
		RemoteRegion:          result.Event.LocationID,
		EndpointType:          "dst",
		ReplicationSchedule:   googleproxyclient.NewOptVolumeReplicationCreateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalReplicationSchedule(*result.Event.CreateReplicationParams.ReplicationSchedule)),
		SourceVolumeUuid:      googleproxyclient.NewOptString(result.Event.SourceVolume.UUID),
		SourcePoolUuid:        googleproxyclient.NewOptString(result.Event.SourcePool.UUID),
		SourceHostName:        result.Event.SourcePool.ClusterDetails.ExternalName,
		SourceServerName:      *result.SrcSvm,
		SourceVolumeName:      result.Event.SourceVolume.Name,
		DestinationHostName:   result.DstPool.ClusterName.Value,
		DestinationServerName: *result.DstSvm,
		DestinationVolumeName: result.DstVolume.ResourceId,
		VolumeReplicationUuid: googleproxyclient.NewOptString(result.DbVolReplication.UUID),
		DestinationVolumeUuid: googleproxyclient.NewOptString(result.DstVolume.VolumeId.Value),
		DestinationPoolUuid:   googleproxyclient.NewOptString(result.DstPool.PoolId.Value),
		Name:                  googleproxyclient.NewOptString(*result.Event.CreateReplicationParams.ResourceID),
		Description:           googleproxyclient.NewOptString(nillable.GetString(result.Event.CreateReplicationParams.Description, "")),
		ReplicationPolicy:     googleproxyclient.NewOptVolumeReplicationCreateInternalV1betaReplicationPolicy(googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationPolicyMirrorAllSnapshots),
		ReplicationType:       googleproxyclient.NewOptVolumeReplicationCreateInternalV1betaReplicationType(googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationTypeCROSSREGIONREPLICATION),
		ReverseResume:         googleproxyclient.NewOptBool(false),
		CcfeURI:               googleproxyclient.NewOptString(result.DbVolReplication.Uri),
		CcfeRemoteURI:         googleproxyclient.NewOptString(result.DbVolReplication.RemoteUri),
	}

	return createReplicationParams
}

func _convertReplicationScheduleToInternalReplicationSchedule(in string) googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationSchedule {
	switch in {
	case "EVERY_10_MINUTES":
		return googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationSchedule10minutely
	case "HOURLY":
		return googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationScheduleHourly
	case "DAILY":
		return googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationScheduleDaily
	default:
		return ""
	}
}

func convertBlockDeviceOsType(in string) googleproxyclient.BlockDeviceV1betaOsType {
	switch in {
	case "LINUX":
		return googleproxyclient.BlockDeviceV1betaOsTypeLINUX
	case "WINDOWS":
		return googleproxyclient.BlockDeviceV1betaOsTypeWINDOWS
	case "ESXI":
		return googleproxyclient.BlockDeviceV1betaOsTypeESXI
	default:
		return googleproxyclient.BlockDeviceV1betaOsTypeOSTYPEUNSPECIFIED
	}
}

func (a *VolumeReplicationCreateActivity) CreateSnapmirrorFirewall(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	snHostProject := result.Event.SourcePool.SnHostProject
	if snHostProject == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("SnHostProject", nil))
	}

	network := result.Event.SourcePool.ClusterDetails.Network
	if network == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.NewNotFoundErr("SnHostProjectNetwork", nil))
	}

	op, err := activities.InsertFirewall(gcpService, snHostProject, snapmirrorFirewallName, network, snapmirrorFirewallPriority, snapmirrorFirewallDirection, strings.Split(snapmirrorFirewallSourceRanges, ","), strings.Split(snapmirrorFirewallPortRules, ","))
	if err != nil {
		logger.Errorf("Failed to create firewall %s: %v", snapmirrorFirewallName, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// If operation name is empty, it means the firewall already exists or no operation was needed
	if op == "" {
		logger.Infof("Firewall %s already exists", snapmirrorFirewallName)
		operation := commonparams.Operations{
			OperationName:      "",
			OperationType:      "firewall",
			IsDone:             true,
			IsRegionalResource: false,
			Project:            snHostProject,
		}
		result.Operation = &operation
		return result, nil
	}

	operation := commonparams.Operations{
		OperationName:      op,
		OperationType:      "firewall",
		IsDone:             false,
		IsRegionalResource: false,
		Project:            snHostProject,
	}
	result.Operation = &operation
	return result, nil
}

func (a *VolumeReplicationCreateActivity) PollSnapmirrorFirewallOperation(ctx context.Context, result *replication.CreateReplicationResult) (*replication.CreateReplicationResult, error) {
	logger := util.GetLogger(ctx)

	if result.Operation == nil || result.Operation.IsDone == true {
		return result, nil
	}

	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	opStatus, err := activities.GetComputeOpStatus(gcpService, result.Event.SourcePool.SnHostProject, result.Operation.IsRegionalResource, result.Operation.OperationName)
	if err != nil {
		logger.Errorf("Failed to get operation status for %s: %v", result.Operation.OperationName, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if opStatus == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, vsaerrors.New("Failed to get operation status"))
	}

	if opStatus.Status == "DONE" {
		result.Operation.IsDone = true
		return result, nil
	}

	return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, vsaerrors.Newf("Firewall operation %s is not completed. Status: %s", result.Operation.OperationName, opStatus.Status))
}
