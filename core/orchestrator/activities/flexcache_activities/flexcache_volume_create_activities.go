package flexcache_activities

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type FlexCacheVolumeCreateActivity struct {
	SE database.Storage
}

var (
	utilGetLogger                = util.GetLogger
	hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
)

// CreateFlexCacheVolumeInOntapActivity creates a FlexCache volume in ONTAP
func (a FlexCacheVolumeCreateActivity) CreateFlexCacheVolumeInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	cacheParams := volume.CacheParameters
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	params := vsa.CreateFlexCacheVolumeParams{
		Name:             volume.Name,
		SvmName:          volume.Svm.Name,
		AggregateName:    activities.AggregateName,
		OriginSVMName:    cacheParams.PeerSvmName,
		OriginVolumeName: cacheParams.PeerVolumeName,
		JunctionPath:     &volume.VolumeAttributes.FileProperties.JunctionPath,
	}

	res, err := provider.CreateFlexCacheVolume(params)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrCreatingFlexCacheVolume, err)
	}

	logger.Debug("flexcache volume created successfully")

	result.VolumeResponse = res

	return result, nil
}

func (a FlexCacheVolumeCreateActivity) CreateClusterPeerInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	cacheParams := volume.CacheParameters
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	params := vsa.CreateClusterPeerParams{
		PeerAddresses: cacheParams.PeerIpAddresses,
		PeerName:      cacheParams.PeerClusterName,
		IPSpace:       activities.IpSpace,
	}

	if cacheParams.CommandExpiryTime != nil {
		expiry := strfmt.DateTime(*cacheParams.CommandExpiryTime)
		params.ExpiryTime = &expiry
	}

	clusterPeer, err := provider.CreateClusterPeer(params)
	if err != nil {
		volume.CacheParameters.CacheStateDetailsCode = coremodels.ErrorDuringClusterPeerCode
		volume.CacheParameters.CacheStateDetails = coremodels.ErrorDuringClusterPeer
		return nil, err
	}

	result.ClusterPeer = clusterPeer
	logger.Infof("cluster peer created successfully with UUID: %s", clusterPeer.UUID)

	return result, nil
}

func (a FlexCacheVolumeCreateActivity) UpdateFlexCacheVolumeForClusterPeeringActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	clusterPeer := result.ClusterPeer
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	interClusterLifs, err := provider.GetInterclusterLIFs(vsa.InterclusterServicePolicyName)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	var icLifs []string
	for _, icLif := range interClusterLifs {
		icLifs = append(icLifs, string(icLif.Address))
	}

	peerCommand := fmt.Sprintf(
		"cluster peer create -peer-addrs %s -initial-allowed-vserver-peers %s",
		strings.Join(icLifs, ","),
		volume.CacheParameters.PeerSvmName,
	)

	convertedTime := time.Time(*clusterPeer.ExpiryTime)

	volume.ClusterPeerUUID = &clusterPeer.ExternalUUID
	volume.CacheParameters.Passphrase = (*string)(clusterPeer.Passphrase)
	volume.CacheParameters.CommandExpiryTime = &convertedTime
	volume.CacheParameters.Command = &peerCommand
	volume.CacheParameters.CacheStateDetailsCode = coremodels.WaitingForClusterPeeringCode
	volume.CacheParameters.CacheStateDetails = coremodels.WaitingForClusterPeering
	a.setCacheStates(result, models.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING)

	updates := map[string]interface{}{
		"cache_parameters":  volume.CacheParameters,
		"cluster_peer_uuid": volume.ClusterPeerUUID,
	}
	if err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updates); err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	logger.Debug("cluster peer command updated successfully")

	return result, nil
}

func (a FlexCacheVolumeCreateActivity) WaitForClusterPeerActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
	}
	clusterPeer, err := provider.GetClusterPeer(*result.DBVolume.ClusterPeerUUID)
	if err != nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
	}

	if clusterPeer.AuthenticationState == vsa.ClusterPeerAuthenticationStateProblem || clusterPeer.AuthenticationState == vsa.ClusterPeerAuthenticationStateAbsent {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrClusterPeerError, fmt.Errorf("cluster peer authentication state is %s", clusterPeer.AuthenticationState)))
	}

	if clusterPeer.AuthenticationState == vsa.ClusterPeerAuthenticationStateOK && clusterPeer.Availability == vsa.ClusterPeerAvailabilityStateAvailable {
		return result, nil
	}

	return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("cluster peer is not ready yet"))
}

func (a FlexCacheVolumeCreateActivity) CreateSVMPeeringInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	volume := result.DBVolume
	cacheParams := volume.CacheParameters
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	params := vsa.CreateSVMPeerParams{
		LocalSVMName:    volume.Svm.Name,
		PeerSVMName:     cacheParams.PeerSvmName,
		PeerClusterName: cacheParams.PeerClusterName,
		Applications:    []ontaprestmodels.SvmPeerApplications{ontaprestmodels.SvmPeerApplicationsFlexcache},
	}

	svmPeer, err := provider.CreateSVMPeer(params)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	result.SVMPeer = svmPeer

	return result, nil
}

func (a FlexCacheVolumeCreateActivity) UpdateFlexCacheVolumeForSVMPeeringActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume
	cacheParams := volume.CacheParameters

	peerCommand := fmt.Sprintf("vserver peer accept -vserver %s -peer-vserver %s", cacheParams.PeerSvmName, volume.Svm.Name)

	volume.SvmPeerUUID = &result.SVMPeer.UUID
	cacheParams.Passphrase = nil
	cacheParams.CommandExpiryTime = nil
	cacheParams.Command = &peerCommand
	cacheParams.CacheStateDetailsCode = coremodels.WaitingForSVMPeeringCode
	cacheParams.CacheStateDetails = coremodels.WaitingForSVMPeering
	a.setCacheStates(result, models.FlexCacheV1betaCacheStatePENDINGSVMPEERING)

	updates := map[string]interface{}{
		"cache_parameters": volume.CacheParameters,
		"svm_peer_uuid":    volume.SvmPeerUUID,
	}
	if err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updates); err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	logger.Debug("svm peer command updated successfully")

	return result, nil
}

func (a FlexCacheVolumeCreateActivity) WaitForSVMPeeringActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
	}
	svmPeer, err := provider.GetSVMPeer(&result.DBVolume.Svm.Name, &result.DBVolume.CacheParameters.PeerSvmName)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if svmPeer.State == vsa.SvmPeerStateRejected || svmPeer.State == vsa.SvmPeerStateSuspended {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSVMPeerError, fmt.Errorf("svm peer state is %s", svmPeer.State)))
	}

	if svmPeer.State == vsa.SvmPeerStatePeered {
		return result, nil
	}

	return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("svm peer is not ready yet"))
}

func (a FlexCacheVolumeCreateActivity) UpdateFlexCacheVolumeDetailsActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	volume := result.DBVolume
	volume.CacheParameters.CacheStateDetailsCode = coremodels.DefaultCode
	volume.CacheParameters.CacheStateDetails = ""
	volume.CacheParameters.Command = nil
	volume.CacheParameters.CommandExpiryTime = nil
	volume.CacheParameters.Passphrase = nil
	volume.VolumeAttributes.ExternalUUID = result.VolumeResponse.ExternalUUID
	a.setCacheStates(result, models.FlexCacheV1betaCacheStatePEERED)

	updates := map[string]interface{}{
		"cache_parameters":  volume.CacheParameters,
		"volume_attributes": volume.VolumeAttributes,
		"state":             coremodels.LifeCycleStateREADY,
		"state_details":     coremodels.LifeCycleStateAvailableDetails,
	}
	if err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updates); err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return result, nil
}

func (a FlexCacheVolumeCreateActivity) UpdateVolumeDetailsOnErrorActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) error {
	volume := result.DBVolume
	updates := map[string]interface{}{
		"cache_parameters": volume.CacheParameters,
	}

	if a.shouldSetErrorState(result) {
		updates["state"] = coremodels.LifeCycleStateError
		updates["state_details"] = coremodels.LifeCycleStateCreationErrorDetails
	}

	if err := a.SE.UpdateVolumeFields(ctx, volume.UUID, updates); err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return nil
}

func (a FlexCacheVolumeCreateActivity) setCacheStates(result *flexcache.CreateFlexCacheResult, state string) {
	result.DBVolume.CacheParameters.PreviousCacheState = result.DBVolume.CacheParameters.CacheState
	result.DBVolume.CacheParameters.CacheState = state
}

// shouldSetErrorState determines if the volume state should be set to error based on the CacheStateDetailsCode
func (a FlexCacheVolumeCreateActivity) shouldSetErrorState(result *flexcache.CreateFlexCacheResult) bool {
	nonErrorCodes := []int{
		coremodels.ClusterPeeringExpiredCode,
		coremodels.SourceClusterUnreachableCode,
		coremodels.SVMPeeringExpiredCode,
		coremodels.ErrorDuringClusterPeerCode,
		coremodels.ErrorDuringSVMPeeringCode,
	}

	for _, code := range nonErrorCodes {
		if result.DBVolume.CacheParameters.CacheStateDetailsCode == code {
			return false
		}
	}

	return true
}
