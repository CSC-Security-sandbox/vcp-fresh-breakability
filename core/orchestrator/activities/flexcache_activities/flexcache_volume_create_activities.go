package flexcache_activities

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type FlexCacheVolumeCreateActivity struct {
	SE database.Storage
}

var (
	hydrationEnabled   = env.GetBool("GCP_HYDRATE_ENABLED", false)
	isHydrationEnabled = _isHydrationEnabled

	utilGetLogger                = util.GetLogger
	hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
	commonHydrateFlexCacheState  = common.HydrateFlexCacheState
	authGenerateCallbackToken    = auth.GenerateCallbackToken
)

type completeJobOpts struct {
	WorkflowID    string
	ResourceUUID  string
	JobType       string
	GetErrCode    int
	UpdateErrCode int
}

func _isHydrationEnabled() bool {
	return hydrationEnabled
}

// CreateFlexCacheVolumeInOntapActivity creates a FlexCache volume in ONTAP
func (a *FlexCacheVolumeCreateActivity) CreateFlexCacheVolumeInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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

func (a *FlexCacheVolumeCreateActivity) CreateClusterPeerInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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

func (a *FlexCacheVolumeCreateActivity) UpdateFlexCacheVolumeForClusterPeeringActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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
	a.setCacheStates(result, cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING)

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

func (a *FlexCacheVolumeCreateActivity) WaitForClusterPeerActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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

func (a *FlexCacheVolumeCreateActivity) CreateSVMPeeringInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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

func (a *FlexCacheVolumeCreateActivity) UpdateFlexCacheVolumeForSVMPeeringActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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
	a.setCacheStates(result, cvpModels.FlexCacheV1betaCacheStatePENDINGSVMPEERING)

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

func (a *FlexCacheVolumeCreateActivity) WaitForSVMPeeringActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
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

func (a *FlexCacheVolumeCreateActivity) UpdateFlexCacheVolumeDetailsActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	volume := result.DBVolume
	volume.CacheParameters.CacheStateDetailsCode = coremodels.DefaultCode
	volume.CacheParameters.CacheStateDetails = ""
	volume.CacheParameters.Command = nil
	volume.CacheParameters.CommandExpiryTime = nil
	volume.CacheParameters.Passphrase = nil
	volume.VolumeAttributes.ExternalUUID = result.VolumeResponse.ExternalUUID
	a.setCacheStates(result, cvpModels.FlexCacheV1betaCacheStatePEERED)

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

func (a *FlexCacheVolumeCreateActivity) HydrateFlexCacheState(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	if !isHydrationEnabled() {
		logger.Debugf("hydration is disabled, skipping HydrateFlexCacheState")
		return result, nil
	}

	volume := result.DBVolume
	callbackToken, err := authGenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return result, err
	}

	err = commonHydrateFlexCacheState(ctx, logger, result.Event.LocationID, result.Event.ProjectNumber, volume.Name, volume.CacheParameters.CacheState, volume.State, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating flexcache state: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrHydrateFlexCacheVolume, err)
	}

	logger.Debugf("hydration completed successfully for volume: %s", volume.Name)

	return result, nil
}

func (a *FlexCacheVolumeCreateActivity) UpdateVolumeDetailsOnErrorActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) error {
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

func (a *FlexCacheVolumeCreateActivity) setCacheStates(result *flexcache.CreateFlexCacheResult, state string) {
	result.DBVolume.CacheParameters.PreviousCacheState = result.DBVolume.CacheParameters.CacheState
	result.DBVolume.CacheParameters.CacheState = state
}

// shouldSetErrorState determines if the volume state should be set to error based on the CacheStateDetailsCode
func (a *FlexCacheVolumeCreateActivity) shouldSetErrorState(result *flexcache.CreateFlexCacheResult) bool {
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

// CompleteFlexCacheCreateJobActivity finds and completes an existing FlexCache create job if it exists.
func (a *FlexCacheVolumeCreateActivity) CompleteFlexCacheCreateJobActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	input := result.JobInput
	logger.Debugf("Completing existing FlexCache create job for resource: %s (%s)", input.ResourceName, input.WorkflowID)

	if input.WorkflowID == "" {
		logger.Errorf("no existing FlexCache create job found for resource %s: missing workflowID", input.ResourceName)
		return nil, vsaerrors.NewVCPError(
			vsaerrors.ErrCreatingFlexCacheVolume,
			fmt.Errorf("no existing FlexCache create job found for resource %s", input.ResourceName),
		)
	}

	_, err := a.completeJob(ctx, completeJobOpts{
		WorkflowID:    input.WorkflowID,
		GetErrCode:    vsaerrors.ErrCreatingFlexCacheVolume,
		UpdateErrCode: vsaerrors.ErrCreatingFlexCacheVolume,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// CreatePeeringJobActivity creates a job for peering establishment.
// It starts immediately and remains PROCESSING until the system reaches PENDING_CLUSTER_PEERING.
func (a *FlexCacheVolumeCreateActivity) CreatePeeringJobActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	input := result.JobInput
	logger.Debugf("Creating establish peering job for resource: %s (%s)", input.ResourceName, input.ResourceUUID)

	if existing, err := a.SE.GetJobByResourceUUID(ctx, input.ResourceUUID, string(coremodels.JobTypeFlexCacheEstablishPeering)); err == nil && existing != nil {
		if existing.State == string(coremodels.JobsStatePROCESSING) ||
			existing.State == string(coremodels.JobsStateDONE) ||
			existing.State == string(coremodels.JobsStateERROR) {
			logger.Debugf("Peering job already exists for %s (state=%s)", input.ResourceUUID, existing.State)
			return result, nil
		}
	}

	job := &datamodel.Job{
		Type:          string(coremodels.JobTypeFlexCacheEstablishPeering),
		State:         string(coremodels.JobsStatePROCESSING),
		ResourceName:  input.ResourceName,
		AccountID:     sql.NullInt64{Int64: input.AccountID, Valid: true},
		CorrelationID: input.CorrelationID,
		RequestID:     input.RequestID,
		WorkflowID:    input.WorkflowID,
		ScheduledAt:   time.Now(),
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: input.ResourceUUID,
		},
	}

	createdJob, err := a.SE.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create peering job: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrEstablishPeeringJobFailed, err)
	}

	logger.Debugf("Created peering job: %s", createdJob.UUID)
	return result, nil
}

// CompletePeeringJobActivity completes the peering job when we reach PENDING_CLUSTER_PEERING.
func (a *FlexCacheVolumeCreateActivity) CompletePeeringJobActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) error {
	logger := utilGetLogger(ctx)
	resourceUUID := result.DBVolume.UUID
	observedCacheState := result.DBVolume.CacheParameters.CacheState
	target := cvpModels.FlexCacheV1betaCacheStatePENDINGCLUSTERPEERING

	if observedCacheState != target {
		logger.Debugf("Peering job for %s remains PROCESSING; waiting for state=%s (observed=%s)",
			resourceUUID, target, observedCacheState)
		return nil
	}

	_, err := a.completeJob(ctx, completeJobOpts{
		ResourceUUID:  resourceUUID,
		JobType:       string(coremodels.JobTypeFlexCacheEstablishPeering),
		GetErrCode:    vsaerrors.ErrDescribingJobNotFound,
		UpdateErrCode: vsaerrors.ErrEstablishPeeringJobFailed,
	})
	return err
}

// StartInternalJobActivity creates and starts the internal monitoring job
func (a *FlexCacheVolumeCreateActivity) StartInternalJobActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	input := result.JobInput
	logger.Debugf("Starting internal job for resource: %s (%s)", input.ResourceName, input.ResourceUUID)

	if existing, err := a.SE.GetJobByResourceUUID(ctx, input.ResourceUUID, string(coremodels.JobTypeFlexCacheInternalPeering)); err == nil && existing != nil {
		if existing.State == string(coremodels.JobsStatePROCESSING) ||
			existing.State == string(coremodels.JobsStateDONE) ||
			existing.State == string(coremodels.JobsStateERROR) {
			logger.Debugf("Internal job already exists for %s (state=%s)", input.ResourceUUID, existing.State)
			return result, nil
		}
	}

	job := &datamodel.Job{
		Type:          string(coremodels.JobTypeFlexCacheInternalPeering),
		State:         string(coremodels.JobsStatePROCESSING),
		ResourceName:  input.ResourceName,
		AccountID:     sql.NullInt64{Int64: input.AccountID, Valid: true},
		CorrelationID: input.CorrelationID,
		RequestID:     input.RequestID,
		WorkflowID:    input.WorkflowID,
		ScheduledAt:   time.Now(),
		IsAdminJob:    true,
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: input.ResourceUUID,
		},
	}

	createdJob, err := a.SE.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create internal job: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInternalPeeringJobFailed, err)
	}

	logger.Debugf("Started internal job: %s", createdJob.UUID)
	return result, nil
}

// CompleteInternalJobActivity completes the internal job once volume creation succeeds.
func (a *FlexCacheVolumeCreateActivity) CompleteInternalJobActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) error {
	logger := utilGetLogger(ctx)
	resourceUUID := result.DBVolume.UUID
	observedCacheState := result.DBVolume.CacheParameters.CacheState
	target := cvpModels.FlexCacheV1betaCacheStatePEERED

	if observedCacheState != target {
		logger.Debugf("Peering job for %s remains PROCESSING; waiting for state=%s (observed=%s)",
			resourceUUID, target, observedCacheState)
		return nil
	}

	_, err := a.completeJob(ctx, completeJobOpts{
		ResourceUUID:  resourceUUID,
		JobType:       string(coremodels.JobTypeFlexCacheInternalPeering),
		GetErrCode:    vsaerrors.ErrDescribingJobNotFound,
		UpdateErrCode: vsaerrors.ErrInternalPeeringJobFailed,
	})
	return err
}

// FailJobActivity marks the specified job as ERROR.
func (a *FlexCacheVolumeCreateActivity) FailJobActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) error {
	logger := utilGetLogger(ctx)

	resourceUUID := result.DBVolume.UUID
	jobType := string(result.ActiveJobType)
	trackingID := result.ErrorTrackingID
	errorDetails := result.ErrorMessage

	logger.Warnf("Failing job for resource=%s type=%s", resourceUUID, jobType)

	job, err := a.SE.GetJobByResourceUUID(ctx, resourceUUID, jobType)
	if err != nil {
		logger.Errorf("FailJobActivity: unable to get job for resource=%s type=%s: %v", resourceUUID, jobType, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrDescribingJobNotFound, err)
	}
	if job == nil {
		logger.Errorf("FailJobActivity: job not found for resource=%s type=%s", resourceUUID, jobType)
		return vsaerrors.NewVCPError(vsaerrors.ErrDescribingJobNotFound,
			fmt.Errorf("job not found for resource=%s type=%s", resourceUUID, jobType))
	}

	switch job.State {
	case string(coremodels.JobsStateDONE):
		logger.Debugf("FailJobActivity: job %s is already DONE; not failing", job.UUID)
		return nil
	case string(coremodels.JobsStateERROR):
		logger.Debugf("FailJobActivity: job %s already in ERROR; no-op", job.UUID)
		return nil
	}

	// Default to existing values if none provided, to avoid blanking info.
	if trackingID == 0 {
		trackingID = job.TrackingID
	}
	if errorDetails == "" {
		if job.ErrorDetails != "" {
			errorDetails = job.ErrorDetails
		} else {
			errorDetails = "unspecified error"
		}
	}

	if err := a.SE.UpdateJob(ctx, job.UUID, string(coremodels.JobsStateERROR), trackingID, errorDetails); err != nil {
		logger.Errorf("FailJobActivity: failed to update job %s to ERROR: %v", job.UUID, err)
		return vsaerrors.NewVCPError(mapJobTypeToError(jobType), err)
	}

	logger.Warnf("FailJobActivity: job %s marked ERROR (type=%s; trackingID=%d)", job.UUID, jobType, trackingID)
	return nil
}

// completeJob is an internal helper to complete a job based on various criteria.
func (a *FlexCacheVolumeCreateActivity) completeJob(ctx context.Context, opts completeJobOpts) (*datamodel.Job, error) {
	logger := utilGetLogger(ctx)

	var (
		job *datamodel.Job
		err error
	)

	// Fetch the job by workflowID or resourceUUID+jobType
	switch {
	case opts.WorkflowID != "":
		job, err = a.SE.GetJob(ctx, opts.WorkflowID)
	case opts.ResourceUUID != "" && opts.JobType != "":
		job, err = a.SE.GetJobByResourceUUID(ctx, opts.ResourceUUID, opts.JobType)
	default:
		logger.Errorf("invalid completeJob options: missing job identifiers")
		return nil, vsaerrors.NewVCPError(opts.GetErrCode, fmt.Errorf("invalid completeJob options: missing identifiers"))
	}

	if err != nil {
		logger.Errorf("failed to get job: %v", err)
		return nil, vsaerrors.NewVCPError(opts.GetErrCode, err)
	}
	if job == nil {
		logger.Errorf("job not found")
		return nil, vsaerrors.NewVCPError(opts.GetErrCode, fmt.Errorf("job not found"))
	}

	// Idempotent short-circuit
	switch job.State {
	case string(coremodels.JobsStateDONE):
		logger.Debugf("job already completed: %s", job.UUID)
		return job, nil
	case string(coremodels.JobsStateERROR):
		logger.Debugf("job is in error state: %s", job.UUID)
		return job, nil
	}

	// Mark as DONE
	if err := a.SE.UpdateJob(ctx, job.UUID, string(coremodels.JobsStateDONE), job.TrackingID, job.ErrorDetails); err != nil {
		logger.Errorf("failed to mark job %s as completed: %v", job.UUID, err)
		return nil, vsaerrors.NewVCPError(opts.UpdateErrCode, err)
	}
	job.State = string(coremodels.JobsStateDONE)
	logger.Debugf("marked job as completed: %s", job.UUID)

	return job, nil
}

// mapJobTypeToError maps job types to the closest VCP error code for reporting.
func mapJobTypeToError(jobType string) int {
	switch jobType {
	case string(coremodels.JobTypeFlexCacheEstablishPeering):
		return vsaerrors.ErrEstablishPeeringJobFailed
	case string(coremodels.JobTypeFlexCacheInternalPeering):
		return vsaerrors.ErrInternalPeeringJobFailed
	case string(coremodels.JobTypeFlexCacheCreateVolume):
		return vsaerrors.ErrCreatingFlexCacheVolume
	default:
		// Fallback to a generic error code
		return vsaerrors.ErrInternalPeeringJobFailed
	}
}

// EnsureClusterPeerInOntapActivity checks the status of an existing cluster peer or decides to create a new one.
func (a FlexCacheVolumeCreateActivity) EnsureClusterPeerInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume

	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// No stored UUID -> create
	if volume.ClusterPeerUUID == nil {
		result.ClusterPeerAction = flexcache.ActionCreate
		logger.Debug("cluster peer UUID absent; will create")
		return result, nil
	}

	// Fetch existing
	cp, getErr := provider.GetClusterPeer(*volume.ClusterPeerUUID)
	if getErr != nil || cp == nil {
		result.ClusterPeerAction = flexcache.ActionCreate
		logger.Infof("stored cluster peer UUID not found; will create new (err=%v)", getErr)
		return result, nil
	}
	result.ClusterPeer = cp
	now := time.Now().UTC()

	switch {
	// Unrecoverable states -> recreate
	case cp.AuthenticationState == vsa.ClusterPeerAuthenticationStateProblem,
		cp.AuthenticationState == vsa.ClusterPeerAuthenticationStateAbsent,
		cp.Availability == vsa.ClusterPeerAvailabilityStateUnidentified:
		logger.Warnf("cluster peer invalid (auth=%s availability=%s); recreating",
			cp.AuthenticationState, cp.Availability)
		result.ClusterPeerAction = flexcache.ActionCreate
		return result, nil

	// Expired command and not yet peered -> recreate
	case cp.ExpiryTime != nil &&
		now.After(time.Time(*cp.ExpiryTime).UTC()) &&
		!(cp.AuthenticationState == vsa.ClusterPeerAuthenticationStateOK &&
			cp.Availability == vsa.ClusterPeerAvailabilityStateAvailable):
		logger.Infof("cluster peer command expired (uuid=%s); scheduling recreation", *volume.ClusterPeerUUID)
		result.ClusterPeerAction = flexcache.ActionCreate
		return result, nil

	// Ready -> reuse
	case cp.AuthenticationState == vsa.ClusterPeerAuthenticationStateOK &&
		cp.Availability == vsa.ClusterPeerAvailabilityStateAvailable:
		result.ClusterPeerAction = flexcache.ActionReady
		logger.Debug("reusing existing cluster peer (ready)")
		return result, nil

	// Wait (pending or partial)
	case cp.AuthenticationState == vsa.ClusterPeerAuthenticationStatePending ||
		cp.Availability == vsa.ClusterPeerAvailabilityStatePending ||
		cp.Availability == vsa.ClusterPeerAvailabilityStatePartial:
		result.ClusterPeerAction = flexcache.ActionWait
		logger.Debugf("cluster peer not ready (auth=%s availability=%s); will wait",
			cp.AuthenticationState, cp.Availability)
		return result, nil

	// Fallback -> recreate
	default:
		logger.Warnf("unknown cluster peer state (auth=%s availability=%s); recreating",
			cp.AuthenticationState, cp.Availability)
		result.ClusterPeerAction = flexcache.ActionCreate
		return result, nil
	}
}

// EnsureSVMPeerInOntapActivity checks the status of an existing SVM peer or decides to create a new one.
func (a FlexCacheVolumeCreateActivity) EnsureSVMPeerInOntapActivity(ctx context.Context, result *flexcache.CreateFlexCacheResult) (*flexcache.CreateFlexCacheResult, error) {
	logger := utilGetLogger(ctx)
	volume := result.DBVolume

	provider, err := hyperscalerGetProviderByNode(ctx, result.Node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// No stored UUID -> create
	if volume.SvmPeerUUID == nil {
		result.SVMPeerAction = flexcache.ActionCreate
		logger.Debug("SVM peer UUID absent; will create")
		return result, nil
	}

	// Fetch existing (lookup by SVM names; UUID retained for later persistence steps)
	svmPeer, err := provider.GetSVMPeer(&volume.Svm.Name, &volume.CacheParameters.PeerSvmName)
	if err != nil {
		// retryable if not found and we have no existing peer
		if errors.IsNotFoundErr(err) {
			result.SVMPeerAction = flexcache.ActionCreate
			logger.Infof("stored SVM peer UUID not found; will create new (err=%v)", err)
			return result, nil
		}
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	result.SVMPeer = svmPeer

	switch {
	// Invalid states -> recreate
	case svmPeer.State == vsa.SvmPeerStateRejected,
		svmPeer.State == vsa.SvmPeerStateSuspended:
		logger.Warnf("SVM peer invalid (state=%s); recreating", svmPeer.State)
		result.SVMPeerAction = flexcache.ActionCreate
		return result, nil

	// Ready -> reuse
	case svmPeer.State == vsa.SvmPeerStatePeered:
		result.SVMPeerAction = flexcache.ActionReady
		logger.Debug("reusing existing SVM peer (ready)")
		return result, nil

	// Wait
	case svmPeer.State == vsa.SvmPeerStatePending:
		result.SVMPeerAction = flexcache.ActionWait
		logger.Debugf("SVM peer not ready (state=%s); will wait", svmPeer.State)
		return result, nil

	// Fallback unknown -> recreate
	default:
		logger.Warnf("unknown SVM peer state (state=%s); recreating", svmPeer.State)
		result.SVMPeerAction = flexcache.ActionCreate
		return result, nil
	}
}
