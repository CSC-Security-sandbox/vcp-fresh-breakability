package activities

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

const (
	VolumeTypeRW                 = "rw"
	VolumeTypeDP                 = "dp"
	SnapshotPolicyNone           = "none"
	CrossRegionBackupType        = "CROSS_REGION"
	CrossRegionBackupVaultErrMsg = "Cross region backup vaults are not supported for ISCSI volumes"
	RestoreBackupWorkflow        = "RestoreBackupWorkflow"
	BytesPerGB                   = 1073741824 // 1024^3 bytes = 1 GB
)

type VolumeCreateActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

var (
	GetResourceNamesForBackup        = _getResourceNamesForBackup
	FindTenancy                      = _findTenancy
	CreateBucket                     = _createBucket
	GenerateResourceNames            = _generateResourceNames
	GetOrCreateAndGCSResources       = _getOrCreateAndGCSResources
	CheckBackupVaultExistsInVCP      = _checkBackupVaultExistsInVCP
	CheckForBucketResourceName       = _checkForBucketResourceName
	CheckIfBackupPolicyExistsInVCP   = _checkIfBackupPolicyExistsInVCP
	CreateBackupPolicyFetchedFromSDE = _createBackupPolicyFetchedFromSDE
	CreateBackupPolicySchedule       = _createBackupPolicySchedule
	GetPoolServiceAccountName        = _getPoolServiceAccount
	GrantStorageObjectAdminRole      = _grantStorageObjectAdminRole
)

var fetchTemporalClient = _fetchTemporalClient

// GetSignedJwtTokenFunc is a variable to allow mocking of auth.GetSignedJwtToken in tests
var GetSignedJwtTokenFunc = auth.GetSignedJwtToken

func _fetchTemporalClient(ctx context.Context) client.Client {
	return activity.GetClient(ctx)
}

func (a VolumeCreateActivity) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	se := a.SE

	return se.CreateVolume(ctx, volume)
}

func (a VolumeCreateActivity) GetAggregatesFromOntap(ctx context.Context, volume *datamodel.Volume, node *models.Node, totalNodes int) (*models.AggregateDistributionResult, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// By default we get all aggregates in a cluster
	res, err := provider.GetAggregates()
	if err != nil || res == nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	var largeVolumeConstituentCount int64
	// We can't have a large volume constituent count as zero or negative, unless it is updated intentionally. This is being checked at API level
	if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeVolumeConstituentCount != nil && *volume.LargeVolumeAttributes.LargeVolumeConstituentCount > 0 {
		largeVolumeConstituentCount = int64(*volume.LargeVolumeAttributes.LargeVolumeConstituentCount)
	}

	// Get the VSA instance type detail from Pool table
	vlmConfig := &vlm.VLMConfig{}
	err = json.Unmarshal([]byte(volume.Pool.VLMConfig), vlmConfig)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("error unmarshalling VLM config from pool: %v", err))
	}

	var result *models.AggregateDistributionResult
	// Get the aggregate distribution using the optimized greedy approach
	if volume.Pool.AllowAutoTiering {
		result, err = CalculateAggregatesForConstituentVolumesWithCVLimits(ctx, res, largeVolumeConstituentCount, totalNodes, vlmConfig.Deployment.VSAInstanceType)
	} else {
		result, err = CalculateAggregatesForConstituentVolumesWithSpaceLimits(ctx, res, largeVolumeConstituentCount, volume.SizeInBytes, totalNodes, vlmConfig.Deployment.VSAInstanceType)
	}
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Log the distribution details for debugging
	logger.Debugf("CV distribution - Aggregates: %v, HCF: %d", result.Aggregates, result.AggrMultiplier)

	return result, nil
}

func (a VolumeCreateActivity) CreateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node, snapshot *datamodel.Snapshot, backup *datamodel.Backup, aggrs *models.AggregateDistributionResult) (*vsa.VolumeResponse, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	volumeType := VolumeTypeRW
	if volume.VolumeAttributes.IsDataProtection || backup != nil {
		logger.Debug("create a DP volume !")
		volumeType = VolumeTypeDP
	}

	snapshotPolicyName := SnapshotPolicyNone
	if volume.SnapshotPolicy != nil && volume.SnapshotPolicy.Name != "" {
		snapshotPolicyName = volume.SnapshotPolicy.Name
	}
	var restoreFromSnapshotParam vsa.RestoreFromSnapshotParams
	if snapshot != nil {
		restoreFromSnapshotParam.ParentVolumeExternalUUID = snapshot.Volume.VolumeAttributes.ExternalUUID
		restoreFromSnapshotParam.SnapshotUUID = snapshot.SnapshotAttributes.ExternalUUID
		restoreFromSnapshotParam.ParentVolumeName = snapshot.Volume.Name
		restoreFromSnapshotParam.ParentVolumeSvmName = snapshot.Volume.Svm.Name
		restoreFromSnapshotParam.SnapshotName = snapshot.Name
	}

	params := vsa.CreateVolumeParams{
		VolumeName:          volume.Name,
		SvmName:             volume.Svm.Name,
		Size:                volume.SizeInBytes,
		VolumeType:          volumeType,
		SnapshotPolicyName:  snapshotPolicyName,
		RestoreFromSnapshot: &restoreFromSnapshotParam,
		SnapReserve:         volume.VolumeAttributes.SnapReserve,
		SnapshotDirectory:   volume.VolumeAttributes.SnapshotDirectory,
		TieringPolicy: &vsa.TieringPolicy{
			CoolAccessTieringPolicy: ontapModels.VolumeInlineTieringPolicyNone,
		},
	}

	if volume.LargeVolumeAttributes != nil && volume.LargeVolumeAttributes.LargeCapacity {
		params.Style = nillable.GetStringPtr(volStyleFlexGroup)
		if volume.LargeVolumeAttributes.LargeVolumeConstituentCount != nil {
			params.Aggregates = aggrs.Aggregates
			params.ConstituentsPerAggregate = nillable.GetInt64Ptr(aggrs.AggrMultiplier)
		} else {
			// this is being set for auto-provisioning of constituents
			params.TieringSupported = nillable.GetBoolPtr(true)
		}
	} else {
		params.Aggregates = []string{AggregateName}
	}

	if utils.IsFileProtocolSupported(volume.Account.Name) && volume.VolumeAttributes != nil && volume.VolumeAttributes.FileProperties != nil && volume.VolumeAttributes.FileProperties.ExportPolicy != nil {
		if !utils.IsSMBProtocols(volume.VolumeAttributes.Protocols) {
			params.ExportPolicy = &volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName
		}
		if params.VolumeType != VolumeTypeDP {
			params.JunctionPath = &volume.VolumeAttributes.FileProperties.JunctionPath
		}
	}

	if volume.AutoTieringEnabled && volume.AutoTieringPolicy != nil {
		params.TieringPolicy, err = CreateAutoTieringParams(ctx, se, &params, volume)
		if err != nil {
			return nil, err
		}
	}

	res, err := provider.CreateVolume(params)

	if err != nil {
		logger.Error("Error in provider.CreateVolume", "err", err)
		if errors.IsConflictErr(err) {
			return HandleVolumeCreateConflict(volume, provider)
		}
		return nil, err
	}
	logger.Debug("volume created successfully")

	return res, nil
}

func CreateAutoTieringParams(ctx context.Context, se database.Storage, params *vsa.CreateVolumeParams, volume *datamodel.Volume) (*vsa.TieringPolicy, error) {
	// If auto-tiering is paused for pool, we don't set the all auto-tiering policy during
	// volume creation in ontap. Since this supersedes the tiering fullness threshold and
	// doesn't stop tiering. We let the volume be created with default tiering policy 'none'
	// This will get later corrected when the pool will resume auto-tiering.
	if volume.AutoTieringPolicy.TieringPolicy == ontapModels.VolumeInlineTieringPolicyAll {
		// Fetch pool from db to check if auto-tiering is currently paused
		pool, err := se.GetPool(ctx, volume.Pool.UUID, volume.AccountID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		if !pool.AutoTieringConfig.TieringPaused {
			params.TieringPolicy.CoolAccessTieringPolicy = nillable.GetString(&volume.AutoTieringPolicy.TieringPolicy, ontapModels.VolumeInlineTieringPolicyAuto)
			params.TieringPolicy.CoolAccessRetrievalPolicy = nillable.GetString(&volume.AutoTieringPolicy.RetrievalPolicy, ontapModels.VolumeCloudRetrievalPolicyDefault)
			params.TieringPolicy.CoolnessPeriod = int64(volume.AutoTieringPolicy.CoolingThresholdDays)
		}
	} else {
		params.TieringPolicy.CoolAccessTieringPolicy = nillable.GetString(&volume.AutoTieringPolicy.TieringPolicy, utils.FetchTieringPolicyAsPerVolumeType(!utils.IsSanProtocols(volume.VolumeAttributes.Protocols)))
		params.TieringPolicy.CoolAccessRetrievalPolicy = nillable.GetString(&volume.AutoTieringPolicy.RetrievalPolicy, ontapModels.VolumeCloudRetrievalPolicyDefault)
		params.TieringPolicy.CoolnessPeriod = int64(volume.AutoTieringPolicy.CoolingThresholdDays)
	}

	return params.TieringPolicy, nil
}

func (a VolumeCreateActivity) UpdateLunName(ctx context.Context, volume *datamodel.Volume, node *models.Node, restoreVolCreateResponse *vsa.VolumeResponse) (*vsa.LunResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	lunName := utils.GetLunName(volume.Name)

	lun, err := LunGet(ctx, "", volume.Name, volume.Svm.Name, provider)
	if err != nil {
		logger.Debug("lun not found !")
		return nil, err
	}
	response := lun.ProviderResponse
	uuid := response.ExternalUUID
	logger.Debugf("\n\nLun Name : %s\n\n", lun.Name)
	lunSpace := restoreVolCreateResponse.AFSSize - restoreVolCreateResponse.MetadataSize
	lunUpdateParams := vsa.LunUpdateParams{
		UUID:       uuid,
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	}
	if lunSpace < lun.Size {
		lunUpdateParams.Size = lun.Size
	} else {
		lunUpdateParams.Size = lunSpace
	}
	err = provider.LunUpdate(lunUpdateParams)
	if err != nil {
		return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrRestoreVolumeValidation, err))
	}
	logger.Debug("lun updated successfully")
	lun, err = LunGet(ctx, lunName, volume.Name, volume.Svm.Name, provider)
	if err != nil {
		logger.Debug("lun not found !")
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return lun, nil
}

func (a VolumeCreateActivity) CreateExportPolicyInOntap(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	if volume.VolumeAttributes.FileProperties == nil {
		logger.Info("Skipping export policy creation for non-file volume")
		return nil
	}
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	vsaExportRules := make([]*vsa.ExportRule, 0)
	for _, rule := range volume.VolumeAttributes.FileProperties.ExportPolicy.ExportRules {
		vsaExportRule := &vsa.ExportRule{
			AccessType:     rule.AccessType,
			AllowedClients: rule.AllowedClients,
			CIFS:           rule.CIFS,
			NFSv3:          rule.NFSv3,
			NFSv4:          rule.NFSv4,
			Index:          rule.Index,
			AnonymousUser:  rule.AnonymousUser,
		}
		vsaExportRules = append(vsaExportRules, vsaExportRule)
	}
	vsaExportPolicy := &vsa.ExportPolicy{
		ExportPolicyName: volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName,
		SvmName:          volume.Svm.Name,
		ExportRules:      vsaExportRules,
	}
	err = provider.CreateExportPolicy(vsaExportPolicy)
	if err != nil {
		if errors.IsConflictErr(err) || strings.Contains(err.Error(), "duplicate entry") {
			// If export policy already exists, we can skip creation
			logger.Debug("Export policy already exists, skipping creation", "name", vsaExportPolicy.ExportPolicyName)
			return nil
		}
		return err
	}
	return nil
}

func HandleVolumeCreateConflict(volume *datamodel.Volume, provider vsa.Provider) (*vsa.VolumeResponse, error) {
	isRestore := false
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.RestoredBackupPath != "" {
		isRestore = true
	}
	volumeRes, err := provider.GetVolume(vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		IsRestore:  isRestore,
	})
	if err != nil {
		return nil, err
	}
	if volumeRes.State != ontapModels.VolumeStateOnline {
		err = provider.DeleteVolume(volume.VolumeAttributes.ExternalUUID, volume.Name)
		if err != nil {
			return nil, err
		}
		return nil, errors.New("volume " + volume.Name + " is not in online state, deleting & retrying creation")
	}
	return volumeRes, nil
}

func (a VolumeCreateActivity) CreateIgroup(ctx context.Context, volume *datamodel.Volume, hostParams []*common.HostParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// FixMe: What if a new host is added to the host group?
	for _, host := range hostParams {
		igroupExists, _, err := provider.IgroupExists(host.HostName, &volume.Svm.Name)
		if err != nil {
			return err
		}

		if !igroupExists {
			_, err := provider.IgroupCreate(vsa.IgroupCreateParams{
				IgroupName: host.HostName,
				SvmName:    volume.Svm.Name,
				OsType:     host.OsType,
				Initiator:  host.HostIQNs,
			})
			if err != nil {
				return err
			}
			logger.Debug("Igroup created successfully", "name", host.HostName)
		}
	}

	return nil
}

func (a VolumeCreateActivity) CreateLun(ctx context.Context, volume *datamodel.Volume, node *models.Node, availableSpace int64) (*vsa.LunResponse, error) {
	logger := util.GetLogger(ctx)
	if volume.VolumeAttributes.IsDataProtection {
		logger.Info("Skipping lun creation for data protection volume")
		return &vsa.LunResponse{}, nil
	}
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	lunName := utils.GetLunName(volume.Name)
	osType := ""
	if volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
		osType = (*volume.VolumeAttributes.BlockDevices)[0].OSType
		lunName = (*volume.VolumeAttributes.BlockDevices)[0].Name
	} else {
		osType = volume.VolumeAttributes.BlockProperties.OSType
	}

	lun, err := provider.LunCreate(vsa.LunCreateParams{
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		OsType:     osType,
		Size:       availableSpace,
	})
	if err != nil {
		if errors.IsConflictErr(err) {
			return LunGet(ctx, lunName, volume.Name, volume.Svm.Name, provider)
		}
		return nil, err
	}
	logger.Debug("lun created successfully")

	return lun, nil
}

func (a VolumeCreateActivity) UpdateVolumeStateInDB(ctx context.Context, volumeUUID, state, stateDetails string) error {
	se := a.SE

	err := se.UpdateVolumeFields(ctx, volumeUUID, map[string]interface{}{
		"state":         state,
		"state_details": stateDetails,
	})
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func LunGet(ctx context.Context, lunName, volumeName, svmName string, provider vsa.Provider) (*vsa.LunResponse, error) {
	logger := util.GetLogger(ctx)

	lun, err := provider.LunGet(vsa.LunGetParams{
		SvmName:    svmName,
		VolumeName: volumeName,
		LunName:    lunName,
	})
	if err != nil {
		return nil, err
	}

	logger.Debug("lun retrieved successfully", "lunName", lunName, "volumeName", volumeName, "svmName", svmName)
	return lun, nil
}

func (a VolumeCreateActivity) CreateLunMap(ctx context.Context, volume *datamodel.Volume, params *common.CreateLunMapParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	if volume.VolumeAttributes.IsDataProtection {
		logger.Info("Skipping CreateLunMap for data protection volume")
		return nil
	}
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	err = provider.LunMapCreate(vsa.LunMapCreateParams{
		LunName:    params.LunName,
		SvmName:    params.SvmName,
		IGroupName: params.HostNames,
	})
	if err != nil {
		if errors.IsConflictErr(err) {
			return nil
		}
		return err
	}
	logger.Debug("lun map created successfully")

	return nil
}

func (a VolumeCreateActivity) UpdateVolumeDetails(ctx context.Context, volume *datamodel.Volume, volCreateResponse *vsa.ProviderResponse) error {
	se := a.SE

	volume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.RestoredBackupPath != "" {
		// This is volume restore case
		volume.State = models.LifeCycleStateRestoring
		volume.StateDetails = models.LifeCycleStateRestoringDetails
	} else {
		volume.State = models.LifeCycleStateREADY
		volume.StateDetails = models.LifeCycleStateAvailableDetails
	}
	if err := se.UpdateVolume(ctx, volume); err != nil {
		return err
	}

	return nil
}

func (a VolumeCreateActivity) FinaliseRestoredVolume(ctx context.Context, volume *datamodel.Volume) error {
	se := a.SE
	volume.State = models.LifeCycleStateREADY
	volume.StateDetails = models.LifeCycleStateAvailableDetails
	if err := se.UpdateVolume(ctx, volume); err != nil {
		return err
	}

	return nil
}

func (a VolumeCreateActivity) GetHosts(ctx context.Context, volume *datamodel.Volume) ([]*datamodel.HostGroup, error) {
	se := a.SE

	if volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
		blockDevice := (*volume.VolumeAttributes.BlockDevices)[0]
		uuids := utils.GetHgUUIDs(blockDevice.HostGroupDetails)

		dbHostGroups, err := se.GetMultipleHostGroups(ctx, uuids, volume.AccountID)
		if err != nil {
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		if len(dbHostGroups) != len(uuids) {
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrAllHostGroupsNotFoundError, errors.New("all host groups could not be found")))
		}
		return dbHostGroups, nil
	}

	if volume.VolumeAttributes.BlockProperties == nil {
		return nil, errors.New("block properties not found")
	}

	uuids := utils.GetHgUUIDs(volume.VolumeAttributes.BlockProperties.HostGroupDetails)

	dbHostGroups, err := se.GetMultipleHostGroups(ctx, uuids, volume.AccountID)
	if err != nil {
		return nil, err
	}

	if len(dbHostGroups) != len(uuids) {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrAllHostGroupsNotFoundError, errors.New("all host groups could not be found")))
	}

	return dbHostGroups, nil
}

func (a VolumeCreateActivity) GetVolumesByPoolID(ctx context.Context, poolID int64) ([]*datamodel.Volume, error) {
	se := a.SE
	volumes, err := se.GetVolumesByPoolID(ctx, poolID)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return volumes, err
}

func _findTenancy(gcpService hyperscaler.GoogleServices, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*common.TenancyInfo, error) {
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	if tenantProjectRegion == nil {
		tenantProjectRegion = &Region
	}

	tenantProjectNumber, err := gcpService.GetTenantProject(consumerVPC, customerProjectNumber, *tenantProjectRegion)
	if err != nil {
		gcpService.GetLogger().Errorf("Error finding tenancy unit: %v", err)
		return nil, err
	}

	return &common.TenancyInfo{
		RegionalTenantProject: tenantProjectNumber,
	}, nil
}

func (a VolumeCreateActivity) FindTenancy(ctx context.Context, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*common.TenancyInfo, error) {
	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	gcpService.Logger.Debug("gcpService initialized")
	return FindTenancy(gcpService, consumerVPC, customerProjectNumber, tenantProjectRegion)
}

func _checkBackupVaultExistsInVCP(ctx context.Context, se database.Storage, volume *datamodel.Volume, region string) (*datamodel.BackupVault, error) {
	bvId := volume.DataProtection.BackupVaultID
	backupVault, err := se.GetBackupVaultByUUIDndOwnerID(ctx, bvId, volume.AccountID)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return nil, err
		}
	}
	if backupVault != nil {
		if backupVault.ImmutableAttributes != nil && !utils.IsImmutableBackupEnabled() {
			err := validateImmutableBackupVault(*backupVault.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration)
			if err != nil {
				return nil, err
			}
		}
		err := validateCRBBackupVault(backupVault)
		if err != nil {
			return nil, err
		}
		return backupVault, nil
	}
	bvParams := &datamodel.BackupVault{}

	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := CvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	vaults, err := cvpClient.BackupVault.V1betaListBackupVaults(&backup_vault.V1betaListBackupVaultsParams{
		LocationID:     region,
		ProjectNumber:  volume.Account.Name,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return nil, errors.NewNotFoundErr("Backup vault", nil)
		}
		logger.Error("Error checking backupVault : ", err)
		return nil, err
	}

	bvs := vaults.Payload.BackupVaults

	for _, bv := range bvs {
		if bv.BackupVaultID == bvId {
			if bv.BackupRetentionPolicy != nil && !utils.IsImmutableBackupEnabled() {
				err := validateImmutableBackupVault(*bv.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDays)
				if err != nil {
					return nil, err
				}
			}
			bvModel, err := ConvertToBackupVaultDataModel(bv, region)
			if err != nil {
				return nil, err
			}
			err = validateCRBBackupVault(bvModel)
			if err != nil {
				return nil, err
			}
			bvParams = bvModel
			break
		}
	}

	bvParams.AccountID = volume.AccountID
	createdBackupVault, err := se.CreateBackupVaultEntryInVCP(ctx, bvParams)
	if err != nil {
		return nil, err
	}

	return createdBackupVault, nil
}

func validateCRBBackupVault(backupVault *datamodel.BackupVault) error {
	if backupVault.BackupVaultType == CrossRegionBackupType {
		if !utils.IsCrossRegionBackupEnabled() {
			return errors.NewBadRequestErr(CrossRegionBackupVaultErrMsg)
		}
		if backupVault.SourceRegionName == nil || *backupVault.SourceRegionName == "" {
			return errors.NewBadRequestErr("Source region must be specified for cross-region backup vault")
		}
		if backupVault.BackupRegionName == nil || *backupVault.BackupRegionName == "" {
			return errors.NewBadRequestErr("Backup region must be specified for cross-region backup vault")
		}
		if *backupVault.SourceRegionName == *backupVault.BackupRegionName {
			return errors.NewBadRequestErr("Backup region must be different from source region for cross-region backup vault")
		}
		if backupVault.CrossRegionBackupVaultName == nil || *backupVault.CrossRegionBackupVaultName == "" {
			return errors.NewBadRequestErr("Cross-region backup vault name must be specified for cross-region backup vault")
		}
		if backupVault.LifeCycleState != models.LifeCycleStateREADY {
			return errors.NewBadRequestErr("Cross-region backup vault must be in READY state")
		}
	}
	return nil
}

func validateImmutableBackupVault(minRetentionDuration int64) error {
	if minRetentionDuration > 0 {
		return errors.NewBadRequestErr(utils.ImmutableBackupVaultErrMsg)
	}
	return nil
}

func (a VolumeCreateActivity) CheckBackupVaultExistsInVCP(ctx context.Context, volume *datamodel.Volume, region string) (*datamodel.BackupVault, error) {
	return CheckBackupVaultExistsInVCP(ctx, a.SE, volume, region)
}

func (a VolumeCreateActivity) CheckOrCreateRemoteBackupVaultInVCP(ctx context.Context, volume *datamodel.Volume, backupVault *datamodel.BackupVault, bucketDetails *common.BucketDetails) (*datamodel.BackupVault, error) {
	return CheckOrCreateRemoteBackupVaultInVCP(ctx, volume, backupVault, bucketDetails)
}

func (a VolumeCreateActivity) UpdateRemoteBackupVaultWithBucketDetails(ctx context.Context, volume *datamodel.Volume, sourceBV *datamodel.BackupVault, remoteBV *datamodel.BackupVault, bucketDetails *common.BucketDetails) error {
	return UpdateRemoteBackupVaultWithBucketDetails(ctx, volume, sourceBV, remoteBV, bucketDetails)
}

// SetupCrossRegionBackupPermissionsActivity sets up IAM permissions for cross-region backup vaults
// This activity grants the necessary permissions for the backup vault in region2 to access resources in region1
func (a *VolumeCreateActivity) SetupCrossRegionBackupPermissionsActivity(ctx context.Context, backupVault *datamodel.BackupVault, pool *datamodel.Pool, bucketDetails *common.BucketDetails) error {
	logger := util.GetLogger(ctx)
	volumeRegion := pool.ClusterDetails.RegionalTenantProject
	backupRegion := *backupVault.BackupRegionName
	if volumeRegion == backupRegion {
		logger.Infof("Volume and backup are in same region, skipping cross-region permission setup")
		return nil
	}

	poolServiceAccount, err := getBackupVaultPoolServiceAccount(pool, pool.ClusterDetails.RegionalTenantProject)
	if err != nil {
		logger.Errorf("Failed to get pool service account name: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Grant storage.objectAdmin role to the pool service account in the backup tenant project
	backupTenantProject := bucketDetails.TenantProjectNumber
	if backupTenantProject == "" {
		logger.Errorf("Backup vault %s missing TenantProjectNumber in bucket details", backupVault.UUID)
		return temporal.NewNonRetryableApplicationError(
			"TenantProjectNumber is required for cross-region permission setup",
			"MissingTenantProjectNumber",
			nil,
		)
	}

	logger.Infof("Backup tenant project: %s", backupTenantProject)
	err = grantBackupVaultStorageObjectAdminRole(ctx, poolServiceAccount, backupTenantProject)
	if err != nil {
		logger.Errorf("Failed to grant storage.objectAdmin role for cross-region backup: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully granted storage.objectAdmin role to service account %s in backup project %s for cross-region access", poolServiceAccount, backupTenantProject)

	return nil
}

// getBackupVaultPoolServiceAccount extracts the service account from the pool for backup vault operations
func getBackupVaultPoolServiceAccount(pool *datamodel.Pool, projectID string) (string, error) {
	saEmail := utils.ConstructServiceAccountEmail(pool.ServiceAccountId, projectID)
	return saEmail, nil
}

// grantBackupVaultStorageObjectAdminRole grants the storage.objectAdmin role to a service account for backup vault operations
func grantBackupVaultStorageObjectAdminRole(ctx context.Context, serviceAccountEmail, projectID string) error {
	gcpService, err := GetCloudService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Grant the specific role needed for backup access
	roles := []string{"roles/storage.objectAdmin"}
	err = gcpService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

func (a VolumeCreateActivity) CheckForBucketResourceName(ctx context.Context, volume *datamodel.Volume) (*common.BucketDetails, error) {
	return CheckForBucketResourceName(ctx, a.SE, volume)
}

func _checkForBucketResourceName(ctx context.Context, se database.Storage, volume *datamodel.Volume) (*common.BucketDetails, error) {
	logger := util.GetLogger(ctx)

	bvDetails, err := getBackupVaultDetails(se, ctx, volume.DataProtection.BackupVaultID, volume.AccountID)
	if err != nil {
		logger.Errorf("Error getting backup vault details: %v", err)
		return nil, err
	}
	var buckets datamodel.BucketDetailsArray
	if bvDetails != nil {
		if bvDetails.BucketDetails != nil {
			buckets = bvDetails.BucketDetails
			for _, bucket := range buckets {
				if strings.Contains(bucket.BucketName, volume.DataProtection.BackupVaultID) && volume.VolumeAttributes.VendorSubnetID == bucket.VendorSubnetID {
					return &common.BucketDetails{
						BucketName:          bucket.BucketName,
						ServiceAccountName:  bucket.ServiceAccountName,
						VendorSubnetID:      bucket.VendorSubnetID,
						TenantProjectNumber: bucket.TenantProjectNumber,
					}, nil
				}
			}
		}
	}
	return nil, nil
}

func getBackupVaultDetails(se database.Storage, ctx context.Context, bvID string, accountId int64) (*datamodel.BackupVault, error) {
	backupVault, err := se.GetBackupVaultByUUIDndOwnerID(ctx, bvID, accountId)
	if err != nil {
		if !strings.Contains(err.Error(), "backup vault not found") {
			return nil, err
		}
	}
	return backupVault, nil
}

func (a VolumeCreateActivity) GenerateResourceNames(ctx context.Context, volume *datamodel.Volume, tenancyDetails *common.TenancyInfo, gcpRegion string) (*common.ResourceNames, error) {
	return GenerateResourceNames(ctx, volume, tenancyDetails, gcpRegion)
}

func _generateResourceNames(ctx context.Context, volume *datamodel.Volume, tenancyDetails *common.TenancyInfo, gcpRegion string) (*common.ResourceNames, error) {
	logger := util.GetLogger(ctx)

	email, bucketName, serviceAccountId, err := GetResourceNamesForBackup(gcpRegion, gcpRegion, tenancyDetails.RegionalTenantProject, volume.DataProtection.BackupVaultID)
	if err != nil {
		logger.Errorf("Error generating resource names: %v", err)
		return nil, err
	}
	return &common.ResourceNames{
		Email:            email,
		BucketName:       bucketName,
		ServiceAccountId: serviceAccountId,
	}, nil
}

func (a VolumeCreateActivity) CreateBucket(ctx context.Context, resourceName *common.ResourceNames, tenancyDetails *common.TenancyInfo, region string) (*common.BucketDetails, error) {
	return CreateBucket(ctx, resourceName, tenancyDetails, region)
}

func _createBucket(ctx context.Context, resourceName *common.ResourceNames, tenancyDetails *common.TenancyInfo, region string) (*common.BucketDetails, error) {
	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	_, bucketDetails, err := GetOrCreateAndGCSResources(gcpService, resourceName.ServiceAccountId, tenancyDetails.RegionalTenantProject, resourceName.Email, resourceName.BucketName, region, "region")
	if err != nil {
		gcpService.Logger.Errorf("Error creating bucket: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return bucketDetails[0], nil
}

func UpdateBackupVaultWithBucketDetails(se database.Storage, ctx context.Context, volume *datamodel.Volume, bucketDetails *common.BucketDetails) error {
	existingBackupVault, err := se.GetBackupVaultByUUIDndOwnerID(ctx, volume.DataProtection.BackupVaultID, volume.AccountID)
	if err != nil {
		return err
	}

	convertCommonToDatamodel := func(bucketDetails *common.BucketDetails) *datamodel.BucketDetails {
		return &datamodel.BucketDetails{
			BucketName:          bucketDetails.BucketName,
			ServiceAccountName:  "", // No service accounts created
			TenantProjectNumber: bucketDetails.TenantProjectNumber,
			VendorSubnetID:      volume.VolumeAttributes.VendorSubnetID,
			SatisfiesPzi:        bucketDetails.SatisfiesPzi,
			SatisfiesPzs:        bucketDetails.SatisfiesPzs,
		}
	}

	if existingBackupVault.BucketDetails != nil {
		for _, bucket := range existingBackupVault.BucketDetails {
			if bucket.BucketName == bucketDetails.BucketName && bucket.VendorSubnetID == volume.VolumeAttributes.VendorSubnetID {
				return nil
			}
		}
	}

	newBucketDetail := convertCommonToDatamodel(bucketDetails)
	existingBackupVault.BucketDetails = append(existingBackupVault.BucketDetails, newBucketDetail)

	err = se.UpdateBackupVault(ctx, existingBackupVault)
	if err != nil {
		return err
	}

	return nil
}

func UpdateRemoteBackupVaultWithBucketDetails(ctx context.Context, volume *datamodel.Volume, sourceBV *datamodel.BackupVault, remoteBV *datamodel.BackupVault, bucketDetails *common.BucketDetails) error {
	logger := util.GetLogger(ctx)
	if sourceBV.BackupVaultType != CrossRegionBackupType ||
		sourceBV.SourceRegionName == nil || sourceBV.BackupRegionName == nil ||
		*sourceBV.SourceRegionName == *sourceBV.BackupRegionName {
		return nil
	}

	newBucketDetail := &datamodel.BucketDetails{
		BucketName:          bucketDetails.BucketName,
		ServiceAccountName:  bucketDetails.ServiceAccountName,
		TenantProjectNumber: bucketDetails.TenantProjectNumber,
		VendorSubnetID:      volume.VolumeAttributes.VendorSubnetID,
		SatisfiesPzi:        bucketDetails.SatisfiesPzi,
		SatisfiesPzs:        bucketDetails.SatisfiesPzs,
	}

	if bucketDetailsExist(remoteBV.BucketDetails, newBucketDetail) {
		logger.Info("Bucket details already exist in remote BackupVault",
			"backupVaultID", remoteBV.UUID,
			"bucketName", bucketDetails.BucketName)
		return nil
	}

	projectNumber := volume.Account.Name
	backupRegion := *remoteBV.BackupRegionName
	basePath, jwtToken, err := common.GetRemoteRegionConfig(backupRegion, projectNumber)
	if err != nil {
		logger.Error("Failed to get remote region configuration", "region", backupRegion, "error", err.Error())
		return err
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	updatedBucketDetails := append(sourceBV.BucketDetails, newBucketDetail)

	internalBucketDetails := make([]googleproxyclient.BackupVaultInternalUpdateV1betaBucketDetailsItem, 0, len(updatedBucketDetails))
	for _, bd := range updatedBucketDetails {
		internalBucketDetails = append(internalBucketDetails, googleproxyclient.BackupVaultInternalUpdateV1betaBucketDetailsItem{
			BucketName:          googleproxyclient.NewOptString(bd.BucketName),
			ServiceAccountName:  googleproxyclient.NewOptString(bd.ServiceAccountName),
			VendorSubnetId:      googleproxyclient.NewOptString(bd.VendorSubnetID),
			TenantProjectNumber: googleproxyclient.NewOptString(bd.TenantProjectNumber),
			SatisfiesPzi:        googleproxyclient.NewOptBool(bd.SatisfiesPzi),
			SatisfiesPzs:        googleproxyclient.NewOptBool(bd.SatisfiesPzs),
		})
	}

	updateRequest := &googleproxyclient.BackupVaultInternalUpdateV1beta{
		BucketDetails: internalBucketDetails,
	}

	params := googleproxyclient.V1betaInternalUpdateBackupVaultParams{
		BackupVaultId:  sourceBV.UUID,
		ProjectNumber:  projectNumber,
		LocationId:     backupRegion,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalUpdateBackupVault(ctx, updateRequest, params)
	if err != nil {
		logger.Error("Failed to call V1betaInternalUpdateBackupVault",
			"error", err.Error(),
			"backupVaultID", remoteBV.UUID,
			"region", backupRegion)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to update remote backup vault: %v", err),
			"InternalUpdateBackupVaultFailed",
			err,
		)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		isDone := r.Done.Value
		if !isDone {
			logger.Warn("Update operation for remote backup vault not marked as done, but treating as synchronous",
				"backupVaultID", remoteBV.UUID)
		}
		logger.Info("Successfully updated remote backup vault with new bucket details",
			"backupVaultID", remoteBV.UUID,
			"bucketName", bucketDetails.BucketName)
		return nil

	case *googleproxyclient.V1betaInternalUpdateBackupVaultBadRequest:
		logger.Error("Bad request updating remote backup vault", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Bad request updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultBadRequest",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultUnauthorized:
		logger.Error("Unauthorized to update remote backup vault", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unauthorized to update remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultUnauthorized",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultForbidden:
		logger.Error("Forbidden to update remote backup vault", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Forbidden to update remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultForbidden",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultNotFound:
		logger.Error("Remote backup vault not found", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Remote backup vault not found: %s", r.Message),
			"V1betaInternalUpdateBackupVaultNotFound",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultConflict:
		logger.Warn("Conflict updating remote backup vault", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Conflict updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultConflict",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultUnprocessableEntity:
		logger.Error("Unprocessable entity updating remote backup vault", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unprocessable entity updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultUnprocessableEntity",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultInternalServerError:
		logger.Error("Internal server error updating remote backup vault", "message", r.Message, "backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Internal server error updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultInternalServerError",
			errors.New(r.Message),
		)

	default:
		logger.Error("Unexpected response type from internal update backup vault endpoint",
			"type", fmt.Sprintf("%T", r),
			"backupVaultID", remoteBV.UUID)
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unexpected response type from internal update backup vault endpoint: %T", r),
			"UnexpectedUpdateResponseType",
			fmt.Errorf("unexpected response type: %T", r),
		)
	}
}

// CheckOrCreateRemoteBackupVaultInVCP checks if the remote BackupVault exists in VCP for cross-region backups
func CheckOrCreateRemoteBackupVaultInVCP(ctx context.Context, volume *datamodel.Volume, backupVault *datamodel.BackupVault, bucketDetails *common.BucketDetails) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	if backupVault.BackupVaultType != CrossRegionBackupType ||
		backupVault.SourceRegionName == nil || backupVault.BackupRegionName == nil ||
		*backupVault.SourceRegionName == *backupVault.BackupRegionName {
		return nil, nil
	}
	projectNumber := volume.Account.Name

	remoteBV, err := FetchRemoteBackupVaultFromVCP(ctx, backupVault.UUID, projectNumber, *backupVault.BackupRegionName)
	if err != nil && !errors.IsNotFoundErr(err) {
		logger.Error("Failed to fetch remote BackupVault from VCP", "error", err.Error())
		return nil, err
	}

	if remoteBV != nil {
		logger.Info("Remote BackupVault already exists in VCP", "backupVaultID", remoteBV.Name, "region", *backupVault.BackupRegionName)
		return remoteBV, nil
	}

	bv, err := CreateRemoteBackupVaultInVCP(ctx, projectNumber, backupVault, bucketDetails)
	if err != nil {
		logger.Error("Failed to create remote BackupVault with bucket details", "error", err.Error())
		return nil, err
	}

	return bv, nil
}

// FetchRemoteBackupVaultFromVCP calls the internal GET endpoint to fetch BackupVault from a backup region
func FetchRemoteBackupVaultFromVCP(ctx context.Context, backupVaultUUID, projectNumber, region string) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	basePath, jwtToken, err := common.GetRemoteRegionConfig(region, projectNumber)
	if err != nil {
		logger.Error("Failed to get remote region configuration", "region", region, "error", err)
		return nil, err
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	params := googleproxyclient.V1betaInternalDescribeBackupVaultParams{
		ProjectNumber:  projectNumber,
		LocationId:     region,
		BackupVaultId:  backupVaultUUID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDescribeBackupVault(ctx, params)
	if err != nil {
		logger.Error("Failed to fetch remote BackupVault", "error", err.Error(), "region", region, "backupVaultID", backupVaultUUID)
		return nil, errors.NewNotFoundErr("remote backup vault", &backupVaultUUID)
	}

	backupVault, ok := res.(*googleproxyclient.BackupVaultInternalV1beta)
	if !ok {
		logger.Error("Unexpected response type from remote BackupVault fetch", "type", fmt.Sprintf("%T", res))
		return nil, errors.NewNotFoundErr("remote backup vault", &backupVaultUUID)
	}

	result := convertInternalAPIToDatamodel(backupVault)
	logger.Info("Successfully fetched remote BackupVault", "backupVaultID", result.Name, "region", region)
	return result, nil
}

// CreateRemoteBackupVaultInVCP calls the internal POST endpoint to create BackupVault in a backup region
func CreateRemoteBackupVaultInVCP(ctx context.Context, projectNumber string, backupVault *datamodel.BackupVault, bucketDetails *common.BucketDetails) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	BackupRegion := *backupVault.BackupRegionName

	basePath, jwtToken, err := common.GetRemoteRegionConfig(BackupRegion, projectNumber)
	if err != nil {
		logger.Error("Failed to get remote region configuration", "region", BackupRegion, "error", err.Error())
		return nil, err
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	params := googleproxyclient.V1betaInternalCreateBackupVaultParams{
		ProjectNumber:  projectNumber,
		LocationId:     BackupRegion,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	if backupVault.BucketDetails == nil {
		backupVault.BucketDetails = append(backupVault.BucketDetails, &datamodel.BucketDetails{
			BucketName:          bucketDetails.BucketName,
			ServiceAccountName:  bucketDetails.ServiceAccountName,
			TenantProjectNumber: bucketDetails.TenantProjectNumber,
			VendorSubnetID:      bucketDetails.VendorSubnetID,
			SatisfiesPzi:        bucketDetails.SatisfiesPzi,
			SatisfiesPzs:        bucketDetails.SatisfiesPzs,
		})
	}

	res, err := googleProxyClient.Invoker.V1betaInternalCreateBackupVault(ctx, convertDatamodelToInternalAPI(backupVault), params)
	if err != nil {
		logger.Error("Failed to call V1betaInternalCreateBackupVault", "error", err.Error(), "region", BackupRegion, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to create remote backup vault: %v", err),
			"InternalCreateBackupVaultFailed",
			err,
		)
	}

	switch r := res.(type) {
	case *googleproxyclient.BackupVaultInternalV1beta:
		result := convertInternalAPIToDatamodel(r)
		return result, nil

	case *googleproxyclient.V1betaInternalCreateBackupVaultBadRequest:
		logger.Error("Bad request creating remote backup vault", "message", r.Message, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Bad request creating remote backup vault: %s", r.Message),
			"V1betaInternalCreateBackupVaultBadRequest",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalCreateBackupVaultUnauthorized:
		logger.Error("Unauthorized to create remote backup vault", "message", r.Message, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unauthorized to create remote backup vault: %s", r.Message),
			"V1betaInternalCreateBackupVaultUnauthorized",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalCreateBackupVaultForbidden:
		logger.Error("Forbidden to create remote backup vault", "message", r.Message, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Forbidden to create remote backup vault: %s", r.Message),
			"V1betaInternalCreateBackupVaultForbidden",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalCreateBackupVaultConflict:
		logger.Warn("Conflict creating remote backup vault - may already exist", "message", r.Message, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Conflict creating remote backup vault: %s", r.Message),
			"V1betaInternalCreateBackupVaultConflict",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalCreateBackupVaultUnprocessableEntity:
		logger.Error("Unprocessable entity creating remote backup vault", "message", r.Message, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unprocessable entity creating remote backup vault: %s", r.Message),
			"V1betaInternalCreateBackupVaultUnprocessableEntity",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalCreateBackupVaultInternalServerError:
		logger.Error("Internal server error creating remote backup vault", "message", r.Message, "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Internal server error creating remote backup vault: %s", r.Message),
			"V1betaInternalCreateBackupVaultInternalServerError",
			errors.New(r.Message),
		)

	default:
		logger.Error("Unexpected response type from internal create backup vault endpoint", "type", fmt.Sprintf("%T", r), "backupVaultID", backupVault.UUID)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unexpected response type from internal create backup vault endpoint: %T", r),
			"UnexpectedCreateResponseType",
			fmt.Errorf("unexpected response type: %T", r),
		)
	}
}

func _getOrCreateAndGCSResources(gcpServices hyperscaler.GoogleServices, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType string) (*hyperscalermodels.ServiceAccount, []*common.BucketDetails, error) {
	var bucketDetailsArr []*common.BucketDetails
	var err error

	// Only create the bucket - no service account creation
	err = gcpServices.CreateBucketIfNotExists(context.Background(), projectNumber, bucketName, tenantProjectRegion)
	if err != nil {
		return nil, nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Use empty service account name since we're not creating service accounts
	bucketDetails := &common.BucketDetails{
		BucketName:          bucketName,
		ServiceAccountName:  "", // No service account created
		TenantProjectNumber: projectNumber,
		Location:            locationType,
	}
	bucketDetailsArr = append(bucketDetailsArr, bucketDetails)

	return nil, bucketDetailsArr, nil
}

func (a VolumeCreateActivity) UpdateBackupVaultWithBucketDetails(ctx context.Context, volume *datamodel.Volume, bucketDetails *common.BucketDetails) error {
	return UpdateBackupVaultWithBucketDetails(a.SE, ctx, volume, bucketDetails)
}

func _getResourceNamesForBackup(gcpRegion, region, tenantProjectNumber, bvID string) (string, string, string, error) {
	return utils.GetResourcesNameForBackup(gcpRegion, region, tenantProjectNumber, bvID)
}

func (a VolumeCreateActivity) CreateSnapshotPolicyInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	if node != nil && volume != nil && volume.SnapshotPolicy != nil && volume.SnapshotPolicy.Name != "" {
		logger := util.GetLogger(ctx)
		provider, err := hyperscaler.GetProviderByNode(ctx, node)
		if err != nil {
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		err = provider.CreateSnapshotPolicy(&vsa.SnapshotPolicy{
			Name:      volume.SnapshotPolicy.Name,
			IsEnabled: volume.SnapshotPolicy.IsEnabled,
			Schedules: ConvertToVSASnapshotPolicySchedules(volume.SnapshotPolicy.Schedules),
		})
		if err != nil {
			logger.Errorf("failed to create snapshot policy: %v", err)
			return err
		}
	}
	return nil
}

// LunSizeUpdateValidation Validates if the LUN size can be updated based on the available space and SnapReserve constraints.
func (a VolumeCreateActivity) LunSizeUpdateValidation(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	logger := util.GetLogger(ctx)
	requiredLunSpace := volume.SizeInBytes * (100 - int64(volume.VolumeAttributes.SnapReserve)) / 100
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	lun, err := LunGet(ctx, "", volume.Name, volume.Svm.Name, provider)
	if err != nil {
		logger.Debug("lun not found !")
		return err
	}
	// Check if the available space is less than the current LUN size
	if requiredLunSpace < lun.Size {
		logger.Errorf("Lun size %d cannot be reduced to %d", lun.Size, requiredLunSpace)
		err = vsaerrors.NewVCPError(vsaerrors.ErrRestoreVolumeValidation, fmt.Errorf("Error restoring volume - Cannot restore a volume with this given size and snapReserve. Please consider increasing the volume size to at least of size %.2f GB along with this snapReserve", float64(lun.Size)/float64(BytesPerGB)*(utils.PercentageBase/float64(utils.PercentageBase-volume.VolumeAttributes.SnapReserve))))
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(err)
	}
	return nil
}

// UpdateClonedVolumeBeforeSplit updates the size, snapReserve of the cloned volume before split in ONTAP.
func (a VolumeCreateActivity) UpdateClonedVolumeBeforeSplit(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.VolumeResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// By initializing snapReserve to 0, we avoid inheriting the parent's snapReserve and can safely update it to the customer-specified value after cloning.
	// Reason: ONTAP restricts increasing snapReserve beyond the parent's availableSpace if the parent volume's available space is fully consumed.
	err = updateVolume(ctx, provider, vsa.UpdateVolumeParams{
		UUID:        volume.VolumeAttributes.ExternalUUID,
		SnapReserve: nillable.GetInt64Ptr(0),
	})
	if err != nil {
		logger.Errorf("Failed to update snapReserve of cloned volume %s in ontap before split: %v", volume.Name, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	preSplitUpdateParams := vsa.UpdateVolumeParams{
		UUID:               volume.VolumeAttributes.ExternalUUID,
		Size:               volume.SizeInBytes,
		SnapshotPolicyName: volume.SnapshotPolicy.Name,
		SnapReserve:        &volume.VolumeAttributes.SnapReserve,
	}
	if volume.VolumeAttributes != nil && utils.IsNasProtocols(volume.VolumeAttributes.Protocols) && volume.VolumeAttributes.FileProperties != nil && volume.VolumeAttributes.FileProperties.ExportPolicy != nil {
		preSplitUpdateParams.ExportPolicy = &volume.VolumeAttributes.FileProperties.ExportPolicy.ExportPolicyName
		preSplitUpdateParams.JunctionPath = &volume.VolumeAttributes.FileProperties.JunctionPath
		preSplitUpdateParams.SnapshotDirectoryAccess = &volume.VolumeAttributes.SnapshotDirectory
	}
	err = updateVolume(ctx, provider, preSplitUpdateParams)
	if err != nil {
		logger.Errorf("Failed to update cloned volume %s in ontap before split: %v", volume.Name, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Debugf("Cloned volume %s updated successfully in ontap", volume.Name)
	volumeRes, err := provider.GetVolume(vsa.GetVolumeParams{
		UUID:       volume.VolumeAttributes.ExternalUUID,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	})
	if err != nil {
		logger.Errorf("Failed to get volume %s from ontap after pre-split update: %v", volume.Name, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return volumeRes, nil
}

// InitiateSplitForVolume initiates a split for the given volume in ONTAP.
func (a VolumeCreateActivity) InitiateSplitForVolume(ctx context.Context, volume *datamodel.Volume, node *models.Node, snapshot *datamodel.Snapshot) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	updateVolumeParams := &vsa.UpdateVolumeParams{
		UUID:          volume.VolumeAttributes.ExternalUUID,
		InitiateSplit: true,
	}
	err = updateVolume(ctx, provider, *updateVolumeParams)
	if err != nil {
		logger.Errorf("Failed to initiate split %s in ontap: %v", volume.Name, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	var cloneSnapshot *datamodel.Snapshot
	// Get the clone volume snapshot that has the same name as the parent snapshot
	if volume.VolumeAttributes != nil && volume.VolumeAttributes.CloneParentInfo != nil && volume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID != "" && volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID != "" {
		// Get the parent volume to access its account ID and volume ID
		parentVolume, err := a.SE.GetVolume(ctx, volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID)
		if err != nil {
			logger.Warnf("Failed to get parent volume %s: %v", volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID, err)
			return nil
		}

		// Get the parent snapshot by UUID to retrieve its name
		parentSnapshot, err := a.SE.GetSnapshotByUUID(ctx, volume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID, parentVolume.AccountID, parentVolume.ID)
		if err != nil {
			logger.Warnf("Failed to get parent snapshot %s: %v", volume.VolumeAttributes.CloneParentInfo.ParentSnapshotUUID, err)
			return nil
		}

		// Get the clone volume snapshot that has the same name as the parent snapshot
		cloneSnapshot, err = a.SE.GetSnapshotByNameAndVolumeId(ctx, parentSnapshot.Name, volume.AccountID, volume.ID)
		if err != nil {
			logger.Warnf("Failed to get clone volume snapshot with name %s for volume %s: %v", parentSnapshot.Name, volume.Name, err)
			return nil
		}
		logger.Debugf("Found clone volume snapshot %s (UUID: %s) with same name as parent snapshot %s", cloneSnapshot.Name, cloneSnapshot.UUID, parentSnapshot.Name)

		if cloneSnapshot != nil {
			_, err := a.SE.DeleteSnapshot(ctx, cloneSnapshot.UUID)
			if err != nil {
				logger.Warnf("Snapshot %s not found, assuming it is already deleted", cloneSnapshot.Name)
				return nil
			}

			logger.Debugf("Snapshot %s (UUID: %s) marked as deleted successfully in the db", cloneSnapshot.Name, cloneSnapshot.UUID)

			// Hydrate the clone snapshot to CCFE after split
			hydrateErr := hydrationActivities.HydrateBatchSnapshotstoCCFE(ctx, nil, []*datamodel.Snapshot{cloneSnapshot})
			if hydrateErr != nil {
				logger.Warnf("Failed to hydrate snapshots to CCFE after volume revert: %v, snapshots: %+v", hydrateErr, cloneSnapshot)
			}
		}
	}

	logger.Debugf("Split %s initiated successfully in ontap", volume.Name)
	return nil
}

func ConvertToVSASnapshotPolicySchedules(schedules []*datamodel.SnapshotPolicySchedule) []*vsa.SnapshotPolicySchedule {
	if schedules == nil {
		return nil
	}
	var vsaPolicySchedules []*vsa.SnapshotPolicySchedule
	for _, schedule := range schedules {
		vsaSchedule := &vsa.Schedule{
			DaysOfMonth: schedule.DaysOfMonth,
			DaysOfWeek:  schedule.DaysOfWeek,
			Hours:       schedule.Hours,
			Minutes:     schedule.Minutes,
		}
		vsaPolicySchedules = append(vsaPolicySchedules, &vsa.SnapshotPolicySchedule{
			Schedule:        vsaSchedule,
			Prefix:          schedule.SnapmirrorLabel,
			Count:           schedule.Count,
			SnapmirrorLabel: schedule.SnapmirrorLabel,
		})
	}
	return vsaPolicySchedules
}

func (a VolumeCreateActivity) CheckIfBackupPolicyExistsInVCP(ctx context.Context, backupPolicyUUID string, accountId int64) (bool, error) {
	return _checkIfBackupPolicyExistsInVCP(ctx, a.SE, backupPolicyUUID, accountId)
}

func (a VolumeCreateActivity) CreateBackupPolicyFetchedFromSDE(ctx context.Context, volume *datamodel.Volume, region string) (*datamodel.BackupPolicy, error) {
	return _createBackupPolicyFetchedFromSDE(ctx, a.SE, volume, region)
}

func _checkIfBackupPolicyExistsInVCP(ctx context.Context, se database.Storage, backupPolicyUUID string, accountId int64) (bool, error) {
	backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyUUID, accountId)
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return false, err
		}
	}
	if backupPolicy != nil {
		return true, nil
	}
	return false, nil
}

func _createBackupPolicyFetchedFromSDE(ctx context.Context, se database.Storage, volume *datamodel.Volume, region string) (*datamodel.BackupPolicy, error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := CvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	backupPolicyUUID := volume.DataProtection.BackupPolicyID

	cvpBackupPolicy, err := cvpClient.BackupPolicy.V1betaDescribeBackupPolicy(&backup_policy.V1betaDescribeBackupPolicyParams{
		BackupPolicyID: backupPolicyUUID,
		LocationID:     region,
		ProjectNumber:  volume.Account.Name,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		logger.Errorf("Error checking backup policy in SDE : %v", err)
		return nil, err
	}
	if cvpBackupPolicy == nil || cvpBackupPolicy.Payload == nil {
		logger.Error("No backup policy found in SDE")
		return nil, errors.NewNotFoundErr("Backup policy", &backupPolicyUUID)
	}

	backupPolicy := ConvertToBackupPolicyDataModel(cvpBackupPolicy.Payload)
	backupPolicy.AccountID = volume.AccountID

	dbBackupPolicy, err := se.CreateBackupPolicyEntryInVCP(ctx, backupPolicy)
	if err != nil {
		return nil, err
	}
	return dbBackupPolicy, nil
}

func (a VolumeCreateActivity) CreateBackupPolicySchedule(ctx context.Context, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
	return _createBackupPolicySchedule(ctx, a.Scheduler, vcpBackupPolicy, customSchedule)
}

func _createBackupPolicySchedule(ctx context.Context, temporalScheduler *scheduler.TemporalScheduler, vcpBackupPolicy *datamodel.BackupPolicy, customSchedule string) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Creating backup policy schedule for policy: %s", vcpBackupPolicy.Name)

	var cronExpr string
	if customSchedule != "" {
		// Use the custom schedule if provided
		cronExpr = customSchedule
		logger.Infof("Using custom backup schedule: %s", cronExpr)
	} else {
		// Default cron expression based on the created time of the backup policy
		backupPolicyCreatedTime := vcpBackupPolicy.CreatedAt
		cronExpr = fmt.Sprintf("%d %d * * *", backupPolicyCreatedTime.Minute(), backupPolicyCreatedTime.Hour())
		logger.Infof("Using default backup schedule based on creation time: %s", cronExpr)
	}

	createParams := scheduler.CreateScheduleParams{
		ScheduleParams: scheduler.ScheduleParams{
			ScheduleID: vcpBackupPolicy.UUID,
			Args: []interface{}{
				vcpBackupPolicy,
			},
		},
		TemporalScheduleOptions: scheduler.TemporalCreateScheduleParams{
			WorkflowID: utils.RandomUUID(),
			Workflow:   "CreateScheduledBackupInitWorkflow",
			Spec: client.ScheduleSpec{
				CronExpressions: []string{cronExpr},
			},
		},
	}

	_, err := temporalScheduler.Create(ctx, createParams)
	if err != nil {
		logger.Errorf("Failed to create backup policy schedule: %v", err)
		return err
	}
	return nil
}

func (a VolumeCreateActivity) CreateRestoreWorkflow(ctx context.Context, createVolumeParams *common.CreateVolumeParams, volume *datamodel.Volume, hostParams []*common.HostParams, backupVault *datamodel.BackupVault, backup *datamodel.Backup, volCreateResponse *vsa.VolumeResponse) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Creating backup restore workflow for backup: %s", backup.Name)
	se := a.SE
	jobType := models.JobTypeRestoreBackup
	job := &datamodel.Job{
		Type:          string(jobType),
		State:         string(models.JobsStateNEW),
		ResourceName:  volume.Name,
		AccountID:     sql.NullInt64{Int64: volume.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create snapshot delete job in database %v", err)
		return err
	}

	temporalClient := fetchTemporalClient(ctx)
	_, err = temporalClient.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		RestoreBackupWorkflow,
		createVolumeParams,
		volume,
		backupVault,
		backup,
		hostParams,
		volCreateResponse,
	)
	if err != nil {
		logger.Error("Failed to start restore backup workflow: ", "error", err)
		return err
	}

	return nil
}

func (a VolumeCreateActivity) UpdateVolumeAttributesInDB(ctx context.Context, volumeUUID string, volumeAttributes *datamodel.VolumeAttributes) error {
	se := a.SE

	err := se.UpdateVolumeFields(ctx, volumeUUID, map[string]interface{}{
		"volume_attributes": volumeAttributes,
	})
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// CrossPoolOrVPCRestorationActivity handles the VPC pool restoration logic when restoring a backup to a different VPC pool
func (a *VolumeCreateActivity) CrossPoolOrVPCRestorationActivity(ctx context.Context, targetPool *datamodel.Pool, backup *datamodel.Backup) error {
	log := util.GetLogger(ctx)

	targetPoolTenantProject, err := GetPoolTenantProject(targetPool)
	if err != nil {
		log.Errorf("Failed to get target pool tenant project: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	backupTenantProject, err := GetBackupTenantProject(backup)
	if err != nil {
		log.Errorf("Failed to get backup tenant project: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if strings.EqualFold(targetPoolTenantProject, backupTenantProject) {
		return nil
	}

	log.Infof("Target pool tenant project (%s) differs from backup tenant project (%s), setting up cross-project permissions", targetPoolTenantProject, backupTenantProject)

	err = a.SetupCrossTenantProjectPermissions(ctx, targetPool, backupTenantProject)
	if err != nil {
		log.Errorf("Failed to setup cross-project permissions: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	log.Infof("Successfully set up cross-project permissions for VPC pool restoration")
	return nil
}

// setupCrossTenantProjectPermissions sets up the required IAM permissions for cross-project backup restoration
func (a *VolumeCreateActivity) SetupCrossTenantProjectPermissions(ctx context.Context, targetPool *datamodel.Pool, backupTenantProject string) error {
	log := util.GetLogger(ctx)

	// Get the service account from the target pool
	poolServiceAccount, err := GetPoolServiceAccountName(targetPool, targetPool.ClusterDetails.RegionalTenantProject)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Grant the storage.objectAdmin role to the pool service account in the backup tenant project
	err = GrantStorageObjectAdminRole(ctx, poolServiceAccount, backupTenantProject)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	log.Infof("Successfully granted storage.objectAdmin role to service account %s in project %s", poolServiceAccount, backupTenantProject)

	return nil
}

// getPoolServiceAccount extracts the service account from the target pool
func _getPoolServiceAccount(pool *datamodel.Pool, projectID string) (string, error) {
	saEmail := utils.ConstructServiceAccountEmail(pool.ServiceAccountId, projectID)
	return saEmail, nil
}

// _grantStorageObjectAdminRole  grants the storage.objectAdmin role to a service account in a project
func _grantStorageObjectAdminRole(ctx context.Context, serviceAccountEmail, projectID string) error {
	gcpService, err := GetCloudService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Grant the specific role needed for backup restoration
	roles := []string{"roles/storage.objectAdmin"}
	err = gcpService.AttachOrUpdateRolesForServiceAccounts(roles, serviceAccountEmail, projectID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// DeleteServiceAccountInBackupTenantProject deletes the service account in the backup tenant project after restoration is complete
func (a VolumeCreateActivity) DeleteRolesForServiceAccountInBackupTenantProject(ctx context.Context, targetPool *datamodel.Pool, backup *datamodel.Backup) error {
	log := util.GetLogger(ctx)

	// Get the service account from the target pool
	poolServiceAccount, err := GetPoolServiceAccountName(targetPool, targetPool.ClusterDetails.RegionalTenantProject)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	backupTenantProject, err := GetBackupTenantProject(backup)
	if err != nil {
		log.Errorf("Failed to get backup tenant project: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	gcpService, err := GetCloudService(ctx)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Grant the specific role needed for backup restoration
	roles := []string{"roles/storage.objectAdmin"}
	err = gcpService.RemoveRolesFromServiceAccounts(roles, poolServiceAccount, backupTenantProject)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// DeleteObjectStoreForCrossVPC deletes object store if the target pool and backup are in different tenant projects
func (a VolumeCreateActivity) DeleteObjectStoreForCrossVPC(ctx context.Context, targetPool *datamodel.Pool, backup *datamodel.Backup, node *models.Node, name string) (*vsa.OntapAsyncResponse, error) {
	log := util.GetLogger(ctx)

	// TODO - Ideally the Cloud Target should be deleted post-restoration. However, due to errors related to
	//  dangling snapmirror relationships, this validation is skipped for cross-region backup restores
	if backup.BackupVault != nil && backup.BackupVault.BackupVaultType == CrossRegionBackupType {
		log.Infof("Skipping DeleteObjectStoreForCrossVPC for cross-region backup restore")
		return nil, nil
	}
	targetPoolTenantProject, err := GetPoolTenantProject(targetPool)
	if err != nil {
		log.Errorf("Failed to get target pool tenant project: %v", err)
		return nil, nil
	}

	backupTenantProject, err := GetBackupTenantProject(backup)
	if err != nil {
		log.Errorf("Failed to get backup tenant project: %v", err)
		return nil, nil
	}

	if strings.EqualFold(targetPoolTenantProject, backupTenantProject) {
		return nil, nil
	}

	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	// Handle both return values from CloudTargetGet
	objectStore, err := provider.CloudTargetGet(&name)
	if err != nil {
		// If there is an error, it means the object store does not exist
		log.Infof("Object store %s does not exist, nothing to delete", name)
		return nil, nil
	}
	if objectStore == nil || objectStore.UUID == nil {
		log.Infof("Object store %s does not exist, nothing to delete", name)
		return nil, nil
	}
	asyncResp, err := provider.CloudTargetDelete(*objectStore.UUID)
	if err != nil {
		return nil, err
	}
	return asyncResp, nil
}

func (a VolumeCreateActivity) ConfigureLdap(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	se := a.SE
	logger := util.GetLogger(ctx)
	if volume.VolumeAttributes == nil || volume.VolumeAttributes.FileProperties == nil {
		logger.Info("Skipping ldap configuration for non-file volume")
		return nil
	}
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	pool, err := se.GetPool(ctx, volume.Pool.UUID, volume.AccountID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	ldapEnabled := false
	if pool.PoolAttributes != nil {
		ldapEnabled = pool.PoolAttributes.LdapEnabled
	}
	logger.Infof("Configure LDAP for volume %s", ldapEnabled)
	if !ldapEnabled {
		logger.Info("Skipping ldap configuration for non-LDAP pool")
		return nil
	}

	ad, err := se.GetActiveDirectoryForPoolByPoolID(ctx, pool.ID)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if ad == nil {
		logger.Error("Skipping ldap configuration for non-active directory pool")
		return vsaerrors.WrapAsTemporalApplicationError(errors.New("Active Directory configuration is required for LDAP-enabled pools but is missing"))
	}

	err = provider.CreateLdap(ad, volume)
	if err != nil {
		if errors.IsConflictErr(err) {
			// If LDAP config already exists, we can skip creation
			logger.Info("LDAP config already exists, skipping LDAP configuration")
			return nil
		}
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

// convertInternalAPIToDatamodel converts internal API BackupVault to datamodel BackupVault
func convertInternalAPIToDatamodel(apiBackupVault *googleproxyclient.BackupVaultInternalV1beta) *datamodel.BackupVault {
	if apiBackupVault == nil {
		return nil
	}

	backupVault := &datamodel.BackupVault{
		Name:            apiBackupVault.BackupVaultId,
		AccountVendorID: apiBackupVault.AccountVendorId,
		LifeCycleState:  string(apiBackupVault.LifeCycleState),
		BackupVaultType: string(apiBackupVault.BackupVaultType),
	}

	if apiBackupVault.Description.IsSet() && apiBackupVault.Description.Value != "" {
		desc := apiBackupVault.Description.Value
		backupVault.Description = &desc
	}

	if apiBackupVault.LifeCycleStateDetails.IsSet() && apiBackupVault.LifeCycleStateDetails.Value != "" {
		details := apiBackupVault.LifeCycleStateDetails.Value
		backupVault.LifeCycleStateDetails = details
	}

	if apiBackupVault.SourceRegion.IsSet() && apiBackupVault.SourceRegion.Value != "" {
		sourceRegion := apiBackupVault.SourceRegion.Value
		backupVault.SourceRegionName = &sourceRegion
	}

	if apiBackupVault.BackupRegion.IsSet() && apiBackupVault.BackupRegion.Value != "" {
		backupRegion := apiBackupVault.BackupRegion.Value
		backupVault.BackupRegionName = &backupRegion
	}

	if apiBackupVault.CrossRegionBackupVaultName.IsSet() && apiBackupVault.CrossRegionBackupVaultName.Value != "" {
		crossRegionName := apiBackupVault.CrossRegionBackupVaultName.Value
		backupVault.CrossRegionBackupVaultName = &crossRegionName
	}

	if apiBackupVault.ExternalUuid.IsSet() && apiBackupVault.ExternalUuid.Value != "" {
		externalUuid := apiBackupVault.ExternalUuid.Value
		backupVault.ExternalUUID = &externalUuid
	}

	if len(apiBackupVault.BucketDetails) > 0 {
		bucketDetails := make(datamodel.BucketDetailsArray, 0, len(apiBackupVault.BucketDetails))
		for _, bucket := range apiBackupVault.BucketDetails {
			bucketDetail := &datamodel.BucketDetails{}

			if bucket.BucketName.IsSet() && bucket.BucketName.Value != "" {
				bucketDetail.BucketName = bucket.BucketName.Value
			}
			if bucket.ServiceAccountName.IsSet() {
				bucketDetail.ServiceAccountName = bucket.ServiceAccountName.Value
			}
			if bucket.VendorSubnetId.IsSet() && bucket.VendorSubnetId.Value != "" {
				bucketDetail.VendorSubnetID = bucket.VendorSubnetId.Value
			}
			if bucket.TenantProjectNumber.IsSet() && bucket.TenantProjectNumber.Value != "" {
				bucketDetail.TenantProjectNumber = bucket.TenantProjectNumber.Value
			}

			if bucketDetail.BucketName != "" {
				bucketDetails = append(bucketDetails, bucketDetail)
			}
		}
		if len(bucketDetails) > 0 {
			backupVault.BucketDetails = bucketDetails
		}
	}

	return backupVault
}

// convertDatamodelToInternalAPI converts datamodel BackupVault to internal API BackupVault
func convertDatamodelToInternalAPI(datamodelBackupVault *datamodel.BackupVault) *googleproxyclient.BackupVaultInternalV1beta {
	if datamodelBackupVault == nil {
		return nil
	}

	apiBackupVault := &googleproxyclient.BackupVaultInternalV1beta{
		BackupVaultId:   datamodelBackupVault.UUID,
		ResourceId:      datamodelBackupVault.Name,
		AccountVendorId: datamodelBackupVault.AccountVendorID,
	}

	apiBackupVault.BackupVaultType = googleproxyclient.BackupVaultInternalV1betaBackupVaultType(datamodelBackupVault.BackupVaultType)
	apiBackupVault.LifeCycleState = googleproxyclient.BackupVaultInternalV1betaLifeCycleState(datamodelBackupVault.LifeCycleState)

	if datamodelBackupVault.Description != nil {
		apiBackupVault.Description = googleproxyclient.NewOptString(*datamodelBackupVault.Description)
	}

	if datamodelBackupVault.LifeCycleStateDetails != "" {
		apiBackupVault.LifeCycleStateDetails = googleproxyclient.NewOptString(datamodelBackupVault.LifeCycleStateDetails)
	}

	if datamodelBackupVault.SourceRegionName != nil {
		apiBackupVault.SourceRegion = googleproxyclient.NewOptString(*datamodelBackupVault.SourceRegionName)
	}

	if datamodelBackupVault.BackupRegionName != nil {
		apiBackupVault.BackupRegion = googleproxyclient.NewOptString(*datamodelBackupVault.BackupRegionName)
	}

	if datamodelBackupVault.CrossRegionBackupVaultName != nil {
		apiBackupVault.CrossRegionBackupVaultName = googleproxyclient.NewOptString(*datamodelBackupVault.CrossRegionBackupVaultName)
	}

	if datamodelBackupVault.ExternalUUID != nil {
		apiBackupVault.ExternalUuid = googleproxyclient.NewOptString(*datamodelBackupVault.ExternalUUID)
	}

	if len(datamodelBackupVault.BucketDetails) > 0 {
		var bucketDetails []googleproxyclient.BackupVaultInternalV1betaBucketDetailsItem
		for _, bucket := range datamodelBackupVault.BucketDetails {
			bucketDetail := googleproxyclient.BackupVaultInternalV1betaBucketDetailsItem{
				BucketName:          googleproxyclient.NewOptString(bucket.BucketName),
				ServiceAccountName:  googleproxyclient.NewOptString(bucket.ServiceAccountName),
				VendorSubnetId:      googleproxyclient.NewOptString(bucket.VendorSubnetID),
				TenantProjectNumber: googleproxyclient.NewOptString(bucket.TenantProjectNumber),
				SatisfiesPzs:        googleproxyclient.NewOptBool(bucket.SatisfiesPzs),
				SatisfiesPzi:        googleproxyclient.NewOptBool(bucket.SatisfiesPzi),
			}
			bucketDetails = append(bucketDetails, bucketDetail)
		}
		apiBackupVault.BucketDetails = bucketDetails
	}

	return apiBackupVault
}

// bucketDetailsExist checks if bucket details already exist in the array
func bucketDetailsExist(existingBuckets datamodel.BucketDetailsArray, newBucket *datamodel.BucketDetails) bool {
	if newBucket == nil {
		return false
	}

	for _, existing := range existingBuckets {
		if existing.BucketName == newBucket.BucketName &&
			existing.VendorSubnetID == newBucket.VendorSubnetID {
			return true
		}
	}
	return false
}
