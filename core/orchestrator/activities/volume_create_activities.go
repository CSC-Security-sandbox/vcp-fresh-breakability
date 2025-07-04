package activities

import (
	"context"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/iam/v1"
)

const (
	VolumeTypeRW                 = "rw"
	VolumeTypeDP                 = "dp"
	SnapshotPolicyNone           = "none"
	CrossRegionBackupType        = "CROSS_REGION"
	ImmutableBackupVaultErrMsg   = "Immutable backup vaults are not supported for ISCSI volumes"
	CrossRegionBackupVaultErrMsg = "Cross region backup vaults are not supported for ISCSI volumes"
)

type VolumeCreateActivity struct {
	SE database.Storage
}

var (
	GetResourceNamesForBackup   = _getResourceNamesForBackup
	FindTenancy                 = _findTenancy
	CreateBucket                = _createBucket
	GenerateResourceNames       = _generateResourceNames
	GetOrCreateAndGCSResources  = _getOrCreateAndGCSResources
	CheckBackupVaultExistsInVCP = _checkBackupVaultExistsInVCP
	CheckForBucketResourceName  = _checkForBucketResourceName
)

func (a VolumeCreateActivity) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	se := a.SE

	return se.CreateVolume(ctx, volume)
}

func (a VolumeCreateActivity) CreateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node, snapshot *datamodel.Snapshot) (*vsa.VolumeResponse, error) {
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	volumeType := VolumeTypeRW
	if volume.VolumeAttributes.IsDataProtection {
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
		AggregateName:       aggregateName,
		Size:                volume.SizeInBytes,
		VolumeType:          volumeType,
		SnapshotPolicyName:  snapshotPolicyName,
		RestoreFromSnapshot: &restoreFromSnapshotParam,
		TieringPolicy: &vsa.TieringPolicy{
			CoolAccessTieringPolicy: ontapModels.VolumeInlineTieringPolicyNone,
		},
	}

	if volume.CoolAccess {
		params.TieringPolicy.CoolAccessTieringPolicy = nillable.GetString(&volume.CoolAccessTieringPolicy, ontapModels.VolumeInlineTieringPolicyAuto)
		params.TieringPolicy.CoolAccessRetrievalPolicy = nillable.GetString(&volume.CoolAccessRetrievalPolicy, ontapModels.VolumeCloudRetrievalPolicyDefault)
		params.TieringPolicy.CoolnessPeriod = int64(volume.CoolnessPeriod)
	}

	res, err := provider.CreateVolume(params)

	if err != nil {
		if errors.IsConflictErr(err) {
			return HandleVolumeCreateConflict(volume, provider)
		}
		return nil, err
	}
	logger.Debug("volume created successfully")

	return res, nil
}

func HandleVolumeCreateConflict(volume *datamodel.Volume, provider vsa.Provider) (*vsa.VolumeResponse, error) {
	volumeRes, err := provider.GetVolume(vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
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
	provider, err := GetProviderByNode(ctx, node)
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
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	lunName := utils.GetLunName(volume.Name)

	lun, err := provider.LunCreate(vsa.LunCreateParams{
		LunName:    lunName,
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
		OsType:     volume.VolumeAttributes.BlockProperties.OSType,
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
		return err
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
	provider, err := GetProviderByNode(ctx, node)
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
	volume.State = models.LifeCycleStateREADY
	volume.StateDetails = models.LifeCycleStateAvailableDetails

	if err := se.UpdateVolume(ctx, volume); err != nil {
		return err
	}

	return nil
}

func (a VolumeCreateActivity) GetHosts(ctx context.Context, volume *datamodel.Volume) ([]*datamodel.HostGroup, error) {
	se := a.SE

	if volume.VolumeAttributes.BlockProperties == nil {
		return nil, errors.New("block properties not found")
	}

	uuids := utils.GetHgUUIDs(volume.VolumeAttributes.BlockProperties.HostGroupDetails)

	dbHostGroups, err := se.GetMultipleHostGroups(ctx, uuids, volume.AccountID)
	if err != nil {
		return nil, err
	}

	if len(dbHostGroups) != len(uuids) {
		return nil, errors.New("all host groups could not be found")
	}

	return dbHostGroups, nil
}

func _findTenancy(gcpService hyperscaler.GoogleServices, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*common.TenancyInfo, error) {
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	if tenantProjectRegion == nil {
		tenantProjectRegion = &localRegion
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
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	gcpService.Logger.Debug("gcpService initialized")
	return FindTenancy(gcpService, consumerVPC, customerProjectNumber, tenantProjectRegion)
}

func _checkBackupVaultExistsInVCP(ctx context.Context, se database.Storage, volume *datamodel.Volume, region string) error {
	bvId := volume.DataProtection.BackupVaultID
	backupVault, err := se.GetBackupVaultByUUIDndOwnerID(ctx, bvId, volume.AccountID)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return err
		}
	}
	if backupVault != nil {
		if backupVault.ImmutableAttributes != nil {
			err := validateImmutableBackupVault(*backupVault.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration)
			if err != nil {
				return err
			}
		}
		err := validateCRBBackupVault(backupVault.BackupVaultType)
		if err != nil {
			return err
		}
	}
	bvParams := &datamodel.BackupVault{}

	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := CvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	vaults, err := cvpClient.BackupVault.V1betaListBackupVaults(&backup_vault.V1betaListBackupVaultsParams{
		LocationID:     region,
		ProjectNumber:  volume.Account.Name,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return errors.NewNotFoundErr("Backup vault", nil)
		}
		logger.Error("Error checking backupVault : ", err)
		return err
	}

	bvs := vaults.Payload.BackupVaults

	for _, bv := range bvs {
		if bv.BackupVaultID == bvId {
			if bv.BackupRetentionPolicy != nil {
				err := validateImmutableBackupVault(*bv.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDays)
				if err != nil {
					return err
				}
			}
			err := validateCRBBackupVault(*bv.BackupVaultType)
			if err != nil {
				return err
			}

			bvModel, err := convertToBackupVaultDataModel(bv, region)
			if err != nil {
				return err
			}
			bvParams = bvModel
			break
		}
	}

	bvParams.AccountID = volume.AccountID
	_, err = se.CreateBackupVaultEntryInVCP(ctx, bvParams)
	if err != nil {
		return err
	}

	return nil
}

func validateCRBBackupVault(backupVaultType string) error {
	if backupVaultType == CrossRegionBackupType {
		return errors.NewBadRequestErr(CrossRegionBackupVaultErrMsg)
	}
	return nil
}

func validateImmutableBackupVault(minRetentionDuration int64) error {
	if minRetentionDuration > 0 {
		return errors.NewBadRequestErr(ImmutableBackupVaultErrMsg)
	}
	return nil
}

func (a *VolumeCreateActivity) CreateBackupPolicyWhenVolumeAttachedInVCP(ctx context.Context, volume *datamodel.Volume, region string) error {
	se := a.SE

	backupPolicyId := volume.DataProtection.BackupPolicyID
	backupPolicy, err := se.GetBackupPolicyByUUIDAndOwnerID(ctx, backupPolicyId, volume.AccountID)
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return err
		}
	}
	if backupPolicy != nil {
		return nil
	}

	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := CvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	cvpBackupPolicy, err := cvpClient.BackupPolicy.V1betaDescribeBackupPolicy(&backup_policy.V1betaDescribeBackupPolicyParams{
		BackupPolicyID: backupPolicyId,
		LocationID:     region,
		ProjectNumber:  volume.Account.Name,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		logger.Errorf("Error checking backup policy : %v", err)
		return err
	}
	if cvpBackupPolicy == nil || cvpBackupPolicy.Payload == nil {
		logger.Error("No backup policy found in SDE")
		return errors.NewNotFoundErr("Backup policy", &backupPolicyId)
	}

	backupPolicyParams := convertToBackupPolicyDataModel(cvpBackupPolicy.Payload)

	// Backup policy is not found in VCP and attached to a volume
	backupPolicyParams.AccountID = volume.AccountID
	_, err = se.CreateBackupPolicyEntryInVCP(ctx, backupPolicyParams)
	if err != nil {
		return err
	}
	return nil
}

func (a VolumeCreateActivity) CheckBackupVaultExistsInVCP(ctx context.Context, volume *datamodel.Volume, region string) error {
	return CheckBackupVaultExistsInVCP(ctx, a.SE, volume, region)
}

func (a *VolumeCreateActivity) CheckForBucketResourceName(ctx context.Context, volume *datamodel.Volume) (*common.BucketDetails, error) {
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

func (a *VolumeCreateActivity) GenerateResourceNames(ctx context.Context, volume *datamodel.Volume, tenancyDetails *common.TenancyInfo, gcpRegion string) (*common.ResourceNames, error) {
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

func (a *VolumeCreateActivity) CreateBucket(ctx context.Context, resourceName *common.ResourceNames, tenancyDetails *common.TenancyInfo, region string) (*common.BucketDetails, error) {
	return CreateBucket(ctx, resourceName, tenancyDetails, region)
}

func _createBucket(ctx context.Context, resourceName *common.ResourceNames, tenancyDetails *common.TenancyInfo, region string) (*common.BucketDetails, error) {
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	_, bucketDetails, err := GetOrCreateAndGCSResources(gcpService, resourceName.ServiceAccountId, tenancyDetails.RegionalTenantProject, resourceName.Email, resourceName.BucketName, region, "region")
	if err != nil {
		gcpService.Logger.Errorf("Error creating bucket: %v", err)
		return nil, err
	}
	return bucketDetails[0], err
}

func UpdateBackupVaultWithBucketDetails(se database.Storage, ctx context.Context, volume *datamodel.Volume, bucketDetails *common.BucketDetails) error {
	saName := bucketDetails.ServiceAccountName + "@" + bucketDetails.TenantProjectNumber + ".iam.gserviceaccount.com"
	convertCommonToDatamodel := func(bucketDetails *common.BucketDetails) *datamodel.BucketDetails {
		return &datamodel.BucketDetails{
			BucketName:          bucketDetails.BucketName,
			ServiceAccountName:  saName,
			TenantProjectNumber: bucketDetails.TenantProjectNumber,
			VendorSubnetID:      volume.VolumeAttributes.VendorSubnetID,
		}
	}
	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: volume.DataProtection.BackupVaultID,
		},
	}
	backupVault.BucketDetails = append(backupVault.BucketDetails, convertCommonToDatamodel(bucketDetails))

	err := se.UpdateBackupVault(ctx, backupVault)
	if err != nil {
		return err
	}

	return nil
}

func _getOrCreateAndGCSResources(gcpServices hyperscaler.GoogleServices, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType string) (*iam.ServiceAccount, []*common.BucketDetails, error) {
	var account *iam.ServiceAccount
	var bucketDetailsArr []*common.BucketDetails
	var err error

	account, err = gcpServices.GetServiceAccount(projectNumber, email)
	if err != nil {
		request := &iam.CreateServiceAccountRequest{
			AccountId: serviceAccountId,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: bucketName,
			},
		}
		_, err = gcpServices.CreateServiceAccount(request, projectNumber, email)
		if err != nil {
			return nil, bucketDetailsArr, err
		}

		// Just to ensure that after service account is created we proceed further
		// inside isKmsServiceAccountCreated we have wait logic for some time as create SA has some latency
		account, _, err = gcpServices.IsServiceAccountCreated(email)
		if err != nil {
			gcpServices.GetLogger().Error("createServiceAccount failed getOrCreateAndGCSResources")
			return nil, nil, err
		}
	}
	roles := []string{
		"roles/storage.hmacKeyAdmin",
		"roles/storage.objectAdmin",
		"roles/storage.admin",
		"roles/iam.serviceAccountAdmin",
	}
	// Attach roles to created SA
	err = gcpServices.AttachOrUpdateRolesForServiceAccounts(roles, email, projectNumber)
	if err != nil {
		gcpServices.GetLogger().Error("AttachOrUpdateRolesForServiceAccounts() failed in getOrCreateAndGCSResources")
		return nil, bucketDetailsArr, err
	}

	err = gcpServices.CreateBucketIfNotExists(context.Background(), projectNumber, bucketName, tenantProjectRegion)
	if err != nil {
		return nil, nil, err
	}
	bucketDetails := &common.BucketDetails{BucketName: bucketName, ServiceAccountName: serviceAccountId, TenantProjectNumber: projectNumber, Location: locationType}
	bucketDetailsArr = append(bucketDetailsArr, bucketDetails)

	return account, bucketDetailsArr, nil
}

func (a *VolumeCreateActivity) UpdateBackupVaultWithBucketDetails(ctx context.Context, volume *datamodel.Volume, bucketDetails *common.BucketDetails) error {
	return UpdateBackupVaultWithBucketDetails(a.SE, ctx, volume, bucketDetails)
}

func _getResourceNamesForBackup(gcpRegion, region, tenantProjectNumber, bvID string) (string, string, string, error) {
	return utils.GetResourcesNameForBackup(gcpRegion, region, tenantProjectNumber, bvID)
}

func (a VolumeCreateActivity) CreateSnapshotPolicyInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) error {
	if node != nil && volume != nil && volume.SnapshotPolicy != nil && volume.SnapshotPolicy.Name != "" {
		logger := util.GetLogger(ctx)
		provider, err := GetProviderByNode(ctx, node)
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

// InitiateSplitForVolume initiates a split for the given volume in ONTAP.
func (a VolumeCreateActivity) InitiateSplitForVolume(ctx context.Context, volume *datamodel.Volume, node *models.Node, snapshot *datamodel.Snapshot) error {
	if snapshot == nil {
		return nil
	}
	logger := util.GetLogger(ctx)
	provider, err := GetProviderByNode(ctx, node)
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
		return err
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
