package activities

import (
	"context"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/iam/v1"
)

const (
	VolumeTypeRW = "rw"
	VolumeTypeDP = "dp"
)

type VolumeCreateActivity struct {
	SE database.Storage
}

var (
	GetResourceNamesForBackup  = _getResourceNamesForBackup
	FindTenancy                = _findTenancy
	GetOrCreateAndGCSResources = _getOrCreateAndGCSResources
)

func (a *VolumeCreateActivity) CreateVolume(ctx context.Context, volume *datamodel.Volume) (*datamodel.Volume, error) {
	se := a.SE

	return se.CreateVolume(ctx, volume)
}

func (a *VolumeCreateActivity) CreateVolumeInONTAP(ctx context.Context, volume *datamodel.Volume, node *models.Node) (*vsa.VolumeResponse, error) {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)
	volumeType := VolumeTypeRW
	if volume.VolumeAttributes.IsDataProtection {
		volumeType = VolumeTypeDP
	}
	res, err := provider.CreateVolume(vsa.CreateVolumeParams{
		VolumeName:    volume.Name,
		SvmName:       volume.Svm.Name,
		AggregateName: aggregateName,
		Size:          volume.SizeInBytes,
		VolumeType:    volumeType,
	})
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

func (a *VolumeCreateActivity) CreateIgroup(ctx context.Context, volume *datamodel.Volume, hostParams []*common.HostParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider := GetProviderByNode(node)

	// FixMe: What if a new host is added to the host group?
	for _, host := range hostParams {
		igroupExists, err := provider.IgroupExists(host.HostName, volume.Svm.Name)
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

func (a *VolumeCreateActivity) CreateLun(ctx context.Context, volume *datamodel.Volume, node *models.Node, availableSpace int64) (*vsa.LunResponse, error) {
	logger := util.GetLogger(ctx)
	if volume.VolumeAttributes.IsDataProtection {
		logger.Info("Skipping lun creation for data protection volume")
		return &vsa.LunResponse{}, nil
	}
	provider := GetProviderByNode(node)
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

func (a *VolumeCreateActivity) CreateLunMap(ctx context.Context, volume *datamodel.Volume, params *common.CreateLunMapParams, node *models.Node) error {
	logger := util.GetLogger(ctx)
	if volume.VolumeAttributes.IsDataProtection {
		logger.Info("Skipping CreateLunMap for data protection volume")
		return nil
	}
	var provider = GetProviderByNode(node)
	err := provider.LunMapCreate(vsa.LunMapCreateParams{
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

func (a *VolumeCreateActivity) UpdateVolumeDetails(ctx context.Context, volume *datamodel.Volume, volCreateResponse *vsa.ProviderResponse) error {
	se := a.SE

	volume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID
	volume.State = models.LifeCycleStateREADY
	volume.StateDetails = models.LifeCycleStateAvailableDetails

	if err := se.UpdateVolume(ctx, volume); err != nil {
		return err
	}

	return nil
}

func (a *VolumeCreateActivity) GetHosts(ctx context.Context, volume *datamodel.Volume) ([]*datamodel.HostGroup, error) {
	se := a.SE

	if volume.VolumeAttributes.BlockProperties == nil {
		return nil, errors.New("block properties not found")
	}

	uuids := volume.VolumeAttributes.BlockProperties.HostGroupUUIDs

	dbHostGroups, err := se.GetMultipleHostGroups(ctx, uuids, volume.AccountID)
	if err != nil {
		return nil, err
	}

	if len(dbHostGroups) != len(uuids) {
		return nil, errors.New("all host groups could not be found")
	}

	return dbHostGroups, nil
}

func _findTenancy(ctx context.Context, gcpService hyperscaler.GoogleServices, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*common.TenancyInfo, error) {
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

func (a *VolumeCreateActivity) FindTenancy(ctx context.Context, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*common.TenancyInfo, error) {
	gcpService, err := GetGCPService(ctx)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	gcpService.Logger.Debug("gcpService initialized")
	return FindTenancy(ctx, gcpService, consumerVPC, customerProjectNumber, tenantProjectRegion)
}

func (a *VolumeCreateActivity) CheckBackupVaultExistsInVCP(ctx context.Context, volume *datamodel.Volume, region string) error {
	se := a.SE

	bvId := volume.DataProtection.BackupVaultID
	backupVault, err := se.GetBackupVaultByUUID(ctx, bvId)
	if err != nil {
		if !strings.Contains(err.Error(), "backup vault not found") {
			return err
		}
	}
	if backupVault != nil {
		return nil
	}
	bvParams := &datamodel.BackupVault{}

	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
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
			bvModel, err := convertToBackupVaultDataModel(bv, region)
			if err != nil {
				return err
			}
			bvParams = bvModel
			break
		}
	}

	_, err = se.CreateBackupVaultEntryInVCP(ctx, bvParams)
	if err != nil {
		return err
	}

	return nil
}

func (a *VolumeCreateActivity) CheckForBucketResourceName(ctx context.Context, volume *datamodel.Volume) (*common.BucketDetails, error) {
	logger := util.GetLogger(ctx)

	bvDetails, err := getBackupVaultDetails(a, ctx, volume.DataProtection.BackupVaultID)
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

func getBackupVaultDetails(a *VolumeCreateActivity, ctx context.Context, bvID string) (*datamodel.BackupVault, error) {
	se := a.SE

	backupVault, err := se.GetBackupVaultByUUID(ctx, bvID)
	if err != nil {
		if !strings.Contains(err.Error(), "backup vault not found") {
			return nil, err
		}
	}

	return backupVault, nil
}

func (a *VolumeCreateActivity) GenerateResourceNames(ctx context.Context, volume *datamodel.Volume, tenancyDetails *common.TenancyInfo, gcpRegion string) (*common.ResourceNames, error) {
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
	logger := util.GetLogger(ctx)

	var gService hyperscaler.GoogleServices
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: logger,
	}
	gService = gcpService

	gcpService.Logger.Debug("gcpService initialized")
	gcpService.Logger.Debug("Calling InitializeClients")
	err := gService.InitializeClients()
	if err != nil || !gService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, errors.New("initialisation of service failed")
	}
	_, bucketDetails, err := GetOrCreateAndGCSResources(gcpService, resourceName.ServiceAccountId, tenancyDetails.RegionalTenantProject, resourceName.Email, resourceName.BucketName, region, "region")
	if err != nil {
		gcpService.Logger.Errorf("Error creating bucket: %v", err)
		return nil, err
	}
	return bucketDetails[0], err
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
	se := a.SE

	convertCommonToDatamodel := func(bucketDetails *common.BucketDetails) *datamodel.BucketDetails {
		return &datamodel.BucketDetails{
			BucketName:          bucketDetails.BucketName,
			ServiceAccountName:  bucketDetails.ServiceAccountName,
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

func _getResourceNamesForBackup(gcpRegion, region, tenantProjectNumber, bvID string) (string, string, string, error) {
	return utils.GetResourcesNameForBackup(gcpRegion, region, tenantProjectNumber, bvID)
}
