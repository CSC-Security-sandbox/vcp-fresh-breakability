package kms_activities

import (
	"context"
	"database/sql"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/retry"
	googleOauth2 "golang.org/x/oauth2/google"
	"google.golang.org/api/cloudkms/v1"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/kms_configurations"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"google.golang.org/api/iam/v1"
)

func TestPollKmsConfigOperationActivity(t *testing.T) {
	t.Run("PollKmsConfigOperationActivityReturnsErrorWhenResponseIsNil", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kfp := "kfp"
		params := &common.CreateKmsConfigParams{OperationUri: "operation-id",
			OperationDone: false,
			KeyFullPath:   kfp,
		}
		mockClient := kms_configurations.NewMockClientService(t)

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		pollCvpOperationForWorkflow = func(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
			return nil, errors.New("new error")
		}

		err := activity.PollKmsConfigOperationActivity(ctx, params)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("PollKmsConfigOperationActivityReturnsUpdatedKmsConfigOnSuccess", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kp := "kp"
		params := &common.CreateKmsConfigParams{OperationUri: "operation-id",
			OperationDone: false,
			KeyFullPath:   "kp"}
		mockClient := kms_configurations.NewMockClientService(t)
		// Define mock response
		mockResponse := &kms_configurations.V1betaCreateKmsConfigurationAccepted{
			Payload: &cvpModels.OperationV1beta{
				Name:     "operation-id",
				Done:     nillable.GetBoolPtr(false),
				Response: cvpModels.KmsConfigV1beta{UUID: "external-uuid", KeyFullPath: &kp},
			},
		}
		// Set up the mock client behavior

		cvpClient := &cvpapi.Cvp{KmsConfigurations: mockClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
			pollCvpOperationForWorkflow = _pollCvpOperationForWorkflow
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		pollCvpOperationForWorkflow = func(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*cvpModels.OperationV1beta, error) {
			return mockResponse.Payload, nil
		}

		err := activity.PollKmsConfigOperationActivity(ctx, params)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestFailedKmsConfigCreateActivity(t *testing.T) {
	t.Run("FailedKmsConfigCreateActivityUpdatesStateAndDetails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, State: models.LifeCycleStateError, StateDetails: "failure reason",
			ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("UpdateKmsConfigState", mock.Anything, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails).Return(nil, nil)
		mockSE.On("UpdateServiceAccountState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{}, nil)
		err := activity.FailedKmsConfigCreateActivity(context.Background(), kmsConfig, "failure reason")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if kmsConfig.State != models.LifeCycleStateError {
			t.Errorf("expected state to be error, got %v", kmsConfig.State)
		}
		if kmsConfig.StateDetails != "failure reason" {
			t.Errorf("expected state details to be set")
		}
	})
	t.Run("WhenUpdateKmsConfigStateFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, State: models.LifeCycleStateError, StateDetails: "failure reason",
			ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("UpdateKmsConfigState", mock.Anything, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails).Return(nil, errors.New("failure reason"))
		err := activity.FailedKmsConfigCreateActivity(context.Background(), kmsConfig, "failure reason")
		assert.Error(tt, err)
	})
	t.Run("WhenUpdateKmsConfigStateFailsWithErrorNotFound", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, State: models.LifeCycleStateError, StateDetails: "failure reason",
			ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("UpdateKmsConfigState", mock.Anything, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails).Return(nil, errors.NewNotFoundErr("failure reason", nil))
		err := activity.FailedKmsConfigCreateActivity(context.Background(), kmsConfig, "failure reason")
		assert.NoError(tt, err)
	})
}

func TestCreatedKmsConfigActivity(t *testing.T) {
	t.Run("CreatedKmsConfigActivityUpdatesStateToReady", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, State: models.LifeCycleStateREADY, StateDetails: models.LifeCycleStateCreatedDetails,
			ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("UpdateKmsConfigState", mock.Anything, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails).Return(kmsConfig, nil)
		mockSE.On("UpdateServiceAccountState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{}, nil)
		err := activity.CreatedKmsConfigActivity(context.Background(), kmsConfig)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if kmsConfig.State != models.LifeCycleStateREADY {
			t.Errorf("expected state to be READY, got %v", kmsConfig.State)
		}
		if kmsConfig.StateDetails != models.LifeCycleStateCreatedDetails {
			t.Errorf("expected state details to be set to ready details")
		}
	})
	t.Run("WhenUpdateKmsConfigStateFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, State: models.LifeCycleStateREADY, StateDetails: models.LifeCycleStateCreatedDetails,
			ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("UpdateKmsConfigState", mock.Anything, kmsConfig.UUID, kmsConfig.State, kmsConfig.StateDetails).Return(nil, errors.New("some one"))
		err := activity.CreatedKmsConfigActivity(context.Background(), kmsConfig)
		assert.Error(tt, err)
	})
}

func TestCreateVSAKmsConfigSAKeyActivity(t *testing.T) {
	t.Run("returns error if getGcpService fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			KmsAttributes: &datamodel.KmsAttributes{SdeServiceAccountEmail: "prefix-test@project.iam.gserviceaccount.com"},
		}
		defer func() { getGcpService = activities.GetGCPService }()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp error")
		}
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("creates new service account if not found in db", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			Name:         "test",
			Description:  "desc",
			AccountID:    1,
			StateDetails: "details",
			KmsAttributes: &datamodel.KmsAttributes{
				SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com",
			},
		}
		defer func() {
			getGcpService = activities.GetGCPService
			gcpServiceCreateServiceAccountKey = _gcpServiceCreateServiceAccountKey
		}()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*iam.ServiceAccountKey, error) {
			return &iam.ServiceAccountKey{PrivateKeyData: "keydata"}, nil
		}
		pass := "enc"
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return &pass, nil }
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(nil, errors.NewNotFoundErr("service account", nil))
		mockSE.On("CreateKmsServiceAccount", mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{ServiceAccountEmail: "test@project.iam.gserviceaccount.com"}, nil)
		mockSE.On("UpdateKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockSE.On("UpdateServiceAccountState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{}, nil)
		result, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err != nil {
			tt.Fatalf("expected no error, got %v", err)
		}
		if result.ServiceAccount.ServiceAccountEmail != "test@project.iam.gserviceaccount.com" {
			tt.Errorf("expected ServiceAccountEmail to be set, got %v", result.ServiceAccount.ServiceAccountEmail)
		}
	})

	t.Run("returns error if CreateKmsServiceAccount fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			Name:         "test",
			Description:  "desc",
			AccountID:    1,
			StateDetails: "details",
			KmsAttributes: &datamodel.KmsAttributes{
				SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com",
			},
		}
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		defer func() {
			gcpServiceCreateServiceAccountKey = _gcpServiceCreateServiceAccountKey
		}()
		gcpServiceCreateServiceAccountKey = func(gcpService hyperscaler.GoogleServices, ctx context.Context, email string) (*iam.ServiceAccountKey, error) {
			return &iam.ServiceAccountKey{PrivateKeyData: "keydata"}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(nil, errors.NewNotFoundErr("service account", nil))
		mockSE.On("CreateKmsServiceAccount", mock.Anything, mock.Anything).Return(nil, errors.New("db error"))
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error if DecryptPassword fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}},
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com"},
		}
		defer func() { getGcpService = activities.GetGCPService }()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(&datamodel.ServiceAccount{ServiceAccountPasswordLocation: "bad"}, nil)
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return nil, errors.New("decrypt error") }
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error if synchronizeServiceAccountKeys fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}},
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com"},
		}
		defer func() {
			getGcpService = activities.GetGCPService
			synchronizeServiceAccountKeys = _synchronizeServiceAccountKeys
		}()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountPasswordLocation: "enc"}, nil)
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		empty := ""
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return &empty, nil }
		synchronizeServiceAccountKeys = func(ctx context.Context, gcpService hyperscaler.GoogleServices, email string) (*string, error) {
			return nil, errors.New("sync error")
		}
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error if UpdateServiceAccountEmailAndKey fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}},
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com"},
		}
		defer func() {
			getGcpService = activities.GetGCPService
			synchronizeServiceAccountKeys = _synchronizeServiceAccountKeys
		}()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountPasswordLocation: "enc"}, nil)
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		empty := ""
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return &empty, nil }
		synchronizeServiceAccountKeys = func(ctx context.Context, gcpService hyperscaler.GoogleServices, email string) (*string, error) {
			val := "enc"
			return &val, nil
		}
		mockSE.On("UpdateServiceAccountEmailAndKey", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(nil, errors.New("update error"))
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error if UpdateKmsConfig fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}},
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com"},
		}
		defer func() {
			getGcpService = activities.GetGCPService
			synchronizeServiceAccountKeys = _synchronizeServiceAccountKeys
		}()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountPasswordLocation: "enc"}, nil)
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		empty := ""
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return &empty, nil }
		synchronizeServiceAccountKeys = func(ctx context.Context, gcpService hyperscaler.GoogleServices, email string) (*string, error) {
			val := "enc"
			return &val, nil
		}
		mockSE.On("UpdateServiceAccountEmailAndKey", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountEmail: "test@project.iam.gserviceaccount.com"}, nil)
		mockSE.On("UpdateKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("update kms error"))
		mockSE.On("UpdateServiceAccountState", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{}, nil)
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error if UpdateServiceAccountState fails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}},
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com"},
		}
		defer func() {
			getGcpService = activities.GetGCPService
			synchronizeServiceAccountKeys = _synchronizeServiceAccountKeys
		}()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountPasswordLocation: "enc"}, nil)
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		empty := ""
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return &empty, nil }
		synchronizeServiceAccountKeys = func(ctx context.Context, gcpService hyperscaler.GoogleServices, email string) (*string, error) {
			val := "enc"
			return &val, nil
		}
		mockSE.On("UpdateServiceAccountEmailAndKey", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountEmail: "test@project.iam.gserviceaccount.com"}, nil)
		mockSE.On("UpdateServiceAccountState", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(nil, errors.New("update state error"))
		_, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err == nil {
			tt.Fatal("expected error, got nil")
		}
	})

	t.Run("returns updated kmsConfig on success", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		kmsConfig := &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}},
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "test@project.iam.gserviceaccount.com"},
		}
		defer func() {
			getGcpService = activities.GetGCPService
			synchronizeServiceAccountKeys = _synchronizeServiceAccountKeys
		}()
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockSE.On("GetServiceAccountFromEmail", mock.Anything, "test@project.iam.gserviceaccount.com").Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountPasswordLocation: "enc"}, nil)
		origDecryptPassword := utils.DecryptPassword
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		empty := ""
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return &empty, nil }
		synchronizeServiceAccountKeys = func(ctx context.Context, gcpService hyperscaler.GoogleServices, email string) (*string, error) {
			val := "enc"
			return &val, nil
		}
		mockSE.On("UpdateServiceAccountEmailAndKey", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{BaseModel: datamodel.BaseModel{UUID: "uuid"}, ServiceAccountEmail: "test@project.iam.gserviceaccount.com"}, nil)
		mockSE.On("UpdateKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockSE.On("UpdateServiceAccountState", mock.Anything, "uuid", mock.Anything, mock.Anything).Return(&datamodel.ServiceAccount{}, nil)
		result, err := activity.CreateVSAKmsConfigSAKeyActivity(context.Background(), kmsConfig)
		if err != nil {
			tt.Fatalf("expected no error, got %v", err)
		}
		if result.ServiceAccount.ServiceAccountEmail != "test@project.iam.gserviceaccount.com" {
			tt.Errorf("expected ServiceAccountEmail to be set, got %v", result.ServiceAccount.ServiceAccountEmail)
		}
	})
}

func Test_gcpServiceCreateServiceAccountKey(t *testing.T) {
	ctx := context.Background()
	email := "test@project.iam.gserviceaccount.com"

	t.Run("returns key on success", func(t *testing.T) {
		expectedKey := &iam.ServiceAccountKey{PrivateKeyData: "keydata"}
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateServiceAccountKey", mock.Anything, email).Return(expectedKey, nil)
		key, err := _gcpServiceCreateServiceAccountKey(mockGCPService, ctx, email)
		assert.NoError(t, err)
		assert.Equal(t, expectedKey, key)
	})

	t.Run("returns error on failure", func(t *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateServiceAccountKey", mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		key, err := _gcpServiceCreateServiceAccountKey(mockGCPService, ctx, email)
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestGrantRoleActivity(t *testing.T) {
	t.Run("GrantRoleActivityReturnsErrorWhenServiceAccountEmailIsEmpty", func(t *testing.T) {
		activity := &KmsConfigActivity{}
		kmsConfig := &datamodel.KmsConfig{
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: ""},
			ServiceAccount: &datamodel.ServiceAccount{ServiceAccountEmail: ""},
		}
		mockGcpService := &google.GcpServices{}
		origGetGcpService := getGcpService
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		defer func() { getGcpService = origGetGcpService }()
		origGrant := gcpGrantServiceAccountRole
		gcpGrantServiceAccountRole = func(ctx context.Context, gcpService *google.GcpServices, serviceAccountEmail, member, role string) error {
			if serviceAccountEmail == "" || member == "" {
				return errors.New("missing email")
			}
			return nil
		}
		defer func() { gcpGrantServiceAccountRole = origGrant }()
		err := activity.GrantRoleActivity(context.Background(), kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing email")
	})
	t.Run("GrantRoleActivityReturnsErrorWhenServiceAccountIsNil", func(t *testing.T) {
		activity := &KmsConfigActivity{}
		kmsConfig := &datamodel.KmsConfig{
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "svc@project.iam.gserviceaccount.com"},
			ServiceAccount: &datamodel.ServiceAccount{ServiceAccountEmail: ""},
		}
		mockGcpService := &google.GcpServices{}
		origGetGcpService := getGcpService
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}
		defer func() { getGcpService = origGetGcpService }()
		origGrant := gcpGrantServiceAccountRole
		gcpGrantServiceAccountRole = func(ctx context.Context, gcpService *google.GcpServices, serviceAccountEmail, member, role string) error {
			if member == "" {
				return errors.New("missing email")
			}
			return nil
		}
		defer func() { gcpGrantServiceAccountRole = origGrant }()
		err := activity.GrantRoleActivity(context.Background(), kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing email")
	})
	t.Run("GrantRoleActivityReturnsErrorWhenGetGcpServiceFails", func(t *testing.T) {
		activity := &KmsConfigActivity{}
		kmsConfig := &datamodel.KmsConfig{
			KmsAttributes:  &datamodel.KmsAttributes{SdeServiceAccountEmail: "svc@project.iam.gserviceaccount.com"},
			ServiceAccount: &datamodel.ServiceAccount{ServiceAccountEmail: ""},
		}
		origGetGcpService := getGcpService
		getGcpService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("some error")
		}
		defer func() { getGcpService = origGetGcpService }()
		err := activity.GrantRoleActivity(context.Background(), kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "some error")
	})
}

func TestUpdatePoolWithKmsConfigActivity(t *testing.T) {
	t.Run("UpdatePoolWithKmsConfigActivityReturnsUpdatedPoolOnSuccess", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
		kmsConfigID := "kms-uuid"
		updatedPool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		mockSE.On("UpdatePoolWithKmsConfigID", ctx, pool, kmsConfigID).Return(updatedPool, nil)

		result, err := activity.UpdatePoolWithKmsConfigActivity(ctx, pool, kmsConfigID)
		assert.NoError(t, err)
		assert.Equal(t, updatedPool, result)
	})
	t.Run("UpdatePoolWithKmsConfigActivityReturnsErrorOnFailure", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
		kmsConfigID := "kms-uuid"
		mockSE.On("UpdatePoolWithKmsConfigID", ctx, pool, kmsConfigID).Return(nil, errors.New("update error"))

		result, err := activity.UpdatePoolWithKmsConfigActivity(ctx, pool, kmsConfigID)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "update error")
	})
}

func TestAccessCryptoKeyActivity(t *testing.T) {
	t.Run("AccessCryptoKeyActivityReturnsNoErrorOnSuccess", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}}
		origAccessCryptoKey := AccessCryptoKey
		defer func() { AccessCryptoKey = origAccessCryptoKey }()
		AccessCryptoKey = func(ctx context.Context, se database.Storage, dbKmsConfig *datamodel.KmsConfig) error {
			return nil
		}
		err := activity.AccessCryptoKeyWithImpersonationActivity(ctx, kmsConfig)
		assert.NoError(t, err)
	})
	t.Run("AccessCryptoKeyActivityReturnsErrorOnFailure", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		kmsConfig := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}}
		origAccessCryptoKey := AccessCryptoKey
		defer func() { AccessCryptoKey = origAccessCryptoKey }()
		AccessCryptoKey = func(ctx context.Context, se database.Storage, dbKmsConfig *datamodel.KmsConfig) error {
			return errors.New("access error")
		}
		err := activity.AccessCryptoKeyWithImpersonationActivity(ctx, kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access error")
	})
}

func Test_accessCryptoKey(t *testing.T) {
	t.Run("WhenGetCryptoFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "uuid"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountPasswordLocation: "encrypted-location",
			},
			KmsAttributes: &datamodel.KmsAttributes{
				SdeServiceAccountEmail: "svc@project.iam.gserviceaccount.com",
			},
			KeyProjectID:    "project",
			KeyRingLocation: "location",
			KeyRing:         "keyring",
			KeyName:         "keyname",
		}
		origProcessCredentials := utils.ProcessCredentials
		origRetryDo := retryDo
		defer func() {
			utils.ProcessCredentials = origProcessCredentials
			retryDo = origRetryDo
		}()
		utils.ProcessCredentials = func(ctx context.Context, secretPassword string) (*googleOauth2.Credentials, error) {
			return &googleOauth2.Credentials{}, nil
		}
		retryDo = func(ctx context.Context, timeout, wait time.Duration, caller string, fn retry.Retriable) error {
			_, err := fn(1)
			return err
		}
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		err := _accessCryptoKey(ctx, mockSE, kmsConfig)
		assert.Error(t, err)
	})
	t.Run("ReturnsErrorWhenProcessCredentialsFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "uuid"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountPasswordLocation: "bad-location",
			},
			KmsAttributes: &datamodel.KmsAttributes{
				SdeServiceAccountEmail: "svc@project.iam.gserviceaccount.com",
			},
		}
		origProcessCredentials := utils.ProcessCredentials
		defer func() { utils.ProcessCredentials = origProcessCredentials }()
		utils.ProcessCredentials = func(ctx context.Context, secretPassword string) (*googleOauth2.Credentials, error) {
			return nil, errors.New("decrypt error")
		}
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		err := _accessCryptoKey(ctx, mockSE, kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decrypt error")
	})
	t.Run("ReturnsErrorWhenRetryDoFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "uuid"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountPasswordLocation: "encrypted-location",
			},
			KmsAttributes: &datamodel.KmsAttributes{
				SdeServiceAccountEmail: "svc@project.iam.gserviceaccount.com",
			},
			KeyProjectID:    "project",
			KeyRingLocation: "location",
			KeyRing:         "keyring",
			KeyName:         "keyname",
		}
		origProcessCredentials := utils.ProcessCredentials
		origRetryDo := retryDo
		origGetCloudKmsService := getImpersonatedKmsService
		defer func() {
			utils.ProcessCredentials = origProcessCredentials
			retryDo = origRetryDo
			getImpersonatedKmsService = origGetCloudKmsService
		}()

		utils.ProcessCredentials = func(ctx context.Context, secretPassword string) (*googleOauth2.Credentials, error) {
			return &googleOauth2.Credentials{}, nil
		}

		getImpersonatedKmsService = func(ctx context.Context, targetEmail string, scopeCreds *googleOauth2.Credentials) (*cloudkms.Service, error) {
			return &cloudkms.Service{}, nil
		}

		retryDo = func(ctx context.Context, timeout, wait time.Duration, caller string, fn retry.Retriable) error {
			return errors.New("retry error")
		}
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		err := _accessCryptoKey(ctx, mockSE, kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "retry error")
	})
	t.Run("WhenGetCloudKmsServiceFails", func(t *testing.T) {
		ctx := context.Background()
		mockSE := database.NewMockStorage(t)
		kmsConfig := &datamodel.KmsConfig{
			BaseModel: datamodel.BaseModel{UUID: "uuid"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountPasswordLocation: "encrypted-location",
			},
			KmsAttributes: &datamodel.KmsAttributes{
				SdeServiceAccountEmail: "svc@project.iam.gserviceaccount.com",
			},
			KeyProjectID:    "project",
			KeyRingLocation: "location",
			KeyRing:         "keyring",
			KeyName:         "keyname",
		}
		origProcessCredentials := utils.ProcessCredentials
		origRetryDo := retryDo
		origGetCloudKmsService := getImpersonatedKmsService
		defer func() {
			utils.ProcessCredentials = origProcessCredentials
			retryDo = origRetryDo
			getImpersonatedKmsService = origGetCloudKmsService
		}()

		utils.ProcessCredentials = func(ctx context.Context, secretPassword string) (*googleOauth2.Credentials, error) {
			return &googleOauth2.Credentials{}, nil
		}

		getImpersonatedKmsService = func(ctx context.Context, targetEmail string, scopeCreds *googleOauth2.Credentials) (*cloudkms.Service, error) {
			return nil, errors.New("cloudkms error")
		}

		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		err := _accessCryptoKey(ctx, mockSE, kmsConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cloudkms error")
	})
}

func Test_synchronizeServiceAccountKeys(t *testing.T) {
	ctx := context.Background()
	email := "test@project.iam.gserviceaccount.com"

	t.Run("returns key data on success", func(t *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteAllServiceAccountKeys", ctx, email).Return(nil)
		expectedKey := &iam.ServiceAccountKey{PrivateKeyData: "keydata"}
		mockGCPService.On("CreateServiceAccountKey", ctx, email).Return(expectedKey, nil)

		key, err := _synchronizeServiceAccountKeys(ctx, mockGCPService, email)
		assert.NoError(t, err)
		assert.NotNil(t, key)
		assert.Equal(t, "keydata", *key)
	})

	t.Run("returns error if DeleteAllServiceAccountKeys fails", func(t *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteAllServiceAccountKeys", ctx, email).Return(errors.New("delete error"))

		key, err := _synchronizeServiceAccountKeys(ctx, mockGCPService, email)
		assert.Error(t, err)
		assert.Nil(t, key)
	})

	t.Run("returns error if CreateServiceAccountKey fails", func(t *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteAllServiceAccountKeys", ctx, email).Return(nil)
		mockGCPService.On("CreateServiceAccountKey", ctx, email).Return(nil, errors.New("create error"))

		key, err := _synchronizeServiceAccountKeys(ctx, mockGCPService, email)
		assert.Error(t, err)
		assert.Nil(t, key)
	})
}

func TestPollCvpOperationForWorkflow(t *testing.T) {
	t.Run("WhenV1betaDescribeOperationFails", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		params := &async.V1betaDescribeOperationParams{}
		mockAsyncClient := async.NewMockClientService(t)
		// Set up the mock client behavior

		cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).Return(nil, errors.New("some error")).Once()
		resp, err := pollCvpOperationForWorkflow(ctx, *cvpClient, params)
		assert.NotNil(tt, err)
		assert.Nil(t, resp)
	})
	t.Run("WhenDoneButOperationFailed", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		params := &async.V1betaDescribeOperationParams{}
		mockAsyncClient := async.NewMockClientService(t)
		// Set up the mock client behavior
		done := true
		mockOp := cvpModels.OperationV1beta{
			Done: &done,
			Error: &cvpModels.StatusV1Beta{
				Code:    http.StatusConflict,
				Message: "Failed",
			},
		}
		mockResp := &async.V1betaDescribeOperationOK{
			Payload: &mockOp,
		}
		cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).Return(mockResp, nil).Once()
		resp, err := pollCvpOperationForWorkflow(ctx, *cvpClient, params)
		assert.NotNil(tt, err)
		assert.Nil(t, resp)
	})
	t.Run("WhenDoneAndOperationSuccess", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		params := &async.V1betaDescribeOperationParams{}
		mockAsyncClient := async.NewMockClientService(t)
		// Set up the mock client behavior
		done := true
		mockOp := cvpModels.OperationV1beta{
			Done: &done,
		}
		mockResp := &async.V1betaDescribeOperationOK{
			Payload: &mockOp,
		}
		cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).Return(mockResp, nil).Once()
		resp, err := pollCvpOperationForWorkflow(ctx, *cvpClient, params)
		assert.Nil(tt, err)
		assert.NotNil(t, resp)
	})
	t.Run("WhenNotDoneAndOperationSuccess", func(tt *testing.T) {
		mockLogger := log.NewLogger()
		ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
		params := &async.V1betaDescribeOperationParams{}
		mockAsyncClient := async.NewMockClientService(t)
		// Set up the mock client behavior
		done := false
		mockOp := cvpModels.OperationV1beta{
			Done: &done,
			Error: &cvpModels.StatusV1Beta{
				Code:    http.StatusConflict,
				Message: "Failed",
			},
		}
		mockResp := &async.V1betaDescribeOperationOK{
			Payload: &mockOp,
		}
		cvpClient := &cvpapi.Cvp{Async: mockAsyncClient}
		originalCreateClient := createClient
		defer func() {
			createClient = originalCreateClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		mockAsyncClient.On("V1betaDescribeOperation", mock.Anything).Return(mockResp, nil).Once()
		resp, err := pollCvpOperationForWorkflow(ctx, *cvpClient, params)
		assert.NotNil(tt, err)
		assert.Nil(t, resp)
	})
}
