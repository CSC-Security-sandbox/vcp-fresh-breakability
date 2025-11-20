package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/workflow"
)

func TestCreateActiveDirectory_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId:         "test-ad",
		AccountId:          "123",
		LocationId:         "local",
		Username:           "admin@test.local",
		Password:           "SecurePass123!",
		Domain:             "test.local",
		DNS:                "10.0.0.1",
		NetBIOS:            "TEST",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site",
		KdcIP:              "10.0.0.2",
		KdcHostname:        "kdc.test.local",
		AesEncryption:      true,
		BackupOperators:    []string{"backup-user"},
		Administrators:     []string{"admin-user"},
		SecurityOperators:  []string{"security-user"},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	adRecord := &datamodel.ActiveDirectory{
		BaseModel:      datamodel.BaseModel{UUID: "ad-uuid-123"},
		AdName:         params.ResourceId,
		Username:       params.Username,
		Domain:         params.Domain,
		DNS:            params.DNS,
		NetBIOS:        params.NetBIOS,
		CredentialPath: "secret-path",
		AccountId:      accountID,
		State:          models.LifeCycleStateCreating,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: params.OrganizationalUnit,
			Site:               params.Site,
			KdcIP:              params.KdcIP,
			KdcHostname:        params.KdcHostname,
			AesEncryption:      params.AesEncryption,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
		WorkflowID: "workflow-123",
		Type:       string(models.JobTypeCreateActiveDirectory),
		State:      string(models.JobsStateNEW),
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone

	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()
	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.AdName == params.ResourceId
	})).Return(adRecord, nil)
	mockStorage.On("CreateJob", mock.Anything, mock.MatchedBy(func(j *datamodel.Job) bool {
		return j.Type == string(models.JobTypeCreateActiveDirectory)
	})).Return(job, nil)

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid-123", jobUUID)
	assert.Equal(t, adRecord.UUID, ad.UUID)
	assert.Equal(t, params.ResourceId, ad.AdName)
	assert.Equal(t, params.Username, ad.Username)
	assert.Equal(t, models.LifeCycleStateCreating, ad.State)
}

func TestCreateActiveDirectory_Success_WithCVPHost(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId:         "test-ad-cvp",
		AccountId:          "123",
		Username:           "admin@test.local",
		Password:           "SecurePass123!",
		Domain:             "test.local",
		DNS:                "10.0.0.1",
		NetBIOS:            "TEST",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site",
		KdcIP:              "10.0.0.2",
		KdcHostname:        "kdc.test.local",
		AesEncryption:      true,
		BackupOperators:    []string{"backup-user"},
		Administrators:     []string{"admin-user"},
		SecurityOperators:  []string{"security-user"},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid-cvp-123"},
		WorkflowID: "workflow-cvp-123",
		Type:       string(models.JobTypeCreateActiveDirectory),
		State:      string(models.JobsStateNEW),
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone

	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()

	mockStorage.On("CreateJob", mock.Anything, mock.MatchedBy(func(j *datamodel.Job) bool {
		return j.Type == string(models.JobTypeCreateActiveDirectory)
	})).Return(job, nil)

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "https://cvp.example.com"
	originalCreateCommonResourcesInVCP := utils.CreateCommonResourcesInVCP
	utils.CreateCommonResourcesInVCP = false
	defer func() {
		cvp.CVP_HOST = originalCVPHost
		utils.CreateCommonResourcesInVCP = originalCreateCommonResourcesInVCP
	}()

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid-cvp-123", jobUUID)
	assert.Equal(t, params.ResourceId, ad.AdName)
	assert.Equal(t, "", ad.UUID)
	assert.Equal(t, params.Username, ad.Username)
	assert.Equal(t, models.LifeCycleStateCreating, ad.State)
}

func TestCreateActiveDirectory_ValidationError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "",
		AccountId:  "123",
		Username:   "admin",
		Password:   "pass",
		Domain:     "test.local",
	}

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)

	var validationErr *customerrors.UserInputValidationErr
	assert.True(t, errors.As(err, &validationErr))
}

func TestCreateActiveDirectory_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId:         "test-ad",
		AccountId:          "123",
		Username:           "admin@test.local",
		Password:           "SecurePass123!",
		Domain:             "test.local",
		DNS:                "10.0.0.1",
		NetBIOS:            "TEST",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site",
		KdcIP:              "10.0.0.2",
		KdcHostname:        "kdc.test.local",
		AesEncryption:      true,
		BackupOperators:    []string{"backup-user"},
		Administrators:     []string{"admin-user"},
		SecurityOperators:  []string{"security-user"},
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
		WorkflowID: "workflow-123",
		Type:       string(models.JobTypeCreateActiveDirectory),
		State:      string(models.JobsStateNEW),
	}

	adRecord := &datamodel.ActiveDirectory{
		BaseModel:      datamodel.BaseModel{UUID: "ad-uuid-123"},
		AdName:         params.ResourceId,
		Username:       params.Username,
		Domain:         params.Domain,
		DNS:            params.DNS,
		NetBIOS:        params.NetBIOS,
		CredentialPath: "secret-path",
		AccountId:      accountID,
		State:          models.LifeCycleStateCreating,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: params.OrganizationalUnit,
			Site:               params.Site,
			KdcIP:              params.KdcIP,
			KdcHostname:        params.KdcHostname,
			AesEncryption:      params.AesEncryption,
		},
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	mockStorage.On("GetAccount", mock.Anything, "123").
		Return(nil, errors.New("account not found")).Maybe()
	mockStorage.On("CreateAccount", mock.Anything, mock.Anything).
		Return(account, nil).Maybe()

	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.AdName == params.ResourceId
	})).Return(adRecord, nil)
	mockStorage.On("CreateJob", mock.Anything, mock.MatchedBy(func(j *datamodel.Job) bool {
		return j.Type == string(models.JobTypeCreateActiveDirectory)
	})).Return(job, nil)

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid-123", jobUUID)
	assert.Equal(t, adRecord.UUID, ad.UUID)
	assert.Equal(t, params.ResourceId, ad.AdName)
	assert.Equal(t, params.Username, ad.Username)
	assert.Equal(t, models.LifeCycleStateCreating, ad.State)
}

func TestCreateActiveDirectory_DefaultOrganizationalUnit(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId:         "test-ad",
		AccountId:          "123",
		Username:           "admin@test.local",
		Password:           "SecurePass123!",
		Domain:             "test.local",
		DNS:                "10.0.0.1",
		NetBIOS:            "TEST",
		OrganizationalUnit: "",
	}

	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
	}

	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    params.ResourceId,
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: DefaultOrganizationalUnit,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID: "workflow-id",
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()
	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.ActiveDirectoryAttributes.OrganizationalUnit == DefaultOrganizationalUnit
	})).Return(adRecord, nil)
	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(job, nil)

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	ad, _, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.NoError(t, err)
	assert.Equal(t, DefaultOrganizationalUnit, ad.ActiveDirectoryAttributes.OrganizationalUnit)
}

func TestCreateActiveDirectory_JobCreationFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "test-ad",
		AccountId:  "123",
		Username:   "admin@test.local",
		Password:   "SecurePass123!",
		Domain:     "test.local",
		DNS:        "10.0.0.1",
		NetBIOS:    "TEST",
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 123},
	}

	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()
	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(adRecord, nil)
	mockStorage.On("CreateJob", mock.Anything, mock.Anything).
		Return(nil, errors.New("database error"))

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
}

func TestCreateActiveDirectory_WorkflowStartFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "test-ad",
		AccountId:  "123",
		Username:   "admin@test.local",
		Password:   "SecurePass123!",
		Domain:     "test.local",
		DNS:        "10.0.0.1",
		NetBIOS:    "TEST",
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 123},
	}

	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID: "workflow-id",
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()
	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(adRecord, nil)
	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(job, nil)
	mockStorage.On("UpdateJob", mock.Anything, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).
		Return(nil)

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return errors.New("workflow execution failed")
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	mockStorage.AssertCalled(t, "UpdateJob", mock.Anything, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything)
}

func TestCreateActiveDirectory_DatabaseRecordCreationFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "test-ad",
		AccountId:  "123",
		Username:   "admin@test.local",
		Password:   "SecurePass123!",
		Domain:     "test.local",
		DNS:        "10.0.0.1",
		NetBIOS:    "TEST",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	mockStorage.On("GetAccount", mock.Anything, "123").Return(account, nil).Maybe()
	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).
		Return(nil, errors.New("database insert failed"))

	originalStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(ctx context.Context, password string, secretID string) error {
		return nil
	}
	defer func() { adHelper.StorePasswordSecret = originalStorePassword }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	ad, jobUUID, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "database insert failed")
}

func TestConvertActiveDirectoryParamsToModel(t *testing.T) {
	params := &common.CreateActiveDirectoryParams{
		ResourceId:                 "test-ad",
		Username:                   "admin@test.local",
		Domain:                     "test.local",
		DNS:                        "10.0.0.1",
		NetBIOS:                    "TEST",
		OrganizationalUnit:         "CN=Computers",
		Site:                       "Default-First-Site",
		SecurityOperators:          []string{"security-user"},
		BackupOperators:            []string{"backup-user"},
		Administrators:             []string{"admin-user"},
		KdcIP:                      "10.0.0.2",
		KdcHostname:                "kdc.test.local",
		AesEncryption:              true,
		EncryptDCConnections:       true,
		LdapSigning:                true,
		AllowLocalNFSUsersWithLdap: false,
		Description:                "Test AD",
	}

	ad := convertActiveDirectoryParamsToModel(params)

	assert.Equal(t, "test-ad", ad.AdName)
	assert.Equal(t, "admin@test.local", ad.Username)
	assert.Equal(t, "test.local", ad.Domain)
	assert.Equal(t, "10.0.0.1", ad.DNS)
	assert.Equal(t, "TEST", ad.NetBIOS)
	assert.Equal(t, models.LifeCycleStateCreating, ad.State)
	assert.Equal(t, "CN=Computers", ad.ActiveDirectoryAttributes.OrganizationalUnit)
	assert.Equal(t, []string{"security-user"}, ad.ActiveDirectoryAttributes.SecurityOperators)
	assert.True(t, ad.ActiveDirectoryAttributes.AesEncryption)
}

func TestCreateAdRecordForNonSDE(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId:         "test-ad",
		Username:           "admin@test.local",
		Domain:             "test.local",
		DNS:                "10.0.0.1",
		NetBIOS:            "TEST",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site",
		BackupOperators:    []string{"backup-user"},
		Administrators:     []string{"admin-user"},
		SecurityOperators:  []string{"security-user"},
		KdcIP:              "10.0.0.2",
		KdcHostname:        "kdc.test.local",
		AesEncryption:      true,
	}

	accountID := int64(123)

	expectedRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: utils.RandomUUID()},
		AdName:    params.ResourceId,
		Username:  params.Username,
		State:     models.LifeCycleStateCreating,
	}

	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.MatchedBy(func(ad *datamodel.ActiveDirectory) bool {
		return ad.AdName == params.ResourceId &&
			ad.Username == params.Username &&
			ad.AccountId == accountID &&
			ad.ActiveDirectoryAttributes.OrganizationalUnit == params.OrganizationalUnit &&
			ad.ActiveDirectoryAttributes.PrimaryAD == true
	})).Return(expectedRecord, nil)

	adRecord, err := createAdRecordForNonSDE(ctx, mockStorage, params, accountID)

	assert.NoError(t, err)
	assert.NotNil(t, adRecord)
}

func TestCreateAdRecordForNonSDE_DatabaseError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "test-ad",
		Username:   "admin@test.local",
		Domain:     "test.local",
	}

	mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).
		Return(nil, errors.New("database error"))

	adRecord, err := createAdRecordForNonSDE(ctx, mockStorage, params, 123)

	assert.Error(t, err)
	assert.Nil(t, adRecord)
	assert.Contains(t, err.Error(), "database error")
}

func TestOrchestratorCreateActiveDirectory(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	orchestrator := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "test-ad",
		AccountId:  "123",
		Username:   "admin@test.local",
		Password:   "SecurePass123!",
		Domain:     "test.local",
		DNS:        "10.0.0.1",
		NetBIOS:    "TEST",
	}

	expectedAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
	}

	originalCreate := createActiveDirectory
	createActiveDirectory = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateActiveDirectoryParams) (*models.ActiveDirectory, string, error) {
		return expectedAD, "job-uuid", nil
	}
	defer func() { createActiveDirectory = originalCreate }()

	ad, jobUUID, err := orchestrator.CreateActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid", jobUUID)
	assert.Equal(t, "test-ad", ad.AdName)
}

func TestOrchestratorCreateActiveDirectory_Error(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	orchestrator := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	params := &common.CreateActiveDirectoryParams{
		ResourceId: "test-ad",
		AccountId:  "123",
	}

	originalCreate := createActiveDirectory
	createActiveDirectory = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateActiveDirectoryParams) (*models.ActiveDirectory, string, error) {
		return nil, "", errors.New("creation failed")
	}
	defer func() { createActiveDirectory = originalCreate }()

	ad, jobUUID, err := orchestrator.CreateActiveDirectory(ctx, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "creation failed")
}

func Test_getActiveDirectory_Success(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	adUUID := "test-ad-uuid"

	adFromDB := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: adUUID,
		},
		AdName:         "test-ad",
		Username:       "testuser",
		CredentialPath: log.PasswordMask,
		Domain:         "example.com",
		DNS:            "8.8.8.8",
		NetBIOS:        "EXAMPLE",
		State:          "READY",
		StateDetails:   "Active Directory is ready",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test",
			Site:               "Default-Site",
			AdUsers: map[string][]string{
				"SeSecurityPrivilege":      {"user1"},
				`BUILTIN\Backup Operators`: {"user2"},
				`BUILTIN\Administrators`:   {"user3"},
			},
			KdcIP:                      "1.2.3.4",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                true,
			AllowLocalNFSUsersWithLdap: false,
			Description:                "Test AD",
		},
	}

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, adUUID, int64(0)).Return(adFromDB, nil)

	ad, err := _getActiveDirectory(ctx, mockSe, adUUID)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, adUUID, ad.UUID)
	assert.Equal(t, "test-ad", ad.AdName)
	assert.Equal(t, "testuser", ad.Username)
	assert.Equal(t, log.PasswordMask, ad.Password)
	assert.Equal(t, "READY", ad.State)
	assert.NotNil(t, ad.ActiveDirectoryAttributes)
	assert.Equal(t, "OU=Test", ad.ActiveDirectoryAttributes.OrganizationalUnit)
	assert.Equal(t, []string{"user1"}, ad.ActiveDirectoryAttributes.SecurityOperators)
	assert.Equal(t, []string{"user2"}, ad.ActiveDirectoryAttributes.BackupOperators)
	assert.Equal(t, []string{"user3"}, ad.ActiveDirectoryAttributes.Administrators)
	mockSe.AssertExpectations(t)
}

func Test_getActiveDirectory_NotFound(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	adUUID := "non-existent-uuid"

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, adUUID, int64(0)).Return(nil, nil)

	ad, err := _getActiveDirectory(ctx, mockSe, adUUID)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Contains(t, err.Error(), "not found")
	mockSe.AssertExpectations(t)
}

func Test_getActiveDirectory_DatabaseError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	adUUID := "test-ad-uuid"

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, adUUID, int64(0)).Return(nil, errors.New("database error"))

	ad, err := _getActiveDirectory(ctx, mockSe, adUUID)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Contains(t, err.Error(), "database error")
	mockSe.AssertExpectations(t)
}

func Test_listActiveDirectories_Success(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	accountName := "test-account"
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: accountName}

	adsFromDB := []*datamodel.ActiveDirectory{
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-1"},
			AdName:    "ad-name-1",
			Username:  "user1",
			Domain:    "example.com",
			DNS:       "8.8.8.8",
			NetBIOS:   "EXAMPLE1",
			State:     "READY",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				OrganizationalUnit: "OU=Test1",
				AdUsers:            map[string][]string{},
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-2"},
			AdName:    "ad-name-2",
			Username:  "user2",
			Domain:    "example2.com",
			DNS:       "8.8.4.4",
			NetBIOS:   "EXAMPLE2",
			State:     "CREATING",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				OrganizationalUnit: "OU=Test2",
				AdUsers:            map[string][]string{},
			},
		},
	}

	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("ListActiveDirectories", mock.Anything, int64(42)).Return(adsFromDB, nil)

	ads, err := _listActiveDirectories(ctx, mockSe, accountName)

	assert.NoError(t, err)
	assert.NotNil(t, ads)
	assert.Len(t, ads, 2)
	assert.Equal(t, "ad-1", ads[0].UUID)
	assert.Equal(t, "ad-name-1", ads[0].AdName)
	assert.Equal(t, "ad-2", ads[1].UUID)
	assert.Equal(t, "ad-name-2", ads[1].AdName)
	mockSe.AssertExpectations(t)
}

func Test_listActiveDirectories_EmptyList(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	accountName := "test-account"
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: accountName}

	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("ListActiveDirectories", mock.Anything, int64(42)).Return([]*datamodel.ActiveDirectory{}, nil)

	ads, err := _listActiveDirectories(ctx, mockSe, accountName)

	assert.NoError(t, err)
	assert.Len(t, ads, 0)
	mockSe.AssertExpectations(t)
}

func Test_listActiveDirectories_AccountError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	accountName := "test-account"

	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, errors.New("account not found")
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	ads, err := _listActiveDirectories(ctx, mockSe, accountName)

	assert.Error(t, err)
	assert.Nil(t, ads)
	assert.Contains(t, err.Error(), "account not found")
}

func Test_listActiveDirectories_DatabaseError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	accountName := "test-account"
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: accountName}

	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("ListActiveDirectories", mock.Anything, int64(42)).Return(nil, errors.New("database error"))

	ads, err := _listActiveDirectories(ctx, mockSe, accountName)

	assert.Error(t, err)
	assert.Nil(t, ads)
	assert.Contains(t, err.Error(), "database error")
	mockSe.AssertExpectations(t)
}

func Test_getMultipleActiveDirectories_Success(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	uuids := []string{"ad-1", "ad-2", "ad-3"}

	adsFromDB := []*datamodel.ActiveDirectory{
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-1"},
			AdName:    "ad-name-1",
			Username:  "user1",
			Domain:    "example.com",
			DNS:       "8.8.8.8",
			NetBIOS:   "EXAMPLE1",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-2"},
			AdName:    "ad-name-2",
			Username:  "user2",
			Domain:    "example2.com",
			DNS:       "8.8.4.4",
			NetBIOS:   "EXAMPLE2",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-3"},
			AdName:    "ad-name-3",
			Username:  "user3",
			Domain:    "example3.com",
			DNS:       "1.1.1.1",
			NetBIOS:   "EXAMPLE3",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		},
	}

	mockSe.On("GetMultipleActiveDirectoriesByUUIDs", mock.Anything, uuids).Return(adsFromDB, nil)

	ads, err := _getMultipleActiveDirectories(ctx, mockSe, uuids)

	assert.NoError(t, err)
	assert.NotNil(t, ads)
	assert.Len(t, ads, 3)
	assert.Equal(t, "ad-1", ads[0].UUID)
	assert.Equal(t, "ad-2", ads[1].UUID)
	assert.Equal(t, "ad-3", ads[2].UUID)
	mockSe.AssertExpectations(t)
}

func Test_getMultipleActiveDirectories_PartialResults(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	uuids := []string{"ad-1", "ad-2", "non-existent"}

	adsFromDB := []*datamodel.ActiveDirectory{
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-1"},
			AdName:    "ad-name-1",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "ad-2"},
			AdName:    "ad-name-2",
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				AdUsers: map[string][]string{},
			},
		},
	}

	mockSe.On("GetMultipleActiveDirectoriesByUUIDs", mock.Anything, uuids).Return(adsFromDB, nil)

	ads, err := _getMultipleActiveDirectories(ctx, mockSe, uuids)

	assert.NoError(t, err)
	assert.NotNil(t, ads)
	assert.Len(t, ads, 2)
	mockSe.AssertExpectations(t)
}

func Test_getMultipleActiveDirectories_EmptyList(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	uuids := []string{}

	mockSe.On("GetMultipleActiveDirectoriesByUUIDs", mock.Anything, uuids).Return([]*datamodel.ActiveDirectory{}, nil)

	ads, err := _getMultipleActiveDirectories(ctx, mockSe, uuids)

	assert.NoError(t, err)
	assert.Len(t, ads, 0)
	mockSe.AssertExpectations(t)
}

func Test_getMultipleActiveDirectories_DatabaseError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	uuids := []string{"ad-1", "ad-2"}

	mockSe.On("GetMultipleActiveDirectoriesByUUIDs", mock.Anything, uuids).Return(nil, errors.New("database error"))

	ads, err := _getMultipleActiveDirectories(ctx, mockSe, uuids)

	assert.Error(t, err)
	assert.Nil(t, ads)
	assert.Contains(t, err.Error(), "database error")
	mockSe.AssertExpectations(t)
}

func TestOrchestrator_GetActiveDirectory(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	origGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return &models.ActiveDirectory{
			BaseModel: models.BaseModel{UUID: "test-uuid"},
			AdName:    "test-ad",
		}, nil
	}
	defer func() { getActiveDirectory = origGetActiveDirectory }()

	ad, err := o.GetActiveDirectory(ctx, "test-uuid")
	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "test-uuid", ad.UUID)
	assert.Equal(t, "test-ad", ad.AdName)
}

func TestOrchestrator_ListActiveDirectories(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	origListActiveDirectories := listActiveDirectories
	listActiveDirectories = func(ctx context.Context, se database.Storage, accountName string) ([]*models.ActiveDirectory, error) {
		return []*models.ActiveDirectory{
			{BaseModel: models.BaseModel{UUID: "ad-1"}, AdName: "ad-name-1"},
			{BaseModel: models.BaseModel{UUID: "ad-2"}, AdName: "ad-name-2"},
		}, nil
	}
	defer func() { listActiveDirectories = origListActiveDirectories }()

	ads, err := o.ListActiveDirectories(ctx, "test-account")
	assert.NoError(t, err)
	assert.NotNil(t, ads)
	assert.Len(t, ads, 2)
}

func TestOrchestrator_GetMultipleActiveDirectories(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	origGetMultipleActiveDirectories := getMultipleActiveDirectories
	getMultipleActiveDirectories = func(ctx context.Context, se database.Storage, uuids []string) ([]*models.ActiveDirectory, error) {
		return []*models.ActiveDirectory{
			{BaseModel: models.BaseModel{UUID: "ad-1"}, AdName: "ad-name-1"},
			{BaseModel: models.BaseModel{UUID: "ad-2"}, AdName: "ad-name-2"},
		}, nil
	}
	defer func() { getMultipleActiveDirectories = origGetMultipleActiveDirectories }()

	ads, err := o.GetMultipleActiveDirectories(ctx, []string{"ad-1", "ad-2"})
	assert.NoError(t, err)
	assert.NotNil(t, ads)
	assert.Len(t, ads, 2)
}

func Test_convertDatastoreActiveDirectoryToModel_Success(t *testing.T) {
	now := time.Now()
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: now,
			UpdatedAt: now,
		},
		AdName:         "test-ad",
		Username:       "testuser",
		CredentialPath: log.PasswordMask,
		Domain:         "example.com",
		DNS:            "8.8.8.8",
		NetBIOS:        "EXAMPLE",
		State:          "READY",
		StateDetails:   "Ready",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test",
			Site:               "Default-Site",
			AdUsers: map[string][]string{
				"SeSecurityPrivilege":      {"sec-user"},
				`BUILTIN\Backup Operators`: {"backup-user"},
				`BUILTIN\Administrators`:   {"admin-user"},
			},
			KdcIP:                      "1.2.3.4",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                false,
			AllowLocalNFSUsersWithLdap: true,
			Description:                "Test Description",
		},
	}

	result := convertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Equal(t, "testuser", result.Username)
	assert.Equal(t, log.PasswordMask, result.Password)
	assert.Equal(t, "example.com", result.Domain)
	assert.Equal(t, "READY", result.State)
	assert.NotNil(t, result.ActiveDirectoryAttributes)
	assert.Equal(t, "OU=Test", result.ActiveDirectoryAttributes.OrganizationalUnit)
	assert.Equal(t, []string{"sec-user"}, result.ActiveDirectoryAttributes.SecurityOperators)
	assert.Equal(t, []string{"backup-user"}, result.ActiveDirectoryAttributes.BackupOperators)
	assert.Equal(t, []string{"admin-user"}, result.ActiveDirectoryAttributes.Administrators)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.AesEncryption)
	assert.Equal(t, "Test Description", result.ActiveDirectoryAttributes.Description)
}

func Test_convertDatastoreActiveDirectoryToModel_NilInput(t *testing.T) {
	result := convertDatastoreActiveDirectoryToModel(nil)
	assert.Nil(t, result)
}

func Test_convertDatastoreActiveDirectoryToModel_NilAttributes(t *testing.T) {
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-uuid",
		},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: nil,
	}

	result := convertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Nil(t, result.ActiveDirectoryAttributes)
}

func TestOrchestrator_GetADConfig_Success(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	params := &common.GetADParams{
		UUID:          "test-ad-uuid",
		AccountName:   "test-account",
		LocationID:    "us-central1",
		ProjectNumber: "12345",
		ResourceID:    "test-ad",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}

	// Mock getAccountWithName to return our account
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getAccountWithName = _getAccountWithName }()

	adFromDB := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-ad-uuid",
		},
		AdName:         "test-ad",
		Username:       "testuser",
		CredentialPath: log.PasswordMask,
		Domain:         "example.com",
		DNS:            "8.8.8.8",
		NetBIOS:        "EXAMPLE",
		State:          "READY",
		StateDetails:   "Active Directory is ready",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test",
			Site:               "Default-Site",
			AdUsers: map[string][]string{
				"SeSecurityPrivilege":      {"user1"},
				`BUILTIN\Backup Operators`: {"user2"},
				`BUILTIN\Administrators`:   {"user3"},
			},
			KdcIP:                      "1.2.3.4",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                true,
			AllowLocalNFSUsersWithLdap: false,
			Description:                "Test AD",
		},
	}

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "test-ad-uuid", int64(42)).Return(adFromDB, nil)

	result, err := o.GetADConfig(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-ad-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Equal(t, "testuser", result.Username)
	assert.Equal(t, "example.com", result.Domain)
	assert.Equal(t, "READY", result.State)
	assert.NotNil(t, result.ActiveDirectoryAttributes)
	assert.Equal(t, "OU=Test", result.ActiveDirectoryAttributes.OrganizationalUnit)
	mockSe.AssertExpectations(t)
}

func TestOrchestrator_GetADConfig_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	params := &common.GetADParams{
		UUID:        "test-ad-uuid",
		AccountName: "non-existent-account",
	}

	// Mock getAccountWithName to return error
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, customerrors.NewUserInputValidationErr("account not found")
	}
	defer func() { getAccountWithName = _getAccountWithName }()

	result, err := o.GetADConfig(ctx, params)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "account not found")
}

func TestOrchestrator_GetADConfig_ADNotFound(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	params := &common.GetADParams{
		UUID:        "non-existent-ad-uuid",
		AccountName: "test-account",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}

	// Mock getAccountWithName to return our account
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getAccountWithName = _getAccountWithName }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "non-existent-ad-uuid", int64(42)).Return(nil, customerrors.NewNotFoundErr("Active Directory", nil))

	result, err := o.GetADConfig(ctx, params)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "Active Directory not found")
	mockSe.AssertExpectations(t)
}

func TestOrchestrator_GetADConfig_DatabaseError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	params := &common.GetADParams{
		UUID:        "test-ad-uuid",
		AccountName: "test-account",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}

	// Mock getAccountWithName to return our account
	getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getAccountWithName = _getAccountWithName }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "test-ad-uuid", int64(42)).Return(nil, customerrors.New("database error"))

	result, err := o.GetADConfig(ctx, params)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database error")
	mockSe.AssertExpectations(t)
}

func TestOrchestrator_GetSDEActiveDirectory_Phase2Placeholder(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	params := &common.GetADParams{
		UUID:          "test-ad-uuid",
		AccountName:   "test-account",
		LocationID:    "us-central1",
		ProjectNumber: "12345",
		ResourceID:    "test-ad",
	}

	// Phase 2 implementation returns nil, nil
	result, err := o.GetSDEActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.Nil(t, result)
}

func Test_convertActiveDirectoryToModel_Success(t *testing.T) {
	now := time.Now()
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: now,
			UpdatedAt: now,
		},
		AdName:         "test-ad",
		Username:       "testuser",
		CredentialPath: "secret-path",
		Domain:         "example.com",
		DNS:            "8.8.8.8",
		NetBIOS:        "EXAMPLE",
		State:          "READY",
		StateDetails:   "Ready",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test",
			Site:               "Default-Site",
			AdUsers: map[string][]string{
				"SeSecurityPrivilege":      {"sec-user"},
				`BUILTIN\Backup Operators`: {"backup-user"},
				`BUILTIN\Administrators`:   {"admin-user"},
			},
			KdcIP:                      "1.2.3.4",
			KdcHostname:                "kdc.example.com",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                false,
			AllowLocalNFSUsersWithLdap: true,
			Description:                "Test Description",
		},
	}

	result := convertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Equal(t, "testuser", result.Username)
	assert.Equal(t, "******************", result.Password)
	assert.Equal(t, "example.com", result.Domain)
	assert.Equal(t, "8.8.8.8", result.DNS)
	assert.Equal(t, "EXAMPLE", result.NetBIOS)
	assert.Equal(t, "READY", result.State)
	assert.Equal(t, "Ready", result.StateDetails)
	assert.NotNil(t, result.ActiveDirectoryAttributes)
	assert.Equal(t, "OU=Test", result.ActiveDirectoryAttributes.OrganizationalUnit)
	assert.Equal(t, "Default-Site", result.ActiveDirectoryAttributes.Site)
	assert.Equal(t, []string{"sec-user"}, result.ActiveDirectoryAttributes.SecurityOperators)
	assert.Equal(t, []string{"backup-user"}, result.ActiveDirectoryAttributes.BackupOperators)
	assert.Equal(t, []string{"admin-user"}, result.ActiveDirectoryAttributes.Administrators)
	assert.Equal(t, "1.2.3.4", result.ActiveDirectoryAttributes.KdcIP)
	assert.Equal(t, "kdc.example.com", result.ActiveDirectoryAttributes.KdcHostname)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.AesEncryption)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.EncryptDCConnections)
	assert.Equal(t, false, result.ActiveDirectoryAttributes.LdapSigning)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap)
	assert.Equal(t, "Test Description", result.ActiveDirectoryAttributes.Description)
}

func Test_convertActiveDirectoryToModel_NilInput(t *testing.T) {
	result := convertDatastoreActiveDirectoryToModel(nil)
	assert.Nil(t, result)
}

func Test_convertActiveDirectoryToModel_NilAttributes(t *testing.T) {
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID: "test-uuid",
		},
		AdName:                    "test-ad",
		ActiveDirectoryAttributes: nil,
	}

	result := convertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Nil(t, result.ActiveDirectoryAttributes)
}

func TestUpdateActiveDirectory_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:                  "123",
		ActiveDirectoryId:          "ad-uuid-123",
		Username:                   nillable.GetStringPtr("updated-admin@test.local"),
		Domain:                     nillable.GetStringPtr("test.local"),
		DNS:                        nillable.GetStringPtr("10.0.0.2"),
		NetBIOS:                    nillable.GetStringPtr("TEST"),
		SecurityOperators:          []string{"updated-security-user"},
		BackupOperators:            []string{"updated-backup-user"},
		Administrators:             []string{"updated-admin-user"},
		OrganizationalUnit:         nillable.GetStringPtr("CN=UpdatedComputers"),
		Site:                       nillable.GetStringPtr("Updated-Site"),
		KdcIP:                      nillable.GetStringPtr("10.0.0.3"),
		KdcHostname:                nillable.GetStringPtr("updated-kdc.test.local"),
		AesEncryption:              nillable.GetBoolPtr(false),
		EncryptDCConnections:       nillable.GetBoolPtr(true),
		LdapSigning:                nillable.GetBoolPtr(true),
		AllowLocalNFSUsersWithLdap: nillable.GetBoolPtr(false),
		Description:                nillable.GetStringPtr("Updated Test AD"),
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}
	existingAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		Username:  "admin@test.local",
		Domain:    "test.local",
		DNS:       "10.0.0.1",
		NetBIOS:   "TEST",
		State:     models.LifeCycleStateREADY,
	}

	createdJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid-123"},
		WorkflowID:    "workflow-id-123",
		Type:          string(models.JobTypeUpdateActiveDirectory),
		State:         string(models.JobsStateNEW),
		ResourceName:  "test-ad",
		AccountID:     sql.NullInt64{Int64: 123, Valid: true},
		CorrelationID: "correlation-id",
		RequestID:     "request-id",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "ad-uuid-123",
		},
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock getOrCreateAccount
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	// Mock _getActiveDirectory
	originalGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return existingAD, nil
	}
	defer func() { getActiveDirectory = originalGetActiveDirectory }()

	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(createdJob, nil)

	// Mock workflow execution
	originalWorkflowExecute := workflowsExecuteWorkflowSequentially
	workflowsExecuteWorkflowSequentially = func(client client.Client, ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, childOptions workflow.ChildWorkflowOptions, args ...interface{}) error {
		return nil
	}
	defer func() { workflowsExecuteWorkflowSequentially = originalWorkflowExecute }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	adRecord := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
	}

	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(adRecord, nil)
	mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).Return(adRecord, nil)

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid-123", jobUUID)
	assert.Equal(t, models.LifeCycleStateUpdating, ad.State)
	assert.Equal(t, models.LifeCycleStateUpdatingDetails, ad.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestUpdateActiveDirectory_ValidationError(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		DNS:               nillable.GetStringPtr(""), // Invalid empty DNS
		ActiveDirectoryId: "ad-uuid-123",
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()
	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID},
		Name:      "test-account",
	}
	mockStorage.On("GetAccount", mock.Anything, mock.Anything).Return(account, nil).Maybe()

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
}

func TestUpdateActiveDirectory_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "non-existent-account",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock getOrCreateAccount to return error
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, customerrors.NewNotFoundErr("Account", &accountName)
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "non-existent-account")
}

func TestUpdateActiveDirectory_ADNotFound(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "non-existent-ad",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock getOrCreateAccount
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	// Mock _getActiveDirectory to return error
	originalGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return nil, customerrors.NewNotFoundErr("ActiveDirectory", &activeDirectoryUUID)
	}
	defer func() { getActiveDirectory = originalGetActiveDirectory }()

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateActiveDirectory_JobCreationFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}
	existingAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock getOrCreateAccount
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	// Mock _getActiveDirectory
	originalGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return existingAD, nil
	}
	defer func() { getActiveDirectory = originalGetActiveDirectory }()

	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(nil, errors.New("database error"))

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func TestUpdateActiveDirectory_WorkflowStartFailed(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}
	existingAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
	}

	createdJob := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
		WorkflowID: "workflow-id-123",
		Type:       string(models.JobTypeUpdateActiveDirectory),
		State:      string(models.JobsStateNEW),
	}

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	// Mock getOrCreateAccount
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	// Mock _getActiveDirectory
	originalGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return existingAD, nil
	}
	defer func() { getActiveDirectory = originalGetActiveDirectory }()

	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(createdJob, nil)
	mockStorage.On("UpdateJob", mock.Anything, createdJob.UUID, string(models.JobsStateERROR), 0, "workflow start error").Return(nil)

	// Mock workflow execution to fail
	originalWorkflowExecute := workflowsExecuteWorkflowSequentially
	workflowsExecuteWorkflowSequentially = func(client client.Client, ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, childOptions workflow.ChildWorkflowOptions, args ...interface{}) error {
		return errors.New("workflow start error")
	}
	defer func() { workflowsExecuteWorkflowSequentially = originalWorkflowExecute }()

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "workflow start error")
	mockStorage.AssertExpectations(t)
}

func TestUpdateActiveDirectory_Success_WithCVPHost(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}
	existingAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
	}

	createdJob := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
		WorkflowID: "workflow-id-123",
		Type:       string(models.JobTypeUpdateActiveDirectory),
		State:      string(models.JobsStateNEW),
	}

	// Mock getOrCreateAccount
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	// Mock _getActiveDirectory
	originalGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return existingAD, nil
	}
	defer func() { getActiveDirectory = originalGetActiveDirectory }()

	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(createdJob, nil)

	// Mock workflow execution
	originalWorkflowExecute := workflowsExecuteWorkflowSequentially
	workflowsExecuteWorkflowSequentially = func(client client.Client, ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, childOptions workflow.ChildWorkflowOptions, args ...interface{}) error {
		return nil
	}
	defer func() { workflowsExecuteWorkflowSequentially = originalWorkflowExecute }()

	// Set CVP_HOST to simulate SDE environment
	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "http://cvp.example.com"
	originalCreateCommonResourcesInVCP := utils.CreateCommonResourcesInVCP
	utils.CreateCommonResourcesInVCP = false
	defer func() {
		cvp.CVP_HOST = originalCVPHost
		utils.CreateCommonResourcesInVCP = originalCreateCommonResourcesInVCP
	}()

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid-123", jobUUID)
	assert.Equal(t, models.LifeCycleStateUpdating, ad.State)
	assert.Equal(t, models.LifeCycleStateUpdatingDetails, ad.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestUpdateActiveDirectory_ADRecordNotFoundInDB(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 123}, Name: "test-account"}
	existingAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
	}

	createdJob := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-uuid-123"},
		WorkflowID: "workflow-id-123",
		Type:       string(models.JobTypeUpdateActiveDirectory),
		State:      string(models.JobsStateNEW),
	}

	// Mock getOrCreateAccount
	originalGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = originalGetOrCreateAccount }()

	// Mock _getActiveDirectory
	originalGetActiveDirectory := getActiveDirectory
	getActiveDirectory = func(ctx context.Context, se database.Storage, activeDirectoryUUID string) (*models.ActiveDirectory, error) {
		return existingAD, nil
	}
	defer func() { getActiveDirectory = originalGetActiveDirectory }()

	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(createdJob, nil)

	// Mock workflow execution
	originalWorkflowExecute := workflowsExecuteWorkflowSequentially
	workflowsExecuteWorkflowSequentially = func(client client.Client, ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, childOptions workflow.ChildWorkflowOptions, args ...interface{}) error {
		return nil
	}
	defer func() { workflowsExecuteWorkflowSequentially = originalWorkflowExecute }()

	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	// Mock AD record not found in DB
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "test-ad", int64(123)).Return(nil, nil)

	// Save original function
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	// Mock to return parsed region and zone
	utils.ParseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "us-central1-a", nil
	}
	// Restore original function after test
	defer func() {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
	}()

	ad, jobUUID, err := _updateActiveDirectory(ctx, mockStorage, mockTemporal, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "not found")
	mockStorage.AssertExpectations(t)
}

func TestOrchestratorUpdateActiveDirectory(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	orchestrator := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	expectedAD := &models.ActiveDirectory{
		BaseModel: models.BaseModel{UUID: "ad-uuid-123"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateUpdating,
	}

	originalUpdate := updateActiveDirectory
	updateActiveDirectory = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateActiveDirectoryParams) (*models.ActiveDirectory, string, error) {
		return expectedAD, "job-uuid", nil
	}
	defer func() { updateActiveDirectory = originalUpdate }()

	ad, jobUUID, err := orchestrator.UpdateActiveDirectory(ctx, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid", jobUUID)
	assert.Equal(t, "test-ad", ad.AdName)
	assert.Equal(t, models.LifeCycleStateUpdating, ad.State)
}

func TestOrchestratorUpdateActiveDirectory_Error(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := mocks.NewClient(t)

	orchestrator := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	params := &common.UpdateActiveDirectoryParams{
		AccountId:         "123",
		ActiveDirectoryId: "ad-uuid-123",
		Username:          nillable.GetStringPtr("admin@test.local"),
		Domain:            nillable.GetStringPtr("test.local"),
	}

	originalUpdate := updateActiveDirectory
	updateActiveDirectory = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.UpdateActiveDirectoryParams) (*models.ActiveDirectory, string, error) {
		return nil, "", errors.New("update failed")
	}
	defer func() { updateActiveDirectory = originalUpdate }()

	ad, jobUUID, err := orchestrator.UpdateActiveDirectory(ctx, params)

	assert.Error(t, err)
	assert.Nil(t, ad)
	assert.Empty(t, jobUUID)
	assert.Contains(t, err.Error(), "update failed")
}

// Delete Active Directory Tests

func Test_deleteActiveDirectory_GetAccountError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	// Patch getOrCreateAccount to return error
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, errors.New("account error")
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "account error")
	assert.Empty(t, jobUUID)
}

func Test_deleteActiveDirectory_GetADError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(nil, errors.New("get AD error"))

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get AD error")
	assert.Empty(t, jobUUID)
	mockSe.AssertExpectations(t)
}

func Test_deleteActiveDirectory_AlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateDeleted,
		AccountId: 42,
	}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(ad, nil)

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	// Should return empty job UUID and no error (lines 546-547)
	assert.NoError(t, err)
	assert.Empty(t, jobUUID)
	mockSe.AssertExpectations(t)
}

func Test_deleteActiveDirectory_AlreadyDeletingWithExistingJob(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateDeleting,
		AccountId: 42,
	}
	existingJob := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "existing-job-uuid"},
		WorkflowID:   "existing-workflow-id",
		AccountID:    sql.NullInt64{Int64: 42, Valid: true},
		ResourceName: "test-ad",
	}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(ad, nil)
	// GetJobByResourceUUID returns existing job (lines 556-557)
	mockSe.On("GetJobByResourceUUID", mock.Anything, "ad-uuid", string(models.JobTypeDeleteActiveDirectory)).Return(existingJob, nil)

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	// Should return existing job UUID (lines 556-557)
	assert.NoError(t, err)
	assert.Equal(t, "existing-job-uuid", jobUUID)
	mockSe.AssertExpectations(t)
}

func Test_deleteActiveDirectory_AlreadyDeletingNoJob(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateDeleting,
		AccountId: 42,
	}
	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:   "workflow-id",
		AccountID:    sql.NullInt64{Int64: 42, Valid: true},
		ResourceName: "test-ad",
	}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	// Mock ExecuteWorkflowSequentially
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(ad, nil)
	// GetJobByResourceUUID returns error (no job found)
	mockSe.On("GetJobByResourceUUID", mock.Anything, "ad-uuid", string(models.JobTypeDeleteActiveDirectory)).Return(nil, errors.New("job not found"))
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	// Should succeed and create new job (line 207 warning logged)
	assert.NoError(t, err)
	assert.Equal(t, "job-uuid", jobUUID)
	mockSe.AssertExpectations(t)
}

func Test_deleteActiveDirectory_CreateJobError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
		AccountId: 42,
	}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(ad, nil)
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("create job error"))

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create job error")
	assert.Empty(t, jobUUID)
	mockSe.AssertExpectations(t)
}

func Test_deleteActiveDirectory_WorkflowErrorWithUpdateJobError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
		AccountId: 42,
	}
	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:   "workflow-id",
		AccountID:    sql.NullInt64{Int64: 42, Valid: true},
		ResourceName: "test-ad",
	}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	// Mock ExecuteWorkflowSequentially to return error
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return errors.New("workflow execution error")
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(ad, nil)
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
	// Mock UpdateJob to also fail (lines 255-256)
	mockSe.On("UpdateJob", mock.Anything, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(errors.New("update job error"))

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow execution error")
	assert.Empty(t, jobUUID)
	mockSe.AssertExpectations(t)
}

func Test_deleteActiveDirectory_WorkflowError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
		AdName:    "test-ad",
		State:     models.LifeCycleStateREADY,
		AccountId: 42,
	}
	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:   "workflow-id",
		AccountID:    sql.NullInt64{Int64: 42, Valid: true},
		ResourceName: "test-ad",
	}

	// Patch getOrCreateAccount
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	// Mock ExecuteWorkflowSequentially to return error
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return errors.New("workflow execution error")
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	mockSe.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-uuid", int64(42)).Return(ad, nil)
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
	// Mock UpdateJob to succeed
	mockSe.On("UpdateJob", mock.Anything, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(nil)

	jobUUID, err := _deleteActiveDirectory(ctx, mockSe, mockTemporal, params)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow execution error")
	assert.Empty(t, jobUUID)
	mockSe.AssertExpectations(t)
}

func TestOrchestrator_DeleteActiveDirectory_Error(t *testing.T) {
	ctx := context.Background()
	params := &common.DeleteActiveDirectoryParams{
		ProjectNumber:       "test-account",
		ActiveDirectoryUUID: "ad-uuid",
	}
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}

	origDeleteActiveDirectory := deleteActiveDirectory
	deleteActiveDirectory = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.DeleteActiveDirectoryParams) (string, error) {
		return "", errors.New("delete error")
	}
	defer func() { deleteActiveDirectory = origDeleteActiveDirectory }()

	jobUUID, err := o.DeleteActiveDirectory(ctx, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "delete error")
	assert.Empty(t, jobUUID)
}
