package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"go.temporal.io/sdk/mocks"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func Test_createActiveDirectory_Success(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.CreateActiveDirectoryParams{
		AccountId:                  "test-account",
		ResourceId:                 "ad-resource",
		Username:                   "admin",
		Password:                   "pass",
		Domain:                     "example.com",
		DNS:                        "8.8.8.8",
		NetBIOS:                    "NETBIOS",
		OrganizationalUnit:         "",
		Site:                       "site1",
		SecurityOperators:          []string{"secop"},
		BackupOperators:            []string{"backupop"},
		Administrators:             []string{"admin"},
		KdcIP:                      "1.2.3.4",
		KdcHostname:                "kdc-host",
		AesEncryption:              true,
		EncryptDCConnections:       true,
		LdapSigning:                true,
		AllowLocalNFSUsersWithLdap: false,
		Description:                "desc",
	}

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "job-uuid",
		},
		WorkflowID:   "workflow-id",
		AccountID:    sql.NullInt64{Int64: 42, Valid: true},
		ResourceName: "ad-resource",
	}
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)

	// Patch getOrCreateAccount to return our account
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
	mockSe.On("GetAccount", mock.Anything, "test-account").Return(account, nil)
	mockSe.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, "ad-resource", int64(42)).Return(nil, sql.ErrNoRows)

	ad, jobUUID, err := createActiveDirectory(ctx, mockSe, mockTemporal, params)

	assert.NoError(t, err)
	assert.NotNil(t, ad)
	assert.Equal(t, "job-uuid", jobUUID)
	assert.Equal(t, params.ResourceId, ad.AdName)
}

func Test_createActiveDirectory_AccountError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.CreateActiveDirectoryParams{
		AccountId:          "test-account",
		ResourceId:         "ad-resource",
		Username:           "admin",
		Password:           "password123",
		Domain:             "example.com",
		DNS:                "8.8.8.8",
		NetBIOS:            "EXAMPLE",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site-Name",
	}
	// Patch getOrCreateAccount to return error
	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return nil, errors.New("account error")
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	mockSe.On("GetAccount", mock.Anything, "test-account").Return(account, nil)
	mockSe.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, mock.Anything, int64(42)).Return(nil, sql.ErrNoRows)

	_, _, err := _createActiveDirectory(ctx, mockSe, mockTemporal, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "account error")
}

func Test_createActiveDirectory_CreateJobError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.CreateActiveDirectoryParams{
		AccountId:          "test-account",
		ResourceId:         "ad-resource",
		Username:           "admin",
		Password:           "password123",
		Domain:             "example.com",
		DNS:                "8.8.8.8",
		NetBIOS:            "EXAMPLE",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site-Name",
	}
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(&datamodel.Job{}, errors.New("db error"))

	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	mockSe.On("GetAccount", mock.Anything, "test-account").Return(account, nil)
	mockSe.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, mock.Anything, int64(42)).Return(nil, sql.ErrNoRows)

	_, _, err := _createActiveDirectory(ctx, mockSe, mockTemporal, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
}

func Test_createActiveDirectory_WorkflowError(t *testing.T) {
	ctx := context.Background()
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	params := &common.CreateActiveDirectoryParams{
		AccountId:          "test-account",
		ResourceId:         "ad-resource",
		Username:           "admin",
		Password:           "password123",
		Domain:             "example.com",
		DNS:                "8.8.8.8",
		NetBIOS:            "EXAMPLE",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site-Name",
	}
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 42}, Name: "test-account"}
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "job-uuid",
		},
		WorkflowID:   "workflow-id",
		AccountID:    sql.NullInt64{Int64: 42, Valid: true},
		ResourceName: "ad-resource",
	}
	mockSe.On("CreateJob", mock.Anything, mock.AnythingOfType("*datamodel.Job")).Return(job, nil)
	mockSe.On("UpdateJob", mock.Anything, "job-uuid", string(models.JobsStateERROR), 0, mock.Anything).Return(nil)

	origGetOrCreateAccount := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	defer func() { getOrCreateAccount = origGetOrCreateAccount }()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return errors.New("workflow error")
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	mockSe.On("GetAccount", mock.Anything, "test-account").Return(account, nil)
	mockSe.On("GetActiveDirectoryByNameAndAccountID", mock.Anything, mock.Anything, int64(42)).Return(nil, sql.ErrNoRows)

	_, _, err := _createActiveDirectory(ctx, mockSe, mockTemporal, params)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow error")
}

func Test_convertActiveDirectoryParamsToModel(t *testing.T) {
	params := &common.CreateActiveDirectoryParams{
		ResourceId:                 "ad-resource",
		Username:                   "admin",
		Password:                   "pass",
		Domain:                     "example.com",
		DNS:                        "8.8.8.8",
		NetBIOS:                    "NETBIOS",
		OrganizationalUnit:         "OU=Test",
		Site:                       "site1",
		SecurityOperators:          []string{"secop"},
		BackupOperators:            []string{"backupop"},
		Administrators:             []string{"admin"},
		KdcIP:                      "1.2.3.4",
		KdcHostname:                "kdc-host",
		AesEncryption:              true,
		EncryptDCConnections:       true,
		LdapSigning:                true,
		AllowLocalNFSUsersWithLdap: false,
		Description:                "desc",
	}
	uuid := "ad-uuid"
	ad := convertActiveDirectoryParamsToModel(params, uuid)
	assert.Equal(t, uuid, ad.UUID)
	assert.Equal(t, params.ResourceId, ad.AdName)
	assert.Equal(t, params.Username, ad.Username)
	assert.Equal(t, params.Password, ad.Password)
	assert.Equal(t, params.Domain, ad.Domain)
	assert.Equal(t, params.DNS, ad.DNS)
	assert.Equal(t, params.NetBIOS, ad.NetBIOS)
	assert.Equal(t, params.OrganizationalUnit, ad.ActiveDirectoryAttributes.OrganizationalUnit)
	assert.Equal(t, params.Site, ad.ActiveDirectoryAttributes.Site)
	assert.Equal(t, params.SecurityOperators, ad.ActiveDirectoryAttributes.SecurityOperators)
	assert.Equal(t, params.BackupOperators, ad.ActiveDirectoryAttributes.BackupOperators)
	assert.Equal(t, params.Administrators, ad.ActiveDirectoryAttributes.Administrators)
	assert.Equal(t, params.KdcIP, ad.ActiveDirectoryAttributes.KdcIP)
	assert.Equal(t, params.KdcHostname, ad.ActiveDirectoryAttributes.KdcHostname)
	assert.Equal(t, params.AesEncryption, ad.ActiveDirectoryAttributes.AesEncryption)
	assert.Equal(t, params.EncryptDCConnections, ad.ActiveDirectoryAttributes.EncryptDCConnections)
	assert.Equal(t, params.LdapSigning, ad.ActiveDirectoryAttributes.LdapSigning)
	assert.Equal(t, params.AllowLocalNFSUsersWithLdap, ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap)
	assert.Equal(t, params.Description, ad.ActiveDirectoryAttributes.Description)
}

func TestOrchestrator_CreateActiveDirectory(t *testing.T) {
	ctx := context.Background()
	params := &common.CreateActiveDirectoryParams{
		AccountId:          "test-account",
		ResourceId:         "ad-resource",
		Username:           "admin",
		Password:           "password123",
		Domain:             "example.com",
		DNS:                "8.8.8.8",
		NetBIOS:            "EXAMPLE",
		OrganizationalUnit: "CN=Computers",
		Site:               "Default-First-Site-Name",
	}
	mockSe := new(database.MockStorage)
	mockTemporal := new(mocks.Client)
	o := &Orchestrator{
		storage:  mockSe,
		temporal: mockTemporal,
	}
	origCreateActiveDirectory := createActiveDirectory
	createActiveDirectory = func(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateActiveDirectoryParams) (*models.ActiveDirectory, string, error) {
		return &models.ActiveDirectory{AdName: "ad-resource"}, "job-uuid", nil
	}
	defer func() { createActiveDirectory = origCreateActiveDirectory }()

	ad, jobUUID, err := o.CreateActiveDirectory(ctx, params)
	assert.NoError(t, err)
	assert.Equal(t, "ad-resource", ad.AdName)
	assert.Equal(t, "job-uuid", jobUUID)
}
