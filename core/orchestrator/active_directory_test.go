package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
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

	mockSe.On("GetActiveDirectoryByUUID", mock.Anything, adUUID).Return(adFromDB, nil)

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

	mockSe.On("GetActiveDirectoryByUUID", mock.Anything, adUUID).Return(nil, nil)

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

	mockSe.On("GetActiveDirectoryByUUID", mock.Anything, adUUID).Return(nil, errors.New("database error"))

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

	result := convertActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Equal(t, "testuser", result.Username)
	assert.Equal(t, "secret-path", result.Password) // CredentialPath maps to Password
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
	result := convertActiveDirectoryToModel(nil)
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

	result := convertActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Nil(t, result.ActiveDirectoryAttributes)
}
