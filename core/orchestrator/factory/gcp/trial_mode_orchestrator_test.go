package gcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/workflow"
)

func trialWindow(t *testing.T) (start, end time.Time, params *common.TrialModeParams) {
	t.Helper()
	start = time.Date(2025, 5, 14, 11, 0, 0, 0, time.UTC)
	end = time.Date(2025, 6, 13, 11, 0, 0, 0, time.UTC)
	s, e := start, end
	return start, end, &common.TrialModeParams{Start: &s, End: &e}
}

func stubGetOrCreateAccount(account *datamodel.Account) func() {
	orig := getOrCreateAccount
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return account, nil
	}
	return func() { getOrCreateAccount = orig }
}

func expectTrialMetadataUpdate(
	t *testing.T,
	mockStorage *database.MockStorage,
	ctx context.Context,
	account *datamodel.Account,
	start, end time.Time,
	retErr error,
) {
	t.Helper()
	withTrialAccountSyncEnabled(t)

	mockStorage.On(
		"UpdateAccountTrialMetadata",
		ctx,
		account,
		&datamodel.AccountTrialMode{StartTime: &start, EndTime: &end},
	).Return(retErr).Once()
}

func setupADCreateTestEnv(t *testing.T) {
	t.Helper()
	origParse := utils.ParseAndValidateRegionAndZone
	utils.ParseAndValidateRegionAndZone = func(string) (string, string, *gcpgenserver.Error) {
		return "us-central1", "", nil
	}
	t.Cleanup(func() { utils.ParseAndValidateRegionAndZone = origParse })

	origWorkflow := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(client.Client, context.Context, client.StartWorkflowOptions, interface{}, workflow.ChildWorkflowOptions, ...interface{}) error {
		return nil
	}
	t.Cleanup(func() { workflows.ExecuteWorkflowSeq = origWorkflow })

	origStorePassword := adHelper.StorePasswordSecret
	adHelper.StorePasswordSecret = func(context.Context, string, string) error { return nil }
	t.Cleanup(func() { adHelper.StorePasswordSecret = origStorePassword })

	origCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	t.Cleanup(func() { cvp.CVP_HOST = origCVPHost })

	origEnableMultiAD := utils.EnableMultiAD
	origMaxAD := utils.MaxNumberOfADPerAccount
	utils.EnableMultiAD = false
	utils.MaxNumberOfADPerAccount = 1
	t.Cleanup(func() {
		utils.EnableMultiAD = origEnableMultiAD
		utils.MaxNumberOfADPerAccount = origMaxAD
	})

	t.Cleanup(func() {
		getOrCreateAccount = _getOrCreateAccount
		getAccountWithName = _getAccountWithName
		createAccount = _createAccount
	})
	getOrCreateAccount = _getOrCreateAccount
	getAccountWithName = _getAccountWithName
	createAccount = _createAccount
}

func Test_createHostGroup_TrialMode(t *testing.T) {
	ctx := context.Background()
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct-uuid"},
		Name:      "project-1",
	}

	t.Run("PersistsTrialMetadataWhenSet", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		start, end, trial := trialWindow(t)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)

		created := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid"},
			Name:      "hg-1",
			AccountID: account.ID,
		}
		mockStorage.On("CreateHostGroup", ctx, mock.Anything).Return(created, nil)

		params := &common.CreateHostGroupParams{
			AccountName: "project-1",
			Name:        "hg-1",
			OSType:      "LINUX",
			Hosts:       []string{"iqn.1998-01.com.vmware:host1"},
			TrialMode:   trial,
		}

		got, err := _createHostGroup(ctx, mockStorage, params)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "hg-uuid", got.UUID)
		mockStorage.AssertExpectations(t)
	})

	t.Run("SkipsTrialMetadataWhenOmitted", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		created := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid"},
			Name:      "hg-1",
			AccountID: account.ID,
		}
		mockStorage.On("CreateHostGroup", ctx, mock.Anything).Return(created, nil)

		got, err := _createHostGroup(ctx, mockStorage, &common.CreateHostGroupParams{
			AccountName: "project-1",
			Name:        "hg-1",
			OSType:      "LINUX",
			Hosts:       []string{"iqn.1998-01.com.vmware:host1"},
		})
		require.NoError(t, err)
		require.NotNil(t, got)
		mockStorage.AssertNotCalled(t, "UpdateAccountTrialMetadata", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("ReturnsErrorWhenTrialMetadataUpdateFails", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		start, end, trial := trialWindow(t)
		trialErr := errors.New("trial metadata failed")
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, trialErr)

		_, err := _createHostGroup(ctx, mockStorage, &common.CreateHostGroupParams{
			AccountName: "project-1",
			Name:        "hg-1",
			OSType:      "LINUX",
			Hosts:       []string{"iqn.1998-01.com.vmware:host1"},
			TrialMode:   trial,
		})
		require.Error(t, err)
		assert.Equal(t, trialErr, err)
		mockStorage.AssertNotCalled(t, "CreateHostGroup", mock.Anything, mock.Anything)
	})

	t.Run("ReturnsErrorWhenTrialModeInvalid", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		start, end, _ := trialWindow(t)
		invalidTrial := &common.TrialModeParams{Start: &end, End: &start}

		_, err := _createHostGroup(ctx, mockStorage, &common.CreateHostGroupParams{
			AccountName: "project-1",
			Name:        "hg-1",
			OSType:      "LINUX",
			Hosts:       []string{"iqn.1998-01.com.vmware:host1"},
			TrialMode:   invalidTrial,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "trialMode startTime must be before endTime")
		mockStorage.AssertNotCalled(t, "UpdateAccountTrialMetadata", mock.Anything, mock.Anything, mock.Anything)
	})
}

func Test_createActiveDirectory_TrialMode(t *testing.T) {
	ctx := context.Background()
	mockTemporal := mocks.NewClient(t)
	accountID := int64(123)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: accountID, UUID: "acct-uuid"},
		Name:      "123",
	}

	baseParams := func(trial *common.TrialModeParams) *common.CreateActiveDirectoryParams {
		return &common.CreateActiveDirectoryParams{
			ResourceId:         "test-ad",
			AccountId:          "123",
			LocationId:         "local",
			Username:           "admin@test.local",
			Password:           "SecurePass123!",
			Domain:             "test.local",
			DNS:                "10.0.0.1",
			NetBIOS:            "TEST",
			OrganizationalUnit: "CN=Computers",
			TrialMode:          trial,
		}
	}

	t.Run("PersistsTrialMetadataWhenSet", func(t *testing.T) {
		withTrialAccountSyncEnabled(t)
		setupADCreateTestEnv(t)
		mockStorage := database.NewMockStorage(t)
		start, end, trial := trialWindow(t)

		mockStorage.EXPECT().GetAccount(mock.Anything, "123").Return(account, nil)
		mockStorage.EXPECT().UpdateAccountTrialMetadata(mock.Anything, account, &datamodel.AccountTrialMode{StartTime: &start, EndTime: &end}).Return(nil)
		mockStorage.EXPECT().ListActiveDirectories(mock.Anything, accountID).Return([]*datamodel.ActiveDirectory{}, nil)
		mockStorage.EXPECT().CreateActiveDirectory(mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
			AdName:    "test-ad",
			AccountId: accountID,
			State:     datamodel.LifeCycleStateCreating,
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				OrganizationalUnit: "CN=Computers",
			},
		}, nil)
		mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		}, nil)

		_, _, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, baseParams(trial))
		require.NoError(t, err)
	})

	t.Run("SkipsTrialMetadataWhenOmitted", func(t *testing.T) {
		setupADCreateTestEnv(t)
		mockStorage := database.NewMockStorage(t)

		mockStorage.EXPECT().GetAccount(mock.Anything, "123").Return(account, nil)
		mockStorage.EXPECT().ListActiveDirectories(mock.Anything, accountID).Return([]*datamodel.ActiveDirectory{}, nil)
		mockStorage.EXPECT().CreateActiveDirectory(mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
			AdName:    "test-ad",
			AccountId: accountID,
			State:     datamodel.LifeCycleStateCreating,
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				OrganizationalUnit: "CN=Computers",
			},
		}, nil)
		mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid"},
		}, nil)

		_, _, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, baseParams(nil))
		require.NoError(t, err)
	})

	t.Run("ReturnsErrorWhenTrialMetadataUpdateFails", func(t *testing.T) {
		withTrialAccountSyncEnabled(t)
		setupADCreateTestEnv(t)
		mockStorage := database.NewMockStorage(t)
		start, end, trial := trialWindow(t)
		trialErr := errors.New("trial metadata failed")

		mockStorage.EXPECT().GetAccount(mock.Anything, "123").Return(account, nil)
		mockStorage.EXPECT().UpdateAccountTrialMetadata(mock.Anything, account, &datamodel.AccountTrialMode{StartTime: &start, EndTime: &end}).Return(trialErr)

		_, _, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, baseParams(trial))
		require.Error(t, err)
		assert.Equal(t, trialErr, err)
	})
}

func Test_createPool_TrialMode(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, mockLogger)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct-uuid"},
		Name:      "project-1",
	}

	minimalPoolParams := func(trial *common.TrialModeParams) *commonparams.CreatePoolParams {
		iops := int64(1000)
		return &commonparams.CreatePoolParams{
			AccountName:  account.Name,
			Name:         "pool-1",
			Region:       "us-central1",
			ServiceLevel: ServiceLevelNameFLEX,
			SizeInBytes:  2199023255552,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: 0,
				Iops:            &iops,
			},
			TrialMode: trial,
		}
	}

	t.Run("PersistsTrialMetadataWhenSet", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origValidate := ValidateCreatePoolParams
		ValidateCreatePoolParams = func(*commonparams.CreatePoolParams, log.Logger) error { return nil }
		defer func() { ValidateCreatePoolParams = origValidate }()

		mockStorage := new(database.MockStorage)
		start, end, trial := trialWindow(t)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)
		mockStorage.On("CreatingPool", ctx, mock.Anything).Return(nil, errors.New("stop after trial metadata"))

		_, _, err := _createPool(ctx, mockStorage, nil, minimalPoolParams(trial))
		require.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("SkipsTrialMetadataWhenOmitted", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origValidate := ValidateCreatePoolParams
		ValidateCreatePoolParams = func(*commonparams.CreatePoolParams, log.Logger) error { return nil }
		defer func() { ValidateCreatePoolParams = origValidate }()

		mockStorage := new(database.MockStorage)
		mockStorage.On("CreatingPool", ctx, mock.Anything).Return(nil, errors.New("stop after create path"))

		_, _, err := _createPool(ctx, mockStorage, nil, minimalPoolParams(nil))
		require.Error(t, err)
		mockStorage.AssertNotCalled(t, "UpdateAccountTrialMetadata", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("ReturnsErrorWhenTrialMetadataUpdateFails", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origValidate := ValidateCreatePoolParams
		ValidateCreatePoolParams = func(*commonparams.CreatePoolParams, log.Logger) error { return nil }
		defer func() { ValidateCreatePoolParams = origValidate }()

		mockStorage := new(database.MockStorage)
		start, end, trial := trialWindow(t)
		trialErr := errors.New("trial metadata failed")
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, trialErr)

		_, _, err := _createPool(ctx, mockStorage, nil, minimalPoolParams(trial))
		require.Error(t, err)
		assert.Equal(t, trialErr, err)
		mockStorage.AssertNotCalled(t, "CreatingPool", mock.Anything, mock.Anything)
	})
}

func TestCreateBackupPolicy_TrialMode(t *testing.T) {
	ctx := context.Background()
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct-uuid"},
		Name:      "project-1",
	}

	t.Run("DoesNotPersistTrialMetadataWhenSetOnParams", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		mockTemporal := new(mocks.Client)
		_, _, trial := trialWindow(t)
		mockStorage.On("GetBackupPolicyByNameAndOwnerID", ctx, "policy-1", account.ID).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).
			Return(nil, errors.New("stop after create path"))

		o := &GCPOrchestrator{storage: mockStorage, temporal: mockTemporal}
		_, err := o.CreateBackupPolicy(ctx, &common.CreateBackupPolicyParams{
			Name:        "policy-1",
			AccountName: account.Name,
			TrialMode:   trial,
		})
		require.Error(t, err)
		mockStorage.AssertNotCalled(t, "UpdateAccountTrialMetadata", mock.Anything, mock.Anything, mock.Anything)
	})
}

func TestCreateBackupVault_TrialMode(t *testing.T) {
	ctx := context.Background()
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct-uuid"},
		Name:      "project-1",
	}

	t.Run("DoesNotPersistTrialMetadataWhenSetOnParams", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		_, _, trial := trialWindow(t)
		mockStorage.On("CreateBackupVaultEntryInVCP", ctx, mock.Anything).
			Return(nil, errors.New("stop after create"))

		o := &GCPOrchestrator{storage: mockStorage}
		_, err := o.CreateBackupVault(ctx, &common.CreateBackupVaultParams{
			ProjectNumber: account.Name,
			LocationId:    "us-east4",
			ResourceId:    "bv-1",
			TrialMode:     trial,
		})
		require.Error(t, err)
		mockStorage.AssertNotCalled(t, "UpdateAccountTrialMetadata", mock.Anything, mock.Anything, mock.Anything)
	})
}

func Test_createKmsConfig_TrialMode(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct-uuid"},
		Name:      "project-1",
	}
	temporal := mocks.NewClient(t)

	t.Run("PersistsTrialMetadataOnVCPPath", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origCVPHost := cvp.CVP_HOST
		origForce := utils.ForceVCPKMSPathForTesting
		cvp.CVP_HOST = "localhost:8009"
		utils.ForceVCPKMSPathForTesting = true
		defer func() {
			cvp.CVP_HOST = origCVPHost
			utils.ForceVCPKMSPathForTesting = origForce
		}()

		mockStorage := new(database.MockStorage)
		start, end, trial := trialWindow(t)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)
		parseKeyFullPathResource = func(string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		defer func() { parseKeyFullPathResource = utils.ParseKeyFullPathResource }()
		mockStorage.On("CreateKmsConfig", mock.Anything, mock.Anything).
			Return(nil, errors.New("stop after trial metadata"))

		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, &common.CreateKmsConfigParams{
			AccountName: account.Name,
			KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k",
			TrialMode:   trial,
		})
		require.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("PersistsTrialMetadataOnSDEPath", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origCVPHost := cvp.CVP_HOST
		origForce := utils.ForceVCPKMSPathForTesting
		cvp.CVP_HOST = "localhost:8009"
		utils.ForceVCPKMSPathForTesting = false
		defer func() {
			cvp.CVP_HOST = origCVPHost
			utils.ForceVCPKMSPathForTesting = origForce
		}()

		start, end, trial := trialWindow(t)
		mockStorage := new(database.MockStorage)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)
		parseKeyFullPathResource = func(string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		defer func() { parseKeyFullPathResource = utils.ParseKeyFullPathResource }()
		mockStorage.On("CreateKmsConfig", mock.Anything, mock.Anything).
			Return(nil, errors.New("stop after trial metadata"))

		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, &common.CreateKmsConfigParams{
			AccountName: account.Name,
			KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k",
			TrialMode:   trial,
		})
		require.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ReturnsErrorWhenTrialMetadataUpdateFails", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origCVPHost := cvp.CVP_HOST
		origForce := utils.ForceVCPKMSPathForTesting
		cvp.CVP_HOST = ""
		utils.ForceVCPKMSPathForTesting = true
		defer func() {
			cvp.CVP_HOST = origCVPHost
			utils.ForceVCPKMSPathForTesting = origForce
		}()

		mockStorage := new(database.MockStorage)
		start, end, trial := trialWindow(t)
		trialErr := errors.New("trial metadata update failed")
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, trialErr)
		parseKeyFullPathResource = func(string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		defer func() { parseKeyFullPathResource = utils.ParseKeyFullPathResource }()

		_, _, err := _createKmsConfig(ctx, mockStorage, temporal, &common.CreateKmsConfigParams{
			AccountName: account.Name,
			KeyFullPath: "projects/p/locations/l/keyRings/r/cryptoKeys/k",
			UUID:        "kms-uuid",
			TrialMode:   trial,
		})
		require.Error(t, err)
		assert.Equal(t, trialErr, err)
		mockStorage.AssertNotCalled(t, "CreateKmsConfig", mock.Anything, mock.Anything)
		mockStorage.AssertExpectations(t)
	})
}

// TestTrialMode_SecondCreateWithoutTrial_RetainsPriorTrialMetadata verifies that a second create
// omitting trialMode does not write trial metadata again (prior DB values are retained).
func TestTrialMode_SecondCreateWithoutTrial_RetainsPriorTrialMetadata(t *testing.T) {
	withTrialAccountSyncEnabled(t)

	ctx := context.Background()
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "acct-uuid"},
		Name:      "project-1",
	}
	start, end, trial := trialWindow(t)

	t.Run("host_group", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		mockStorage := new(database.MockStorage)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)

		created := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid"},
			Name:      "hg-1",
			AccountID: account.ID,
		}
		mockStorage.On("CreateHostGroup", ctx, mock.Anything).Return(created, nil).Twice()

		_, err := _createHostGroup(ctx, mockStorage, &common.CreateHostGroupParams{
			AccountName: account.Name,
			Name:        "hg-1",
			OSType:      "LINUX",
			Hosts:       []string{"iqn.1998-01.com.vmware:host1"},
			TrialMode:   trial,
		})
		require.NoError(t, err)

		created.Name = "hg-2"
		_, err = _createHostGroup(ctx, mockStorage, &common.CreateHostGroupParams{
			AccountName: account.Name,
			Name:        "hg-2",
			OSType:      "LINUX",
			Hosts:       []string{"iqn.1998-01.com.vmware:host2"},
		})
		require.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("pool", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origValidate := ValidateCreatePoolParams
		ValidateCreatePoolParams = func(*commonparams.CreatePoolParams, log.Logger) error { return nil }
		defer func() { ValidateCreatePoolParams = origValidate }()

		mockStorage := new(database.MockStorage)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)
		mockStorage.On("CreatingPool", ctx, mock.Anything).Return(nil, errors.New("stop")).Twice()

		iops := int64(1000)
		first := &commonparams.CreatePoolParams{
			AccountName:  account.Name,
			Name:         "pool-1",
			Region:       "us-central1",
			ServiceLevel: ServiceLevelNameFLEX,
			SizeInBytes:  2199023255552,
			CustomPerformanceParams: &commonparams.CustomPerformanceParams{
				ThroughputMibps: 0,
				Iops:            &iops,
			},
			TrialMode: trial,
		}
		_, _, err := _createPool(ctx, mockStorage, nil, first)
		require.Error(t, err)

		second := *first
		second.Name = "pool-2"
		second.TrialMode = nil
		_, _, err = _createPool(ctx, mockStorage, nil, &second)
		require.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("kms_config", func(t *testing.T) {
		defer stubGetOrCreateAccount(account)()
		origCVPHost := cvp.CVP_HOST
		origForce := utils.ForceVCPKMSPathForTesting
		cvp.CVP_HOST = ""
		utils.ForceVCPKMSPathForTesting = true
		defer func() {
			cvp.CVP_HOST = origCVPHost
			utils.ForceVCPKMSPathForTesting = origForce
		}()

		mockStorage := new(database.MockStorage)
		expectTrialMetadataUpdate(t, mockStorage, ctx, account, start, end, nil)
		parseKeyFullPathResource = func(string) (*utils.ParsedKeyFullPathResource, error) {
			return &utils.ParsedKeyFullPathResource{CryptoKey: "k", ProjectID: "p", Location: "l", KeyRing: "r"}, nil
		}
		defer func() { parseKeyFullPathResource = utils.ParseKeyFullPathResource }()
		mockStorage.On("CreateKmsConfig", mock.Anything, mock.Anything).Return(nil, errors.New("stop")).Twice()

		keyPath := "projects/p/locations/l/keyRings/r/cryptoKeys/k"
		_, _, err := _createKmsConfig(ctx, mockStorage, mocks.NewClient(t), &common.CreateKmsConfigParams{
			AccountName: account.Name,
			KeyFullPath: keyPath,
			TrialMode:   trial,
		})
		require.Error(t, err)

		_, _, err = _createKmsConfig(ctx, mockStorage, mocks.NewClient(t), &common.CreateKmsConfigParams{
			AccountName: account.Name,
			KeyFullPath: keyPath + "-2",
			TrialMode:   nil,
		})
		require.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("active_directory", func(t *testing.T) {
		adAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 123, UUID: "acct-uuid"},
			Name:      "123",
		}
		setupADCreateTestEnv(t)
		mockStorage := database.NewMockStorage(t)
		mockTemporal := mocks.NewClient(t)
		mockStorage.EXPECT().GetAccount(mock.Anything, "123").Return(adAccount, nil).Maybe()
		expectTrialMetadataUpdate(t, mockStorage, ctx, adAccount, start, end, nil)

		mockStorage.EXPECT().ListActiveDirectories(mock.Anything, adAccount.ID).Return([]*datamodel.ActiveDirectory{}, nil).Twice()
		mockStorage.EXPECT().CreateActiveDirectory(mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: "ad-uuid-1"},
			AdName:    "ad-1",
			AccountId: adAccount.ID,
			State:     datamodel.LifeCycleStateCreating,
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				OrganizationalUnit: "CN=Computers",
			},
		}, nil)
		mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid-1"},
		}, nil)

		_, _, err := _createActiveDirectory(ctx, mockStorage, mockTemporal, &common.CreateActiveDirectoryParams{
			ResourceId:         "ad-1",
			AccountId:          adAccount.Name,
			LocationId:         "local",
			Username:           "admin@test.local",
			Password:           "SecurePass123!",
			Domain:             "test.local",
			DNS:                "10.0.0.1",
			NetBIOS:            "TEST",
			OrganizationalUnit: "CN=Computers",
			TrialMode:          trial,
		})
		require.NoError(t, err)

		mockStorage.EXPECT().CreateActiveDirectory(mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{UUID: "ad-uuid-2"},
			AdName:    "ad-2",
			AccountId: adAccount.ID,
			State:     datamodel.LifeCycleStateCreating,
			ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
				OrganizationalUnit: "CN=Computers",
			},
		}, nil)
		mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid-2"},
		}, nil)

		_, _, err = _createActiveDirectory(ctx, mockStorage, mockTemporal, &common.CreateActiveDirectoryParams{
			ResourceId:         "ad-2",
			AccountId:          adAccount.Name,
			LocationId:         "local",
			Username:           "admin@test.local",
			Password:           "SecurePass123!",
			Domain:             "test.local",
			DNS:                "10.0.0.1",
			NetBIOS:            "TEST",
			OrganizationalUnit: "CN=Computers",
		})
		require.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
}
