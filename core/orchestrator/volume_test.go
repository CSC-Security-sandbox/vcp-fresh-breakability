package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/net/context"
)

func TestGetVolume(t *testing.T) {
	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		volume, err := orch.GetVolume(ctx, "non-existent-uuid")
		assert.EqualError(tt, err, "volume not found")
		assert.Nil(tt, volume, "Expected nil volume")
	})
	t.Run("WhenVolumeExists", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		result, err := orch.GetVolume(ctx, "test-volume-uuid")
		assert.NoError(tt, err, "Failed to get volume")
		assert.Equal(tt, volume.Name, result.DisplayName)
		assert.Equal(tt, account.Name, result.AccountName)
	})
	t.Run("WhenVolumeExistsWithNoLif", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		assert.NoError(tt, err, "Failed to create node")

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		result, err := orch.GetVolume(ctx, "test-volume-uuid")

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif not found")
			assert.Nil(tt, result, "Expected nil volume")
		} else {
			t.Fatalf("Expected CustomError, got %v", err)
		}
	})
}

func TestCreateVolume(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := database.Storage(nil)

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			VendorID:     "test_vendor",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, se, temporal, params)
		assert.EqualError(tt, err, "account not found")
		assert.Nil(tt, volume)
	})
	t.Run("WhenValidateCreateVolumeParamFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := database.Storage(nil)

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			VendorID:     "test_vendor",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
			return errors.New("invalid volume params")
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, se, temporal, params)
		assert.EqualError(tt, err, "invalid volume params")
		assert.Nil(tt, volume)
	})
	t.Run("WhenGetPoolForCreateVolumeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			VendorID:     "test_vendor",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, store, temporal, params)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Nil(tt, volume, "Expected nil volume")
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Fatalf("Expected CustomError, got %v", err)
		}
	})
	t.Run("WhenGetSvmForCreateVolumeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			VendorID:     "test_vendor",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.EqualError(tt, err, "svm not found")
	})
	t.Run("WhenCreateVolumeSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "test-backup-vault-id",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
				PolicyEnforced:         &[]bool{true}[0],
			},
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.NotNil(tt, volume, "Expected nil volume")
		assert.NoError(tt, err, "error not found")
		assert.Equal(tt, volume.DisplayName, "test_volume")
		assert.Equal(tt, volume.AccountName, "test_account")
		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
		assert.Equal(tt, volume.PoolName, "test_pool")
		assert.Equal(tt, volume.VendorID, "")
		assert.Equal(tt, volume.CreationToken, "test-creation-token")
		assert.Equal(tt, volume.Description, "Some description")
		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
		assert.Equal(tt, volume.LifeCycleState, "CREATING")
		assert.Equal(tt, volume.LifeCycleStateDetails, "Creation in progress")
	})
	t.Run("WhenCreateVolumeAsyncFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return errors.New("workflow error")
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.EqualError(tt, err, "workflow error")
	})
	t.Run("WhenCreateVolumeFailsWithInvalidVendorID", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		// Create a PersistenceStore instance with the in-memory database
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/", // Intentionally invalid VendorID
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			VendorID:      "test_vendor",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Contains(tt, err.Error(), "invalid vendor ID")
	})
}

func Test_createVolume_WithSnapshotPolicy(t *testing.T) {
	tt := t
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	mockLogger := log.NewLogger()
	store, err := database.SetupStorageForTest(mockLogger)
	if err != nil {
		tt.Fatalf("Failed to create test storage: %v", err)
	}

	// Clear the in-memory database
	err = database.ClearInMemoryDB(store.DB())
	if err != nil {
		tt.Fatalf("Failed to clean up test storage: %v", err)
	}

	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
		Name:      "test_account",
	}
	err = store.DB().Create(account).Error
	if err != nil {
		tt.Fatalf("Failed to create account: %v", err)
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test_pool",
		AccountID: account.ID,
		State:     models.LifeCycleStateREADY,
		Network:   "somevpc",
		VendorID:  "/projects/project123/locations/location123/pools/pool123",
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone: "us-west1-a",
		},
	}
	err = store.DB().Create(pool).Error
	if err != nil {
		tt.Fatalf("Failed to create pool: %v", err)
	}

	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
		Name:      "test_svm",
		AccountID: account.ID,
		PoolID:    pool.ID,
		Pool:      pool,
		State:     models.LifeCycleStateREADY,
	}
	err = store.DB().Create(svm).Error
	if err != nil {
		tt.Fatalf("Failed to create svm: %v", err)
	}

	node1 := &datamodel.Node{
		BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
		Name:            "test_node1",
		AccountID:       account.ID,
		EndpointAddress: "12.12.12.12",
		PoolID:          pool.ID,
		State:           models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node1).Error
	assert.NoError(tt, err, "Failed to create node")

	node2 := &datamodel.Node{
		BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
		Name:            "test_node2",
		AccountID:       account.ID,
		EndpointAddress: "12.12.12.12",
		PoolID:          pool.ID,
		State:           models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node2).Error
	assert.NoError(tt, err, "Failed to create node")

	lif1 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
		Name:      "test_node1",
		AccountID: account.ID,
		IPAddress: "1.1.1.1",
		NodeID:    node1.ID,
	}
	err = store.DB().Create(lif1).Error
	assert.NoError(tt, err, "Failed to create lif1")

	lif2 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
		Name:      "test_node2",
		AccountID: account.ID,
		IPAddress: "1.1.1.2",
		NodeID:    node2.ID,
	}
	err = store.DB().Create(lif2).Error
	assert.NoError(tt, err, "Failed to create lif2")

	params := &common.CreateVolumeParams{
		AccountName:   "test_account",
		Region:        "test_region",
		Name:          "test_volume",
		VendorID:      "test_vendor",
		QuotaInBytes:  minQuotaInBytesVolume + 1,
		Protocols:     []string{"NFS"},
		Description:   "Some description",
		DisplayName:   "Some display name",
		PoolID:        "test-pool-uuid",
		CreationToken: "test-creation-token",
		SnapshotPolicy: &models.SnapshotPolicy{
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Count:           3,
					SnapmirrorLabel: "daily",
					Schedule: &models.Schedule{
						DaysOfMonth: []int{1, 15},
						DaysOfWeek:  []int{2, 3},
						Hours:       []int{4},
						Minutes:     []int{30},
					},
				},
			},
		},
	}

	dbAccount := &datamodel.Account{
		BaseModel: datamodel.BaseModel{
			UUID: "test-uuid",
			ID:   account.ID,
		},
		Name: "test_account",
	}
	getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
		return dbAccount, nil
	}
	validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, accountID int64) error {
		return nil
	}
	defer func() {
		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
	}()

	temporal := workflowEngineMock.NewMockTemporalTestClient(t)

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	volume, _, err := createVolume(ctx, store, temporal, params)
	assert.NoError(tt, err)
	assert.NotNil(tt, volume)
	assert.NotNil(tt, volume.SnapshotPolicy)
	assert.True(tt, volume.SnapshotPolicy.IsEnabled)
	assert.Len(tt, volume.SnapshotPolicy.Schedules, 1)
	assert.Equal(tt, int64(3), volume.SnapshotPolicy.Schedules[0].Count)
	assert.Equal(tt, "daily", volume.SnapshotPolicy.Schedules[0].SnapmirrorLabel)
	assert.Equal(tt, []int{1, 15}, volume.SnapshotPolicy.Schedules[0].Schedule.DaysOfMonth)
	assert.Equal(tt, []int{2, 3}, volume.SnapshotPolicy.Schedules[0].Schedule.DaysOfWeek)
	assert.Equal(tt, []int{4}, volume.SnapshotPolicy.Schedules[0].Schedule.Hours)
	assert.Equal(tt, []int{30}, volume.SnapshotPolicy.Schedules[0].Schedule.Minutes)
}

func TestOrchestrator_CreateVolume(t *testing.T) {
	// Arrange
	mockStorage := &database.MockStorage{}
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orch := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	// Override createVolume for isolation
	createVolume = func(ctx context.Context, se database.Storage, te client.Client, params *common.CreateVolumeParams) (*models.Volume, string, error) {
		return &models.Volume{DisplayName: "vol"}, "job-id", nil
	}
	defer func() { createVolume = _createVolume }()

	params := &common.CreateVolumeParams{Name: "vol"}

	// Act
	vol, jobID, err := orch.CreateVolume(context.Background(), params)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "vol", vol.DisplayName)
	assert.Equal(t, "job-id", jobID)
}

func TestDeleteVolume(t *testing.T) {
	t.Run("WhenGetVolumeNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		_, _, err = orch.DeleteVolume(ctx, "non-existent-uuid")
		assert.EqualError(tt, err, "volume not found")
	})
	t.Run("WhenVolumeExistsAndSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		volumeResp, _, err := deleteVolume(ctx, store, temporal, "test-volume-uuid")
		assert.NoError(tt, err, "Failed to get volume")
		assert.Equal(tt, volume.Name, volumeResp.DisplayName)
		assert.Equal(tt, account.Name, volumeResp.AccountName)
		assert.Equal(tt, volumeResp.LifeCycleState, models.LifeCycleStateDeleting)
		assert.Equal(tt, volumeResp.LifeCycleStateDetails, models.LifeCycleStateDeletingDetails)
	})
	t.Run("WhenVolumeAlreadyDeletingVolume", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:         "test_volume",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateDeleting,
			StateDetails: models.LifeCycleStateDeletingDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volumeResp, _, err := deleteVolume(ctx, store, temporal, "test-volume-uuid")
		assert.Contains(tt, err.Error(), "volume is in transition state and cannot be deleted, state: DELETING")
		assert.Nil(tt, volumeResp, "Expected nil volume")
	})
	t.Run("WhenVolumeAlreadyDeletingVolumeAndAsyncFlowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:         "test_volume",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return errors.New("some error")
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		volumeResp, _, err := deleteVolume(ctx, store, temporal, "test-volume-uuid")
		assert.EqualError(tt, err, "some error")
		assert.Nil(tt, volumeResp, "Expected nil volume")
	})
}

func TestGetMultipleVolumes(t *testing.T) {
	t.Run("WhenGetMultipleVolumesSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "a"},
			Name:         "a",
			AccountID:    account.ID,
			Account:      account,
			Pool:         pool,
			PoolID:       pool.ID,
			SvmID:        svm.ID,
			Svm:          svm,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		volume = &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "b"},
			Name:         "b",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		orch := Orchestrator{
			storage: store,
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
		}()

		volumeResp, err := orch.GetMultipleVolumes(ctx, []string{"a", "b"}, account.Name)
		assert.Nil(tt, err, "some error")
		assert.Len(tt, volumeResp, 2)
	})
	t.Run("WhenGetMultipleVolumesGetIPAddressForVolumeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "a"},
			Name:         "a",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		volume = &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "b"},
			Name:         "b",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		orch := Orchestrator{
			storage: store,
		}

		getIPAddressForVolume = func(ctx context.Context, se database.Storage, volume *datamodel.Volume) (string, error) {
			return "", errors.New("some error")
		}
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			getIPAddressForVolume = _getIPAddressForVolume
		}()

		volumeResp, err := orch.GetMultipleVolumes(ctx, []string{"a", "b"}, account.Name)
		assert.EqualError(tt, err, "some error")
		assert.Len(tt, volumeResp, 0)
	})
}

func TestValidateCreateVolumeParams(t *testing.T) {
	t.Run("WhenValidateCreateVolumeParamsSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
			Network:   "somevpc",
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "a"},
			Name:         "a",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			SvmID:        svm.ID,
			Svm:          svm,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node2.ID,
		}
		err = store.DB().Create(lif2).Error
		assert.NoError(tt, err, "Failed to create node")

		volume = &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "b"},
			Name:         "b",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesPool + 1,
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "test-backup-vault-id",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
				PolicyEnforced:         &[]bool{true}[0],
			},
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.Nil(tt, err, "some error")
	})
	t.Run("WhenPoolNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       "non-existent-pool",
			QuotaInBytes: minQuotaInBytesPool + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "pool not found")
		} else {
			tt.Fatalf("Expected a CustomError, got: %v", err)
		}
	})
	t.Run("WhenPoolStateNotReady", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateAvailable,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesPool + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "pool is not ready")
	})
	t.Run("WhenQuotaIsTooSmall", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume - 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "volume size must be between 100 GiB and 102,400 GiB.")
	})
	t.Run("WhenPoolNetworkIsNotSameAsVolume", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			Network:      "dummy-network",
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "pool network and volume network should be same")
	})
	t.Run("WhenSvmforPoolIdIsNotThere", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "svm not found")
	})
	t.Run("WhenSvmforPoolIdNotInRightState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateDeleted,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "svm is not ready")
	})
	t.Run("WhenCountOfNodes<2", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "a"},
			Name:         "a",
			AccountID:    account.ID,
			Pool:         pool,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "required count of nodes not found")
	})
	t.Run("WhenNodesNotInReadyState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateDeleted,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "node is not ready")
	})
	t.Run("WhenGetLifForNodeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif not found")
		} else {
			tt.Fatalf("Expected a CustomError, got: %v", err)
		}
	})
	t.Run("WhenGetLifNameNotAvailable", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node2.ID,
		}
		err = store.DB().Create(lif2).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "lif for node test_node1 is not available")
	})
	t.Run("WhenBPAvailableWithNoHG", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "name",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node2.ID,
		}
		err = store.DB().Create(lif2).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			BlockProperties: &models.BlockProperties{
				OSType: "linux",
			},
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.Nil(tt, err)
	})
	t.Run("WhenBPAvailableWithInvalidHG", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "name",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node2.ID,
		}
		err = store.DB().Create(lif2).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			BlockProperties: &models.BlockProperties{
				OSType:         "linux",
				HostGroupUUIDs: []string{"1"},
			},
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "could not find some of the host groups, please check the hostgroup details and try with valid host group names.")
	})
	t.Run("WhenBPAvailableWithInvalidHGState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "name",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node2.ID,
		}
		err = store.DB().Create(lif2).Error
		assert.NoError(tt, err, "Failed to create node")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "testhg"},
			Name:      "testhg",
			AccountID: account.ID,
			State:     models.LifeCycleStateDeleted,
		}
		err = store.DB().Create(hg).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			BlockProperties: &models.BlockProperties{
				OSType:         "linux",
				HostGroupUUIDs: []string{"testhg"},
			},
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "host group testhg is not available")
	})
	t.Run("WhenBPAvailableWithRightState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:            "test_node1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		assert.NoError(tt, err, "Failed to create node")

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:            "test_node2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		assert.NoError(tt, err, "Failed to create node")

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "name",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node2.ID,
		}
		err = store.DB().Create(lif2).Error
		assert.NoError(tt, err, "Failed to create node")

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid2"},
			Name:      "testhg",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(hg).Error
		assert.NoError(tt, err, "Failed to create node")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			BlockProperties: &models.BlockProperties{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-volume-uuid2"},
			},
		}

		err = validateCreateVolumeParams(ctx, store, params, account.ID)
		assert.Nil(tt, err)
	})
}

func TestUpdateVolume(t *testing.T) {
	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid"}

		se.On("GetVolume", ctx, "vid").Return(nil, errors.New("volume not found"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.EqualError(tt, err, "volume not found")
		assert.Nil(tt, volume)
	})

	t.Run("WhenValidateUpdateVolumeParamsFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 10}
		dbVolume := &datamodel.Volume{SizeInBytes: 100, State: "READY"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.EqualError(tt, err, "volume size cannot be reduced")
		assert.Nil(tt, volume)
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol", State: "READY"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("job error"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.EqualError(tt, err, "job error")
		assert.Nil(tt, volume)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol", State: "READY"}
		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("update state error"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.EqualError(tt, err, "update state error")
		assert.Nil(tt, volume)
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol", State: "READY"}
		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error")).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.EqualError(tt, err, "workflow error")
		assert.Nil(tt, volume)
	})

	t.Run("WhenUpdateVolumeSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200, Name: "vol", SnapshotPolicy: &models.SnapshotPolicy{
			IsEnabled: false,
			Schedules: nil,
		},
			DataProtection: &models.DataProtection{
				BackupVaultID: "vault-1",
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: 100,
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-2",
			},
			State: "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVault", ctx, "vault-2").Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeSuccessWithNoBackupVaultIDInDB", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200, Name: "vol", DataProtection: &models.DataProtection{
			BackupVaultID: "vault-1",
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: 100,
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "",
			},
			State: "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeSuccessWithDetachBackupVault", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200, Name: "vol", DataProtection: &models.DataProtection{
			BackupVaultID: "",
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: 100,
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-1",
			},
			State: "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVault", ctx, "vault-1").Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeGetBackupsByBackupVaultErrors", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200, Name: "vol", DataProtection: &models.DataProtection{
			BackupVaultID: "",
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: 100,
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool"},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-1",
			},
			State: "READY",
		}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVault", ctx, "vault-1").Return(nil, errors.New("no backups found"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeGetBackupsByBackupVaultErrors", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200, Name: "vol", DataProtection: &models.DataProtection{
			BackupVaultID: "",
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: 100,
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool"},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-1",
			},
			State: "READY",
		}

		backups := []*datamodel.Backup{
			{
				BaseModel: datamodel.BaseModel{UUID: "backup-1"},
				Name:      "backup1",
			},
		}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVault", ctx, "vault-1").Return(backups, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeSuccessWithAttachBackupVault", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200, Name: "vol", DataProtection: &models.DataProtection{
			BackupVaultID: "",
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: 100,
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: nil,
			State:          "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})

	t.Run("WhenUpdateVolumeFailsIfVolumeInTransitioningState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: 200}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol", State: models.LifeCycleStateUpdating}

		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param)
		assert.Contains(tt, err.Error(), "volume is not in a valid state for update")
		assert.Nil(tt, volume)
	})
}

func TestOrchestrator_UpdateVolume(t *testing.T) {
	// Arrange
	mockStorage := &database.MockStorage{}
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orch := &Orchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	// override updateVolume for isolation
	updateVolume = func(ctx context.Context, se database.Storage, te client.Client, param *common.UpdateVolumeParams) (*models.Volume, string, error) {
		return &models.Volume{DisplayName: "vol"}, "job-id", nil
	}
	defer func() { updateVolume = _updateVolume }()

	param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid"}

	// Act
	vol, jobID, err := orch.UpdateVolume(context.Background(), param)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "vol", vol.DisplayName)
	assert.Equal(t, "job-id", jobID)
}

func TestGetVolumeCount(t *testing.T) {
	t.Run("WhenStorageReturnsCount", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockOrchestrator := &Orchestrator{storage: mockStorage}

		projectNumber := "test-project"
		expectedCount := int64(5)

		mockStorage.On("GetVolumeCount", ctx, projectNumber).Return(expectedCount, nil)

		actualCount, err := mockOrchestrator.GetVolumeCount(ctx, projectNumber)
		assert.Nil(tt, err)
		assert.Equal(tt, expectedCount, actualCount)
	})

	t.Run("WhenStorageReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockOrchestrator := &Orchestrator{storage: mockStorage}

		projectNumber := "test-project"
		expectedError := errors.New("database error")

		mockStorage.On("GetVolumeCount", ctx, projectNumber).Return(int64(0), expectedError)

		actualCount, err := mockOrchestrator.GetVolumeCount(ctx, projectNumber)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError, err)
		assert.Equal(tt, int64(0), actualCount)
	})
}

func TestListVolumes(t *testing.T) {
	t.Run("WhenAccountExistsAndHasVolumes", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockOrchestrator := &Orchestrator{storage: mockStorage}

		projectNumber := "test-project"

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
		}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		conditions := [][]interface{}{{"account_id = ?", int64(1)}}

		volumeObj := &datamodel.Volume{
			Name:        "vol1",
			Account:     account,
			AccountID:   account.ID,
			SizeInBytes: int64(1024),
			Description: "test",
			PoolID:      1,
			SvmID:       1,
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"}, PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			VolumeAttributes: &datamodel.VolumeAttributes{
				CreationToken:    "token1",
				Protocols:        []string{"iscsi"},
				VendorSubnetID:   "network",
				IsDataProtection: false,
			},
		}

		mockStorage.On("ListVolumes", ctx, conditions).Return([]*datamodel.Volume{volumeObj}, nil)

		volumes, err := mockOrchestrator.ListVolumes(ctx, projectNumber)
		assert.NoError(tt, err)
		assert.Len(tt, volumes, 1)
		assert.Equal(tt, "vol1", volumes[0].DisplayName)
		getAccountWithName = _getAccountWithName
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := new(database.MockStorage)
		mockOrchestrator := &Orchestrator{storage: mockStorage}
		getAccountWithName = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("account not found")
		}
		volumes, err := mockOrchestrator.ListVolumes(ctx, "non-existent-account")
		assert.Error(tt, err, "Expected error for non-existent account")
		assert.Nil(tt, volumes, "Expected nil volumes")
		getAccountWithName = _getAccountWithName
	})

	t.Run("WhenAccountExistsButNoVolumes", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(tt, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to clear in-memory database")

		account := &datamodel.Account{
			Name: "test-account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(tt, err, "Failed to create account")

		orch := Orchestrator{storage: store}

		volumes, err := orch.ListVolumes(ctx, account.Name)
		assert.NoError(tt, err, "Failed to list volumes")
		assert.Len(tt, volumes, 0)
	})
}

func TestConvertToDBSnapshotPolicySchedule(t *testing.T) {
	t.Run("SingleSchedule_MapsFieldsCorrectly", func(tt *testing.T) {
		schedule := &models.SnapshotPolicySchedule{
			Count:           5,
			SnapmirrorLabel: "label1",
			Schedule: &models.Schedule{
				DaysOfMonth: []int{1, 15},
				DaysOfWeek:  []int{2, 3},
				Hours:       []int{4},
				Minutes:     []int{30},
			},
		}
		result := convertToDBSnapshotPolicySchedule([]*models.SnapshotPolicySchedule{schedule})
		assert.Len(tt, result, 1)
		dbSched := result[0]
		assert.Equal(tt, int64(5), dbSched.Count)
		assert.Equal(tt, "label1", dbSched.SnapmirrorLabel)
		assert.Equal(tt, []int{1, 15}, dbSched.DaysOfMonth)
		assert.Equal(tt, []int{2, 3}, dbSched.DaysOfWeek)
		assert.Equal(tt, []int{4}, dbSched.Hours)
		assert.Equal(tt, []int{30}, dbSched.Minutes)
	})

	t.Run("MultipleSchedules_MapsAll", func(tt *testing.T) {
		s1 := &models.SnapshotPolicySchedule{
			Count:           1,
			SnapmirrorLabel: "l1",
			Schedule:        &models.Schedule{DaysOfMonth: []int{1}},
		}
		s2 := &models.SnapshotPolicySchedule{
			Count:           2,
			SnapmirrorLabel: "l2",
			Schedule:        &models.Schedule{DaysOfWeek: []int{2}},
		}
		result := convertToDBSnapshotPolicySchedule([]*models.SnapshotPolicySchedule{s1, s2})
		assert.Len(tt, result, 2)
		assert.Equal(tt, int64(1), result[0].Count)
		assert.Equal(tt, "l1", result[0].SnapmirrorLabel)
		assert.Equal(tt, []int{1}, result[0].DaysOfMonth)
		assert.Equal(tt, int64(2), result[1].Count)
		assert.Equal(tt, "l2", result[1].SnapmirrorLabel)
		assert.Equal(tt, []int{2}, result[1].DaysOfWeek)
	})
}
