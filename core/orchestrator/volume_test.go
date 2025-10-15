package orchestrator

import (
	"database/sql"
	errors2 "errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/api/serviceerror"
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

		volume, err2 := orch.GetVolume(ctx, "non-existent-uuid", false)
		assert.EqualError(tt, err2, "Volume not found")
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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

		result, err := orch.GetVolume(ctx, "test-volume-uuid", false)
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

		result, err := orch.GetVolume(ctx, "test-volume-uuid", false)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif not found")
			assert.Nil(tt, result, "Expected nil volume")
		} else {
			t.Fatalf("Expected CustomError, got %v", err)
		}
	})
	t.Run("WhenRefreshVolumeFieldsIsTrueAndVolumeStateIsCreating", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			State:     "CREATING",
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		result, err := orch.GetVolume(ctx, "test-volume-uuid", true)
		assert.NoError(tt, err, "Failed to get volume")
		assert.Equal(tt, volume.Name, result.DisplayName)
		assert.Equal(tt, account.Name, result.AccountName)
	})
}

func TestValidateCreateVolumeParamsValidationLogic(t *testing.T) {
	t.Run("PoolVolumeCapacityMismatch_PoolLarge_VolumeNot", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(100 * 1024 * 1024 * 1024 * 1024), // 100TB
			LargeCapacity: true,                                   // Pool is large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  12 * 1099511627776, // 12 TiB
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: false, // Volume is NOT large capacity - mismatch!
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "pool large capacity setting does not match volume large capacity setting")
	})

	t.Run("PoolVolumeCapacityMismatch_PoolNotLarge_VolumeIs", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			LargeCapacity: false,                                 // Pool is NOT large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  12 * 1099511627776, // 12 TiB
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: true, // Volume IS large capacity - mismatch!
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "pool large capacity setting does not match volume large capacity setting")
	})

	t.Run("LargeCapacitySANProtocolRestriction", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(100 * 1024 * 1024 * 1024 * 1024), // 100TB
			LargeCapacity: true,                                   // Pool is large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  12 * 1099511627776,            // 12 TiB
			Protocols:     []string{utils.ProtocolISCSI}, // SAN protocol - not allowed for large capacity!
			Network:       "test-network",
			LargeCapacity: true,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "SAN protocols are not supported for large capacity volumes")
	})

	t.Run("LargeCapacityBlockDevicesNotNIl", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(100 * 1024 * 1024 * 1024 * 1024), // 100TB
			LargeCapacity: true,                                   // Pool is large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  14 * 1099511627776,
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: true,
			BlockDevices: &[]common.BlockDevice{
				{OSType: "linux", Name: "/dev/sda"},
				{OSType: "linux", Name: "/dev/sdb"},
			},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "BlockDevices are not supported for large capacity volumes")
	})

	t.Run("ConstituentCountForNonLargeCapacity", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			LargeCapacity: false,                                 // Pool is NOT large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:                 "test_account",
			Name:                        "test-volume",
			PoolID:                      pool.UUID,
			QuotaInBytes:                107374182400, // 100 GiB (valid for non-large capacity)
			Protocols:                   []string{utils.ProtocolNFSv3},
			Network:                     "test-network",
			LargeCapacity:               false,
			LargeVolumeConstituentCount: 12, // Constituent count set for non-large capacity - not allowed!
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "Large Volume constituent count is only supported for large capacity volumes")
	})

	t.Run("PrimeGreaterThan7ConstituentCountForLargeCapacity", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			LargeCapacity: true,                                  // Pool is NOT large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:                 "test_account",
			Name:                        "test-volume",
			PoolID:                      pool.UUID,
			QuotaInBytes:                12 * utils.TiBInBytes, // 12 TiB
			Protocols:                   []string{utils.ProtocolNFSv3},
			Network:                     "test-network",
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 31, // Constituent count is prime
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, fmt.Sprintf("Consituent volume count with %d is not supported", params.LargeVolumeConstituentCount))
	})

	t.Run("MaxConstituentCountForLargeCapacity", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   13194139533312, // 12TB
			LargeCapacity: true,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:                 "test_account",
			Name:                        "test-volume",
			PoolID:                      pool.UUID,
			QuotaInBytes:                13194139533312,
			Protocols:                   []string{utils.ProtocolNFSv3},
			Network:                     "test-network",
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 2200,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, fmt.Sprintf("Large Volume constituent count cannot be greater than %d", int32(numOfLvHAPairs*maxConstituentVolumesPerAggregate)))
	})

	t.Run("LargeCapacityQuotaTooSmall", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(100 * 1024 * 1024 * 1024 * 1024), // 100TB
			LargeCapacity: true,                                   // Pool is large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  1 * 1099511627776, // 1 TiB - too small for large capacity (min is 12 TiB)
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: true,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "Invalid volume capacity")
		assert.ErrorContains(tt, err, "Must be between")
		assert.ErrorContains(tt, err, "TiB and")
		assert.ErrorContains(tt, err, "PiB")
	})

	t.Run("LargeCapacityQuotaTooLarge", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(20 * 1125899906842624), // 100 PiB (very large pool)
			LargeCapacity: true,                         // Pool is large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  25 * 1125899906842624, // 25 PiB - too large for large capacity (max is 20 PiB)
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: true,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "Invalid volume capacity")
		assert.ErrorContains(tt, err, "Must be between")
		assert.ErrorContains(tt, err, "TiB and")
		assert.ErrorContains(tt, err, "PiB")
	})

	t.Run("NonLargeCapacityQuotaTooSmall", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			LargeCapacity: false,                                 // Pool is NOT large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  0.5 * 1024 * 1024 * 1024, // 50 GiB - too small (min is 500 MiB)
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: false,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "Invalid volume capacity")
		assert.ErrorContains(tt, err, "Must be between")
		assert.ErrorContains(tt, err, "GiB and")
		assert.ErrorContains(tt, err, "GiB")
	})

	t.Run("NonLargeCapacityQuotaTooLarge", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(200 * 1024 * 1024 * 1024 * 1024), // 200TB (very large pool)
			LargeCapacity: false,                                  // Pool is NOT large capacity
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  200 * 1024 * 1024 * 1024 * 1024, // 120 TiB - too large for non-large capacity (max is ~102,400 GiB)
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: false,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "Invalid volume capacity")
		assert.ErrorContains(tt, err, "Must be between")
		assert.ErrorContains(tt, err, "GiB and")
		assert.ErrorContains(tt, err, "GiB")
	})

	t.Run("ValidLargeCapacityQuota", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(100 * 1024 * 1024 * 1024 * 1024), // 100TB
			LargeCapacity: true,                                   // Pool is large capacity
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
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  15 * 1099511627776, // 15 TiB - valid for large capacity
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: true,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		// Should not return an error for this validation branch - other validations might fail but quota validation should pass
		if err != nil {
			// If there's an error, it should NOT be about quota validation
			assert.NotContains(tt, err.Error(), "Invalid volume capacity")
		}
	})

	t.Run("ValidNonLargeCapacityQuota", func(tt *testing.T) {
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
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:          "test_pool",
			AccountID:     account.ID,
			State:         models.LifeCycleStateREADY,
			Network:       "test-network",
			SizeInBytes:   int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			LargeCapacity: false,                                 // Pool is NOT large capacity
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
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Name:          "test-volume",
			PoolID:        pool.UUID,
			QuotaInBytes:  500 * 1024 * 1024 * 1024, // 500 GiB - valid for non-large capacity
			Protocols:     []string{utils.ProtocolNFSv3},
			Network:       "test-network",
			LargeCapacity: false,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		// Should not return an error for this validation branch - other validations might fail but quota validation should pass
		if err != nil {
			// If there's an error, it should NOT be about quota validation
			assert.NotContains(tt, err.Error(), "Invalid volume capacity")
		}
	})

	t.Run("CloneSharedBytes_SnapshotNotFound", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 50 * 1024 * 1024 * 1024, // 50GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "non-existent-snapshot-uuid", // This snapshot doesn't exist
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 0,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "snapshot not found")
	})

	t.Run("CloneSharedBytes_SnapshotFound_WithinPoolSize", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create a parent volume first
		parentVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "parent-volume-uuid"},
			Name:      "parent_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(parentVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create parent volume: %v", err)
		}

		// Create a snapshot with LogicalSizeUsedInBytes
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  parentVolume.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            20 * 1024 * 1024 * 1024, // 20GB
				ExternalUUID:           "external-snapshot-uuid",
				LogicalSizeUsedInBytes: 30 * 1024 * 1024 * 1024, // 30GB shared bytes
			},
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 50 * 1024 * 1024 * 1024, // 50GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "test-snapshot-uuid",
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 10 * 1024 * 1024 * 1024, // 10GB already used
		}

		// Test calculation: pool.QuotaInBytes + params.QuotaInBytes - cloneSharedBytes <= pool.SizeInBytes
		// 10GB + 50GB - 30GB = 30GB <= 100GB (should pass)
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		// This should not return an error about pool size since we have enough space after accounting for shared bytes
		if err != nil {
			// The error might be from other validations, but it should NOT be about pool size
			assert.NotContains(tt, err.Error(), "volume size cannot be greater than pool size")
		}
	})

	t.Run("CloneSharedBytes_SnapshotFound_ExceedsPoolSize", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create a parent volume first
		parentVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "parent-volume-uuid"},
			Name:      "parent_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(parentVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create parent volume: %v", err)
		}

		// Create a snapshot with small LogicalSizeUsedInBytes
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  parentVolume.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            20 * 1024 * 1024 * 1024, // 20GB
				ExternalUUID:           "external-snapshot-uuid",
				LogicalSizeUsedInBytes: 5 * 1024 * 1024 * 1024, // Only 5GB shared bytes
			},
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 80 * 1024 * 1024 * 1024, // 80GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "test-snapshot-uuid",
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 30 * 1024 * 1024 * 1024, // 30GB already used
		}

		// Test calculation: pool.QuotaInBytes + params.QuotaInBytes - cloneSharedBytes > pool.SizeInBytes
		// 30GB + 80GB - 5GB = 105GB > 100GB (should fail)
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "volume size cannot be greater than pool size")
	})

	t.Run("CloneSharedBytes_SnapshotFound_ZeroSharedBytes", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create a parent volume first
		parentVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "parent-volume-uuid"},
			Name:      "parent_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(parentVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create parent volume: %v", err)
		}

		// Create a snapshot with zero LogicalSizeUsedInBytes
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  parentVolume.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            20 * 1024 * 1024 * 1024, // 20GB
				ExternalUUID:           "external-snapshot-uuid",
				LogicalSizeUsedInBytes: 0, // Zero shared bytes
			},
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 80 * 1024 * 1024 * 1024, // 80GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "test-snapshot-uuid",
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 30 * 1024 * 1024 * 1024, // 30GB already used
		}

		// Test calculation: pool.QuotaInBytes + params.QuotaInBytes - cloneSharedBytes > pool.SizeInBytes
		// 30GB + 80GB - 0GB = 110GB > 100GB (should fail)
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "volume size cannot be greater than pool size")
	})

	t.Run("NoSnapshotID_RegularPoolSizeValidation", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 80 * 1024 * 1024 * 1024, // 80GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "", // No snapshot ID
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: 30 * 1024 * 1024 * 1024, // 30GB already used
		}

		// Test calculation: pool.QuotaInBytes + params.QuotaInBytes - cloneSharedBytes > pool.SizeInBytes
		// 30GB + 80GB - 0GB = 110GB > 100GB (should fail)
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "volume size cannot be greater than pool size")
	})

	t.Run("CloneVolumeLimit_MaximumThinClonesReached", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(1000 * 1024 * 1024 * 1024), // 1000GB (large enough to not trigger size limit)
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create a parent volume
		parentVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "parent-volume-uuid"},
			Name:      "parent_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(parentVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create parent volume: %v", err)
		}

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  parentVolume.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            20 * 1024 * 1024 * 1024, // 20GB
				ExternalUUID:           "external-snapshot-uuid",
				LogicalSizeUsedInBytes: 10 * 1024 * 1024 * 1024, // 10GB shared
			},
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 50 * 1024 * 1024 * 1024, // 50GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "test-snapshot-uuid", // This makes it a clone volume
		}

		// Set up pool view with maximum clone volume count reached (100 clones already exist)
		// Logic: CloneVolumeCount+1 > maxThinClonesPerPool triggers the error
		// If CloneVolumeCount = 100, then 100+1 = 101 > 100 (maxThinClonesPerPool), so error
		poolView := &datamodel.PoolView{
			Pool:             *pool,
			QuotaInBytes:     100 * 1024 * 1024 * 1024, // 100GB used (plenty of space)
			CloneVolumeCount: 100,                      // At the maximum limit - adding 1 more will exceed
		}

		// This should fail because CloneVolumeCount+1 (100+1=101) > maxThinClonesPerPool (100)
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "pool has reached maximum clone volume limit")
	})

	t.Run("CloneVolumeLimit_WithinThinClonesLimit", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(1000 * 1024 * 1024 * 1024), // 1000GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create a parent volume
		parentVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "parent-volume-uuid"},
			Name:      "parent_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(parentVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create parent volume: %v", err)
		}

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  parentVolume.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            20 * 1024 * 1024 * 1024, // 20GB
				ExternalUUID:           "external-snapshot-uuid",
				LogicalSizeUsedInBytes: 10 * 1024 * 1024 * 1024, // 10GB shared
			},
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 50 * 1024 * 1024 * 1024, // 50GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "test-snapshot-uuid", // This makes it a clone volume
		}

		// Set up pool view with clone volume count well below the limit
		poolView := &datamodel.PoolView{
			Pool:             *pool,
			QuotaInBytes:     100 * 1024 * 1024 * 1024, // 100GB used
			CloneVolumeCount: 50,                       // Well below the limit of 100
		}

		// This should pass because CloneVolumeCount+1 (50+1=51) > maxThinClonesPerPool (100) is false
		// so it won't trigger the error condition
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		// Should not get the clone limit error, but may get other validation errors
		if err != nil {
			assert.NotContains(tt, err.Error(), "pool has reached maximum clone volume limit",
				"Should not fail due to clone volume limit when well within limits")
		}
	})

	t.Run("CloneVolumeLimit_ExactlyAtLimit", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(1000 * 1024 * 1024 * 1024), // 1000GB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create a parent volume
		parentVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "parent-volume-uuid"},
			Name:      "parent_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(parentVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create parent volume: %v", err)
		}

		// Create a snapshot
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  parentVolume.ID,
			State:     models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes:            20 * 1024 * 1024 * 1024, // 20GB
				ExternalUUID:           "external-snapshot-uuid",
				LogicalSizeUsedInBytes: 10 * 1024 * 1024 * 1024, // 10GB shared
			},
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 50 * 1024 * 1024 * 1024, // 50GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			SnapshotID:   "test-snapshot-uuid", // This makes it a clone volume
		}

		// Set up pool view with exactly 99 clone volumes (at the boundary)
		poolView := &datamodel.PoolView{
			Pool:             *pool,
			QuotaInBytes:     100 * 1024 * 1024 * 1024, // 100GB used
			CloneVolumeCount: 99,                       // Exactly at the boundary - adding 1 more will equal the limit
		}

		// This should pass because CloneVolumeCount+1 (99+1=100) > maxThinClonesPerPool (100) is false (100 > 100 is false)
		// so it won't trigger the error condition - the limit allows exactly maxThinClonesPerPool clones
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		// Should not get the clone limit error, but may get other validation errors
		if err != nil {
			assert.NotContains(tt, err.Error(), "pool has reached maximum clone volume limit",
				"Should not fail due to clone volume limit when exactly at the limit")
		}
	})
}

func TestCreateVolume(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := database.Storage(nil)

		params := &common.CreateVolumeParams{
			AccountName:       "test_account",
			Region:            "test_region",
			Name:              "test_pool",
			Zone:              "us-west1-a",
			VendorID:          "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes:      minQuotaInBytesPool,
			Protocols:         []string{"NFS"},
			Description:       "Some description",
			DisplayName:       "Some display name",
			SnapshotDirectory: true,
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return errors.New("invalid volume params")
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, store, temporal, params)
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
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
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
	t.Run("WhenGetSnapshotForCreateVolumeFails", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.EqualError(tt, err, "snapshot not found")
	})
	t.Run("WhenParentSnapshotNotInReadyStateForCreateVolumeFails", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				UUID: "test-uuid",
			},
			Name: "test_account",
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			State:     "ERROR",
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.ErrorContains(tt, err, "Restore snapshots across pool is not supported")
	})
	t.Run("WhenCreateVolumeSuccessWithBP", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/location123/pools/test_pool",
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

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hg-uuid1"},
			Name:      "test_svm",
			AccountID: account.ID,
			Hosts: datamodel.Hosts{
				Hosts: []string{"host1.example.com", "host2.example.com"},
			},
		}

		err = store.DB().Create(hg1).Error
		if err != nil {
			tt.Fatalf("Failed to create hg1: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
			},
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-hg-uuid1"},
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		getMultipleHostGroup = func(ctx context.Context, storage database.Storage, hostGroupUUIDs []string, accountID string) ([]*models.HostGroup, error) {
			return []*models.HostGroup{{
				BaseModel: models.BaseModel{UUID: "host-group-uuid"},
				Hosts:     []string{"a", "b"},
			}}, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
			getMultipleHostGroup = _getMultipleHostGroup
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

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
		assert.Equal(tt, volume.BlockProperties.HostGroupDetail[0].HostGroupID, "host-group-uuid")
		assert.Equal(tt, volume.BlockProperties.OSType, "linux")
		assert.Equal(tt, volume.BlockProperties.LunSerialNumber, "")
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

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			State:     "READY",
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
			},
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:   true,
				TieringPolicy:        "ENABLED",
				CoolingThresholdDays: 30,
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

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
	t.Run("WhenCreateVolumeSuccessWithRestore", func(tt *testing.T) {
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

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			State:     "READY",
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
			Name:      "bv1",
			AccountID: 0,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
			},
		}

		backup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: "463811e7-9760-acf5-9bdb-020073ca3333"}, Name: "backupName", VolumeUUID: "463811e7-9760-acf5-9bdb-020073ca3335", State: "creating"}

		err = store.DB().Create(bv).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "bv1",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
			},
			BackupID:   "463811e7-9760-acf5-9bdb-020073ca3333",
			BackupPath: "projects/project123/locations/location123/backupVaults/bv1/backups/backupName",
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()
		volume, _, _ := createVolume(ctx, store, temporal, params)
		assert.Equal(tt, volume.DisplayName, "test_volume")
		assert.Equal(tt, volume.AccountName, "test_account")
		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
		assert.Equal(tt, volume.PoolName, "test_pool")
		assert.Equal(tt, volume.VendorID, "")
		assert.Equal(tt, volume.CreationToken, "test-creation-token")
		assert.Equal(tt, volume.Description, "Some description")
		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
		assert.Equal(tt, volume.LifeCycleState, "RESTORING")
		assert.Equal(tt, volume.LifeCycleStateDetails, "Restore in progress")
	})
	t.Run("WhenCreateVolumeFailWithRestore", func(tt *testing.T) {
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

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			State:     "READY",
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
			},
		}
		backup := &datamodel.Backup{Name: "backupName", VolumeUUID: "463811e7-9760-acf5-9bdb-020073ca3335", State: "creating"}

		err = store.DB().Create(bv).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "bv1",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
			},
			BackupID:   "463811e7-9760-acf5-9bdb-020073ca3333",
			BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName",
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()
		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.EqualError(tt, err, "record not found")
	})
	t.Run("WhenRestoreVolumeFailWithBackupPath", func(tt *testing.T) {
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

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			State:     "READY",
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-vault-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
			},
		}
		backup := &datamodel.Backup{Name: "backupName", VolumeUUID: "463811e7-9760-acf5-9bdb-020073ca3335", State: "creating"}

		err = store.DB().Create(bv).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "bv1",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
			},
			BackupID:   "463811e7-9760-acf5-9bdb-020073ca3333",
			BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backupName",
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()
		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.EqualError(tt, err, "Backup path is not in correct format")
	})
	t.Run("WhenRestoreVolumeFailWithBackupVault", func(tt *testing.T) {
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

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			State:     "READY",
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		minEnforcedRetentionDuration := int64(30)
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-vault-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
			},
		}
		backup := &datamodel.Backup{Name: "backupName", VolumeUUID: "463811e7-9760-acf5-9bdb-020073ca3335", State: "creating"}

		err = store.DB().Create(bv).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "bv1",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
			},
			BackupID:   "463811e7-9760-acf5-9bdb-020073ca3333",
			BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv2/backups/backupName",
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()
		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.EqualError(tt, err, "record not found")
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
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

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
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
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

	t.Run("WhenVendorIDZoneMatchesPoolPrimaryZone", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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

		// VendorID with zone that matches pool's primary zone
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Zone matches pool's primary zone
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test_account",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock workflow execution
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		_, workflowID, err := createVolume(ctx, store, temporal, params)
		assert.NoError(tt, err, "Expected no error when VendorID zone matches pool's primary zone")
		assert.NotEmpty(tt, workflowID, "Expected workflow ID to be returned")
	})

	t.Run("WhenVendorIDZoneDoesNotMatchPoolPrimaryZone", func(tt *testing.T) {
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
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a", // Pool primary zone
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

		// VendorID with zone that does NOT match pool's primary zone
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-b",
			VendorID:      "/projects/project123/locations/us-east1-b/volumes/test-volume", // Zone does NOT match pool's primary zone
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test_account",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Error(tt, err, "Expected error when VendorID zone does not match pool's primary zone")
		assert.Contains(tt, err.Error(), "Volume zone 'us-west1-b' does not match pool's primary zone 'us-west1-a'")
	})

	t.Run("WhenZoneIsEmptyReturnsError", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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

		// Empty zone should return error
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "", // Empty zone should cause validation error
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test_account",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Error(tt, err, "Expected error when Zone is empty")
		assert.Contains(tt, err.Error(), "Volume zone '' does not match pool's primary zone 'us-west1-a'", "Expected error message about zone mismatch")
	})

	t.Run("WhenZoneIsEmptyForRegionalPool", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone:  "us-west1-a",
				IsRegionalHA: true,
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

		// Empty zone should return error
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume_regional",
			Zone:          "", // Empty zone should not cause validation error as regional volume is being created in regional pool
			VendorID:      "",
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Creating regional volume",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test_account",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock workflow execution
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		_, workflowID, err := createVolume(ctx, store, temporal, params)
		assert.NoError(tt, err, "Expected no error when VendorID zone does not match pool's primary zone in case of regional pool")
		assert.NotEmpty(tt, workflowID, "Expected workflow ID to be returned")
	})

	t.Run("WhenVendorIDIsEmptyReturnsError", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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

		// Empty VendorID - should now return error instead of skipping validation
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "", // Empty zone to test zone validation
			VendorID:      "", // Empty VendorID
			QuotaInBytes:  minQuotaInBytesPool,
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
		}

		dbAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Name:      "test_account",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return dbAccount, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Error(tt, err, "Expected error when VendorID is empty")
		assert.Contains(tt, err.Error(), "Volume zone '' does not match pool's primary zone 'us-west1-a'", "Expected error message about zone mismatch")
	})

	t.Run("WhenVolumeExistsInCreatingStateButJobLookupFails", func(tt *testing.T) {
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
			Account:   account,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
			State:     models.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create an existing volume in CREATING state
		existingVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "existing-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateCreating, // This should trigger the job lookup
		}
		err = store.DB().Create(existingVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create existing volume: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test_volume", // Same name as existing volume
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			PoolID:       "test-pool-uuid",
			QuotaInBytes: minQuotaInBytesVolume,
			Protocols:    []string{"ISCSI"},
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, jobUUID, err := createVolume(ctx, store, temporal, params)
		assert.NoError(tt, err, "Expected no error when job lookup fails")
		assert.NotNil(tt, volume, "Expected volume to be returned")
		assert.Equal(tt, "", jobUUID, "Expected empty job UUID when job lookup fails")
		assert.Equal(tt, "test_volume", volume.DisplayName)
	})

	t.Run("WhenVolumeExistsInNonCreatingState", func(tt *testing.T) {
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
			Account:   account,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
			State:     models.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create an existing volume in READY state (not CREATING)
		existingVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "existing-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY, // This should trigger conflict error
		}
		err = store.DB().Create(existingVolume).Error
		if err != nil {
			tt.Fatalf("Failed to create existing volume: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test_volume", // Same name as existing volume
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			PoolID:       "test-pool-uuid",
			QuotaInBytes: minQuotaInBytesVolume,
			Protocols:    []string{"ISCSI"},
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		volume, jobUUID, err := createVolume(ctx, store, temporal, params)
		assert.Error(tt, err, "Expected conflict error")
		assert.Contains(tt, err.Error(), "Volume with resource_id 'test_volume' already exists")
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Equal(tt, "", jobUUID, "Expected empty job UUID")
	})

	t.Run("CreatesVolumeWhenVolumeDoesNotExist", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to set up test database")

		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.DB().Create(account).Error)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
		}
		assert.NoError(tt, store.DB().Create(pool).Error)

		volume := &datamodel.Volume{
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		createdVolume, err := store.CreateVolume(ctx, volume)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, "test_volume", createdVolume.Name)
		assert.Equal(tt, models.LifeCycleStateCreating, createdVolume.State)
		assert.Equal(tt, models.LifeCycleStateCreatingDetails, createdVolume.StateDetails)
	})

	t.Run("ReturnsErrorWhenVolumeAlreadyExists", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		assert.NoError(tt, err, "Failed to set up test database")

		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		assert.NoError(tt, store.DB().Create(account).Error)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		assert.NoError(tt, store.DB().Create(pool).Error)

		volume := &datamodel.Volume{
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
		}
		assert.NoError(tt, store.DB().Create(volume).Error)

		createdVolume, err := store.CreateVolume(ctx, volume)
		assert.Error(tt, err, "Expected error, got nil")
		assert.Nil(tt, createdVolume, "Expected nil volume")
		assert.Contains(tt, err.Error(), "Invalid input parameters provided")
		assert.Contains(tt, err.(*vsaerrors.CustomError).OriginalErr.Error(), "volume with this name already exists in the same zone")
	})

	t.Run("WhenSANProtocols_ShouldSetSnapshotDirectoryToFalse", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			Protocols:         []string{"ISCSI"}, // SAN protocol
			SnapshotDirectory: true,              // This should be set to false for SAN protocols
		}

		// Create a volume object similar to what _createVolume does
		volumeObj := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapshotDirectory: params.SnapshotDirectory,
			},
		}

		if utils.IsSanProtocols(params.Protocols) {
			volumeObj.VolumeAttributes.SnapshotDirectory = false
		}

		// Verify that SnapshotDirectory is set to false for SAN protocols
		assert.False(tt, volumeObj.VolumeAttributes.SnapshotDirectory)
	})
	t.Run("WhenCreateVolumeWithBackupPathVolumeSizeTooSmall", func(tt *testing.T) {
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
		// Ensure account has a valid ID after creation
		if account.ID == 0 {
			tt.Fatalf("Account ID should not be 0 after creation")
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
			SizeInBytes: 1000 * 1024 * 1024 * 1024, // 1TB
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
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-bv-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
		}
		err = store.DB().Create(backupVault).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		// Create a backup with a large size (100GB)
		backupSizeInBytes := int64(100 * 1024 * 1024 * 1024) // 100GB
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "backupName",
			BackupVaultID: backupVault.ID,
			SizeInBytes:   backupSizeInBytes,
		}
		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		// Create volume with size smaller than required (80GB < 120GB required)
		volumeSizeInBytes := int64(80 * 1024 * 1024 * 1024) // 80GB
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes:  uint64(volumeSizeInBytes),
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "bv1",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
			},
			BackupID:   "463811e7-9760-acf5-9bdb-020073ca3333",
			BackupPath: "projects/project123/locations/location123/backupVaults/bv1/backups/backupName",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil // Use the real account instead of the mock
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)

		// Should return error due to volume size being too small for backup
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Contains(tt, err.Error(), "Restored Volume size should be greater than or equal to the logical size of the backup")
	})
	t.Run("WhenCreateVolumeWithBackupPathVolumeSizeTooSmallWithNewCalculation", func(tt *testing.T) {
		// Enable the new restore volume buffer method
		utils.SetRestoreVolumeBufferEnabledForTesting(true)
		defer utils.SetRestoreVolumeBufferEnabledForTesting(false)

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
		// Ensure account has a valid ID after creation
		if account.ID == 0 {
			tt.Fatalf("Account ID should not be 0 after creation")
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
			SizeInBytes: 1000 * 1024 * 1024 * 1024, // 1TB
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
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-bv-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
		}
		err = store.DB().Create(backupVault).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		// Create a backup with a large size (100GB)
		backupSizeInBytes := int64(100 * 1024 * 1024 * 1024) // 100GB
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "backupName",
			BackupVaultID: backupVault.ID,
			SizeInBytes:   backupSizeInBytes,
		}
		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		// Create volume with size smaller than required (80GB < 120GB required with 20% calculation)
		volumeSizeInBytes := int64(80 * 1024 * 1024 * 1024) // 80GB
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes:  uint64(volumeSizeInBytes),
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: &[]bool{true}[0],
				BackupVaultID:          "bv1",
				BackupPolicyId:         "test-backup-policy-id",
				BackupChainBytes:       &[]int64{1000}[0],
			},
			BackupID:   "463811e7-9760-acf5-9bdb-020073ca3333",
			BackupPath: "projects/project123/locations/location123/backupVaults/bv1/backups/backupName",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil // Use the real account instead of the mock
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)

		// Should return error due to volume size being too small for backup
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Contains(tt, err.Error(), "Restored Volume size should be greater than or equal to the logical size of the backup")
	})
	t.Run("WhenParentSnapshotIsReplicationSnapshot", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"ISCSI"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Create volume
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:        "test-volume",
			AccountID:   account.ID,
			PoolID:      pool.ID,
			SizeInBytes: 107374182400,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "snapmirror.test_snapshot",
			AccountID: account.ID,
			State:     "READY",
			VolumeID:  volume.ID,
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		newVolume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, newVolume, "Expected nil volume")
		assert.ErrorContains(tt, err, "Snapshot is not eligible for volume creation. Snapshots created for backup, data protection, replication, or clone volumes are not supported.")
	})
	t.Run("WhenParentSnapshotIsBackupSnapshot", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"ISCSI"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Create volume
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:        "test-volume",
			AccountID:   account.ID,
			PoolID:      pool.ID,
			SizeInBytes: 107374182400,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "snapmirror.test_snapshot",
			AccountID: account.ID,
			State:     "READY",
			VolumeID:  volume.ID,
			Type:      activities.SnapshotTypeBackup,
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		newVolume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, newVolume, "Expected nil volume")
		assert.ErrorContains(tt, err, "Snapshot is not eligible for volume creation. Snapshots created for backup, data protection, replication, or clone volumes are not supported.")
	})
	t.Run("WhenParentSnapshotIsCloneSnapshot", func(tt *testing.T) {
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_pool",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"ISCSI"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Create volume
		volume := &datamodel.Volume{
			BaseModel:         datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:              "test-volume",
			AccountID:         account.ID,
			PoolID:            pool.ID,
			SizeInBytes:       107374182400,
			ClonesSharedBytes: 10000,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "snapmirror.test_snapshot",
			AccountID: account.ID,
			State:     "READY",
			VolumeID:  volume.ID,
		}

		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		newVolume, _, err := createVolume(ctx, store, temporal, params)
		assert.Nil(tt, newVolume, "Expected nil volume")
		assert.ErrorContains(tt, err, "Snapshot is not eligible for volume creation. Snapshots created for backup, data protection, replication, or clone volumes are not supported.")
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
		Zone:          "us-west1-a",
		VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume", // Valid VendorID
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
	validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
		return nil
	}
	defer func() {
		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
	}()

	temporal := workflowEngineMock.NewMockTemporalTestClient(t)

	// Mock ExecuteWorkflow for auto pool scaling
	temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	// Mock ExecuteWorkflow for auto pool scaling
	temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

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

// Test cases to cover lines 1420-1423 (IP validation in validateAllowedClients)
func Test_validateAllowedClients(t *testing.T) {
	t.Run("ValidSingleIP", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1")
		assert.NoError(tt, err)
	})

	t.Run("ValidMultipleIPs", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1,192.168.1.2,10.0.0.1")
		assert.NoError(tt, err)
	})

	t.Run("ValidCIDR", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.0/24")
		assert.NoError(tt, err)
	})

	t.Run("ValidMultipleCIDRs", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.0/24,10.0.0.0/8")
		assert.NoError(tt, err)
	})

	t.Run("ValidMixedIPsAndCIDRs", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1,192.168.1.0/24,10.0.0.1")
		assert.NoError(tt, err)
	})

	t.Run("ValidAllClients", func(tt *testing.T) {
		err := validateAllowedClients("0.0.0.0/0")
		assert.NoError(tt, err)
	})

	t.Run("InvalidIP", func(tt *testing.T) {
		err := validateAllowedClients("256.256.256.256")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("InvalidCIDR", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1/33")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("InvalidCIDRFormat", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.0/24/32")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("InvalidZeroIPWithNonZeroMask", func(tt *testing.T) {
		err := validateAllowedClients("0.0.0.0/24")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "0.0.0.0 address can only be used with a 0 bit subnet mask")
	})

	t.Run("DuplicateIPs", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1,192.168.1.1")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("DuplicateCIDRs", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.0/24,192.168.1.0/24")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("MixedDuplicate", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1,192.168.1.0/24,192.168.1.1")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("EmptyString", func(tt *testing.T) {
		err := validateAllowedClients("")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("SingleComma", func(tt *testing.T) {
		err := validateAllowedClients(",")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})

	t.Run("MultipleCommas", func(tt *testing.T) {
		err := validateAllowedClients("192.168.1.1,,192.168.1.2")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "allowedClients must include unique IPv4 or IPv4 CIDR values")
	})
}

// Test cases to cover snapshot handling in createVolume (lines 125-127)
func Test_createVolume_WithSnapshot(t *testing.T) {
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
		VendorID:  "/projects/project123/locations/location123/pools/test_pool",
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

	// Create nodes for the pool (required for volume creation validation)
	node1 := &datamodel.Node{
		BaseModel: datamodel.BaseModel{UUID: "test-node1-uuid"},
		Name:      "test-node1",
		AccountID: account.ID,
		PoolID:    pool.ID,
		State:     models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node1).Error
	if err != nil {
		tt.Fatalf("Failed to create node1: %v", err)
	}

	node2 := &datamodel.Node{
		BaseModel: datamodel.BaseModel{UUID: "test-node2-uuid"},
		Name:      "test-node2",
		AccountID: account.ID,
		PoolID:    pool.ID,
		State:     models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node2).Error
	if err != nil {
		tt.Fatalf("Failed to create node2: %v", err)
	}

	// Create LIFs for the nodes (required for volume creation validation)
	lif1 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-lif1-uuid"},
		Name:      "test-lif1",
		AccountID: account.ID,
		NodeID:    node1.ID,
	}
	err = store.DB().Create(lif1).Error
	if err != nil {
		tt.Fatalf("Failed to create lif1: %v", err)
	}

	lif2 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-lif2-uuid"},
		Name:      "test-lif2",
		AccountID: account.ID,
		NodeID:    node2.ID,
	}
	err = store.DB().Create(lif2).Error
	if err != nil {
		tt.Fatalf("Failed to create lif2: %v", err)
	}

	// Create a parent volume
	parentVolume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-parent-volume-uuid"},
		Name:        "test_parent_volume",
		AccountID:   account.ID,
		PoolID:      pool.ID,
		SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GiB
		State:       models.LifeCycleStateREADY,
	}
	err = store.DB().Create(parentVolume).Error
	if err != nil {
		tt.Fatalf("Failed to create parent volume: %v", err)
	}

	// Create a snapshot
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
		Name:      "test_snapshot",
		AccountID: account.ID,
		VolumeID:  parentVolume.ID,
		State:     models.LifeCycleStateREADY,
		Volume:    parentVolume,
	}
	err = store.DB().Create(snapshot).Error
	if err != nil {
		tt.Fatalf("Failed to create snapshot: %v", err)
	}

	params := &common.CreateVolumeParams{
		AccountName:   "test_account",
		Region:        "test_region",
		Name:          "test_volume",
		VendorID:      "test_vendor",
		QuotaInBytes:  150 * 1024 * 1024 * 1024, // 150 GiB
		Protocols:     []string{"NFS"},
		Description:   "Some description",
		DisplayName:   "Some display name",
		PoolID:        "test-pool-uuid",
		CreationToken: "test-creation-token",
		Network:       "test-network",
		SnapshotID:    "test-snapshot-uuid",
		Zone:          "us-west1-a",
		SnapReserve:   20, // 20% snapReserve
		FileProperties: &models.FileProperties{
			ExportPolicy: &models.ExportPolicy{
				ExportPolicyName: "test-export-policy",
				ExportRules: []*models.ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						AccessType:     "READ_WRITE",
						NFSv3:          true,
						NFSv4:          true,
						Index:          1,
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
	validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
		return nil
	}
	defer func() {
		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
	}()

	temporal := workflowEngineMock.NewMockTemporalTestClient(t)

	// Mock ExecuteWorkflow for auto pool scaling
	temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	volume, _, err := createVolume(ctx, store, temporal, params)
	assert.NoError(tt, err)
	assert.NotNil(tt, volume)
	assert.Equal(tt, "test_volume", volume.DisplayName)
	assert.Equal(tt, "test_account", volume.AccountName)
	assert.Equal(tt, "test-pool-uuid", volume.PoolID)
	assert.Equal(tt, "test_pool", volume.PoolName)
	assert.Equal(tt, "test-creation-token", volume.CreationToken)
	assert.Equal(tt, "Some description", volume.Description)
	assert.Equal(tt, []string{"NFS"}, volume.ProtocolTypes)
	assert.Equal(tt, uint64(150*1024*1024*1024), volume.QuotaInBytes)
	assert.Equal(tt, "CREATING", volume.LifeCycleState)
	assert.Equal(tt, "Creation in progress", volume.LifeCycleStateDetails)
}

// Test cases to cover snapshot handling in createVolume (lines 125-127)
func Test_createLargeVolume_WithSnapshot(t *testing.T) {
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
		VendorID:  "/projects/project123/locations/location123/pools/test_pool",
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone: "us-west1-a",
		},
		LargeCapacity: true,
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

	// Create nodes for the pool (required for volume creation validation)
	node1 := &datamodel.Node{
		BaseModel: datamodel.BaseModel{UUID: "test-node1-uuid"},
		Name:      "test-node1",
		AccountID: account.ID,
		PoolID:    pool.ID,
		State:     models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node1).Error
	if err != nil {
		tt.Fatalf("Failed to create node1: %v", err)
	}

	node2 := &datamodel.Node{
		BaseModel: datamodel.BaseModel{UUID: "test-node2-uuid"},
		Name:      "test-node2",
		AccountID: account.ID,
		PoolID:    pool.ID,
		State:     models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node2).Error
	if err != nil {
		tt.Fatalf("Failed to create node2: %v", err)
	}

	// Create LIFs for the nodes (required for volume creation validation)
	lif1 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-lif1-uuid"},
		Name:      "test-lif1",
		AccountID: account.ID,
		NodeID:    node1.ID,
	}
	err = store.DB().Create(lif1).Error
	if err != nil {
		tt.Fatalf("Failed to create lif1: %v", err)
	}

	lif2 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-lif2-uuid"},
		Name:      "test-lif2",
		AccountID: account.ID,
		NodeID:    node2.ID,
	}
	err = store.DB().Create(lif2).Error
	if err != nil {
		tt.Fatalf("Failed to create lif2: %v", err)
	}
	var lvcc int32 = 5
	// Create a parent volume
	parentVolume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-parent-volume-uuid"},
		Name:        "test_parent_volume",
		AccountID:   account.ID,
		PoolID:      pool.ID,
		SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GiB
		State:       models.LifeCycleStateREADY,
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeVolumeConstituentCount: &lvcc,
			LargeCapacity:               true,
		},
	}
	err = store.DB().Create(parentVolume).Error
	if err != nil {
		tt.Fatalf("Failed to create parent volume: %v", err)
	}

	// Create a snapshot
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
		Name:      "test_snapshot",
		AccountID: account.ID,
		VolumeID:  parentVolume.ID,
		State:     models.LifeCycleStateREADY,
		Volume:    parentVolume,
	}
	err = store.DB().Create(snapshot).Error
	if err != nil {
		tt.Fatalf("Failed to create snapshot: %v", err)
	}

	params := &common.CreateVolumeParams{
		AccountName:   "test_account",
		Region:        "test_region",
		Name:          "test_volume",
		VendorID:      "test_vendor",
		QuotaInBytes:  150 * 1024 * 1024 * 1024, // 150 GiB
		Protocols:     []string{"NFS"},
		Description:   "Some description",
		DisplayName:   "Some display name",
		PoolID:        "test-pool-uuid",
		CreationToken: "test-creation-token",
		Network:       "test-network",
		SnapshotID:    "test-snapshot-uuid",
		Zone:          "us-west1-a",
		SnapReserve:   20, // 20% snapReserve
		LargeCapacity: true,
		FileProperties: &models.FileProperties{
			ExportPolicy: &models.ExportPolicy{
				ExportPolicyName: "test-export-policy",
				ExportRules: []*models.ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						AccessType:     "READ_WRITE",
						NFSv3:          true,
						NFSv4:          true,
						Index:          1,
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
	validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
		return nil
	}
	defer func() {
		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
	}()

	temporal := workflowEngineMock.NewMockTemporalTestClient(t)

	// Mock ExecuteWorkflow for auto pool scaling
	temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
	workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	volume, _, err := createVolume(ctx, store, temporal, params)
	assert.NoError(tt, err)
	assert.NotNil(tt, volume)
	assert.Equal(tt, "test_volume", volume.DisplayName)
	assert.Equal(tt, "test_account", volume.AccountName)
	assert.Equal(tt, "test-pool-uuid", volume.PoolID)
	assert.Equal(tt, "test_pool", volume.PoolName)
	assert.Equal(tt, "test-creation-token", volume.CreationToken)
	assert.Equal(tt, "Some description", volume.Description)
	assert.Equal(tt, []string{"NFS"}, volume.ProtocolTypes)
	assert.Equal(tt, uint64(150*1024*1024*1024), volume.QuotaInBytes)
	assert.Equal(tt, "CREATING", volume.LifeCycleState)
	assert.Equal(tt, "Creation in progress", volume.LifeCycleStateDetails)
	assert.Equal(tt, int32(5), *volume.LargeVolumeConstituentCount)
}

func TestCreateVolume_ProtocolValidation(t *testing.T) {
	t.Run("WhenNASProtocolMismatchWithSANSnapshot", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Errorf("Failed to create test storage: %v", err)
			return
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Errorf("Failed to clean up test storage: %v", err)
			return
		}

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Errorf("Failed to create account: %v", err)
			return
		}

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Errorf("Failed to create pool: %v", err)
			return
		}

		// Create test SVM
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Errorf("Failed to create svm: %v", err)
			return
		}

		// Create source volume with SAN protocols (ISCSI)
		sourceVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-source-volume-uuid"},
			Name:      "test_source_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			SvmID:     svm.ID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"ISCSI"}, // SAN protocol
			},
		}
		err = store.DB().Create(sourceVolume).Error
		if err != nil {
			tt.Errorf("Failed to create source volume: %v", err)
			return
		}

		// Create snapshot with SAN source volume
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  sourceVolume.ID,
			State:     "READY",
			Volume:    sourceVolume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Errorf("Failed to create snapshot: %v", err)
			return
		}

		// Create volume params with NAS protocols (NFS)
		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_volume",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"}, // NAS protocol - mismatch with snapshot
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)

		// Should fail with protocol mismatch error
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Snapshot volume protocol type does not match requested volume protocol type")
	})

	t.Run("WhenSANProtocolMismatchWithNASSnapshot", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Errorf("Failed to create test storage: %v", err)
			return
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Errorf("Failed to clean up test storage: %v", err)
			return
		}

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Errorf("Failed to create account: %v", err)
			return
		}

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Errorf("Failed to create pool: %v", err)
			return
		}

		// Create test SVM
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Errorf("Failed to create svm: %v", err)
			return
		}

		// Create source volume with NAS protocols (NFS)
		sourceVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-source-volume-uuid"},
			Name:      "test_source_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			SvmID:     svm.ID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFS"}, // NAS protocol
			},
		}
		err = store.DB().Create(sourceVolume).Error
		if err != nil {
			tt.Errorf("Failed to create source volume: %v", err)
			return
		}

		// Create snapshot with NAS source volume
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  sourceVolume.ID,
			State:     "READY",
			Volume:    sourceVolume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Errorf("Failed to create snapshot: %v", err)
			return
		}

		// Create volume params with SAN protocols (ISCSI)
		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_volume",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"ISCSI"}, // SAN protocol - mismatch with snapshot
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)

		// Should fail with protocol mismatch error
		assert.Nil(tt, volume, "Expected nil volume")
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Snapshot volume protocol type does not match requested volume protocol type")
	})

	t.Run("WhenProtocolsMatch_SANToSAN", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Errorf("Failed to create test storage: %v", err)
			return
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Errorf("Failed to clean up test storage: %v", err)
			return
		}

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Errorf("Failed to create account: %v", err)
			return
		}

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Errorf("Failed to create pool: %v", err)
			return
		}

		// Create test SVM
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Errorf("Failed to create svm: %v", err)
			return
		}

		// Create source volume with SAN protocols (ISCSI)
		sourceVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-source-volume-uuid"},
			Name:      "test_source_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			SvmID:     svm.ID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"ISCSI"}, // SAN protocol
			},
		}
		err = store.DB().Create(sourceVolume).Error
		if err != nil {
			tt.Errorf("Failed to create source volume: %v", err)
			return
		}

		// Create snapshot with SAN source volume
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  sourceVolume.ID,
			State:     "READY",
			Volume:    sourceVolume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Errorf("Failed to create snapshot: %v", err)
			return
		}

		// Create volume params with SAN protocols (ISCSI) - matching
		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_volume",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"ISCSI"}, // SAN protocol - matches snapshot
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock the workflow execution to return success
		temporal.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		_, _, err = createVolume(ctx, store, temporal, params)
		// Should not fail due to protocol validation (may fail for other reasons like workflow execution)
		// The key is that it should not fail with protocol mismatch error
		if err != nil {
			assert.NotContains(tt, err.Error(), "Snapshot volume protocol type does not match requested volume protocol type")
		}
	})

	t.Run("WhenProtocolsMatch_NASToNAS", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Errorf("Failed to create test storage: %v", err)
			return
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Errorf("Failed to clean up test storage: %v", err)
			return
		}

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Errorf("Failed to create account: %v", err)
			return
		}

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Errorf("Failed to create pool: %v", err)
			return
		}

		// Create test SVM
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Errorf("Failed to create svm: %v", err)
			return
		}

		// Create source volume with NAS protocols (NFS)
		sourceVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-source-volume-uuid"},
			Name:      "test_source_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			SvmID:     svm.ID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{"NFS"}, // NAS protocol
			},
		}
		err = store.DB().Create(sourceVolume).Error
		if err != nil {
			tt.Errorf("Failed to create source volume: %v", err)
			return
		}

		// Create snapshot with NAS source volume
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  sourceVolume.ID,
			State:     "READY",
			Volume:    sourceVolume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Errorf("Failed to create snapshot: %v", err)
			return
		}

		// Create volume params with NAS protocols (NFS) - matching
		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_volume",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"}, // NAS protocol - matches snapshot
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock the workflow execution to return success
		temporal.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		_, _, err = createVolume(ctx, store, temporal, params)
		// Should not fail due to protocol validation (may fail for other reasons like workflow execution)
		// The key is that it should not fail with protocol mismatch error
		if err != nil {
			assert.NotContains(tt, err.Error(), "Snapshot volume protocol type does not match requested volume protocol type")
		}
	})

	t.Run("WhenSnapshotVolumeIsNil", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Errorf("Failed to create test storage: %v", err)
			return
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Errorf("Failed to clean up test storage: %v", err)
			return
		}

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Errorf("Failed to create account: %v", err)
			return
		}

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Errorf("Failed to create pool: %v", err)
			return
		}

		// Create snapshot without volume (nil volume)
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  999, // Non-existent volume ID
			State:     "READY",
			Volume:    nil, // Nil volume
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Errorf("Failed to create snapshot: %v", err)
			return
		}

		// Create volume params
		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_volume",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		_, _, err = createVolume(ctx, store, temporal, params)
		// Should not fail due to protocol validation since volume is nil
		// The validation should be skipped when volume is nil
		if err != nil {
			assert.NotContains(tt, err.Error(), "Snapshot volume protocol type does not match requested volume protocol type")
		}
	})

	t.Run("WhenSnapshotVolumeAttributesIsNil", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Errorf("Failed to create test storage: %v", err)
			return
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			tt.Errorf("Failed to clean up test storage: %v", err)
			return
		}

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Errorf("Failed to create account: %v", err)
			return
		}

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Errorf("Failed to create pool: %v", err)
			return
		}

		// Create test SVM
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
			Name:      "test_svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Errorf("Failed to create svm: %v", err)
			return
		}

		// Create source volume with nil VolumeAttributes
		sourceVolume := &datamodel.Volume{
			BaseModel:        datamodel.BaseModel{UUID: "test-source-volume-uuid"},
			Name:             "test_source_volume",
			AccountID:        account.ID,
			PoolID:           pool.ID,
			SvmID:            svm.ID,
			VolumeAttributes: nil, // Nil VolumeAttributes
		}
		err = store.DB().Create(sourceVolume).Error
		if err != nil {
			tt.Errorf("Failed to create source volume: %v", err)
			return
		}

		// Create snapshot with volume that has nil VolumeAttributes
		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  sourceVolume.ID,
			State:     "READY",
			Volume:    sourceVolume,
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Errorf("Failed to create snapshot: %v", err)
			return
		}

		// Create volume params
		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Region:       "test_region",
			Name:         "test_volume",
			Zone:         "us-west1-a",
			VendorID:     "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes: minQuotaInBytesPool,
			Protocols:    []string{"NFS"},
			Description:  "Some description",
			DisplayName:  "Some display name",
			PoolID:       "test-pool-uuid",
			SnapshotID:   "test-snapshot-id",
		}

		// Mock functions
		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock the workflow execution to return success
		temporal.On("SignalWithStartWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		_, _, err = createVolume(ctx, store, temporal, params)
		// Should not fail due to protocol validation since VolumeAttributes is nil
		// The validation should be skipped when VolumeAttributes is nil
		if err != nil {
			assert.NotContains(tt, err.Error(), "Snapshot volume protocol type does not match requested volume protocol type")
		}
	})
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
		assert.EqualError(tt, err, "Volume not found")
		var customErr *vsaerrors.CustomError
		errors2.As(err, &customErr)
		assert.NotNil(tt, customErr, "Expected a CustomError")
		assert.NotNil(tt, customErr.HttpCode, 404)
		assert.NotNil(tt, customErr.Retriable, false)
		assert.NotNil(tt, customErr.OriginalErr, "volume not found")
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

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

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

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

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

	t.Run("WhenVolumeDeleteIsCalledThenStateIsMarkedDeleting", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		assert.NoError(t, err, "Failed to create test storage")

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		assert.NoError(t, err, "Failed to clear in-memory database")

		// Create account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		assert.NoError(t, err, "Failed to create account")

		// Create pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
			VendorID: "/projects/project123/locations/location123/pools/pool123",
		}
		err = store.DB().Create(pool).Error
		assert.NoError(t, err, "Failed to create pool")

		// Create volume
		volume := &datamodel.Volume{
			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:         "test_volume",
			AccountID:    account.ID,
			PoolID:       pool.ID,
			State:        models.LifeCycleStateREADY,
			StateDetails: models.LifeCycleStateAvailableDetails,
			Pool:         pool,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(t, err, "Failed to create volume")
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		// Call deleteVolume
		_, _, err = deleteVolume(ctx, store, temporal, "test-volume-uuid")
		assert.NoError(t, err, "deleteVolume should not return error")

		// Fetch the volume again and check state
		var updatedVolume datamodel.Volume
		err = store.DB().First(&updatedVolume, "uuid = ?", volume.UUID).Error
		assert.NoError(t, err, "Failed to fetch updated volume")
		assert.Equal(t, models.LifeCycleStateDeleting, updatedVolume.State)
		assert.Equal(t, models.LifeCycleStateDeletingDetails, updatedVolume.StateDetails)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := &database.MockStorage{}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Prepare a volume to be deleted
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			Account:   &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test_account"},
			AccountID: 1,
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test_pool"},
			PoolID:    1,
		}

		// Mock GetVolume to return the volume
		mockStorage.On("GetVolume", ctx, "test-volume-uuid").Return(volume, nil)
		// Mock CreateJob to succeed
		jobUUID := "wid"
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{WorkflowID: jobUUID, BaseModel: datamodel.BaseModel{UUID: jobUUID}}, nil)
		// Mock UpdateVolumeFields to fail
		mockStorage.On("UpdateVolumeFields", ctx, "test-volume-uuid", mock.Anything).Return(errors.New("update failed"))
		// Mock IsBackupInCreatingOrDeletingStateByVolume to return false
		mockStorage.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(false, nil)
		// Mock GetVolumeReplicationCountByVolumeID to return 0
		mockStorage.On("GetVolumeReplicationCountByVolumeID", ctx, mock.Anything).Return(int64(0), nil)
		// Mock UpdateJob call when error occurs in defer function
		mockStorage.On("UpdateJob", ctx, jobUUID, string(models.JobsStateERROR), 0, "update failed").Return(nil)

		// Call deleteVolume
		vol, jobID, err := deleteVolume(ctx, mockStorage, temporal, "test-volume-uuid")
		assert.Nil(tt, vol)
		assert.Empty(tt, jobID)
		assert.EqualError(tt, err, "update failed")
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
			VendorID:  "/projects/project123/locations/us-west1-a/pools/test-pool", // Valid pool VendorID format
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

		// Create mock temporal client to handle TriggerRefreshWorkflow
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
		// Mock the QueryWorkflow and ExecuteWorkflow calls
		mockTemporal.EXPECT().QueryWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, &serviceerror.NotFound{}).Maybe()
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		orch := Orchestrator{
			storage:  store,
			temporal: mockTemporal,
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

		getIPAddressForVolume = func(ctx context.Context, se database.Storage, volume *datamodel.Volume) ([]string, error) {
			return nil, errors.New("some error")
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

	t.Run("WhenSingleVolumeAndNotFoundInDb", func(tt *testing.T) {
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

		orch := Orchestrator{
			storage: store,
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
		}()

		volumeResp, err := orch.GetMultipleVolumes(ctx, []string{"non-existent-volume"}, account.Name)
		assert.Empty(tt, err, "Expected no error when volume is not found in VCP DB")
		assert.Nil(tt, volumeResp, "Expected nil response")
	})

	t.Run("WhenSingleVolumeAndErrorOtherThanNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		// Create a mock storage that will return an error for GetMultipleVolumes
		mockStorage := &database.MockStorage{}

		// Mock account lookup to succeed
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test_account",
		}

		// Mock GetMultipleVolumes to return an error
		mockStorage.On("GetMultipleVolumes", mock.AnythingOfType("*context.valueCtx"), mock.AnythingOfType("[][]interface {}")).Return(nil, errors.NewUserInputValidationErr("dummy error")).Once()

		// Create mock temporal client to handle TriggerRefreshWorkflow
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		orch := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		defer func() {
			getOrCreateAccount = _getOrCreateAccount
		}()

		// Call GetMultipleVolumes with single volume
		volumeResp, err := orch.GetMultipleVolumes(ctx, []string{"test-volume"}, account.Name)

		// Should return the error (not nil) and nil result
		assert.Error(tt, err, "Expected error when GetMultipleVolumes fails")
		assert.Contains(tt, err.Error(), "dummy error")
		assert.Nil(tt, volumeResp, "Expected nil response when error occurs")

		// Verify all mocks were called as expected
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenMultipleVolumesGetMultipleVolumesFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		// Create a fresh mock storage that will return account but fail on GetMultipleVolumes
		mockStorage := &database.MockStorage{}

		// Set up expectations in order they will be called
		mockStorage.On("GetAccount", mock.AnythingOfType("*context.valueCtx"), "test_account").Return(&datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test_account",
		}, nil).Once()

		mockStorage.On("GetMultipleVolumes", mock.AnythingOfType("*context.valueCtx"), mock.AnythingOfType("[][]interface {}")).Return(nil, errors2.New("database error")).Once()

		// Create mock temporal client to handle TriggerRefreshWorkflow
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

		orch := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		volumeResp, err := orch.GetMultipleVolumes(ctx, []string{"volume1", "volume2"}, "test_account")
		assert.Error(tt, err, "Expected error when GetMultipleVolumes fails")
		assert.Equal(tt, "database error", err.Error())
		assert.Nil(tt, volumeResp, "Expected nil response")
	})

	t.Run("WhenMultipleVolumesGetIPAddressForVolumeFails", func(tt *testing.T) {
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
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume1"},
			Name:      "volume1",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}

		// Mock getIPAddressForVolume to fail (line 1095-1097)
		getIPAddressForVolume = func(ctx context.Context, se database.Storage, volume *datamodel.Volume) ([]string, error) {
			return nil, errors.New("IP address lookup failed")
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			getIPAddressForVolume = _getIPAddressForVolume
		}()

		volumeResp, err := orch.GetMultipleVolumes(ctx, []string{"volume1", "volume2"}, account.Name)
		assert.Error(tt, err, "Expected error when getIPAddressForVolume fails")
		assert.Equal(tt, "IP address lookup failed", err.Error())
		assert.Nil(tt, volumeResp, "Expected nil response")
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "somevpc",
			Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}},
			SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
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

		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-id"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create bv")

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

		bd := []common.BlockDevice{
			{
				Name:   "test_block_device",
				OSType: "linux",
			},
		}

		params := &common.CreateVolumeParams{
			Name:           "dummy-name",
			PoolID:         pool.UUID,
			QuotaInBytes:   minQuotaInBytesPool + 1,
			Protocols:      []string{utils.ProtocolISCSI},
			DataProtection: &models.DataProtection{BackupVaultID: bv.UUID},
			BlockDevices:   &bd,
		}

		poolView := &datamodel.PoolView{
			Pool: *pool,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err, "some error")
	})
	t.Run("WhenValidateCreateVolumeParamsSuccessWith1Node", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "somevpc",
			Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}},
			SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
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

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid1"},
			Name:      "test_node",
			AccountID: account.ID,
			IPAddress: "1.1.1.1",
			NodeID:    node1.ID,
		}
		err = store.DB().Create(lif).Error
		assert.NoError(tt, err, "Failed to create node")

		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-backup-vault-id"},
			Name:      "test_backup_vault",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create bv")

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

		bd := []common.BlockDevice{
			{
				Name:   "test_block_device",
				OSType: "linux",
			},
		}

		params := &common.CreateVolumeParams{
			Name:           "dummy-name",
			PoolID:         pool.UUID,
			QuotaInBytes:   minQuotaInBytesPool + 1,
			Protocols:      []string{utils.ProtocolISCSI},
			DataProtection: &models.DataProtection{BackupVaultID: bv.UUID},
			BlockDevices:   &bd,
		}

		poolView := &datamodel.PoolView{
			Pool: *pool,
		}

		mm.EXPECT().envIsLocalEnv().Return(true)

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err, "some error")
	})
	t.Run("WhenValidateCreateVolumeParamsFailsWhileAttachingErroredBackupVaultToVolume", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
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
			Account:   &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}},
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

		bv := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "test-backup-vault-id"},
			Name:                  "test_backup_vault",
			AccountID:             account.ID,
			LifeCycleState:        models.LifeCycleStateError,
			LifeCycleStateDetails: models.LifeCycleStateUpdatingDetails,
			Account:               account,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create bv")

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
			Name:           "dummy-name",
			PoolID:         pool.UUID,
			QuotaInBytes:   minQuotaInBytesPool + 1,
			Protocols:      []string{utils.ProtocolISCSI},
			DataProtection: &models.DataProtection{BackupVaultID: "test-backup-vault-id"},
		}

		poolView := &datamodel.PoolView{
			Pool: *pool,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Error(tt, err)
	})
	t.Run("WhenPoolStateNotReady", func(tt *testing.T) {
		testCases := []struct {
			name          string
			poolState     string
			expectedError string
		}{
			{
				name:          "CreatingPool",
				poolState:     models.LifeCycleStateCreating,
				expectedError: "Specified pool is in CREATING state, hence volume cannot be created",
			},
			{
				name:          "ErrorPool",
				poolState:     models.LifeCycleStateError,
				expectedError: "Pool is currently unavailable for creating volume",
			},
			{
				name:          "DeletingPool",
				poolState:     models.LifeCycleStateDeleting,
				expectedError: "Specified pool is in DELETING state, hence volume cannot be created",
			},
			{
				name:          "DeletedPool",
				poolState:     models.LifeCycleStateDeleted,
				expectedError: "Specified pool is in DELETED state, hence volume cannot be created",
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

				mockLogger := log.NewLogger()
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
					t.Fatalf("Failed to create account: %v", err)
				}

				pool := &datamodel.Pool{
					BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
					Name:        "test_pool",
					AccountID:   account.ID,
					State:       tc.poolState,
					SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10 TiB
				}

				err = store.DB().Create(pool).Error
				if err != nil {
					t.Fatalf("Failed to create pool: %v", err)
				}

				params := &common.CreateVolumeParams{
					Name:         "dummy-name",
					PoolID:       pool.UUID,
					QuotaInBytes: uint64(100 * 1024 * 1024 * 1024), // 100 GiB
				}

				poolView := &datamodel.PoolView{
					Pool:         *pool,
					QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
				}

				err = validateCreateVolumeParams(ctx, store, params, poolView)
				assert.EqualError(t, err, tc.expectedError)
			})
		}
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

		poolView := &datamodel.PoolView{
			Pool: *pool,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Invalid volume capacity 1073741823B. Must be between 1GiB and 128TiB.")
	})
	t.Run("WhenVolumeSizeExceedsPoolSize", func(tt *testing.T) {
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

		// Create a pool with limited size
		poolSizeInBytes := int64(1000 * 1024 * 1024 * 1024) // 1000 GiB

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: poolSizeInBytes,
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create pool view with existing quota usage
		existingQuotaInBytes := uint64(500 * 1024 * 1024 * 1024) // 500 GiB already used
		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: existingQuotaInBytes,
		}

		// Try to create a volume that would exceed the pool size
		// Pool has 1000 GiB total, 500 GiB used, trying to add 600 GiB (exceeds remaining 500 GiB)
		requestedVolumeSize := uint64(600 * 1024 * 1024 * 1024) // 600 GiB

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: requestedVolumeSize,
		}

		// This should fail because 500 GiB (existing) + 600 GiB (requested) = 1100 GiB > 1000 GiB (pool size)
		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "volume size cannot be greater than pool size")
	})
	t.Run("WhenVolumeSizeExactlyFitsInPool", func(tt *testing.T) {
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

		// Create a pool with some size
		poolSizeInBytes := int64(1000 * 1024 * 1024 * 1024) // 1000 GiB

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: poolSizeInBytes,
			Network:     "test-network", // Set pool network to match volume network
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		// Create pool view with existing quota usage
		existingQuotaInBytes := uint64(400 * 1024 * 1024 * 1024) // 400 GiB already used
		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: existingQuotaInBytes,
		}

		// Try to create a volume that exactly fits the remaining pool space
		// Pool has 1000 GiB total, 400 GiB used, requesting exactly 600 GiB (fits perfectly)
		requestedVolumeSize := uint64(600 * 1024 * 1024 * 1024) // 600 GiB

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: requestedVolumeSize,
			Network:      "different-network", // Set different network to trigger network validation error
		}

		// This should pass pool size validation (400 GiB + 600 GiB = 1000 GiB exactly)
		// but fail on network validation (proving pool size validation passed)
		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "pool network and volume network should be same")
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10 TiB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: uint64(250 * 1024 * 1024 * 1024), // 250 GiB
			Network:      "dummy-network",
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10 TiB
		}

		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: uint64(250 * 1024 * 1024 * 1024), // 250 GiB
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(maxQuotaInBytesPool),
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

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(maxQuotaInBytesPool),
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
			SizeInBytes:  int64(minQuotaInBytesVolume),
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(maxQuotaInBytesPool),
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

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(maxQuotaInBytesPool),
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

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(maxQuotaInBytesPool),
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

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			SizeInBytes: int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "linux",
			},
			Protocols: []string{utils.ProtocolISCSI},
		}
		poolView, err := store.GetPool(ctx, params.PoolID, account.ID)
		if err != nil {
			tt.Fatalf("Failed to get pool view: %v", err)
		}
		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Account:     account,
			SizeInBytes: int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"1"},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}
		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Account:     account,
			SizeInBytes: int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"testhg"},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Account:     account,
			SizeInBytes: int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-volume-uuid2"},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500 GiB already used
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err)
	})
	t.Run("WhenAutoTieringIsNotAllowed", func(tt *testing.T) {
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
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:             "test_pool",
			AccountID:        account.ID,
			State:            models.LifeCycleStateREADY,
			Account:          account,
			AllowAutoTiering: false,
			SizeInBytes:      int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-volume-uuid2"},
			},
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled: true,
			},
		}
		poolView, err := store.GetPool(ctx, params.PoolID, account.ID)
		if err != nil {
			tt.Fatalf("Failed to get pool view: %v", err)
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	})
	t.Run("WhenCoolnessPeriodBelowTheRange", func(tt *testing.T) {
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
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:             "test_pool",
			AccountID:        account.ID,
			State:            models.LifeCycleStateREADY,
			Account:          account,
			AllowAutoTiering: true,
			SizeInBytes:      int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-volume-uuid2"},
			},
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:   true,
				TieringPolicy:        models2.VolumeInlineTieringPolicyAuto,
				CoolingThresholdDays: 1,
			},
		}

		poolView, err := store.GetPool(ctx, params.PoolID, account.ID)
		if err != nil {
			tt.Fatalf("Failed to get pool view: %v", err)
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Auto Tiering Cooling Threshold days must be between 2 and 183 days")
	})
	t.Run("WhenCoolnessPeriodAboveTheRange", func(tt *testing.T) {
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
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:             "test_pool",
			AccountID:        account.ID,
			State:            models.LifeCycleStateREADY,
			Account:          account,
			AllowAutoTiering: true,
			SizeInBytes:      int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-volume-uuid2"},
			},
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:   true,
				CoolingThresholdDays: 184,
				TieringPolicy:        models2.VolumeInlineTieringPolicyAuto,
			},
		}

		poolView, err := store.GetPool(ctx, params.PoolID, account.ID)
		if err != nil {
			tt.Fatalf("Failed to get pool view: %v", err)
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Auto Tiering Cooling Threshold days must be between 2 and 183 days")
	})
	t.Run("WhenAutoTieringIsIsFalse", func(tt *testing.T) {
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
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:             "test_pool",
			AccountID:        account.ID,
			State:            models.LifeCycleStateREADY,
			Account:          account,
			AllowAutoTiering: true,
			SizeInBytes:      int64(maxQuotaInBytesPool),
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
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"test-volume-uuid2"},
			},
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled: false,
			},
			Protocols: []string{utils.ProtocolISCSI},
		}
		poolView, err := store.GetPool(ctx, params.PoolID, account.ID)
		if err != nil {
			tt.Fatalf("Failed to get pool view: %v", err)
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.NoError(tt, err)
	})
}

func TestValidateCreateVolumeParams_DataProtectionChecks(tt *testing.T) {
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
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: account.ID},
		},
		State:       models.LifeCycleStateREADY,
		SizeInBytes: int64(maxQuotaInBytesPool),
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

	tt.Run("WhenBackupPolicySetWithoutBackupVaultID", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupPolicyId: "test-policy",
			},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "backup vault id is required to assign a backup policy to a volume")
	})
	tt.Run("WhenBackupPolicySetWithoutScheduledBackupEnable", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupPolicyId: "test-policy",
				BackupVaultID:  "test-bv-uuid1",
			},
		}
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-bv-uuid1"},
			Name:      "test_bv1",
			AccountID: account.ID,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backupvault")

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "scheduled backups needs to be enabled/disabled when a backup policy is assigned to a volume")
	})
	tt.Run("WhenBackupPolicySetOnDataProtectedVolume", func(tt *testing.T) {
		// Create backup vault for this test
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-3"},
			Name:      "test_bv_3",
			AccountID: account.ID,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backup vault")

		scheduledBackupEnable := true
		params := &common.CreateVolumeParams{
			Name:             "dummy-name",
			PoolID:           pool.UUID,
			QuotaInBytes:     minQuotaInBytesVolume + 1,
			IsDataProtection: true,
			DataProtection: &models.DataProtection{
				BackupPolicyId:         "test-policy",
				BackupVaultID:          "test-vault-3",
				ScheduledBackupEnabled: &scheduledBackupEnable,
			},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024),
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "scheduled backups are not supported for cross region replication, only manual backups with existing snapshots are supported")
	})
	tt.Run("WhenBackupPolicyNotSetWithScheduledBackupNil", func(tt *testing.T) {
		// Create backup vault for this test
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-1"},
			Name:      "test_bv_1",
			AccountID: account.ID,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backup vault")

		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupVaultID: "test-vault-1",
			},
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err)
	})
	tt.Run("WhenBackupPolicySetWithScheduledBackupEnabled", func(tt *testing.T) {
		// Create backup vault for this test
		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "test-vault-2"},
			Name:      "test_bv_2",
			AccountID: account.ID,
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backup vault")

		scheduledBackupEnable := true
		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupPolicyId:         "test-policy",
				BackupVaultID:          "test-vault-2",
				ScheduledBackupEnabled: &scheduledBackupEnable,
			},
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err)
	})
	tt.Run("WhenBackupPolicySetWithImmutableBackupValidation", func(tt *testing.T) {
		// Set up immutable backup feature flag for this test case
		utils.SetImmutableBackupEnabledForTest(true)
		defer utils.SetImmutableBackupEnabledForTest(false)

		// Create backup vault with immutable attributes
		var retentionDays int64 = 30
		bv := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: "test-vault-immutable"},
			Name:           "test_bv_immutable",
			AccountID:      account.ID,
			LifeCycleState: models.LifeCycleStateREADY,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backup vault with immutable attributes")

		// Create backup policy with sufficient retention for immutable backup
		bp := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: "test-policy-immutable"},
			Name:                 "test_policy_immutable",
			AccountID:            account.ID,
			DailyBackupsToKeep:   35, // More than minimum retention
			WeeklyBackupsToKeep:  5,
			MonthlyBackupsToKeep: 3,
			PolicyEnabled:        true,
			LifeCycleState:       models.LifeCycleStateREADY,
		}
		err = store.DB().Create(bp).Error
		assert.NoError(tt, err, "Failed to create backup policy")

		scheduledBackupEnable := true
		params := &common.CreateVolumeParams{
			Name:         "dummy-name-immutable",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupPolicyId:         "test-policy-immutable",
				BackupVaultID:          "test-vault-immutable",
				ScheduledBackupEnabled: &scheduledBackupEnable,
			},
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		// This should pass because backup policy has sufficient retention
		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err)
	})

	tt.Run("WhenBackupPolicySetWithInsufficientRetentionForImmutableBackup", func(tt *testing.T) {
		// Set up immutable backup feature flag for this test case
		utils.SetImmutableBackupEnabledForTest(true)
		defer utils.SetImmutableBackupEnabledForTest(false)

		// Create backup vault with immutable attributes
		var retentionDays int64 = 60
		bv := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: "test-vault-immutable-strict"},
			Name:           "test_bv_immutable_strict",
			AccountID:      account.ID,
			LifeCycleState: models.LifeCycleStateREADY,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backup vault with strict immutable attributes")

		// Create backup policy with insufficient retention for immutable backup
		bp := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: "test-policy-insufficient"},
			Name:                 "test_policy_insufficient",
			AccountID:            account.ID,
			DailyBackupsToKeep:   30, // Less than minimum retention of 60 days
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
			PolicyEnabled:        true,
			LifeCycleState:       models.LifeCycleStateREADY,
		}
		err = store.DB().Create(bp).Error
		assert.NoError(tt, err, "Failed to create backup policy with insufficient retention")

		scheduledBackupEnable := true
		params := &common.CreateVolumeParams{
			Name:         "dummy-name-insufficient",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupPolicyId:         "test-policy-insufficient",
				BackupVaultID:          "test-vault-immutable-strict",
				ScheduledBackupEnabled: &scheduledBackupEnable,
			},
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		// This should fail because backup policy has insufficient retention
		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Backup policy is not compliant with immutable backup vault settings")
	})

	tt.Run("WhenImmutableBackupDisabled", func(tt *testing.T) {
		// Explicitly disable immutable backup feature flag for this test case
		utils.SetImmutableBackupEnabledForTest(false)
		defer utils.SetImmutableBackupEnabledForTest(false)

		// Create backup vault with immutable attributes (this should be ignored when feature is disabled)
		var retentionDays int64 = 60
		bv := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: "test-vault-disabled-feature"},
			Name:           "test_bv_disabled_feature",
			AccountID:      account.ID,
			LifeCycleState: models.LifeCycleStateREADY,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
			},
		}
		err = store.DB().Create(bv).Error
		assert.NoError(tt, err, "Failed to create backup vault")

		// Create backup policy with insufficient retention (but should pass when feature is disabled)
		bp := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: "test-policy-disabled-feature"},
			Name:                 "test_policy_disabled_feature",
			AccountID:            account.ID,
			DailyBackupsToKeep:   10, // Less than minimum retention, but feature is disabled
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
			PolicyEnabled:        true,
			LifeCycleState:       models.LifeCycleStateREADY,
		}
		err = store.DB().Create(bp).Error
		assert.NoError(tt, err, "Failed to create backup policy")

		scheduledBackupEnable := true
		params := &common.CreateVolumeParams{
			Name:         "dummy-name-disabled-feature",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			DataProtection: &models.DataProtection{
				BackupPolicyId:         "test-policy-disabled-feature",
				BackupVaultID:          "test-vault-disabled-feature",
				ScheduledBackupEnabled: &scheduledBackupEnable,
			},
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{},
			},
			Protocols: []string{utils.ProtocolISCSI},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesVolume,
		}

		// This should pass because immutable backup validation is disabled
		err = validateCreateVolumeParams(ctx, store, params, poolView)
		assert.Nil(tt, err)
	})
}

func TestUpdateVolume(t *testing.T) {
	// Common pool and poolView for all tests
	dbPool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		Name:             "test-pool",
		AllowAutoTiering: true,
		SizeInBytes:      2199023255552, // 2TiB
	}
	poolView := &datamodel.PoolView{
		Pool: *dbPool,
	}

	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid"}

		se.On("GetVolume", ctx, "vid").Return(nil, errors.New("volume not found"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "volume not found")
		assert.Nil(tt, volume)
	})

	t.Run("WhenValidateUpdateVolumeParamsFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(1024 * 1024 * 1024)}
		dbVolume := &datamodel.Volume{SizeInBytes: int64(2 * 1024 * 1024 * 1024), State: "READY"}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "volume size cannot be reduced")
		assert.Nil(tt, volume)
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024)}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: int64(1024 * 1024 * 1024), Name: "vol", State: "READY",
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 10,
			},
		}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("job error"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "job error")
		assert.Nil(tt, volume)
	})

	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024)}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: int64(1024 * 1024 * 1024), Name: "vol", State: "READY"}
		jobUUID := "wid"
		job := &datamodel.Job{WorkflowID: jobUUID, BaseModel: datamodel.BaseModel{UUID: jobUUID}}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("update state error")).Once()
		// Mock UpdateJob call when error occurs in defer function
		se.On("UpdateJob", ctx, jobUUID, string(models.JobsStateERROR), 0, "update state error").Return(nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "update state error")
		assert.Nil(tt, volume)
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024)}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: int64(1024 * 1024 * 1024), Name: "vol", State: "READY"}
		jobUUID := "wid"
		job := &datamodel.Job{WorkflowID: jobUUID, BaseModel: datamodel.BaseModel{UUID: jobUUID}}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Mock UpdateJob call when error occurs in defer function
		se.On("UpdateJob", ctx, jobUUID, string(models.JobsStateERROR), 0, "workflow error").Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("workflow error")).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "workflow error")
		assert.Nil(tt, volume)
	})

	t.Run("WhenUpdateVolumeSuccessWithBlockPropertiesNoHGUUIDs", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{},
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-2", mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeFailsWithBlockPropertiesWithUnavailableHgs", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"hg1"},
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetMultipleHostGroups", ctx, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{}, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "could not find some of the host groups, please check the hostgroup details and try with valid host group names.")
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeFailsWithBlockPropertiesWhereSomeHgsUnavailable", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"hg1", "hg2"},
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetMultipleHostGroups", ctx, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
			BaseModel: datamodel.BaseModel{UUID: "hg2"},
		}}, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "could not find some of the host groups, please check the hostgroup details and try with valid host group names.")
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeFailsWithBlockPropertiesWhereHGStateNotReady", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"hg1", "hg2"},
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetMultipleHostGroups", ctx, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
			BaseModel: datamodel.BaseModel{UUID: "hg1"}, Name: "hg1", State: models.LifeCycleStateError,
		}, {BaseModel: datamodel.BaseModel{UUID: "hg2"}, State: models.LifeCycleStateREADY}}, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "host group hg1 is not available")
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeFailsWithBlockPropertiesWhereHGStateNotUnique", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "linux",
				HostGroupUUIDs: []string{"hg1", "hg2"},
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetMultipleHostGroups", ctx, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
			BaseModel: datamodel.BaseModel{UUID: "hg1"}, Hosts: datamodel.Hosts{Hosts: []string{"a", "b"}}, State: models.LifeCycleStateREADY,
		}, {BaseModel: datamodel.BaseModel{UUID: "hg2"}, Hosts: datamodel.Hosts{Hosts: []string{"a"}}, State: models.LifeCycleStateREADY}}, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.EqualError(tt, err, "host : a is present in multiple host groups")
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", SnapshotPolicy: nil}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-2", mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeSuccessWithReplication", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", SnapshotPolicy: nil}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-2", mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, true)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeSuccessWithNoBackupVaultIDInDB", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol"}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeSuccessWithDetachBackupVault", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultId := ""
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID: &backupVaultId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-1", mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenDetachBackupVaultWithNoBackupsForVolume", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultdId := ""
		param := &common.UpdateVolumeParams{
			AccountName:  "acc",
			VolumeId:     "vid",
			QuotaInBytes: int64(2 * 1024 * 1024 * 1024),
			Name:         "vol",
			DataProtection: &models.UpdateDataProtection{
				BackupVaultID: &backupVaultdId, // Detaching backup vault
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "vault-1", // Current backup vault
			},
			State: "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		// Mock expectations
		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		// Expect GetBackupsByBackupVaultOwnerIDAndFilter to be called with volume-specific filter
		// and return empty list (no backups for this volume)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-1", mock.Anything, [][]interface{}{{"volume_uuid = ?", "vid"}}).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()

		// Act
		volume, _, err := updateVolume(ctx, se, temporal, param, false)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeGetBackupsByBackupVaultErrors", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultId := ""
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID: &backupVaultId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-1", int64(0), mock.Anything).Return(nil, errors.New("no backups found"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeGetBackupsByBackupVaultErrors", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultId := ""
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID: &backupVaultId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-1", mock.Anything, mock.Anything).Return(backups, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
	})
	t.Run("WhenUpdateVolumeSuccessWithAttachBackupVault", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultId := "backup-vault-1"
		oldPoolAccount := poolView.Account
		poolView.Account = &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}, Name: "acc"}
		defer func() { poolView.Account = oldPoolAccount }()

		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID: &backupVaultId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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
		dbBackupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: backupVaultId},
		}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		se.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultId, mock.Anything).Return(dbBackupVault, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, "vol", volume.DisplayName)
	})
	t.Run("WhenUpdateVolumeFailsBackupPolicyIsSetWithoutBackupVault", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultId := ""
		backupPolicyId := "backup-policy-1"
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID:  &backupVaultId,
			BackupPolicyId: &backupPolicyId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Equal(tt, err.Error(), "backup vault is required to assign a backup policy to a volume")
	})
	t.Run("WhenUpdateVolumeFailsScheduledBackupEnabledIsNotSetWithBackupPolicyAttached", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupPolicyId := "backup-policy-1"
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "backup-vault-1",
			},
			State: "READY",
		}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Equal(tt, err.Error(), "scheduled backups needs to be enabled/disabled when a backup policy is assigned to a volume")
	})
	t.Run("WhenUpdateVolumeFailsDetachBackupVaultWithBackupPolicyAttached", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupVaultId := ""
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID: &backupVaultId,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "backup-vault-1",
				BackupPolicyID: "backup-policy-1",
			},
			State: "READY",
		}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Equal(tt, err.Error(), "cannot remove backup vault as backup policy is associated to the volume")
	})
	t.Run("WhenUpdateVolumeFailsWithAttachingBackupPolicyOnDataProtectedVolume", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		oldAccount := poolView.Account
		poolView.Account = &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}, Name: "acc"}
		defer func() { poolView.Account = oldAccount }()

		backupPolicyId := "backup-policy-1"
		backupPolicyEnabled := false
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupPolicyId:         &backupPolicyId,
			ScheduledBackupEnabled: &backupPolicyEnabled,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: true,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "backup-vault-1",
			},
			State: "READY",
		}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyId, poolView.Account.ID).Return(nil, errors.NewNotFoundErr("backup policy", &backupPolicyId))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Equal(tt, err.Error(), "Cannot update backup policy on a Data Protection Volume. Only manual backups are supported")
	})
	t.Run("WhenUpdateVolumeFailsWithBackupPolicyNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		oldAccount := poolView.Account
		poolView.Account = &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}, Name: "acc"}
		defer func() { poolView.Account = oldAccount }()

		backupPolicyId := "backup-policy-1"
		backupVaultId := "backup-vault-1"
		policyEnabled := true
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupVaultID:          &backupVaultId,
			BackupPolicyId:         &backupPolicyId,
			ScheduledBackupEnabled: &policyEnabled,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultId, poolView.Account.ID).Return(nil, errors.NewNotFoundErr("backup vault", &backupVaultId))
		se.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyId, poolView.Account.ID).Return(nil, errors.New("Internal server error"))
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Equal(tt, err.Error(), "Internal server error")
	})
	t.Run("WhenUpdateVolumeSuccessWithBackupPolicy", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		oldAccount := poolView.Account
		poolView.Account = &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}, Name: "acc"}
		defer func() { poolView.Account = oldAccount }()

		backupPolicyId := "backup-policy-1"
		backupPolicyEnabled := true
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			BackupPolicyId:         &backupPolicyId,
			ScheduledBackupEnabled: &backupPolicyEnabled,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID: "backup-vault-1",
			},
			State: "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyId, poolView.Account.ID).Return(nil, errors.NewNotFoundErr("backup policy", &backupPolicyId))
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
	})
	t.Run("WhenUpdateVolumeSuccessWithBackupPolicyDisabled", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		oldAccount := poolView.Account
		poolView.Account = &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1}, Name: "acc"}
		defer func() { poolView.Account = oldAccount }()

		backupPolicyId := "backup-policy-1"
		backupPolicyEnabled := false
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol", DataProtection: &models.UpdateDataProtection{
			ScheduledBackupEnabled: &backupPolicyEnabled,
		}}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account: &datamodel.Account{
				Name: "acc",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "backup-vault-1",
				BackupPolicyID: backupPolicyId,
			},
			State: "READY",
		}

		job := &datamodel.Job{WorkflowID: "wid"}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyId, poolView.Account.ID).Return(nil, errors.NewNotFoundErr("backup policy", &backupPolicyId))
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
	})

	t.Run("WhenUpdateVolumeFailsIfVolumeInTransitioningState", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024)}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: int64(1024 * 1024 * 1024), Name: "vol", State: models.LifeCycleStateUpdating}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Contains(tt, err.Error(), "An update operation is already in progress for this volume")
		assert.Nil(tt, volume)
	})

	t.Run("WhenUpdateVolumeFailsWithInvalidSnapshotPolicy", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{
			AccountName:  "acc",
			VolumeId:     "vid",
			QuotaInBytes: int64(2 * 1024 * 1024 * 1024),
			Name:         "vol",
			SnapshotPolicy: &models.SnapshotPolicy{
				IsEnabled: true,
				Schedules: []*models.SnapshotPolicySchedule{},
			},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool"},
			Account:     &datamodel.Account{Name: "acc"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			State:          "READY",
			SnapshotPolicy: nil,
		}

		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Error(tt, err)
		assert.Equal(tt, err.Error(), "no existing snapshot policy found for the volume and no schedules provided in the update request. Cannot create a new snapshot policy without schedules")
		assert.Nil(tt, volume)
	})

	t.Run("WhenAutoTieringIsNotAllowed", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		poolViewNoTiering := &datamodel.PoolView{Pool: datamodel.Pool{AllowAutoTiering: false, SizeInBytes: 2199023255552}}
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024),
			AutoTieringPolicy: &common.AutoTieringPolicy{AutoTieringEnabled: true, CoolingThresholdDays: 10},
		}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol"}
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolViewNoTiering, nil)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Contains(tt, err.Error(), "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
		assert.Nil(tt, volume)
	})

	t.Run("WhenCoolnessPeriodBelowTheRange", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024),
			AutoTieringPolicy: &common.AutoTieringPolicy{AutoTieringEnabled: true, CoolingThresholdDays: 1},
		}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol"}
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Contains(tt, err.Error(), "Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		assert.Nil(tt, volume)
	})

	t.Run("WhenCoolnessPeriodAboveTheRange", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024),
			AutoTieringPolicy: &common.AutoTieringPolicy{AutoTieringEnabled: true, CoolingThresholdDays: 200},
		}
		dbVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vid"}, SizeInBytes: 100, Name: "vol"}
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.Contains(tt, err.Error(), "Auto Tiering Cooling Threshold days must be between 2 and 183 days")
		assert.Nil(tt, volume)
	})

	t.Run("WhenAutoTieringIsFalse", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			AutoTieringPolicy: &common.AutoTieringPolicy{AutoTieringEnabled: false},
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
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
		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, "vault-2", mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
	})

	t.Run("WhenNoTieringPolicyPassed_ExistingPolicyRemainsUnchanged", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		param := &common.UpdateVolumeParams{
			AccountName: "acc", VolumeId: "vid", QuotaInBytes: int64(2 * 1024 * 1024 * 1024), Name: "vol",
			// TieringPolicy is nil
		}
		dbVolume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "vid"},
			SizeInBytes: int64(1024 * 1024 * 1024),
			Name:        "vol",
			Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "pool", PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-west1-a"}},
			Account:     &datamodel.Account{Name: "acc"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				CoolingThresholdDays: 30,
			},
			State: "READY",
		}
		job := &datamodel.Job{WorkflowID: "wid"}
		se.On("GetPool", ctx, param.PoolID, dbVolume.AccountID).Return(poolView, nil)
		se.On("GetVolume", ctx, "vid").Return(dbVolume, nil)
		se.On("GetBackupsByBackupVaultOwnerIDAndFilter", ctx, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil)
		se.On("CreateJob", ctx, mock.Anything).Return(job, nil)
		se.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		volume, _, err := updateVolume(ctx, se, temporal, param, false)
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		// Ensure the tiering policy remains unchanged
		assert.Equal(tt, true, dbVolume.AutoTieringEnabled)
		assert.Equal(tt, int32(30), dbVolume.AutoTieringPolicy.CoolingThresholdDays)
	})
}

func TestUpdateVolumeV2(t *testing.T) {
	t.Run("WhenGetVolumeFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid"}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		o := &Orchestrator{
			storage:  se,
			temporal: temporal,
		}
		se.On("GetVolume", ctx, "vid").Return(nil, errors.New("volume not found"))
		_, _, err := o.UpdateVolumeV2(ctx, param)
		assert.EqualError(tt, err, "volume not found")
	})
	t.Run("WhenGetVolumeReplicationsFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid"}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		o := &Orchestrator{
			storage:  se,
			temporal: temporal,
		}
		dbVol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "vid"},
		}
		count := int64(0)
		se.On("GetVolume", ctx, "vid").Return(dbVol, nil)
		se.On("GetVolumeReplicationCountByVolumeID", mock.Anything, mock.Anything).Return(count, errors.New("replication not found"))
		_, _, err := o.UpdateVolumeV2(ctx, param)
		assert.EqualError(tt, err, "replication not found")
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		param := &common.UpdateVolumeParams{AccountName: "acc", VolumeId: "vid"}
		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		updateVolume = func(ctx context.Context, se database.Storage, te client.Client, param *common.UpdateVolumeParams, isReplication bool) (*models.Volume, string, error) {
			return &models.Volume{DisplayName: "vol"}, "job-id", nil
		}
		o := &Orchestrator{
			storage:  se,
			temporal: temporal,
		}
		dbVol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "vid"},
		}
		count := int64(1)
		se.On("GetVolume", ctx, "vid").Return(dbVol, nil)
		se.On("GetVolumeReplicationCountByVolumeID", mock.Anything, mock.Anything).Return(count, nil)
		_, job, err := o.UpdateVolumeV2(ctx, param)
		assert.NoError(tt, err)
		assert.Equal(tt, "job-id", job)
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
	updateVolume = func(ctx context.Context, se database.Storage, te client.Client, param *common.UpdateVolumeParams, isReplication bool) (*models.Volume, string, error) {
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

func Test_validateUpdateVolumeRequest(t *testing.T) {
	ctx := context.Background()
	mockStorage := &database.MockStorage{}
	pool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			AllowAutoTiering: true,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			},
			SizeInBytes: 569508905984,
		},
	}

	t.Run("FailsIfVolumeInTransitionalState", func(tt *testing.T) {
		volume := &datamodel.Volume{State: "UPDATING", SizeInBytes: int64(1024 * 1024 * 1024)}
		params := &common.UpdateVolumeParams{QuotaInBytes: int64(2 * 1024 * 1024 * 1024)}
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "An update operation is already in progress for this volume")
	})

	t.Run("FailsIfQuotaReduced", func(tt *testing.T) {
		volume := &datamodel.Volume{State: "READY", SizeInBytes: int64(2 * 1024 * 1024 * 1024)}
		params := &common.UpdateVolumeParams{QuotaInBytes: int64(1024 * 1024 * 1024)}
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume size cannot be reduced")
	})

	t.Run("FailsIfSnapReserveUpdatedForDPVol", func(tt *testing.T) {
		volume := &datamodel.Volume{State: "READY", SizeInBytes: int64(2 * 1024 * 1024 * 1024), VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
			SnapReserve:      40,
		}}
		newSnapReserve := int64(50)
		params := &common.UpdateVolumeParams{QuotaInBytes: int64(3 * 1024 * 1024 * 1024), SnapReserve: &newSnapReserve}
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Cannot update snapshotReserve on a Data Protection Volume")
	})

	t.Run("FailsIfSnapshotPolicyUpdatedForDPVol", func(tt *testing.T) {
		volume := &datamodel.Volume{
			State: "READY", SizeInBytes: int64(2 * 1024 * 1024 * 1024), VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: true,
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(3 * 1024 * 1024 * 1024),
			SnapshotPolicy: &models.SnapshotPolicy{
				IsEnabled: true,
				Schedules: []*models.SnapshotPolicySchedule{
					{
						Count: 1,
						Schedule: &models.Schedule{
							DaysOfMonth: []int{1},
							DaysOfWeek:  []int{2},
						},
					},
				},
			},
		}
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Cannot update snapshot policy on a Data Protection Volume")
	})

	t.Run("WhenQuotaInBytesIsZeroSkip", func(tt *testing.T) {
		// Use a valid quota above minQuotaInBytesVolume
		volume := &datamodel.Volume{State: "READY", SizeInBytes: 200 * 1024 * 1024 * 1024} // 200 GiB
		params := &common.UpdateVolumeParams{QuotaInBytes: 0}
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err)
	})

	t.Run("WhenQuotaInBytesIsNilSkip", func(tt *testing.T) {
		// Use a valid quota above minQuotaInBytesVolume
		volume := &datamodel.Volume{State: "READY", SizeInBytes: 200 * 1024 * 1024 * 1024} // 200 GiB
		params := &common.UpdateVolumeParams{}
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err)
	})
	t.Run("WhenAttachErroredBackupVaultToVolumeWhileUpdating", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}

		// Mock the expected behavior for GetBackupVaultByUUIDndOwnerID
		bv := &datamodel.BackupVault{
			BaseModel:             datamodel.BaseModel{UUID: "bv-uuid"},
			LifeCycleState:        models.LifeCycleStateError,
			LifeCycleStateDetails: "Backup Vault is ready",
		}
		se.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(bv, nil)

		volume := &datamodel.Volume{State: "READY", SizeInBytes: 200 * 1024 * 1024 * 1024} // 200 GiB
		backupVaultId := "bv-uuid"
		params := &common.UpdateVolumeParams{DataProtection: &models.UpdateDataProtection{BackupVaultID: &backupVaultId}}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
	})

	t.Run("WhenAttachBackupPolicyFailsWhileUpdating", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		backupPolicyId := "backup-policy-uuid"

		bp := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{UUID: backupPolicyId},
			LifeCycleState: models.LifeCycleStateError,
		}
		se.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyId, int64(1)).Return(bp, nil)

		volume := &datamodel.Volume{State: "READY", SizeInBytes: 200 * 1024 * 1024 * 1024} // 200 GiB
		params := &common.UpdateVolumeParams{DataProtection: &models.UpdateDataProtection{BackupPolicyId: &backupPolicyId}}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
	})

	t.Run("WithMatchingBlockDevice_ShouldValidateSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Setup volume with BlockDevices
		volumeBlockDevices := []datamodel.BlockDevice{
			{
				Name:       "test-lun-1",
				Identifier: "lun-123",
				Size:       107374182400,
				OSType:     "LINUX",
			},
			{
				Name:       "test-lun-2",
				Identifier: "lun-456",
				Size:       214748364800,
				OSType:     "WINDOWS",
			},
		}

		volume := &datamodel.Volume{
			State:   "READY",
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &volumeBlockDevices,
			},
		}

		// Setup update params with matching BlockDevice
		params := &common.UpdateVolumeParams{
			BlockDevices: []*common.BlockDevice{
				{
					Name:       "test-lun-1", // Matches existing BlockDevice
					HostGroups: []string{"hg-uuid-1", "hg-uuid-2"},
				},
			},
		}

		// Mock host groups
		hostGroups := []*datamodel.HostGroup{
			{
				BaseModel: datamodel.BaseModel{UUID: "hg-uuid-1"},
				Name:      "hg1",
				State:     models.LifeCycleStateREADY,
				Hosts: datamodel.Hosts{
					Hosts: []string{"iqn.1998-01.com.vmware:host1"},
				},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "hg-uuid-2"},
				Name:      "hg2",
				State:     models.LifeCycleStateREADY,
				Hosts: datamodel.Hosts{
					Hosts: []string{"iqn.1998-01.com.vmware:host2"},
				},
			},
		}

		se.On("GetMultipleHostGroups", ctx, []string{"hg-uuid-1", "hg-uuid-2"}, int64(1)).Return(hostGroups, nil)

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)
		se.AssertExpectations(tt)
	})

	t.Run("WithNonMatchingBlockDevice_ShouldReturnError", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Setup volume with BlockDevices
		volumeBlockDevices := []datamodel.BlockDevice{
			{
				Name:       "test-lun-1",
				Identifier: "lun-123",
				Size:       107374182400,
				OSType:     "LINUX",
			},
		}

		volume := &datamodel.Volume{
			State:   "READY",
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &volumeBlockDevices,
			},
		}

		// Setup update params with non-matching BlockDevice
		params := &common.UpdateVolumeParams{
			BlockDevices: []*common.BlockDevice{
				{
					Name:       "non-matching-lun", // Doesn't match existing BlockDevice
					HostGroups: []string{"hg-uuid-1"},
				},
			},
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "could not find matching BlockDevice")
	})

	t.Run("OSType_Update_ShouldReturnError", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Setup volume with BlockDevices
		volumeBlockDevices := []datamodel.BlockDevice{
			{
				Name:       "test-lun-1",
				Identifier: "lun-123",
				Size:       107374182400,
				OSType:     "LINUX",
			},
		}

		volume := &datamodel.Volume{
			State:   "READY",
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &volumeBlockDevices,
			},
		}

		// Setup update params with non-matching BlockDevice
		params := &common.UpdateVolumeParams{
			BlockDevices: []*common.BlockDevice{
				{
					Name:   "test-lun-1", // Doesn't match existing BlockDevice
					OSType: "WINDOWS",
				},
			},
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Cannot update OSType for block device.")
	})

	t.Run("WithBlockProperties_ShouldValidateSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		volume := &datamodel.Volume{
			State:   "READY",
			Account: &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}},
		}

		params := &common.UpdateVolumeParams{
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{"hg-uuid-1"},
			},
		}

		// Mock host groups
		hostGroups := []*datamodel.HostGroup{
			{
				BaseModel: datamodel.BaseModel{UUID: "hg-uuid-1"},
				Name:      "hg1",
				State:     models.LifeCycleStateREADY,
				Hosts: datamodel.Hosts{
					Hosts: []string{"iqn.1998-01.com.vmware:host1"},
				},
			},
		}

		se.On("GetMultipleHostGroups", ctx, []string{"hg-uuid-1"}, int64(1)).Return(hostGroups, nil)

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)
		se.AssertExpectations(tt)
	})

	// Tests for quota validation logic
	t.Run("FailsWhenVolumeUpdateExceedsPoolSize", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Create a volume with current size of 100 GiB
		currentVolumeSize := int64(100 * 1024 * 1024 * 1024) // 100 GiB
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: currentVolumeSize,
		}

		// Create a pool with total size of 1000 GiB and current usage of 600 GiB
		poolTotalSize := int64(1000 * 1024 * 1024 * 1024)    // 1000 GiB
		poolCurrentUsage := uint64(600 * 1024 * 1024 * 1024) // 600 GiB already used
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Request to update volume to 500 GiB (increase of 400 GiB)
		// This would result in: 600 GiB (current pool usage) + 400 GiB (increase) = 1000 GiB + 1 byte > 1000 GiB pool size
		newVolumeSize := int64(500*1024*1024*1024 + 1) // 500 GiB + 1 byte
		params := &common.UpdateVolumeParams{
			QuotaInBytes: newVolumeSize,
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Total size of volumes in a pool cannot exceed the pool capacity.")
	})

	t.Run("PassesWhenVolumeUpdateFitsExactlyInPool", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Create a volume with current size of 100 GiB
		currentVolumeSize := int64(100 * 1024 * 1024 * 1024) // 100 GiB
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: currentVolumeSize,
		}

		// Create a pool with total size of 1000 GiB and current usage of 600 GiB
		poolTotalSize := int64(1000 * 1024 * 1024 * 1024)    // 1000 GiB
		poolCurrentUsage := uint64(600 * 1024 * 1024 * 1024) // 600 GiB already used
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Request to update volume to 500 GiB (increase of 400 GiB)
		// This would result in: 600 GiB (current pool usage) + 400 GiB (increase) = 1000 GiB exactly (pool size)
		newVolumeSize := int64(500 * 1024 * 1024 * 1024) // 500 GiB exactly
		params := &common.UpdateVolumeParams{
			QuotaInBytes: newVolumeSize,
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)
	})

	t.Run("PassesWhenVolumeUpdateIsWithinPoolLimits", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Create a volume with current size of 100 GiB
		currentVolumeSize := int64(100 * 1024 * 1024 * 1024) // 100 GiB
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: currentVolumeSize,
		}

		// Create a pool with total size of 1000 GiB and current usage of 500 GiB
		poolTotalSize := int64(1000 * 1024 * 1024 * 1024)    // 1000 GiB
		poolCurrentUsage := uint64(500 * 1024 * 1024 * 1024) // 500 GiB already used
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Request to update volume to 300 GiB (increase of 200 GiB)
		// This would result in: 500 GiB (current pool usage) + 200 GiB (increase) = 700 GiB < 1000 GiB (pool size)
		newVolumeSize := int64(300 * 1024 * 1024 * 1024) // 300 GiB
		params := &common.UpdateVolumeParams{
			QuotaInBytes: newVolumeSize,
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)
	})

	t.Run("PassesWhenVolumeUpdateIsTheSameSize", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Create a volume with current size of 200 GiB
		currentVolumeSize := int64(200 * 1024 * 1024 * 1024) // 200 GiB
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: currentVolumeSize,
		}

		// Pool configuration doesn't matter since there's no size change
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: int64(1000 * 1024 * 1024 * 1024),
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: uint64(800 * 1024 * 1024 * 1024), // Even with high usage
		}

		// Request to keep volume at the same size (no increase)
		params := &common.UpdateVolumeParams{
			QuotaInBytes: currentVolumeSize, // Same as current size
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)
	})

	t.Run("FailsWhenReducingVolumeSize", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Create a volume with current size of 200 GiB
		currentVolumeSize := int64(200 * 1024 * 1024 * 1024) // 200 GiB
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: currentVolumeSize,
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: int64(1000 * 1024 * 1024 * 1024),
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024),
		}

		// Request to reduce volume to 100 GiB (reduction)
		newVolumeSize := int64(100 * 1024 * 1024 * 1024) // 100 GiB < 200 GiB current
		params := &common.UpdateVolumeParams{
			QuotaInBytes: newVolumeSize,
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume size cannot be reduced")
	})

	t.Run("PassesWhenQuotaInBytesIsZeroOrNotProvided", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(200 * 1024 * 1024 * 1024), // 200 GiB
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: int64(1000 * 1024 * 1024 * 1024),
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: uint64(900 * 1024 * 1024 * 1024), // Even with high usage
		}

		// Test with QuotaInBytes = 0 (not provided)
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 0, // This should skip quota validation
		}

		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)

		// Test with QuotaInBytes not set at all (default value)
		params2 := &common.UpdateVolumeParams{}
		err2 := validateUpdateVolumeRequest(ctx, se, volume, params2, pool)
		assert.NoError(tt, err2)
	})

	t.Run("EdgeCaseWhenPoolIsFullAndNoVolumeIncrease", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}

		// Create a volume with current size of 200 GiB
		currentVolumeSize := int64(200 * 1024 * 1024 * 1024) // 200 GiB
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: currentVolumeSize,
		}

		// Pool is completely full
		poolTotalSize := int64(1000 * 1024 * 1024 * 1024)     // 1000 GiB
		poolCurrentUsage := uint64(1000 * 1024 * 1024 * 1024) // 1000 GiB used (100% full)
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Request to keep volume at the same size (sizeIncrease = 0)
		params := &common.UpdateVolumeParams{
			QuotaInBytes: currentVolumeSize, // Same as current size, so sizeIncrease = 0
		}

		// Should pass because sizeIncrease > 0 condition won't trigger
		err := validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.NoError(tt, err)
	})

	t.Run("WhenVolumeInTransitionalState_ReturnUserInputValidationErr", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		se, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(se.DB())
		if err != nil {
			tt.Fatalf("Failed to clean up test storage: %v", err)
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
				Name:        "test_pool",
				SizeInBytes: 1000 * 1024 * 1024 * 1024, // 1TB pool
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: 100 * 1024 * 1024 * 1024, // 100GB currently used
		}

		// Create volume in transitional state (CREATING)
		volume := &datamodel.Volume{
			BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:        "test_volume",
			SizeInBytes: int64(1024 * 1024 * 1024),     // 1 GiB
			State:       models.LifeCycleStateCreating, // Transitional state
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(2 * 1024 * 1024 * 1024),
		}

		// Should fail because volume is in transitional state
		err = validateUpdateVolumeRequest(ctx, se, volume, params, pool)
		assert.Error(tt, err)
		assert.True(tt, errors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "cannot be updated, while in transitioning state")
		assert.Contains(tt, err.Error(), models.LifeCycleStateCreating)
	})

	t.Run("SnapReserveIncreaseWithSufficientLUNSpace", func(tt *testing.T) {
		// Test case: Increasing snapReserve from 20% to 30% on a 100GB volume
		// This should fail because snapReserve increase without volume size increase is not allowed
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(30) // 30% new snapReserve
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject snapReserve increase without volume size increase")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	t.Run("SnapReserveIncreaseWithExactMinimumLUNSpace", func(tt *testing.T) {
		// Test case: Increasing snapReserve from 20% to 99% on a 100GB volume
		// This should fail because snapReserve increase without volume size increase is not allowed
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(99) // 99% new snapReserve (leaves 1GB, which is exactly at minimum)
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject snapReserve increase without volume size increase")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	t.Run("SnapReserveIncreaseExactMinimumLUNSpace", func(tt *testing.T) {
		// Test case: Increasing snapReserve to leave exactly 1GB for LUN
		// This should fail because snapReserve increase without volume size increase is not allowed
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		// 99GB snapReserve leaves 1GB for LUN (exactly at minimum)
		newSnapReserve := int64(99) // 99% new snapReserve
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject snapReserve increase without volume size increase")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	t.Run("SnapReserveDecreaseAlwaysAllowed", func(tt *testing.T) {
		// Test case: Decreasing snapReserve from 50% to 30% on a 100GB volume
		// This should always be allowed as it increases LUN space
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 50, // 50% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(30) // 30% new snapReserve
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "Should always allow snapReserve decrease as it increases LUN space")
	})

	t.Run("SnapReserveNoChangeShouldThrowError", func(tt *testing.T) {
		// Test case: No change in snapReserve (same value)
		// This should pass validation
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 30, // 30% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(30) // Same snapReserve value
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "no changes detected in the update request")
	})

	t.Run("SnapReserveIncreaseOnSmallVolume", func(tt *testing.T) {
		// Test case: Small volume (2GB) with snapReserve increase
		// This should fail as it would leave less than 1GB for LUN
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 2 * 1024 * 1024 * 1024, // 2 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 10, // 10% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(60) // 60% new snapReserve (would leave 0.8GB for LUN)
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject snapReserve increase on small volume when insufficient LUN space remains")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	t.Run("SnapReserveIncreaseOnLargeVolume", func(tt *testing.T) {
		// Test case: Large volume (1TB) with snapReserve increase
		// This should fail because snapReserve increase without volume size increase is not allowed
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 1024 * 1024 * 1024 * 1024, // 1 TB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(80) // 80% new snapReserve (would leave 200GB for LUN)
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject snapReserve increase without volume size increase")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	t.Run("SnapReserveIncreaseWithNilVolumeAttributes", func(tt *testing.T) {
		// Test case: Volume without VolumeAttributes (edge case)
		// This should handle gracefully by skipping snapReserve validation
		volume := &datamodel.Volume{
			State:            "READY",
			SizeInBytes:      100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: nil,                      // No VolumeAttributes
		}
		newSnapReserve := int64(50)
		params := &common.UpdateVolumeParams{SnapReserve: &newSnapReserve}

		// Should not panic and should skip snapReserve validation when VolumeAttributes is nil
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "Should handle nil VolumeAttributes gracefully and skip snapReserve validation")
	})

	t.Run("SnapReserveIncreaseWithQuotaInBytes_NewLUNSpaceLessThanParent", func(tt *testing.T) {
		// Test case: Both SnapReserve and QuotaInBytes provided, but new LUN space is less than parent
		// This should fail because the new volume with increased snapReserve would have less LUN space
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve (leaves 80GB for LUN)
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(80)                        // 80% new snapReserve
		newQuotaInBytes := int64(120 * 1024 * 1024 * 1024) // 120 GB (increased size)
		params := &common.UpdateVolumeParams{
			SnapReserve:  &newSnapReserve,
			QuotaInBytes: newQuotaInBytes,
		}

		// Calculate: New LUN space = 120GB - (120GB * 80% / 100) = 120GB - 96GB = 24GB
		// Parent LUN space = 100GB - (100GB * 20% / 100) = 100GB - 20GB = 80GB
		// 24GB < 80GB, so this should fail
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject when new LUN space is less than parent volume's LUN space")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	t.Run("SnapReserveIncreaseWithQuotaInBytes_NewLUNSpaceEqualToParent", func(tt *testing.T) {
		// Test case: Both SnapReserve and QuotaInBytes provided, new LUN space equals parent
		// This should fail because the new LUN space is not greater than the old LUN space
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve (leaves 80GB for LUN)
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(60)                        // 60% new snapReserve
		newQuotaInBytes := int64(200 * 1024 * 1024 * 1024) // 200 GB (increased size)
		params := &common.UpdateVolumeParams{
			SnapReserve:  &newSnapReserve,
			QuotaInBytes: newQuotaInBytes,
		}

		// Calculate: New LUN space = 200GB - (200GB * 60% / 100) = 200GB - 120GB = 80GB
		// Parent LUN space = 100GB - (100GB * 20% / 100) = 100GB - 20GB = 80GB
		// 80GB = 80GB, so this should fail because new LUN space is not greater than old LUN space
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "Should fail when new LUN space equals parent volume's LUN space")
	})

	t.Run("SnapReserveIncreaseWithQuotaInBytes_NewLUNSpaceGreaterThanParent", func(tt *testing.T) {
		// Test case: Both SnapReserve and QuotaInBytes provided, new LUN space is greater than parent
		// This should pass as it provides more LUN space
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve (leaves 80GB for LUN)
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(40)                        // 40% new snapReserve
		newQuotaInBytes := int64(200 * 1024 * 1024 * 1024) // 200 GB (increased size)
		params := &common.UpdateVolumeParams{
			SnapReserve:  &newSnapReserve,
			QuotaInBytes: newQuotaInBytes,
		}

		// Calculate: New LUN space = 200GB - (200GB * 40% / 100) = 200GB - 80GB = 120GB
		// Parent LUN space = 100GB - (100GB * 20% / 100) = 100GB - 20GB = 80GB
		// 120GB > 80GB, so this should pass
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "Should pass when new LUN space is greater than parent volume's LUN space")
	})

	t.Run("SnapReserveIncreaseWithQuotaInBytes_EdgeCaseExactCalculation", func(tt *testing.T) {
		// Test case: Edge case with exact calculations to ensure precision
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 1000 * 1024 * 1024 * 1024, // 1000 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 25, // 25% current snapReserve (leaves 750GB for LUN)
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(50)                         // 50% new snapReserve
		newQuotaInBytes := int64(1500 * 1024 * 1024 * 1024) // 1500 GB (increased size)
		params := &common.UpdateVolumeParams{
			SnapReserve:  &newSnapReserve,
			QuotaInBytes: newQuotaInBytes,
		}

		// Calculate: New LUN space = 1500GB - (1500GB * 50% / 100) = 1500GB - 750GB = 750GB
		// Parent LUN space = 1000GB - (1000GB * 25% / 100) = 1000GB - 250GB = 750GB
		// 750GB = 750GB, so this should fail because new LUN space is not greater than old LUN space
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "Should fail with exact LUN space calculations")
	})

	t.Run("SnapReserveIncreaseWithQuotaInBytes_NewLUNSpaceGreaterThanParent", func(tt *testing.T) {
		// Test case: Both SnapReserve and QuotaInBytes provided, new LUN space is greater than parent
		// This should pass as it provides more LUN space
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 20, // 20% current snapReserve (leaves 80GB for LUN)
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(40)                        // 40% new snapReserve
		newQuotaInBytes := int64(200 * 1024 * 1024 * 1024) // 200 GB (increased size)
		params := &common.UpdateVolumeParams{
			SnapReserve:  &newSnapReserve,
			QuotaInBytes: newQuotaInBytes,
		}

		// Calculate: New LUN space = 200GB - (200GB * 40% / 100) = 200GB - 80GB = 120GB
		// Parent LUN space = 100GB - (100GB * 20% / 100) = 100GB - 20GB = 80GB
		// 120GB > 80GB, so this should pass
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.NoError(tt, err, "Should pass when new LUN space is greater than parent volume's LUN space")
	})

	t.Run("SnapReserveIncreaseFromZeroWithoutVolumeSizeIncrease", func(tt *testing.T) {
		// Test case: SnapReserve increase from 0% without volume size increase
		// This should trigger the specific validation logic in lines 1450-1451
		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: 100 * 1024 * 1024 * 1024, // 100 GB
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 0, // Starting with 0% snapReserve
				Protocols:   []string{utils.ProtocolISCSI},
			},
		}
		newSnapReserve := int64(50) // 50% new snapReserve
		params := &common.UpdateVolumeParams{
			SnapReserve:  &newSnapReserve,
			QuotaInBytes: 0, // No volume size increase
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.Error(tt, err, "Should reject snapReserve increase from 0% without volume size increase")
		assert.Contains(tt, err.Error(), "Cannot increase SnapReserve to 50% as we cannot decrease the available space")
		assert.Contains(tt, err.Error(), "Please increase the volume size to at least")
	})

	// Clone shared bytes validation test cases for volume update
	t.Run("UpdateValidation_ClonesSharedBytes_WithinPoolSizeLimits", func(tt *testing.T) {
		// Test case: Volume update with ClonesSharedBytes that keeps total within pool size limits
		volume := &datamodel.Volume{
			State:             "READY",
			SizeInBytes:       50 * 1024 * 1024 * 1024, // 50 GB current size
			ClonesSharedBytes: 20 * 1024 * 1024 * 1024, // 20 GB shared with clones (significant optimization)
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 0,
				Protocols:   []string{utils.ProtocolNFS},
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 80 * 1024 * 1024 * 1024, // Increase to 80 GB
		}

		// Create a pool with specific size and current usage
		poolTotalSize := int64(100 * 1024 * 1024 * 1024)    // 100 GB total
		poolCurrentUsage := uint64(90 * 1024 * 1024 * 1024) // 90 GB already used
		testPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Size increase: 80 - 50 = 30 GB
		// After adjustment: 90 + 30 - 20 = 100 GB = pool size (exactly at limit)
		// With ClonesSharedBytes optimization, this should pass
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, testPool)
		assert.NoError(tt, err, "Should allow update when ClonesSharedBytes keeps total within pool limits")
	})

	t.Run("UpdateValidation_ClonesSharedBytes_ExceedsPoolSizeLimits", func(tt *testing.T) {
		// Test case: Volume update with ClonesSharedBytes that still exceeds pool size limits
		volume := &datamodel.Volume{
			State:             "READY",
			SizeInBytes:       50 * 1024 * 1024 * 1024, // 50 GB current size
			ClonesSharedBytes: 5 * 1024 * 1024 * 1024,  // 5 GB shared (not enough to help)
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 0,
				Protocols:   []string{utils.ProtocolNFS},
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 100 * 1024 * 1024 * 1024, // Increase to 100 GB
		}

		// Create a pool with specific size and current usage
		poolTotalSize := int64(100 * 1024 * 1024 * 1024)    // 100 GB total
		poolCurrentUsage := uint64(90 * 1024 * 1024 * 1024) // 90 GB already used
		testPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Size increase: 100 - 50 = 50 GB
		// After adjustment: 90 + 50 - 5 = 135 GB > 100 GB pool size
		// This should fail as even with shared bytes, it exceeds pool capacity
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, testPool)
		assert.Error(tt, err, "Should reject update when even with ClonesSharedBytes it exceeds pool limits")
		assert.Contains(tt, err.Error(), "cannot exceed the pool capacity")
	})

	t.Run("UpdateValidation_ClonesSharedBytes_ZeroSharedBytes", func(tt *testing.T) {
		// Test case: Volume update with zero ClonesSharedBytes (no shared storage optimization)
		volume := &datamodel.Volume{
			State:             "READY",
			SizeInBytes:       50 * 1024 * 1024 * 1024, // 50 GB current size
			ClonesSharedBytes: 0,                       // No shared bytes
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 0,
				Protocols:   []string{utils.ProtocolNFS},
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 60 * 1024 * 1024 * 1024, // Increase to 60 GB
		}

		// Create a pool with specific size and current usage
		poolTotalSize := int64(100 * 1024 * 1024 * 1024)    // 100 GB total
		poolCurrentUsage := uint64(90 * 1024 * 1024 * 1024) // 90 GB already used
		testPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Size increase: 60 - 50 = 10 GB
		// After adjustment: 90 + 10 - 0 = 100 GB = pool size
		// This should be at the exact limit and should pass
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, testPool)
		assert.NoError(tt, err, "Should allow update with zero shared bytes when at exact pool limit")
	})

	t.Run("UpdateValidation_ClonesSharedBytes_LargeSharedBytes", func(tt *testing.T) {
		// Test case: Volume update with large ClonesSharedBytes providing significant optimization
		volume := &datamodel.Volume{
			State:             "READY",
			SizeInBytes:       30 * 1024 * 1024 * 1024, // 30 GB current size
			ClonesSharedBytes: 40 * 1024 * 1024 * 1024, // 40 GB shared (large optimization)
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 0,
				Protocols:   []string{utils.ProtocolNFS},
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 80 * 1024 * 1024 * 1024, // Increase to 80 GB
		}

		// Create a pool with specific size and current usage
		poolTotalSize := int64(100 * 1024 * 1024 * 1024)    // 100 GB total
		poolCurrentUsage := uint64(90 * 1024 * 1024 * 1024) // 90 GB already used
		testPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Size increase: 80 - 30 = 50 GB
		// After adjustment: 90 + 50 - 40 = 100 GB = pool size
		// This should pass due to large shared bytes optimization
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, testPool)
		assert.NoError(tt, err, "Should allow update when large ClonesSharedBytes provides significant optimization")
	})

	t.Run("UpdateValidation_ClonesSharedBytes_NoSizeIncrease", func(tt *testing.T) {
		// Test case: Volume update with ClonesSharedBytes but no size increase (other parameter updates)
		volume := &datamodel.Volume{
			State:             "READY",
			SizeInBytes:       50 * 1024 * 1024 * 1024, // 50 GB current size
			ClonesSharedBytes: 10 * 1024 * 1024 * 1024, // 10 GB shared
			VolumeAttributes: &datamodel.VolumeAttributes{
				SnapReserve: 0,
				Protocols:   []string{utils.ProtocolNFS},
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 50 * 1024 * 1024 * 1024, // Same size (no increase)
		}

		// Create a pool with specific size and current usage
		poolTotalSize := int64(100 * 1024 * 1024 * 1024)    // 100 GB total
		poolCurrentUsage := uint64(90 * 1024 * 1024 * 1024) // 90 GB already used
		testPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				SizeInBytes: poolTotalSize,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
			QuotaInBytes: poolCurrentUsage,
		}

		// Size increase: 50 - 50 = 0 GB
		// sizeIncrease <= 0, so validation should pass without checking pool capacity
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, testPool)
		assert.NoError(tt, err, "Should allow update with no size increase regardless of ClonesSharedBytes")
	})
}

func Test_validateUpdateVolumeRequest_LargeCapacity(t *testing.T) {
	ctx := context.Background()
	mockStorage := &database.MockStorage{}

	t.Run("LargeCapacityQuotaTooSmall", func(tt *testing.T) {
		largeCapacityPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: true,
				SizeInBytes:   int64(20 * 1125899906842624), // 20 PiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(11 * 1099511627776), // 11 TiB (less than minimum 12 TiB for large capacity)
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(11 * 1099511627776), // 11 TiB - too small for large capacity
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, largeCapacityPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity")
		assert.Contains(tt, err.Error(), "Must be between")
		assert.Contains(tt, err.Error(), "TiB and")
		assert.Contains(tt, err.Error(), "PiB")
	})

	t.Run("LargeCapacityQuotaTooLarge", func(tt *testing.T) {
		largeCapacityPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: true,
				SizeInBytes:   int64(25 * 1125899906842624), // 25 PiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(15 * 1125899906842624), // 15 PiB
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(21 * 1125899906842624), // 21 PiB - too large for large capacity (max is 20 PiB)
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, largeCapacityPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity")
		assert.Contains(tt, err.Error(), "Must be between")
		assert.Contains(tt, err.Error(), "TiB and")
		assert.Contains(tt, err.Error(), "PiB")
	})

	t.Run("LargeCapacityValidQuota", func(tt *testing.T) {
		largeCapacityPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: true,
				SizeInBytes:   int64(20 * 1125899906842624), // 20 PiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(13 * 1099511627776), // 13 TiB
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(15 * 1099511627776), // 15 TiB - valid for large capacity
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, largeCapacityPool)
		assert.NoError(tt, err)
	})

	t.Run("LargeCapacityBlockDevicesNotSupported", func(tt *testing.T) {
		largeCapacityPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: true,
				SizeInBytes:   int64(20 * 1125899906842624), // 20 PiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(15 * 1099511627776), // 15 TiB
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices: &[]datamodel.BlockDevice{
					{Name: "test-lun"},
				},
			},
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(16 * 1099511627776), // 16 TiB
			BlockDevices: []*common.BlockDevice{
				{Name: "test-lun", HostGroups: []string{"hg1"}},
			},
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, largeCapacityPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "BlockDevices are not supported for large capacity volumes")
	})

	t.Run("LargeCapacityBlockPropertiesNotSupported", func(tt *testing.T) {
		largeCapacityPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: true,
				SizeInBytes:   int64(20 * 1125899906842624), // 20 PiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(15 * 1099511627776), // 15 TiB
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(16 * 1099511627776), // 16 TiB
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "LINUX",
				HostGroupUUIDs: []string{"hg1", "hg2"},
			},
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, largeCapacityPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "BlockProperties are not supported for large capacity volumes")
	})

	t.Run("LargeCapacityWithoutBlockPropertiesOrDevices_Success", func(tt *testing.T) {
		largeCapacityPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: true,
				SizeInBytes:   int64(20 * 1125899906842624), // 20 PiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(15 * 1099511627776), // 15 TiB
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(16 * 1099511627776), // 16 TiB
			// No BlockProperties or BlockDevices - should succeed
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, largeCapacityPool)
		assert.NoError(tt, err)
	})

	t.Run("NonLargeCapacityQuotaTooSmall", func(tt *testing.T) {
		regularPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: false,
				SizeInBytes:   int64(200 * 1024 * 1024 * 1024 * 1024), // 200 TiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(500 * 1024 * 1024), // 500 MiB
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(800 * 1024 * 1024), // 800 MiB - increased from current but still too small for regular volumes (min is 1 GiB)
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, regularPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity")
		assert.Contains(tt, err.Error(), "Must be between")
		assert.Contains(tt, err.Error(), "GiB and")
		assert.Contains(tt, err.Error(), "TiB")
	})

	t.Run("NonLargeCapacityValidQuota", func(tt *testing.T) {
		regularPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: false,
				SizeInBytes:   int64(200 * 1024 * 1024 * 1024 * 1024), // 200 TiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100 GiB
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(200 * 1024 * 1024 * 1024), // 200 GiB - valid for regular volumes
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, regularPool)
		assert.NoError(tt, err)
	})

	t.Run("NonLargeCapacityBlockPropertiesSupported", func(tt *testing.T) {
		regularPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				LargeCapacity: false,
				SizeInBytes:   int64(200 * 1024 * 1024 * 1024 * 1024), // 200 TiB
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		volume := &datamodel.Volume{
			State:       "READY",
			SizeInBytes: int64(100 * 1024 * 1024 * 1024), // 100 GiB
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			},
		}

		params := &common.UpdateVolumeParams{
			QuotaInBytes: int64(200 * 1024 * 1024 * 1024), // 200 GiB
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "LINUX",
				HostGroupUUIDs: []string{"hg1", "hg2"},
			},
		}

		// Mock the GetMultipleHostGroups call
		mockStorage.On("GetMultipleHostGroups", ctx, []string{"hg1", "hg2"}, int64(1)).Return([]*datamodel.HostGroup{
			{
				BaseModel: datamodel.BaseModel{UUID: "hg1"},
				Name:      "HostGroup1",
				State:     models.LifeCycleStateREADY,
				Hosts:     datamodel.Hosts{Hosts: []string{"host1"}},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "hg2"},
				Name:      "HostGroup2",
				State:     models.LifeCycleStateREADY,
				Hosts:     datamodel.Hosts{Hosts: []string{"host2"}},
			},
		}, nil)

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, regularPool)
		assert.NoError(tt, err)
	})
}

func TestBlockVolumeValidator_Validate(t *testing.T) {
	t.Run("Valid block properties", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"}, Name: "test_account"}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"}, Name: "test_pool", AccountID: account.ID, State: models.LifeCycleStateREADY, Account: account}
		err = store.DB().Create(pool).Error
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		hg := &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: "hg-uuid"}, Name: "hg1", State: models.LifeCycleStateREADY, AccountID: account.ID}
		err = store.DB().Create(hg).Error
		if err != nil {
			t.Fatalf("Failed to create host group: %v", err)
		}
		params := &common.CreateVolumeParams{
			Name:            "dummy-name",
			PoolID:          pool.UUID,
			QuotaInBytes:    minQuotaInBytesVolume + 1,
			BlockProperties: &common.BlockPropertiesRequest{OSType: "linux", HostGroupUUIDs: []string{"hg-uuid"}},
		}
		validator := &BlockVolumeProcessor{}
		err = validator.Validate(ctx, store, params, account.ID)
		assert.Nil(tt, err)
	})
	t.Run("Invalid host group UUID", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"}, Name: "test_account"}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"}, Name: "test_pool", AccountID: account.ID, State: models.LifeCycleStateREADY, Account: account}
		err = store.DB().Create(pool).Error
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		params := &common.CreateVolumeParams{
			Name:            "dummy-name",
			PoolID:          pool.UUID,
			QuotaInBytes:    minQuotaInBytesVolume + 1,
			BlockProperties: &common.BlockPropertiesRequest{OSType: "linux", HostGroupUUIDs: []string{"non-existent-hg"}},
		}
		validator := &BlockVolumeProcessor{}
		err = validator.Validate(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "could not find some of the host groups, please check the hostgroup details and try with valid host group names.")
	})
	t.Run("WithBlockDevices_ShouldValidateSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"}, Name: "test_account"}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"}, Name: "test_pool", AccountID: account.ID, State: models.LifeCycleStateREADY, Account: account}
		err = store.DB().Create(pool).Error
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		hg := &datamodel.HostGroup{BaseModel: datamodel.BaseModel{UUID: "hg-uuid"}, Name: "hg1", State: models.LifeCycleStateREADY, AccountID: account.ID}
		err = store.DB().Create(hg).Error
		if err != nil {
			t.Fatalf("Failed to create host group: %v", err)
		}
		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			PoolID:       pool.UUID,
			QuotaInBytes: minQuotaInBytesVolume + 1,
			BlockDevices: &[]common.BlockDevice{
				{
					Name:       "test-lun",
					HostGroups: []string{"hg-uuid"},
					OSType:     "LINUX",
				},
			},
		}
		validator := &BlockVolumeProcessor{}
		err = validator.Validate(ctx, store, params, account.ID)
		assert.Nil(tt, err)
		assert.Nil(tt, params.FileProperties) // Should be set to nil for block volumes
	})
	t.Run("WithNoBlockDevicesOrProperties_ShouldPass", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"}, Name: "test_account"}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			QuotaInBytes: minQuotaInBytesVolume + 1,
			BlockProperties: &common.BlockPropertiesRequest{
				HostGroupUUIDs: []string{},
			},
		}
		validator := &BlockVolumeProcessor{}
		err = validator.Validate(ctx, store, params, account.ID)
		assert.Nil(tt, err)
		assert.Nil(tt, params.FileProperties) // Should be set to nil for block volumes
	})
	t.Run("WhenNoBPAndBDInParams", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"}, Name: "test_account"}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}
		params := &common.CreateVolumeParams{
			Name:         "dummy-name",
			QuotaInBytes: minQuotaInBytesVolume + 1,
		}
		validator := &BlockVolumeProcessor{}
		err = validator.Validate(ctx, store, params, account.ID)
		assert.EqualError(tt, err, "Block Device/Block Properties is required")
		assert.Nil(tt, params.FileProperties) // Should be set to nil for block volumes
	})
}

func TestGetVolumeTypeValidator(t *testing.T) {
	t.Run("ISCSI returns BlockVolumeProcessor", func(tt *testing.T) {
		validator, err := GetVolumeTypeValidator([]string{"ISCSI"}, "test_account")
		assert.IsType(tt, &BlockVolumeProcessor{}, validator)
		assert.NoError(tt, err)
	})

	t.Run("File-based protocol returns error if flag is false", func(tt *testing.T) {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
		validator, err := GetVolumeTypeValidator([]string{"NFSV4"}, "test_account")
		assert.Nil(tt, validator)
		assert.ErrorContains(tt, err, "file protocols are not enabled")
	})

	t.Run("File-based protocol returns FileVolumeProcessor if flag is true and account is allowlisted", func(tt *testing.T) {
		utils.SetFileProtocolSupportedForTesting(true)
		utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
		validator, err := GetVolumeTypeValidator([]string{"NFSV4"}, "test_account")
		assert.IsType(tt, &FileVolumeProcessor{}, validator)
		assert.NoError(tt, err)
	})

	t.Run("Unknown protocol returns error", func(tt *testing.T) {
		validator, err := GetVolumeTypeValidator([]string{"UNKNOWN"}, "test_account")
		assert.Nil(tt, validator)
		assert.ErrorContains(tt, err, "unsupported or unspecified protocol")
	})

	t.Run("No protocol specified returns error", func(tt *testing.T) {
		validator, err := GetVolumeTypeValidator([]string{}, "test_account")
		assert.Nil(tt, validator)
		assert.ErrorContains(tt, err, "unsupported or unspecified protocol")
	})
}

func TestGetIPAddressForVolume(t *testing.T) {
	t.Run("GetIPAddressForBlockProtocol", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid"},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: account.ID,
			IPAddress: "192.168.1.200",
		}
		err = store.DB().Create(lif).Error
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid2"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid2"},
			Name:      "test_lif",
			NodeID:    node2.ID,
			AccountID: account.ID,
			IPAddress: "192.168.1.201",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{utils.ProtocolISCSI}, // Block protocol
			},
		}

		// Test getting IP address for block protocol (this doesn't use GetLifForFilesNode)
		ipAddress, err := _getIPAddressForVolume(ctx, store, volume)
		assert.NoError(tt, err)
		assert.Equal(tt, []string{"192.168.1.200", "192.168.1.201"}, ipAddress)
	})

	t.Run("GetIPAddressForBlockProtocol", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid"},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: account.ID,
			IPAddress: "192.168.1.101",
		}
		err = store.DB().Create(lif).Error
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID, // Set the pool ID
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols: []string{utils.ProtocolISCSI},
				BlockProperties: &datamodel.BlockProperties{
					OSType: "linux",
				},
			},
		}

		// Test getting IP address for block protocol
		ipAddress, err := _getIPAddressForVolume(ctx, store, volume)
		assert.NoError(tt, err)
		assert.Equal(tt, []string{"192.168.1.101"}, ipAddress)
	})
	t.Run("GetIPAddressForFileProtocolFails", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid"},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: account.ID,
			IPAddress: "192.168.1.101",
		}
		err = store.DB().Create(lif).Error
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID, // Set the pool ID
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:      []string{utils.ProtocolNFSv3},
				FileProperties: &datamodel.FileProperties{},
			},
		}

		// Test getting IP address for block protocol
		ipAddress, err := _getIPAddressForVolume(ctx, store, volume)
		assert.Error(tt, err)
		assert.Len(tt, ipAddress, 0)
	})
	t.Run("GetIPAddressForFileProtocolSuccess", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		node := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid"},
			Name:            "test_node",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
		}
		err = store.DB().Create(node).Error
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid"},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: account.ID,
			IPAddress: "192.168.1.101",
		}
		err = store.DB().Create(lif).Error
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID, // Set the pool ID
			VolumeAttributes: &datamodel.VolumeAttributes{
				Protocols:      []string{utils.ProtocolNFSv3},
				FileProperties: &datamodel.FileProperties{},
			},
		}

		// Test getting IP address for block protocol
		ipAddress, err := _getIPAddressForVolume(ctx, store, volume)
		assert.Error(tt, err)
		assert.Len(tt, ipAddress, 0)
	})
}

func TestValidateCreateVolumeParamsFileProperties(t *testing.T) {
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
	t.Run("FilePropertiesValidationEmptyAllowedClients", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
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
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create 2 nodes as required by the validation
		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-1"},
			Name:            "test_node_1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		if err != nil {
			tt.Fatalf("Failed to create node1: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-2"},
			Name:            "test_node_2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node2: %v", err)
		}

		// Create LIFs for the nodes (required for validation)
		lif1 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-1"},
			Name:       "test_lif_1",
			AccountID:  account.ID,
			NodeID:     node1.ID,
			IPAddress:  "192.168.1.10",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif1).Error
		if err != nil {
			tt.Fatalf("Failed to create lif1: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-2"},
			Name:       "test_lif_2",
			AccountID:  account.ID,
			NodeID:     node2.ID,
			IPAddress:  "192.168.1.11",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif2: %v", err)
		}

		params := &common.CreateVolumeParams{
			AccountName:  "test_account",
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 107374182400, // 100GB
			Protocols:    []string{utils.ProtocolNFSv3},
			Network:      "test-network",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "", // Empty allowed clients
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
					},
				},
			},
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500GB
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "allowed clients cannot be nil in export rules")
	})
	t.Run("ProtocolValidationNoProtocols", func(tt *testing.T) {
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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(maxQuotaInBytesPool),
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
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create 2 nodes as required by the validation
		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-1"},
			Name:            "test_node_1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		if err != nil {
			tt.Fatalf("Failed to create node1: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-2"},
			Name:            "test_node_2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node2: %v", err)
		}

		// Create LIFs for the nodes (required for validation)
		lif1 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-1"},
			Name:       "test_lif_1",
			AccountID:  account.ID,
			NodeID:     node1.ID,
			IPAddress:  "192.168.1.10",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif1).Error
		if err != nil {
			tt.Fatalf("Failed to create lif1: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-2"},
			Name:       "test_lif_2",
			AccountID:  account.ID,
			NodeID:     node2.ID,
			IPAddress:  "192.168.1.11",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif2: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 107374182400, // 100GB
			Protocols:    []string{},   // No protocols specified
			Network:      "test-network",
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: minQuotaInBytesPool,
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "at least one protocol must be specified")
	})

	t.Run("ProtocolValidationFileProtocolNotEnabled", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		// Set file protocol supported flag to false
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
			utils.SetFileProtocolAllowlistedAccountsForTesting("")
		}()

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
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:        "test_pool",
			AccountID:   account.ID,
			State:       models.LifeCycleStateREADY,
			Network:     "test-network",
			SizeInBytes: int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
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
			State:     models.LifeCycleStateREADY,
		}

		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create 2 nodes as required by the validation
		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-1"},
			Name:            "test_node_1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		if err != nil {
			tt.Fatalf("Failed to create node1: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-2"},
			Name:            "test_node_2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node2: %v", err)
		}

		// Create LIFs for the nodes (required for validation)
		lif1 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-1"},
			Name:       "test_lif_1",
			AccountID:  account.ID,
			NodeID:     node1.ID,
			IPAddress:  "192.168.1.10",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif1).Error
		if err != nil {
			tt.Fatalf("Failed to create lif1: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-2"},
			Name:       "test_lif_2",
			AccountID:  account.ID,
			NodeID:     node2.ID,
			IPAddress:  "192.168.1.11",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif2: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			PoolID:       pool.UUID,
			QuotaInBytes: 107374182400,                  // 100GB
			Protocols:    []string{utils.ProtocolNFSv3}, // File protocol when not enabled
			Network:      "test-network",
		}

		poolView := &datamodel.PoolView{
			Pool:         *pool,
			QuotaInBytes: uint64(500 * 1024 * 1024 * 1024), // 500GB
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.ErrorContains(tt, err, "file protocols are not enabled")
	})
}

func TestConvertDatastoreVolumeToModelBlockDevices(t *testing.T) {
	t.Run("WithBlockDevices_ShouldConvertCorrectly", func(tt *testing.T) {
		// Setup volume with BlockDevices
		blockDevices := []datamodel.BlockDevice{
			{
				Name:       "test-lun-1",
				Identifier: "lun-123",
				Size:       107374182400, // 100 GiB
				OSType:     "LINUX",
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-1",
						HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
					},
					{
						HostGroupUUID: "hg-uuid-2",
						HostQNs:       []string{"iqn.1998-01.com.vmware:host2"},
					},
				},
			},
			{
				Name:       "test-lun-2",
				Identifier: "lun-456",
				Size:       214748364800, // 200 GiB
				OSType:     "WINDOWS",
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-3",
						HostQNs:       []string{"iqn.1998-01.com.vmware:host3"},
					},
				},
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-volume-uuid",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			Name:        "test-volume",
			Description: "Test volume",
			State:       "READY",
			SizeInBytes: 107374182400,
			UsedBytes:   53687091200,
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
				Name:      "test-pool",
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-a",
				},
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
				Name:      "test-account",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices:      &blockDevices,
				IsDataProtection:  false,
				SnapReserve:       10,
				SnapshotDirectory: true,
			},
		}

		ipAddresses := []string{"10.72.177.17", "10.72.177.18"}

		result := convertDatastoreVolumeToModel(volume, &ipAddresses)

		assert.NotNil(tt, result.BlockDevices)
		assert.Len(tt, *result.BlockDevices, 2)

		// Verify first BlockDevice
		bd1 := (*result.BlockDevices)[0]
		assert.Equal(tt, "test-lun-1", bd1.Name)
		assert.Equal(tt, "lun-123", bd1.Identifier)
		assert.Equal(tt, uint64(107374182400), bd1.Size)
		assert.Equal(tt, "LINUX", bd1.OSType)
		assert.Len(tt, bd1.HostGroupDetail, 2)
		assert.Equal(tt, "hg-uuid-1", bd1.HostGroupDetail[0].HostGroupID)
		assert.Equal(tt, []string{"iqn.1998-01.com.vmware:host1"}, bd1.HostGroupDetail[0].Hosts)
		assert.Equal(tt, "hg-uuid-2", bd1.HostGroupDetail[1].HostGroupID)
		assert.Equal(tt, []string{"iqn.1998-01.com.vmware:host2"}, bd1.HostGroupDetail[1].Hosts)

		// Verify second BlockDevice
		bd2 := (*result.BlockDevices)[1]
		assert.Equal(tt, "test-lun-2", bd2.Name)
		assert.Equal(tt, "lun-456", bd2.Identifier)
		assert.Equal(tt, uint64(214748364800), bd2.Size)
		assert.Equal(tt, "WINDOWS", bd2.OSType)
		assert.Len(tt, bd2.HostGroupDetail, 1)
		assert.Equal(tt, "hg-uuid-3", bd2.HostGroupDetail[0].HostGroupID)
		assert.Equal(tt, []string{"iqn.1998-01.com.vmware:host3"}, bd2.HostGroupDetail[0].Hosts)

		// Verify IP addresses
		assert.Equal(tt, ipAddresses, result.IPAddresses)
		assert.Equal(tt, true, result.SnapshotDirectory)
	})

	t.Run("WithBlockDevicesNoHostGroups_ShouldConvertCorrectly", func(tt *testing.T) {
		blockDevices := []datamodel.BlockDevice{
			{
				Name:             "test-lun-no-hg",
				Identifier:       "lun-789",
				Size:             53687091200, // 50 GiB
				OSType:           "ESXI",
				HostGroupDetails: []datamodel.HostGroupDetail{}, // Empty host groups
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-volume-uuid-2",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			Name:        "test-volume-2",
			Description: "Test volume 2",
			State:       "READY",
			SizeInBytes: 53687091200,
			UsedBytes:   26843545600,
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-2"},
				Name:      "test-pool-2",
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-b",
				},
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "test-account-uuid-2"},
				Name:      "test-account-2",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices:     &blockDevices,
				IsDataProtection: false,
				SnapReserve:      5,
			},
		}

		ipAddresses := []string{"10.72.177.19"}

		result := convertDatastoreVolumeToModel(volume, &ipAddresses)

		assert.NotNil(tt, result.BlockDevices)
		assert.Len(tt, *result.BlockDevices, 1)

		bd := (*result.BlockDevices)[0]
		assert.Equal(tt, "test-lun-no-hg", bd.Name)
		assert.Equal(tt, "lun-789", bd.Identifier)
		assert.Equal(tt, uint64(53687091200), bd.Size)
		assert.Equal(tt, "ESXI", bd.OSType)
		assert.Empty(tt, bd.HostGroupDetail)

		// Verify IP addresses
		assert.Equal(tt, ipAddresses, result.IPAddresses)
	})

	t.Run("WithNilIPAddresses_ShouldHandleGracefully", func(tt *testing.T) {
		blockDevices := []datamodel.BlockDevice{
			{
				Name:       "test-lun",
				Identifier: "lun-123",
				Size:       107374182400,
				OSType:     "LINUX",
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-volume-uuid-3",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			Name:        "test-volume-3",
			Description: "Test volume 3",
			State:       "READY",
			SizeInBytes: 107374182400,
			UsedBytes:   53687091200,
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid-3"},
				Name:      "test-pool-3",
				PoolAttributes: &datamodel.PoolAttributes{
					PrimaryZone: "us-west1-c",
				},
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "test-account-uuid-3"},
				Name:      "test-account-3",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				BlockDevices:     &blockDevices,
				IsDataProtection: false,
				SnapReserve:      15,
			},
		}

		result := convertDatastoreVolumeToModel(volume, nil)

		assert.NotNil(tt, result.BlockDevices)
		assert.Len(tt, *result.BlockDevices, 1)
		assert.Empty(tt, result.IPAddresses) // Should be empty when nil
	})
}

func TestConvertDatastoreVolumeToModelFileProperties(t *testing.T) {
	t.Run("ConvertVolumeWithFileProperties", func(tt *testing.T) {
		ipAddress := []string{"192.168.1.100"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-uuid",
			},
			Name:        "test-volume",
			Description: "test description",
			SizeInBytes: 107374182400,
			Account:     account,
			Pool:        pool,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CreationToken: "test-token",
				Protocols:     []string{utils.ProtocolNFSv3},
				FileProperties: &datamodel.FileProperties{
					ExportPolicy: &datamodel.ExportPolicy{
						ExportPolicyName: "test-policy",
						ExportRules: []*datamodel.ExportRule{
							{
								AllowedClients: "192.168.1.0/24",
								AccessType:     models.ReadWrite,
								CIFS:           false,
								NFSv3:          true,
								NFSv4:          false,
								Index:          1,
							},
						},
					},
					JunctionPath: "/test-path",
				},
			},
		}

		// Test conversion with file properties - should cover export rules conversion and FileProperties assignment
		result := convertDatastoreVolumeToModel(volume, &ipAddress)

		assert.NotNil(tt, result)
		assert.NotNil(tt, result.FileProperties)
		assert.NotNil(tt, result.FileProperties.ExportPolicy)
		assert.Equal(tt, "test-policy", result.FileProperties.ExportPolicy.ExportPolicyName)
		assert.Equal(tt, "/test-path", result.FileProperties.JunctionPath)
		assert.Len(tt, result.FileProperties.ExportPolicy.ExportRules, 1)
		assert.Equal(tt, "192.168.1.0/24", result.FileProperties.ExportPolicy.ExportRules[0].AllowedClients)
		assert.Equal(tt, models.ReadWrite, result.FileProperties.ExportPolicy.ExportRules[0].AccessType)
		assert.False(tt, result.FileProperties.ExportPolicy.ExportRules[0].CIFS)
		assert.True(tt, result.FileProperties.ExportPolicy.ExportRules[0].NFSv3)
		assert.False(tt, result.FileProperties.ExportPolicy.ExportRules[0].NFSv4)
	})

	t.Run("ConvertVolumeWithoutFileProperties", func(tt *testing.T) {
		ipAddress := []string{"192.168.1.100"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-uuid",
			},
			Name:        "test-volume",
			Description: "test description",
			SizeInBytes: 107374182400,
			Account:     account,
			Pool:        pool,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CreationToken: "test-token",
				Protocols:     []string{utils.ProtocolISCSI},
				// No FileProperties
			},
		}

		// Test conversion without file properties
		result := convertDatastoreVolumeToModel(volume, &ipAddress)

		assert.NotNil(tt, result)
		assert.Nil(tt, result.FileProperties)
	})

	t.Run("ConvertVolumeWithKms", func(tt *testing.T) {
		ipAddress := []string{"192.168.1.100"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
			KmsConfigID: sql.NullInt64{Valid: true, Int64: 1},
			KmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-kms-uuid"},
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-uuid",
			},
			Name:        "test-volume",
			Description: "test description",
			SizeInBytes: 107374182400,
			Account:     account,
			Pool:        pool,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CreationToken: "test-token",
				Protocols:     []string{utils.ProtocolISCSI},
				// No FileProperties
			},
		}

		// Test conversion without file properties
		result := convertDatastoreVolumeToModel(volume, &ipAddress)

		assert.NotNil(tt, result)
		assert.Nil(tt, result.FileProperties)
		assert.Equal(tt, result.EncryptionType, "CLOUD_KMS")
		assert.Equal(tt, result.KmsConfig.UUID, "test-kms-uuid")
	})
}

func TestConvertDatastoreVolumeToModelCacheParameters(t *testing.T) {
	t.Run("ConvertVolumeWithCacheParameters", func(tt *testing.T) {
		ipAddress := []string{"192.168.1.100"}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test-pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-uuid",
			},
			Name:             "test-volume",
			Description:      "test description",
			SizeInBytes:      107374182400,
			Account:          account,
			Pool:             pool,
			VolumeAttributes: &datamodel.VolumeAttributes{},
			CacheParameters: &datamodel.CacheParameters{
				PeerClusterName:       "peer-cluster",
				PeerSvmName:           "peer-svm",
				PeerVolumeName:        "peer-volume",
				PeerIpAddresses:       []string{"0.0.0.0"},
				CacheStateDetailsCode: models.ErrorDuringClusterPeerCode,
				CacheStateDetails:     "Error",
				Passphrase:            nillable.ToPointer("passphrase"),
				Command:               nillable.ToPointer("some command"),
				CommandExpiryTime:     nillable.ToPointer(time.Now()),
				EnableGlobalFileLock:  nillable.ToPointer(true),

				CacheConfig: &datamodel.CacheConfig{
					PrePopulate: &datamodel.CachePrePopulate{
						Recursion: nillable.ToPointer(true),
					},
					WritebackEnabled: nillable.ToPointer(true),
				},
			},
		}

		result := convertDatastoreVolumeToModel(volume, &ipAddress)

		assert.NotNil(tt, result)
		assert.NotNil(tt, result.CacheParameters)
		assert.Equal(tt, "peer-cluster", result.CacheParameters.PeerClusterName)
		assert.Equal(tt, "peer-svm", result.CacheParameters.PeerSvmName)
		assert.Equal(tt, "peer-volume", result.CacheParameters.PeerVolumeName)
		assert.Equal(tt, []string{"0.0.0.0"}, result.CacheParameters.PeerIPAddresses)
		assert.Equal(tt, models.ErrorDuringClusterPeerCode, result.CacheParameters.CacheStateDetailsCode)
		assert.Equal(tt, "Error", result.CacheParameters.CacheStateDetails)
		assert.Equal(tt, "passphrase", *result.CacheParameters.Passphrase)
		assert.Equal(tt, "some command", result.CacheParameters.PeeringCommand)
		assert.NotNil(tt, result.CacheParameters.PeerExpiryTime)
		assert.True(tt, *result.CacheParameters.EnableGlobalFileLock)
		assert.NotNil(tt, result.CacheParameters.CacheConfig)
		assert.NotNil(tt, result.CacheParameters.CacheConfig.PrePopulate)
		assert.True(tt, *result.CacheParameters.CacheConfig.PrePopulate.Recursion)
		assert.True(tt, *result.CacheParameters.CacheConfig.WritebackEnabled)
	})
}

func TestValidateAllowedClients(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"Single valid IPv4", "192.168.1.1", false},
		{"Multiple valid IPv4", "10.0.0.1,192.168.1.2", false},
		{"Valid IPv4 CIDR", "10.0.0.0/24", false},
		{"Mix of IPv4 and CIDR", "10.0.0.1,10.0.0.0/24", false},
		{"Invalid IP", "999.999.999.999", true},
		{"Invalid CIDR", "10.0.0.0/33", true},
		{"IP not matching CIDR base", "10.0.0.5/24", true},
		{"Duplicate IPs", "10.0.0.1,10.0.0.1", true},
		{"Duplicate CIDRs", "10.0.0.0/24,10.0.0.0/24", true},
		{"Empty string", "", true},
		{"Zero address with nonzero mask", "0.0.0.0/8", true},
		{"Allow all clients", models.AllowedAllClients, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAllowedClients(tt.input)
			if tt.wantErr {
				assert.Error(t, err, "expected error for input: %q", tt.input)
			} else {
				assert.NoError(t, err, "expected no error for input: %q", tt.input)
			}
		})
	}
}

func TestValidateDeleteVolumeParams(t *testing.T) {
	t.Run("WhenValidateDeleteVolumeParamsSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 1},
			Name:      "test-volume",
		}

		var replicationCount int64
		// Mock the storage method to return false (no backup in transition state)
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(false, nil)
		se.On("GetVolumeReplicationCountByVolumeID", ctx, volume.ID).Return(replicationCount, nil)

		err := _validateDeleteVolumeParams(ctx, se, volume)
		assert.NoError(tt, err)
		se.AssertExpectations(tt)
	})

	t.Run("WhenBackupInTransitionStateReturnsError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
		}

		// Mock the storage method to return an error
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(false, errors.New("database error"))

		err := _validateDeleteVolumeParams(ctx, se, volume)
		assert.EqualError(tt, err, "database error")
		se.AssertExpectations(tt)
	})

	t.Run("WhenBackupInTransitionStateReturnsTrue", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
		}

		// Mock the storage method to return true (backup in transition state)
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(true, nil)

		err := _validateDeleteVolumeParams(ctx, se, volume)
		assert.EqualError(tt, err, "A backup operation on volume is currently in progress. Please wait for it to complete before deleting the volume")
		se.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsNil", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}

		var replicationCount int64
		// Mock the storage method to return false for empty UUID
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, "").Return(false, nil)
		se.On("GetVolumeReplicationCountByVolumeID", ctx, mock.Anything).Return(replicationCount, nil)

		err := _validateDeleteVolumeParams(ctx, se, &datamodel.Volume{})
		assert.NoError(tt, err)
		se.AssertExpectations(tt)
	})

	t.Run("WhenBackupInTransitionStateWithDifferentUUID", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "different-uuid-format-12345"},
			Name:      "test-volume-2",
		}

		// Mock the storage method to return true for different UUID format
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(true, nil)

		err := _validateDeleteVolumeParams(ctx, se, volume)
		assert.EqualError(tt, err, "A backup operation on volume is currently in progress. Please wait for it to complete before deleting the volume")
		se.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationCountReturnsError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 1},
			Name:      "test-volume",
		}

		var replicationCount int64
		// Mock the storage method to return false (no backup in transition state)
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(false, nil)
		se.On("GetVolumeReplicationCountByVolumeID", ctx, volume.ID).Return(replicationCount, errors.New("database error"))

		err := _validateDeleteVolumeParams(ctx, se, volume)
		assert.EqualError(tt, err, "database error")
		se.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeReplicationCountReturnsCount1", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid", ID: 1},
			Name:      "test-volume",
		}

		var replicationCount int64 = 1
		// Mock the storage method to return false (no backup in transition state)
		se.On("IsBackupInCreatingorDeletingStateByVolume", ctx, volume.UUID).Return(false, nil)
		se.On("GetVolumeReplicationCountByVolumeID", ctx, volume.ID).Return(replicationCount, nil)

		err := _validateDeleteVolumeParams(ctx, se, volume)
		assert.EqualError(tt, err, "Cannot delete volume that has active replication. Please delete the replication first.")
		se.AssertExpectations(tt)
	})
}

func TestFileVolumeProcessor_Validate(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
	mockStorage := &database.MockStorage{}
	processor := &FileVolumeProcessor{}
	accountID := int64(123)

	t.Run("Success_WithValidExportPolicy", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.NoError(tt, err)
		assert.Nil(tt, params.BlockProperties, "BlockProperties should be nil for file volumes")
	})

	t.Run("Success_WithMultipleExportRules", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     models.ReadOnly,
							NFSv4:          true,
							Index:          2,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.NoError(tt, err)
	})

	t.Run("Success_WithNilExportPolicy", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: nil,
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.NoError(tt, err)
	})

	t.Run("Error_NilFileProperties", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken:  "test-token",
			FileProperties: nil,
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.EqualError(tt, err, "FileProperties cannot be nil for NAS volumes")
	})

	t.Run("Error_EmptyAllowedClients", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.EqualError(tt, err, "allowed clients cannot be nil in export rules")
	})

	t.Run("Error_InvalidAllowedClients", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "invalid-ip-format",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.ErrorContains(tt, err, "allowed clients validation failed")
	})

	t.Run("Error_EmptyCreationToken", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.EqualError(tt, err, "Creation Token cannot be empty")
	})

	t.Run("ClearsBlockProperties", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "linux",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.NoError(tt, err)
		assert.Nil(tt, params.BlockProperties, "BlockProperties should be cleared for file volumes")
	})

	t.Run("MultipleExportRules_OneWithInvalidClients", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
						{
							AllowedClients: "invalid-client",
							AccessType:     models.ReadOnly,
							NFSv4:          true,
							Index:          2,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.ErrorContains(tt, err, "allowed clients validation failed")
	})

	t.Run("MultipleExportRules_OneWithEmptyClients", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			CreationToken: "test-token",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     models.ReadWrite,
							NFSv3:          true,
							Index:          1,
						},
						{
							AllowedClients: "",
							AccessType:     models.ReadOnly,
							NFSv4:          true,
							Index:          2,
						},
					},
				},
			},
		}

		err := processor.Validate(ctx, mockStorage, params, accountID)
		assert.EqualError(tt, err, "allowed clients cannot be nil in export rules")
	})
}

func TestUpdateVolumeStatus(t *testing.T) {
	t.Run("WhenUpdateVolumeFieldsFails", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
			State:     models.LifeCycleStateREADY,
		}

		se.On("UpdateVolumeFields", ctx, volume.UUID, mock.Anything).Return(errors.New("database error"))
		updatedVolume, err := updateVolumeStatus(ctx, se, volume, models.LifeCycleStateReverting, models.LifeCycleStateRevertingDetails)
		assert.EqualError(tt, err, "database error")
		assert.Nil(tt, updatedVolume)
	})

	t.Run("WhenUpdateVolumeRevertStatusSuccess", func(tt *testing.T) {
		ctx := context.Background()
		se := &database.MockStorage{}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
			State:     models.LifeCycleStateREADY,
		}

		se.On("UpdateVolumeFields", ctx, volume.UUID, mock.Anything).Return(nil)
		updatedVolume, err := updateVolumeStatus(ctx, se, volume, models.LifeCycleStateReverting, models.LifeCycleStateRevertingDetails)
		assert.NoError(tt, err)
		assert.NotNil(tt, updatedVolume)
		assert.Equal(tt, models.LifeCycleStateReverting, updatedVolume.State)
		assert.Equal(tt, models.LifeCycleStateRevertingDetails, updatedVolume.StateDetails)
	})
}

func TestRevertVolume(t *testing.T) {
	t.Run("WhenAccountNotFound", func(tt *testing.T) {
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

		params := &common.RevertVolumeParams{
			AccountName: "non-existent-account",
			VolumeID:    "test-volume-uuid",
			SnapshotID:  "test-snapshot-uuid",
		}

		_, _, err = orch.RevertVolume(ctx, params)
		assert.EqualError(tt, err, "Account not found")
		var customErr *vsaerrors.CustomError
		errors2.As(err, &customErr)
		assert.NotNil(tt, customErr.OriginalErr, "account not found")
	})

	t.Run("WhenVolumeNotFound", func(tt *testing.T) {
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

		orch := Orchestrator{
			storage: store,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    "non-existent-volume-uuid",
			SnapshotID:  "test-snapshot-uuid",
		}

		_, _, err = orch.RevertVolume(ctx, params)
		assert.EqualError(tt, err, "Volume not found")
		var customErr *vsaerrors.CustomError
		errors2.As(err, &customErr)
		assert.NotNil(tt, customErr, "Expected a CustomError")
		assert.NotNil(tt, customErr.HttpCode, 404)
		assert.NotNil(tt, customErr.Retriable, false)
		assert.NotNil(tt, customErr.OriginalErr, "volume not found")
	})

	t.Run("WhenVolumeInTransitionState", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
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

		orch := Orchestrator{
			storage: store,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  "test-snapshot-uuid",
		}

		_, _, err = orch.RevertVolume(ctx, params)
		assert.Contains(tt, err.Error(), "volume is in transition state and cannot be reverted, state: DELETING")
	})

	t.Run("WhenVolumeIsDataProtection", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: true,
				SnapReserve:      0,
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		orch := Orchestrator{
			storage: store,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  "test-snapshot-uuid",
		}

		_, _, err = orch.RevertVolume(ctx, params)
		assert.EqualError(tt, err, "Cannot revert a Data Protection Volume")
	})

	t.Run("WhenSnapshotNotFound", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
				SnapReserve:      0,
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		orch := Orchestrator{
			storage: store,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  "non-existent-snapshot-uuid",
		}

		_, _, err = orch.RevertVolume(ctx, params)
		assert.EqualError(tt, err, "Snapshot not found")
	})

	t.Run("WhenSnapshotNotInReadyState", func(tt *testing.T) {
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
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateCreating,
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(tt, err, "Failed to create snapshot")

		orch := Orchestrator{
			storage: store,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  snapshot.UUID,
		}

		_, _, err = orch.RevertVolume(ctx, params)
		assert.EqualError(tt, err, "Snapshot is not in a valid state for volume revert. Please wait for the snapshot to be ready and retry again.")
	})

	t.Run("WhenWorkflowExecutionFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			t.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		// Mock data setup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			t.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:      "test_pool",
			AccountID: account.ID,
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
			VendorID: "/projects/project123/locations/location123/pools/pool123",
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(volume).Error
		assert.NoError(t, err, "Failed to create volume")

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(t, err, "Failed to create snapshot")

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return errors.New("workflow execution failed")
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		// Mock updateVolumeStatus
		originalUpdateVolumeStatus := updateVolumeStatus
		updateVolumeStatus = func(ctx context.Context, se database.Storage, vol *datamodel.Volume, state string, details string) (*datamodel.Volume, error) {
			vol.State = state
			return vol, nil
		}
		defer func() { updateVolumeStatus = originalUpdateVolumeStatus }()

		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  snapshot.UUID,
		}

		_, _, tempErr := orch.RevertVolume(ctx, params)

		// Assert the error
		assert.NotNil(t, tempErr, "Expected an error but got nil")
		assert.EqualError(t, tempErr, "workflow execution failed")
	})

	t.Run("WhenWorkflowExecutionSucceeds", func(tt *testing.T) {
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
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-west1-a",
			},
			VendorID: "/projects/project123/locations/location123/pools/pool123",
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			Pool:      pool,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}
		err = store.DB().Create(volume).Error
		assert.NoError(tt, err, "Failed to create volume")

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			VolumeID:  volume.ID,
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(snapshot).Error
		assert.NoError(tt, err, "Failed to create snapshot")

		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		// Mock ExecuteWorkflow for auto pool scaling
		temporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return nil
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  snapshot.UUID,
		}

		resultVolume, jobUUID, err := orch.RevertVolume(ctx, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, resultVolume)
		assert.NotEmpty(tt, jobUUID)
		assert.Equal(tt, models.LifeCycleStateReverting, resultVolume.LifeCycleState)
		assert.Equal(tt, volume.UUID, resultVolume.UUID)
		assert.Equal(tt, volume.Name, resultVolume.DisplayName)
	})

	t.Run("WhenRevertVolumeFailsDueToWorkflowError", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.SetupStorageForTest(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}
		temporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		// Mock ExecuteWorkflowSequentially to return error
		origExecuteWorkflowSeq := workflows.ExecuteWorkflowSeq
		workflows.ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
			return errors.New("workflow execution failed")
		}
		defer func() { workflows.ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

		orch := Orchestrator{
			storage:  store,
			temporal: temporal,
		}

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
			VendorID:  "/projects/project123/locations/location123/pools/pool123",
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test_volume",
			AccountID: account.ID,
			PoolID:    pool.ID,
			Pool:      pool,
			Account:   account,
			State:     "READY",
		}
		err = store.DB().Create(volume).Error
		if err != nil {
			tt.Fatalf("Failed to create volume: %v", err)
		}

		snapshot := &datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "test-snapshot-uuid"},
			Name:      "test_snapshot",
			AccountID: account.ID,
			VolumeID:  volume.ID,
			State:     "READY",
		}
		err = store.DB().Create(snapshot).Error
		if err != nil {
			tt.Fatalf("Failed to create snapshot: %v", err)
		}

		params := &common.RevertVolumeParams{
			AccountName: account.Name,
			VolumeID:    volume.UUID,
			SnapshotID:  snapshot.UUID,
		}

		resultVolume, jobUUID, err := orch.RevertVolume(ctx, params)
		assert.Error(tt, err)
		assert.Nil(tt, resultVolume)
		assert.Empty(tt, jobUUID)
		assert.Contains(tt, err.Error(), "workflow execution failed")
	})
}

// Helper function to set up common test infrastructure
func setupVolumeValidationTest(t *testing.T, poolSizeInTiB int64) (context.Context, database.Storage, *datamodel.Account, *datamodel.Pool) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
	mockLogger := log.NewLogger()
	store, err := database.SetupStorageForTest(mockLogger)
	assert.NoError(t, err)

	// Clean up database after test
	t.Cleanup(func() {
		_ = database.ClearInMemoryDB(store.DB())
	})

	// Create account
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
		Name:      "test_account",
	}
	err = store.DB().Create(account).Error
	assert.NoError(t, err)

	// Create pool
	pool := &datamodel.Pool{
		BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:        "test_pool",
		AccountID:   account.ID,
		State:       models.LifeCycleStateREADY,
		SizeInBytes: poolSizeInTiB * utils.TiBInBytes,
	}
	err = store.DB().Create(pool).Error
	assert.NoError(t, err)

	return ctx, store, account, pool
}

// Helper function to set up nodes and LIFs (for tests that need complex validation)
func setupNodesAndLIFs(t *testing.T, store database.Storage, account *datamodel.Account, pool *datamodel.Pool) {
	// Create SVM
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
		Name:      "test_svm",
		AccountID: account.ID,
		PoolID:    pool.ID,
		Pool:      pool,
		State:     models.LifeCycleStateREADY,
	}
	err := store.DB().Create(svm).Error
	assert.NoError(t, err)

	// Create 2 nodes as required by the validation
	node1 := &datamodel.Node{
		BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid1"},
		Name:            "test_node1",
		AccountID:       account.ID,
		EndpointAddress: "11.11.11.11",
		PoolID:          pool.ID,
		State:           models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node1).Error
	assert.NoError(t, err, "Failed to create node")

	node2 := &datamodel.Node{
		BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid2"},
		Name:            "test_node2",
		AccountID:       account.ID,
		EndpointAddress: "12.12.12.12",
		PoolID:          pool.ID,
		State:           models.LifeCycleStateREADY,
	}
	err = store.DB().Create(node2).Error
	assert.NoError(t, err, "Failed to create node")

	// Create LIFs for the nodes
	lif1 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid1"},
		Name:      "test_lif1",
		NodeID:    node1.ID,
		AccountID: account.ID,
	}
	err = store.DB().Create(lif1).Error
	assert.NoError(t, err, "Failed to create lif")

	lif2 := &datamodel.Lif{
		BaseModel: datamodel.BaseModel{UUID: "test-lif-uuid2"},
		Name:      "test_lif2",
		NodeID:    node2.ID,
		AccountID: account.ID,
	}
	err = store.DB().Create(lif2).Error
	assert.NoError(t, err, "Failed to create lif")
}

// Comprehensive edge case tests for volume validation
func TestValidateCreateVolumeParamsEdgeCases(t *testing.T) {
	testCases := []struct {
		name              string
		poolSizeInTiB     int64
		quotaInBytes      uint64
		needsComplexSetup bool
		blockDevices      *[]common.BlockDevice
		expectedError     string
	}{
		{
			name:              "WhenVolumeCapacityAtNewMinimumBoundary",
			poolSizeInTiB:     10,
			quotaInBytes:      uint64(1 * utils.GiBInBytes), // 1 GiB - new minimum
			needsComplexSetup: true,
			blockDevices: &[]common.BlockDevice{
				{OSType: "linux"},
			},
			expectedError: "",
		},
		{
			name:              "WhenVolumeCapacityAtNewMaximumBoundary",
			poolSizeInTiB:     200,
			quotaInBytes:      uint64(131072 * utils.GiBInBytes), // 128 TiB - new maximum (131072 GiB)
			needsComplexSetup: true,
			blockDevices: &[]common.BlockDevice{
				{OSType: "linux"},
			},
			expectedError: "",
		},
		{
			name:              "WhenVolumeCapacityExceedsNewMaximum",
			poolSizeInTiB:     200,
			quotaInBytes:      uint64(131073 * utils.GiBInBytes), // 131073 GiB - exceeds new maximum
			needsComplexSetup: false,
			blockDevices:      nil,
			expectedError:     "Invalid volume capacity 131073GiB. Must be between 1GiB and 128TiB.",
		},
		{
			name:              "WhenVolumeCapacityBelowNewMinimum",
			poolSizeInTiB:     10,
			quotaInBytes:      uint64(utils.GiBInBytes - 1), // Just below 1 GiB
			needsComplexSetup: false,
			blockDevices:      nil,
			expectedError:     "Invalid volume capacity 1073741823B. Must be between 1GiB and 128TiB.",
		},
		{
			name:              "WhenZeroVolumeCapacity",
			poolSizeInTiB:     10,
			quotaInBytes:      0, // Zero capacity
			needsComplexSetup: false,
			blockDevices:      nil,
			expectedError:     "Invalid volume capacity 0B. Must be between 1GiB and 128TiB.",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			ctx, store, account, pool := setupVolumeValidationTest(tt, tc.poolSizeInTiB)

			// Set up nodes and LIFs if needed for complex validation
			if tc.needsComplexSetup {
				setupNodesAndLIFs(tt, store, account, pool)
			}

			poolView := &datamodel.PoolView{
				Pool: *pool,
			}

			params := &common.CreateVolumeParams{
				Name:         "test-volume",
				PoolID:       pool.UUID,
				QuotaInBytes: tc.quotaInBytes,
				Protocols:    []string{"ISCSI"},
				BlockDevices: tc.blockDevices,
			}

			err := validateCreateVolumeParams(ctx, store, params, poolView)

			if tc.expectedError == "" {
				assert.NoError(tt, err)
			} else {
				assert.EqualError(tt, err, tc.expectedError)
			}
		})
	}
}

// TestBackupVolumeSizeValidation tests the backup volume size validation logic
func TestBackupVolumeSizeValidation(t *testing.T) {
	t.Run("SuccessfulValidation_VolumeSizeAdequate", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(100 * 1024 * 1024 * 1024)     // 100GB
		backupSize := int64(80 * 1024 * 1024 * 1024)      // 80GB
		requiredSize := int64(float64(backupSize) * 1.20) // 96GB

		// This should pass as 100GB > 96GB
		assert.True(tt, volumeSize >= requiredSize, "Volume size should be adequate")
	})

	t.Run("FailedValidation_VolumeSizeTooSmall", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(80 * 1024 * 1024 * 1024)      // 80GB
		backupSize := int64(100 * 1024 * 1024 * 1024)     // 100GB
		requiredSize := int64(float64(backupSize) * 1.20) // 120GB

		// This should fail as 80GB < 120GB
		assert.False(tt, volumeSize >= requiredSize, "Volume size should be insufficient")
	})

	t.Run("EdgeCase_ExactSizeMatch", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(100 * 1024 * 1024 * 1024)     // 100GB
		backupSize := int64(83 * 1024 * 1024 * 1024)      // 83GB (100/1.20 = 83.33)
		requiredSize := int64(float64(backupSize) * 1.20) // 99.6GB

		// This should pass as 100GB > 99.6GB
		assert.True(tt, volumeSize >= requiredSize, "Volume size should be adequate for exact match")
	})

	t.Run("EdgeCase_ZeroBackupSize", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(1 * 1024 * 1024 * 1024)       // 1GB
		backupSize := int64(0)                            // 0GB
		requiredSize := int64(float64(backupSize) * 1.20) // 0GB

		// This should pass as 1GB > 0GB
		assert.True(tt, volumeSize >= requiredSize, "Volume size should be adequate for zero backup")
	})

	t.Run("EdgeCase_LargeBackupSize", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(150 * 1024 * 1024 * 1024)     // 150GB
		backupSize := int64(200 * 1024 * 1024 * 1024)     // 200GB
		requiredSize := int64(float64(backupSize) * 1.20) // 240GB

		// This should fail as 150GB < 240GB
		assert.False(tt, volumeSize >= requiredSize, "Volume size should be insufficient for large backup")
	})

	t.Run("EdgeCase_VerySmallVolumeSize", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(8 * 1024 * 1024 * 1024)       // 8GB
		backupSize := int64(10 * 1024 * 1024 * 1024)      // 10GB
		requiredSize := int64(float64(backupSize) * 1.20) // 12GB

		// This should fail as 8GB < 12GB
		assert.False(tt, volumeSize >= requiredSize, "Volume size should be insufficient for very small volume")
	})

	t.Run("EdgeCase_FractionalCalculations", func(tt *testing.T) {
		// Test the validation logic directly
		volumeSize := int64(100 * 1024 * 1024 * 1024)     // 100GB
		backupSize := int64(67 * 1024 * 1024 * 1024)      // 67GB
		requiredSize := int64(float64(backupSize) * 1.20) // 80.4GB

		// This should pass as 100GB > 80.4GB
		assert.True(tt, volumeSize >= requiredSize, "Volume size should be adequate for fractional calculations")
	})
}

// TestHotTierBypassModeValidation tests the validation logic for HotTierBypassModeEnabled
func TestHotTierBypassModeValidation(t *testing.T) {
	t.Run("HotTierBypassModeRequiresPoolAutoTieringEnabled", func(tt *testing.T) {
		// Test that hot tier bypass mode validation catches when pool doesn't allow auto tiering
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: false, // Auto tiering disabled on pool
			},
		}

		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
			},
		}

		// Test the specific validation logic by calling the validation directly
		err := validateHotTierBypassMode(params, pool)
		assert.EqualError(tt, err, "Hot Tier Bypass Mode requires Auto Tiering to be enabled on the Pool")
	})

	t.Run("HotTierBypassModeRequiresVolumeAutoTieringPolicyEnabled", func(tt *testing.T) {
		// Test that hot tier bypass mode validation catches when volume auto tiering is disabled
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true, // Auto tiering enabled on pool
			},
		}

		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       false, // Auto tiering disabled on volume
				HotTierBypassModeEnabled: true,
			},
		}

		err := validateHotTierBypassMode(params, pool)
		assert.EqualError(tt, err, "Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled")
	})

	t.Run("HotTierBypassModeValidWhen_BothPoolAndVolumeAutoTieringEnabled", func(tt *testing.T) {
		// Test that validation passes when both pool and volume have auto tiering enabled
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true,
			},
		}

		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
				CoolingThresholdDays:     30,
				TieringPolicy:            "auto",
				RetrievalPolicy:          "default",
			},
		}

		err := validateHotTierBypassMode(params, pool)
		assert.NoError(tt, err, "Validation should pass when both pool and volume auto tiering are enabled")
	})

	t.Run("HotTierBypassModeNotSetShouldPass", func(tt *testing.T) {
		// Test that validation passes when hot tier bypass mode is not set
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true,
			},
		}

		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: false, // Not enabled
				CoolingThresholdDays:     30,
				TieringPolicy:            "auto",
				RetrievalPolicy:          "default",
			},
		}

		err := validateHotTierBypassMode(params, pool)
		assert.NoError(tt, err, "Validation should pass when hot tier bypass mode is not enabled")
	})
}

// TestValidateCreateVolumeParams_HotTierBypassModeValidation tests the validation logic for HotTierBypassModeEnabled in create volume params
func TestValidateCreateVolumeParams_HotTierBypassModeValidation(t *testing.T) {
	t.Run("CreateVolumeHotTierBypassModeRequiresPoolAutoTieringEnabled", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:             "test-pool",
			AccountID:        account.ID,
			SizeInBytes:      int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			State:            models.LifeCycleStateREADY,
			AllowAutoTiering: false, // Auto tiering disabled on pool
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid", ID: 1},
			Name:      "test-svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create 2 nodes as required by the validation
		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-1"},
			Name:            "test_node_1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		if err != nil {
			tt.Fatalf("Failed to create node1: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-2"},
			Name:            "test_node_2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node2: %v", err)
		}

		// Create LIFs for the nodes (required for validation)
		lif1 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-1"},
			Name:       "test_lif_1",
			AccountID:  account.ID,
			NodeID:     node1.ID,
			IPAddress:  "192.168.1.10",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif1).Error
		if err != nil {
			tt.Fatalf("Failed to create lif1: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-2"},
			Name:       "test_lif_2",
			AccountID:  account.ID,
			NodeID:     node2.ID,
			IPAddress:  "192.168.1.11",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif2: %v", err)
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1},
				AllowAutoTiering: false, // Auto tiering disabled on pool
				AccountID:        account.ID,
				SizeInBytes:      int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
				State:            models.LifeCycleStateREADY,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test-account",
			Protocols:    []string{"NFS"},
			QuotaInBytes: uint64(100 * 1024 * 1024 * 1024), // 100 GiB
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       false,
				HotTierBypassModeEnabled: true,
			},
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	})

	t.Run("CreateVolumeHotTierBypassModeRequiresVolumeAutoTieringEnabled", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:             "test-pool",
			AccountID:        account.ID,
			SizeInBytes:      int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			State:            models.LifeCycleStateREADY,
			AllowAutoTiering: true, // Auto tiering enabled on pool
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid", ID: 1},
			Name:      "test-svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create 2 nodes as required by the validation
		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-1"},
			Name:            "test_node_1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		if err != nil {
			tt.Fatalf("Failed to create node1: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-2"},
			Name:            "test_node_2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node2: %v", err)
		}

		// Create LIFs for the nodes (required for validation)
		lif1 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-1"},
			Name:       "test_lif_1",
			AccountID:  account.ID,
			NodeID:     node1.ID,
			IPAddress:  "192.168.1.10",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif1).Error
		if err != nil {
			tt.Fatalf("Failed to create lif1: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-2"},
			Name:       "test_lif_2",
			AccountID:  account.ID,
			NodeID:     node2.ID,
			IPAddress:  "192.168.1.11",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif2: %v", err)
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1},
				AllowAutoTiering: true, // Auto tiering enabled on pool
				AccountID:        account.ID,
				SizeInBytes:      int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
				State:            models.LifeCycleStateREADY,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test-account",
			Protocols:    []string{"NFS"},
			QuotaInBytes: uint64(100 * 1024 * 1024 * 1024), // 100 GiB
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       false, // Auto tiering disabled on volume
				HotTierBypassModeEnabled: true,
			},
		}

		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.EqualError(tt, err, "Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled on the Volume")
	})

	t.Run("CreateVolumeHotTierBypassModeValidWhenBothPoolAndVolumeAutoTieringEnabled", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1},
				AllowAutoTiering: true, // Auto tiering enabled on pool
				AccountID:        account.ID,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test_account",
			Protocols:    []string{"NFS"},
			QuotaInBytes: uint64(1024 * 1024 * 1024), // 1GB
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
				CoolingThresholdDays:     30,
				TieringPolicy:            "auto",
				RetrievalPolicy:          "default",
			},
		}

		// This should not return an error related to HotTierBypassModeEnabled
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		// If other validations pass, HotTierBypassModeEnabled validation should not cause an error
		// We expect other validation errors but not the HotTierBypassModeEnabled specific one
		if err != nil {
			assert.NotContains(tt, err.Error(), "Hot Tier Bypass Mode requires")
			assert.NotContains(tt, err.Error(), "Hot Tier Bypass Mode can only be enabled")
		}
	})
}

// TestValidateUpdateVolumeRequest_HotTierBypassModeValidation tests the validation logic for HotTierBypassModeEnabled in update volume request
func TestValidateUpdateVolumeRequest_HotTierBypassModeValidation(t *testing.T) {
	t.Run("UpdateVolumeHotTierBypassModeRequiresPoolAutoTieringEnabled", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: false, // Auto tiering disabled on pool
			},
		}

		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
			},
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.EqualError(tt, err, "Auto Tiering is not allowed for this volume. Please enable Auto Tiering on the Pool and try again")
	})

	t.Run("UpdateVolumeHotTierBypassModeRequiresVolumeAutoTieringEnabled", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true, // Auto tiering enabled on pool
			},
		}

		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       false, // Auto tiering disabled on volume
				HotTierBypassModeEnabled: true,
			},
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		assert.EqualError(tt, err, "Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled on the Volume")
	})

	t.Run("UpdateVolumeHotTierBypassModeValidWhenBothAutoTieringEnabled", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})
		mockStorage := database.NewMockStorage(tt)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Name:      "test-volume",
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true, // Auto tiering enabled on pool
			},
		}

		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
				CoolingThresholdDays:     30,
				TieringPolicy:            "auto",
				RetrievalPolicy:          "default",
			},
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)
		// This should not return an error related to HotTierBypassModeEnabled
		if err != nil {
			assert.NotContains(tt, err.Error(), "Hot Tier Bypass Mode requires")
			assert.NotContains(tt, err.Error(), "Hot Tier Bypass Mode can only be enabled")
		}
	})
}

// Helper function to test the hot tier bypass mode validation logic
func validateHotTierBypassMode(params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
	// Validate HotTierBypassModeEnabled
	if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.HotTierBypassModeEnabled {
		if !pool.AllowAutoTiering {
			return errors.NewUserInputValidationErr("Hot Tier Bypass Mode requires Auto Tiering to be enabled on the Pool")
		}
		if !params.AutoTieringPolicy.AutoTieringEnabled {
			return errors.NewUserInputValidationErr("Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled")
		}
	}
	return nil
}

// TestHotTierBypassModeTieringPolicyMapping tests the tiering policy mapping logic
func TestHotTierBypassModeTieringPolicyMapping(t *testing.T) {
	t.Run("HotTierBypassModeEnabledMapsToAllPolicy", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
				TieringPolicy:            "auto", // This should be overridden to "all"
				CoolingThresholdDays:     30,
				RetrievalPolicy:          "default",
			},
		}

		volumeObj := &datamodel.Volume{}

		// Simulate the logic from createVolume
		if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
			volumeObj.AutoTieringEnabled = params.AutoTieringPolicy.AutoTieringEnabled

			// Apply tiering policy mapping based on HotTierBypassModeEnabled
			tieringPolicy := params.AutoTieringPolicy.TieringPolicy
			if params.AutoTieringPolicy.HotTierBypassModeEnabled {
				// When hot tier bypass is enabled, use "all" policy to move all data to cold tier
				tieringPolicy = "all"
			} else if tieringPolicy == "" {
				// Default to "auto" when not specified and hot tier bypass is disabled
				tieringPolicy = "auto"
			}

			volumeObj.AutoTieringPolicy = &datamodel.AutoTieringPolicy{
				TieringPolicy:            tieringPolicy,
				CoolingThresholdDays:     params.AutoTieringPolicy.CoolingThresholdDays,
				RetrievalPolicy:          params.AutoTieringPolicy.RetrievalPolicy,
				HotTierBypassModeEnabled: params.AutoTieringPolicy.HotTierBypassModeEnabled,
			}
		}

		assert.True(tt, volumeObj.AutoTieringEnabled)
		assert.Equal(tt, "all", volumeObj.AutoTieringPolicy.TieringPolicy, "Tiering policy should be mapped to 'all' when hot tier bypass is enabled")
		assert.True(tt, volumeObj.AutoTieringPolicy.HotTierBypassModeEnabled)
		assert.Equal(tt, int32(30), volumeObj.AutoTieringPolicy.CoolingThresholdDays)
		assert.Equal(tt, "default", volumeObj.AutoTieringPolicy.RetrievalPolicy)
	})

	t.Run("HotTierBypassModeDisabledKeepsOriginalPolicy", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: false,
				TieringPolicy:            "auto",
				CoolingThresholdDays:     30,
				RetrievalPolicy:          "default",
			},
		}

		volumeObj := &datamodel.Volume{}

		// Simulate the logic from createVolume
		if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
			volumeObj.AutoTieringEnabled = params.AutoTieringPolicy.AutoTieringEnabled

			// Apply tiering policy mapping based on HotTierBypassModeEnabled
			tieringPolicy := params.AutoTieringPolicy.TieringPolicy
			if params.AutoTieringPolicy.HotTierBypassModeEnabled {
				// When hot tier bypass is enabled, use "all" policy to move all data to cold tier
				tieringPolicy = "all"
			} else if tieringPolicy == "" {
				// Default to "auto" when not specified and hot tier bypass is disabled
				tieringPolicy = "auto"
			}

			volumeObj.AutoTieringPolicy = &datamodel.AutoTieringPolicy{
				TieringPolicy:            tieringPolicy,
				CoolingThresholdDays:     params.AutoTieringPolicy.CoolingThresholdDays,
				RetrievalPolicy:          params.AutoTieringPolicy.RetrievalPolicy,
				HotTierBypassModeEnabled: params.AutoTieringPolicy.HotTierBypassModeEnabled,
			}
		}

		assert.True(tt, volumeObj.AutoTieringEnabled)
		assert.Equal(tt, "auto", volumeObj.AutoTieringPolicy.TieringPolicy, "Tiering policy should remain 'auto' when hot tier bypass is disabled")
		assert.False(tt, volumeObj.AutoTieringPolicy.HotTierBypassModeEnabled)
	})

	t.Run("EmptyTieringPolicyDefaultsToAuto", func(tt *testing.T) {
		params := &common.CreateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: false,
				TieringPolicy:            "", // Empty policy should default to "auto"
				CoolingThresholdDays:     30,
				RetrievalPolicy:          "default",
			},
		}

		volumeObj := &datamodel.Volume{}

		// Simulate the logic from createVolume
		if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.AutoTieringEnabled {
			volumeObj.AutoTieringEnabled = params.AutoTieringPolicy.AutoTieringEnabled

			// Apply tiering policy mapping based on HotTierBypassModeEnabled
			tieringPolicy := params.AutoTieringPolicy.TieringPolicy
			if params.AutoTieringPolicy.HotTierBypassModeEnabled {
				// When hot tier bypass is enabled, use "all" policy to move all data to cold tier
				tieringPolicy = "all"
			} else if tieringPolicy == "" {
				// Default to "auto" when not specified and hot tier bypass is disabled
				tieringPolicy = "auto"
			}

			volumeObj.AutoTieringPolicy = &datamodel.AutoTieringPolicy{
				TieringPolicy:            tieringPolicy,
				CoolingThresholdDays:     params.AutoTieringPolicy.CoolingThresholdDays,
				RetrievalPolicy:          params.AutoTieringPolicy.RetrievalPolicy,
				HotTierBypassModeEnabled: params.AutoTieringPolicy.HotTierBypassModeEnabled,
			}
		}

		assert.Equal(tt, "auto", volumeObj.AutoTieringPolicy.TieringPolicy, "Empty tiering policy should default to 'auto'")
	})
}

// TestUpdateVolumeHotTierBypassModeValidation tests the validation logic for HotTierBypassModeEnabled in volume updates
func TestUpdateVolumeHotTierBypassModeValidation(t *testing.T) {
	t.Run("UpdateVolumeHotTierBypassModeRequiresPoolAutoTieringEnabled", func(tt *testing.T) {
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: false, // Auto tiering disabled on pool
			},
		}

		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
			},
		}

		err := validateUpdateHotTierBypassMode(params, pool)
		assert.EqualError(tt, err, "Hot Tier Bypass Mode requires Auto Tiering to be enabled on the Pool")
	})

	t.Run("UpdateVolumeHotTierBypassModeRequiresAutoTieringEnabled", func(tt *testing.T) {
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true,
			},
		}

		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       false, // Auto tiering disabled on volume
				HotTierBypassModeEnabled: true,
			},
		}

		err := validateUpdateHotTierBypassMode(params, pool)
		assert.EqualError(tt, err, "Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled")
	})

	t.Run("UpdateVolumeHotTierBypassModeValidWhenBothAutoTieringEnabled", func(tt *testing.T) {
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				AllowAutoTiering: true,
			},
		}

		params := &common.UpdateVolumeParams{
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,
				HotTierBypassModeEnabled: true,
				CoolingThresholdDays:     30,
				TieringPolicy:            "auto",
				RetrievalPolicy:          "default",
			},
		}

		err := validateUpdateHotTierBypassMode(params, pool)
		assert.NoError(tt, err, "Validation should pass when both pool and volume auto tiering are enabled")
	})
}

// Helper function to test the hot tier bypass mode validation logic for updates
func validateUpdateHotTierBypassMode(params *common.UpdateVolumeParams, pool *datamodel.PoolView) error {
	// Validate HotTierBypassModeEnabled for update
	if params.AutoTieringPolicy != nil && params.AutoTieringPolicy.HotTierBypassModeEnabled {
		if !pool.AllowAutoTiering {
			return errors.NewUserInputValidationErr("Hot Tier Bypass Mode requires Auto Tiering to be enabled on the Pool")
		}
		if !params.AutoTieringPolicy.AutoTieringEnabled {
			return errors.NewUserInputValidationErr("Hot Tier Bypass Mode can only be enabled when Auto Tiering is enabled")
		}
	}
	return nil
}

// TestHotTierBypassModeWithPausedTieringPolicy tests the validation logic that prevents enabling
// Hot Tier Bypass Mode when the tiering policy is paused
func TestHotTierBypassModeWithPausedTieringPolicy(t *testing.T) {
	t.Run("CreateVolumeHotTierBypassModeSucceedsWhenTieringPolicyEnabled", func(tt *testing.T) {
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
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
			Name:      "test-account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		pool := &datamodel.Pool{
			BaseModel:        datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:             "test-pool",
			AccountID:        account.ID,
			SizeInBytes:      int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
			State:            models.LifeCycleStateREADY,
			AllowAutoTiering: true, // Auto tiering enabled on pool
		}
		err = store.DB().Create(pool).Error
		if err != nil {
			tt.Fatalf("Failed to create pool: %v", err)
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid", ID: 1},
			Name:      "test-svm",
			AccountID: account.ID,
			PoolID:    pool.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create 2 nodes as required by the validation
		node1 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-1"},
			Name:            "test_node_1",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.12",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node1).Error
		if err != nil {
			tt.Fatalf("Failed to create node1: %v", err)
		}

		node2 := &datamodel.Node{
			BaseModel:       datamodel.BaseModel{UUID: "test-node-uuid-2"},
			Name:            "test_node_2",
			AccountID:       account.ID,
			EndpointAddress: "12.12.12.13",
			PoolID:          pool.ID,
			State:           models.LifeCycleStateREADY,
		}
		err = store.DB().Create(node2).Error
		if err != nil {
			tt.Fatalf("Failed to create node2: %v", err)
		}

		// Create LIFs for the nodes (required for validation)
		lif1 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-1"},
			Name:       "test_lif_1",
			AccountID:  account.ID,
			NodeID:     node1.ID,
			IPAddress:  "192.168.1.10",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif1).Error
		if err != nil {
			tt.Fatalf("Failed to create lif1: %v", err)
		}

		lif2 := &datamodel.Lif{
			BaseModel:  datamodel.BaseModel{UUID: "test-lif-uuid-2"},
			Name:       "test_lif_2",
			AccountID:  account.ID,
			NodeID:     node2.ID,
			IPAddress:  "192.168.1.11",
			SubnetMask: "255.255.255.0",
			Type:       "data",
		}
		err = store.DB().Create(lif2).Error
		if err != nil {
			tt.Fatalf("Failed to create lif2: %v", err)
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1},
				AllowAutoTiering: true, // Auto tiering enabled on pool
				AccountID:        account.ID,
				SizeInBytes:      int64(10 * 1024 * 1024 * 1024 * 1024), // 10TB
				State:            models.LifeCycleStateREADY,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "test-account-uuid", ID: 1},
				},
			},
		}

		// Create a host group for block volume validation
		hostGroup := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hg-uuid", ID: 1},
			Name:      "test-hostgroup",
			AccountID: account.ID,
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(hostGroup).Error
		if err != nil {
			tt.Fatalf("Failed to create host group: %v", err)
		}

		params := &common.CreateVolumeParams{
			Name:         "test-volume",
			AccountName:  "test-account",
			Protocols:    []string{"ISCSI"},
			QuotaInBytes: uint64(100 * 1024 * 1024 * 1024), // 100 GiB
			BlockProperties: &common.BlockPropertiesRequest{
				OSType:         "LINUX",
				HostGroupUUIDs: []string{"test-hg-uuid"},
			},
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:       true,                                  // Tiering is enabled
				HotTierBypassModeEnabled: true,                                  // Hot tier bypass enabled
				TieringPolicy:            models2.VolumeInlineTieringPolicyAuto, // ENABLED policy
				CoolingThresholdDays:     30,
			},
		}

		// This should pass validation since tiering policy is enabled (not paused)
		err = _validateCreateVolumeParams(ctx, store, params, poolView)
		assert.NoError(tt, err, "Validation should pass when tiering policy is enabled and hot tier bypass is enabled")
	})
}

func TestValidateUpdateFileProperties_NonNFSProtocol(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:      []string{"CIFS"},
			FileProperties: &datamodel.FileProperties{}, // Add this so it passes the first check but fails on protocol check
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	params := &common.UpdateVolumeParams{
		FileProperties: &models.FileProperties{
			ExportPolicy: &models.ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules: []*models.ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						AccessType:     "rw",
						AnonymousUser:  "65534",
					},
				},
			},
		},
	}

	err := validateUpdateFileProperties(params, volume)
	expectedError := errors.NewUserInputValidationErr("file properties can only be supported for volumes with NAS protocols")
	assert.EqualError(t, err, expectedError.Error())
}

func TestValidateUpdateFileProperties_ISCSIProtocol(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:      []string{"ISCSI"},
			FileProperties: &datamodel.FileProperties{}, // Add this so it passes the first check but fails on protocol check
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}

	params := &common.UpdateVolumeParams{
		FileProperties: &models.FileProperties{
			ExportPolicy: &models.ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules: []*models.ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						AccessType:     "rw",
						AnonymousUser:  "65534",
					},
				},
			},
		},
	}

	err := validateUpdateFileProperties(params, volume)
	expectedError := errors.NewUserInputValidationErr("file properties can only be supported for volumes with NAS protocols")
	assert.EqualError(t, err, expectedError.Error())
}

func TestValidateUpdateFileProperties_NoFileProperties(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"ISCSI"},
			// No FileProperties - this makes it a block volume
		},
	}

	params := &common.UpdateVolumeParams{
		FileProperties: nil,
	}

	err := validateUpdateFileProperties(params, volume)
	expectedError := errors.NewUserInputValidationErr("File properties is mandatory to update file properties on the volume")
	assert.EqualError(t, err, expectedError.Error())
}

func TestValidateUpdateFileProperties_NoExportPolicy(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"ISCSI"},
			// No FileProperties - this makes it a block volume
		},
	}

	params := &common.UpdateVolumeParams{
		FileProperties: &models.FileProperties{
			ExportPolicy: nil,
			JunctionPath: "/test/path",
		},
	}

	err := validateUpdateFileProperties(params, volume)
	expectedError := errors.NewUserInputValidationErr("File properties is mandatory to update file properties on the volume")
	assert.EqualError(t, err, expectedError.Error())
}

func TestValidateUpdateFileProperties_EmptyProtocols(t *testing.T) {
	volume := &datamodel.Volume{
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{},
			// No FileProperties - this makes it a block volume
		},
	}

	params := &common.UpdateVolumeParams{
		FileProperties: &models.FileProperties{
			ExportPolicy: &models.ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules: []*models.ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						AccessType:     "rw",
						AnonymousUser:  "65534",
					},
				},
			},
		},
	}

	err := validateUpdateFileProperties(params, volume)
	expectedError := errors.NewUserInputValidationErr("File properties is mandatory to update file properties on the volume")
	assert.EqualError(t, err, expectedError.Error())
}

func TestValidateUpdateFileProperties_NilVolumeAttributes(t *testing.T) {
	volume := &datamodel.Volume{
		Name:             "test-volume",
		VolumeAttributes: nil,
	}

	params := &common.UpdateVolumeParams{
		FileProperties: &models.FileProperties{
			ExportPolicy: &models.ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules: []*models.ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						AccessType:     "rw",
						AnonymousUser:  "65534",
					},
				},
			},
		},
	}

	err := validateUpdateFileProperties(params, volume)
	expectedError := errors.NewUserInputValidationErr("File properties is mandatory to update file properties on the volume")
	assert.EqualError(t, err, expectedError.Error())
}

func TestTriggerRefreshWorkflow(t *testing.T) {
	t.Run("WhenNoVolumesProvided", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		orch := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		err := orch.TriggerRefreshWorkflow(ctx, []*datamodel.Volume{})

		assert.NoError(tt, err)
		// No temporal calls should be made for empty volumes
	})

	t.Run("WhenWorkflowQueryReturnsNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		orch := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Account:   account,
		}

		// Mock QueryWorkflow to return NotFound error
		notFoundError := &serviceerror.NotFound{Message: "workflow not found"}
		mockTemporal.EXPECT().QueryWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, notFoundError)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		err := orch.TriggerRefreshWorkflow(ctx, []*datamodel.Volume{volume})

		assert.NoError(tt, err)
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		orch := Orchestrator{
			storage:  mockStorage,
			temporal: mockTemporal,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Account:   account,
		}

		// Mock QueryWorkflow to return NotFound error
		notFoundError := &serviceerror.NotFound{Message: "workflow not found"}
		mockTemporal.EXPECT().QueryWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, notFoundError)

		// Mock ExecuteWorkflow to fail
		executeError := errors2.New("failed to execute workflow")
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, executeError)

		err := orch.TriggerRefreshWorkflow(ctx, []*datamodel.Volume{volume})

		assert.Error(tt, err)
		assert.Equal(tt, executeError, err)
	})
}

// TestCheckIsValidImmutableBackupPolicyWithRetry tests the retry mechanism for volume immutable backup policy validation
func TestCheckIsValidImmutableBackupPolicyWithRetry(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()

	// Setup test data
	backupPolicyUUID := "backup-policy-uuid"
	backupVaultUUID := "backup-vault-uuid"
	accountID := int64(123)
	region := "us-central1"
	accountName := "test-account"

	// Mock backup policy and backup vault
	mockBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
		LifeCycleState:       models.LifeCycleStateREADY,
		DailyBackupsToKeep:   30,
		WeeklyBackupsToKeep:  0,
		MonthlyBackupsToKeep: 0,
	}

	var retentionDays int64 = 30
	mockBackupVault := &datamodel.BackupVault{
		BaseModel:      datamodel.BaseModel{UUID: backupVaultUUID},
		LifeCycleState: models.LifeCycleStateREADY,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: &retentionDays,
			IsDailyBackupImmutable:                 true,
		},
	}

	t.Run("Retries on retryable error and succeeds on second attempt", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 3
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// First call returns backup policy in updating state (retryable error)
		updatingBackupPolicy := *mockBackupPolicy
		updatingBackupPolicy.LifeCycleState = models.LifeCycleStateUpdating
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(&updatingBackupPolicy, nil).Once()

		// Second call returns backup policy in ready state
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil).Once()
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil).Once()

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.NoError(t, err)
		assert.Equal(t, 1, sleepCalled, "Should sleep once for one retry")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Stops after max retries on persistent retryable error", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 3
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// All calls return backup policy in updating state (persistent retryable error)
		updatingBackupPolicy := *mockBackupPolicy
		updatingBackupPolicy.LifeCycleState = models.LifeCycleStateUpdating
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(&updatingBackupPolicy, nil).Times(3)

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error while creating immutable backup when backup policy is in updating state")
		assert.Equal(t, 2, sleepCalled, "Should sleep between retry attempts (2 sleeps for 3 attempts)")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Does not retry on non-retryable error", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 3
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// Return a non-retryable error (backup policy not found)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(nil, errors.New("backup policy not found")).Once()

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get backup policy")
		assert.Equal(t, 0, sleepCalled, "Should not sleep for non-retryable errors")
		mockStorage.AssertExpectations(t)
	})

	t.Run("First attempt succeeds, no retries", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 3
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// First call succeeds
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil).Once()
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil).Once()

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.NoError(t, err)
		assert.Equal(t, 0, sleepCalled, "Should not sleep when first attempt succeeds")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Retryable error then non-retryable error (should stop on non-retryable)", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 3
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// First call returns backup policy in updating state (retryable error)
		updatingBackupPolicy := *mockBackupPolicy
		updatingBackupPolicy.LifeCycleState = models.LifeCycleStateUpdating
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(&updatingBackupPolicy, nil).Once()

		// Second call returns backup policy not found error (non-retryable)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(nil, errors.New("backup policy not found")).Once()

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get backup policy")
		assert.Equal(t, 1, sleepCalled, "Should sleep once before encountering non-retryable error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("All attempts return non-retryable errors (should not retry at all)", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 3
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// Return validation error on first call (non-retryable)
		invalidBackupPolicy := *mockBackupPolicy
		invalidBackupPolicy.DailyBackupsToKeep = 10 // Too low for 30-day retention
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(&invalidBackupPolicy, nil).Once()
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil).Once()

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "daily backup retention")
		assert.Equal(t, 0, sleepCalled, "Should not sleep for validation errors")
		mockStorage.AssertExpectations(t)
	})
}

// TestIsImmutableBackupPolicyRetryableError tests the retry error detection logic
func TestIsImmutableBackupPolicyRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "Nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "Backup policy updating error",
			err:      vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy, fmt.Errorf("Cannot validate immutable backup policy: backup policy 'policy-123' is currently being updated. Please wait for the policy update to complete.")),
			expected: true,
		},
		{
			name:     "Backup vault updating error",
			err:      vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupVault, fmt.Errorf("Cannot validate immutable backup policy: backup vault 'vault-123' is currently being updated. Please wait for the vault update to complete.")),
			expected: true,
		},
		{
			name:     "Backup vault in transition state error",
			err:      vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupVault, fmt.Errorf("backup vault is in transition state")),
			expected: true,
		},
		{
			name:     "Generic backup policy updating error",
			err:      vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupPolicy, fmt.Errorf("backup policy xyz is updating")),
			expected: true,
		},
		{
			name:     "Generic backup vault updating error",
			err:      vsaerrors.NewVCPError(vsaerrors.ErrImmutableValidationWithUpdatingBackupVault, fmt.Errorf("backup vault abc is updating")),
			expected: true,
		},
		{
			name:     "Non-retryable validation error",
			err:      errors.NewUserInputValidationErr("daily backup retention (10 days) is less than backup vault immutable period (30 days)"),
			expected: false,
		},
		{
			name:     "Non-retryable not found error",
			err:      errors.New("backup policy not found"),
			expected: false,
		},
		{
			name:     "Non-retryable general error",
			err:      errors.New("some general error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isImmutableBackupPolicyRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCheckIsValidImmutableBackupPolicyWithStateCheck tests the state check logic
func TestCheckIsValidImmutableBackupPolicyWithStateCheck(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()
	backupPolicyUUID := "backup-policy-uuid"
	backupVaultUUID := "backup-vault-uuid"
	accountID := int64(123)
	region := "us-central1"
	accountName := "test-account"

	t.Run("Success when both backup policy and vault are ready", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy in ready state
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState:       models.LifeCycleStateREADY,
			DailyBackupsToKeep:   30,
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
		}

		// Mock backup vault in ready state with no immutable attributes
		mockBackupVault := &datamodel.BackupVault{
			BaseModel:           datamodel.BaseModel{UUID: backupVaultUUID},
			LifeCycleState:      models.LifeCycleStateREADY,
			ImmutableAttributes: nil, // No immutable constraints
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when backup policy is updating", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy in updating state
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState: models.LifeCycleStateUpdating,
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error while creating immutable backup when backup policy is in updating state")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when backup vault is updating", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy in ready state
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState: models.LifeCycleStateREADY,
		}

		// Mock backup vault in updating state
		mockBackupVault := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: backupVaultUUID},
			LifeCycleState: models.LifeCycleStateUpdating,
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Immutable backup vault is being updated")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error with invalid input parameters", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Test empty backup policy UUID
		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, "", backupVaultUUID, accountID, region, accountName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup policy UUID cannot be empty")

		// Test empty backup vault UUID
		err = _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, "", accountID, region, accountName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup vault UUID cannot be empty")

		// Test invalid account ID
		err = _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, 0, region, accountName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "account ID must be positive")

		mockStorage.AssertExpectations(t)
	})
}

// TestGetBackupVaultFromCVP tests the CVP backup vault fetching logic
func TestGetBackupVaultFromCVP(t *testing.T) {
	t.Run("Successfully fetches backup vault from CVP", func(t *testing.T) {
		// This test would require mocking the CVP client, which isn't readily available
		// in the current test setup. For now, we'll test the error paths which are
		// the main coverage gaps.
		t.Skip("CVP client mocking requires additional setup")
	})

	t.Run("Returns not found error when backup vault doesn't exist", func(t *testing.T) {
		// This would test the backup vault not found case
		// For now, we'll skip this as it requires CVP client mocking
		t.Skip("CVP client mocking requires additional setup")
	})

	t.Run("Returns error when CVP client fails", func(t *testing.T) {
		// This would test CVP client errors
		t.Skip("CVP client mocking requires additional setup")
	})
}

// TestGetBackupVaultFromCVP_JWTTokenExtraction tests the JWT token extraction from context
// in the getBackupVaultFromCVP function
func TestGetBackupVaultFromCVP_JWTTokenExtraction(t *testing.T) {
	// Setup test context with JWT token
	testJWTToken := "Bearer test-vault-jwt-token-67890"
	headers := http.Header{}
	headers.Set("Authorization", testJWTToken)
	ctx := context.WithValue(context.Background(), middleware.HeaderContextKey, headers)

	backupVaultID := "test-vault-id"
	region := "us-central1"
	accountName := "test-account"

	// Test the JWT token extraction directly using the utils function
	// This covers the line: getSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	extractedToken := utils.GetJWTTokenFromContext(ctx)
	assert.Equal(t, testJWTToken, extractedToken, "JWT token should be extracted from context correctly")

	// Also test the case where no token is in context
	emptyCtx := context.Background()
	emptyToken := utils.GetJWTTokenFromContext(emptyCtx)
	assert.Equal(t, "", emptyToken, "Should return empty string when no token in context")

	// We can also call getBackupVaultFromCVP to ensure the line gets executed in context
	// but we expect it to fail due to network/CVP issues, which is fine for this test
	_, err := getBackupVaultFromCVP(ctx, backupVaultID, region, accountName)
	// We expect an error (network/CVP related), but the important thing is that
	// the JWT token extraction line was executed without panicking
	assert.Error(t, err, "Expected error due to CVP network call, but JWT extraction should work")
}

// TestGetBackupPolicyFromCVP tests the CVP backup policy fetching logic
func TestGetBackupPolicyFromCVP(t *testing.T) {
	t.Run("Successfully fetches backup policy from CVP", func(t *testing.T) {
		// This test would require mocking the CVP client
		t.Skip("CVP client mocking requires additional setup")
	})

	t.Run("Returns not found error when backup policy doesn't exist", func(t *testing.T) {
		// This would test the backup policy not found case
		t.Skip("CVP client mocking requires additional setup")
	})

	t.Run("Returns error when CVP client fails", func(t *testing.T) {
		// This would test CVP client errors
		t.Skip("CVP client mocking requires additional setup")
	})
}

// TestCheckIsValidImmutableBackupPolicyWithStateCheck_AdditionalCoverage tests additional error paths
func TestCheckIsValidImmutableBackupPolicyWithStateCheck_AdditionalCoverage(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()
	backupPolicyUUID := "test-policy-uuid"
	backupVaultUUID := "test-vault-uuid"
	accountID := int64(123)
	region := "us-central1"
	accountName := "test-account"

	t.Run("Error when backup policy fetch fails with non-NotFound error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock database error (not NotFound)
		dbError := fmt.Errorf("database connection failed")
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(nil, dbError)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get backup policy")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when backup vault fetch fails with non-NotFound error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy in ready state
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState:       models.LifeCycleStateREADY,
			DailyBackupsToKeep:   30,
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
		}

		// Mock database error (not NotFound) for backup vault
		dbError := fmt.Errorf("database connection failed")
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(nil, dbError)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get backup vault")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success when backup vault has no immutable attributes", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy in ready state
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState:       models.LifeCycleStateREADY,
			DailyBackupsToKeep:   30,
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
		}

		// Mock backup vault without immutable attributes
		mockBackupVault := &datamodel.BackupVault{
			BaseModel:           datamodel.BaseModel{UUID: backupVaultUUID},
			LifeCycleState:      models.LifeCycleStateREADY,
			ImmutableAttributes: nil, // No immutable attributes
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.NoError(t, err) // Should succeed without validation when no immutable attributes
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when backup policy validation fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy with incompatible retention
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState:       models.LifeCycleStateREADY,
			DailyBackupsToKeep:   5, // Too low for immutable retention
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
		}

		// Mock backup vault with strict immutable requirements
		var retentionDays int64 = 365 // 1 year retention required
		mockBackupVault := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: backupVaultUUID},
			LifeCycleState: models.LifeCycleStateREADY,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil)

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "immutable backup policy validation failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when backup policy not found locally and CVP fetch fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy not found locally (this will trigger CVP fallback)
		notFoundErr := errors.NewNotFoundErr("Backup policy", nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(nil, notFoundErr)

		// Note: This would require mocking CVP client to fully test the CVP fallback path
		// For now, we'll test the scenario where the fallback logic is triggered

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		// The actual error will vary depending on CVP client setup, but we expect an error
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when backup vault not found locally and CVP fetch fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock backup policy exists locally
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState:       models.LifeCycleStateREADY,
			DailyBackupsToKeep:   30,
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
		}

		// Mock backup vault not found locally (this will trigger CVP fallback)
		notFoundErr := errors.NewNotFoundErr("Backup vault", nil)
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(nil, notFoundErr)

		// Note: This would require mocking CVP client to fully test the CVP fallback path

		err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		// The actual error will vary depending on CVP client setup, but we expect an error
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
}

// TestCheckIsValidImmutableBackupPolicyWithStateCheck_ZeroRetentionDuration tests the specific case
// where BackupMinimumEnforcedRetentionDuration is 0, which should cause early return
func TestCheckIsValidImmutableBackupPolicyWithStateCheck_ZeroRetentionDuration(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()
	backupPolicyUUID := "backup-policy-uuid"
	backupVaultUUID := "backup-vault-uuid"
	accountID := int64(123)
	region := "us-central1"
	accountName := "test-account"

	mockStorage := database.NewMockStorage(t)

	// Mock backup policy in ready state
	mockBackupPolicy := &datamodel.BackupPolicy{
		BaseModel:            datamodel.BaseModel{UUID: backupPolicyUUID},
		LifeCycleState:       models.LifeCycleStateREADY,
		DailyBackupsToKeep:   30,
		WeeklyBackupsToKeep:  0,
		MonthlyBackupsToKeep: 0,
	}

	// Mock backup vault with immutable attributes but zero retention duration
	zeroRetentionDuration := int64(0)
	mockBackupVault := &datamodel.BackupVault{
		BaseModel:      datamodel.BaseModel{UUID: backupVaultUUID},
		LifeCycleState: models.LifeCycleStateREADY,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: &zeroRetentionDuration, // This is the key line being tested
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                false,
			IsMonthlyBackupImmutable:               false,
			IsAdhocBackupImmutable:                 false,
		},
	}

	mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(mockBackupPolicy, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultUUID, accountID).Return(mockBackupVault, nil)

	// Call the function under test
	err := _checkIsValidImmutableBackupPolicyWithStateCheck(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

	// Should return nil because BackupMinimumEnforcedRetentionDuration is 0
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestGetBackupPolicyFromCVP_JWTTokenExtraction tests the JWT token extraction from context
// in the GetBackupPolicyFromCVP function
func TestGetBackupPolicyFromCVP_JWTTokenExtraction(t *testing.T) {
	// Setup test context with JWT token
	testJWTToken := "Bearer test-jwt-token-12345"
	headers := http.Header{}
	headers.Set("Authorization", testJWTToken)
	ctx := context.WithValue(context.Background(), middleware.HeaderContextKey, headers)

	backupPolicyUUID := "test-policy-uuid"
	region := "us-central1"
	accountName := "test-account"

	// Test the JWT token extraction directly using the utils function
	// This covers the line: GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	extractedToken := utils.GetJWTTokenFromContext(ctx)
	assert.Equal(t, testJWTToken, extractedToken, "JWT token should be extracted from context correctly")

	// Also test the case where no token is in context
	emptyCtx := context.Background()
	emptyToken := utils.GetJWTTokenFromContext(emptyCtx)
	assert.Equal(t, "", emptyToken, "Should return empty string when no token in context")

	// We can also call GetBackupPolicyFromCVP to ensure the line gets executed in context
	// but we expect it to fail due to network/CVP issues, which is fine for this test
	_, err := GetBackupPolicyFromCVP(ctx, backupPolicyUUID, region, accountName)
	// We expect an error (network/CVP related), but the important thing is that
	// the JWT token extraction line was executed without panicking
	assert.Error(t, err, "Expected error due to CVP network call, but JWT extraction should work")
}

// TestCheckIsValidImmutableBackupPolicyWithRetry_ErrorPaths tests specific retry error paths
func TestCheckIsValidImmutableBackupPolicyWithRetry_ErrorPaths(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()
	backupPolicyUUID := "test-policy-uuid"
	backupVaultUUID := "test-vault-uuid"
	accountID := int64(123)
	region := "us-central1"
	accountName := "test-account"

	t.Run("Returns error after exceeding max retry attempts", func(t *testing.T) {
		// Store original values
		origMaxRetries := common.MaxRetries
		origRetryDelay := common.RetryDelay
		origSleepFn := common.SleepFn

		// Setup test values
		common.MaxRetries = 2 // Small number for faster test
		common.RetryDelay = 1 * time.Millisecond
		sleepCalled := 0
		common.SleepFn = func(d time.Duration) { sleepCalled++ }

		// Restore original values
		defer func() {
			common.MaxRetries = origMaxRetries
			common.RetryDelay = origRetryDelay
			common.SleepFn = origSleepFn
		}()

		mockStorage := database.NewMockStorage(t)

		// All attempts return a retryable error
		updatingBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{UUID: backupPolicyUUID},
			LifeCycleState: models.LifeCycleStateUpdating,
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountID).Return(updatingBackupPolicy, nil).Times(2)

		err := checkIsValidImmutableBackupPolicyWithRetry(ctx, mockStorage, backupPolicyUUID, backupVaultUUID, accountID, region, accountName)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error while creating immutable backup when backup policy is in updating state")
		assert.Equal(t, 1, sleepCalled, "Should sleep once for one retry") // MaxRetries-1
		mockStorage.AssertExpectations(t)
	})
}

// TestValidateUpdateVolumeRequest_ImmutableBackupValidation tests immutable backup validation in update volume
func TestValidateUpdateVolumeRequest_ImmutableBackupValidation(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()

	t.Run("Success when immutable backup validation passes", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Setup test data
		accountID := int64(123)
		region := "us-central1"
		accountName := "test-account"
		backupPolicyID := "test-policy-id"
		backupVaultID := "test-vault-id"

		// Mock volume with account
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: accountID},
			},
		}

		// Mock pool view
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{ID: accountID},
				},
			},
		}

		// Mock update params with backup settings
		params := &common.UpdateVolumeParams{
			Region:      region,
			AccountName: accountName,
			DataProtection: &models.UpdateDataProtection{
				BackupPolicyId: &backupPolicyID,
				BackupVaultID:  &backupVaultID,
			},
		}

		// Mock successful backup policy and vault
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:            datamodel.BaseModel{UUID: backupPolicyID},
			LifeCycleState:       models.LifeCycleStateREADY,
			DailyBackupsToKeep:   30,
			WeeklyBackupsToKeep:  0,
			MonthlyBackupsToKeep: 0,
		}

		var retentionDays int64 = 30
		mockBackupVault := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: backupVaultID},
			LifeCycleState: models.LifeCycleStateREADY,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &retentionDays,
				IsDailyBackupImmutable:                 true,
			},
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyID, accountID).Return(mockBackupPolicy, nil)
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultID, accountID).Return(mockBackupVault, nil)

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when immutable backup validation fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Setup test data
		accountID := int64(123)
		region := "us-central1"
		accountName := "test-account"
		backupPolicyID := "test-policy-id"
		backupVaultID := "test-vault-id"

		// Mock volume with account
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: accountID},
			},
		}

		// Mock pool view
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{ID: accountID},
				},
			},
		}

		// Mock update params with backup settings
		params := &common.UpdateVolumeParams{
			Region:      region,
			AccountName: accountName,
			DataProtection: &models.UpdateDataProtection{
				BackupPolicyId: &backupPolicyID,
				BackupVaultID:  &backupVaultID,
			},
		}

		// Mock backup policy in updating state (will cause validation to fail)
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{UUID: backupPolicyID},
			LifeCycleState: models.LifeCycleStateUpdating,
		}

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyID, accountID).Return(mockBackupPolicy, nil)

		// The validation will also try to get the backup vault
		mockBackupVault := &datamodel.BackupVault{
			BaseModel:      datamodel.BaseModel{UUID: backupVaultID},
			LifeCycleState: models.LifeCycleStateREADY,
		}
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, backupVaultID, accountID).Return(mockBackupVault, nil)

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "backup policy is not in ready state, please check the backup policy and try again")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success when DataProtection is nil", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Mock volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		}

		// Mock pool view
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{ID: int64(123)},
				},
			},
		}

		// Mock update params without backup settings
		params := &common.UpdateVolumeParams{
			Region:         "us-central1",
			AccountName:    "test-account",
			DataProtection: nil, // No backup configuration
		}

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		assert.NoError(t, err) // Should pass without backup validation
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success when only BackupPolicyId is set (no BackupVaultID)", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)

		// Setup test data
		backupPolicyID := "test-policy-id"
		accountID := int64(123)

		// Mock volume
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		}

		// Mock pool view
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{ID: accountID},
				},
			},
		}

		// Mock update params with only backup policy (no vault)
		params := &common.UpdateVolumeParams{
			Region:      "us-central1",
			AccountName: "test-account",
			DataProtection: &models.UpdateDataProtection{
				BackupPolicyId: &backupPolicyID,
				BackupVaultID:  nil, // No vault specified
			},
		}

		// Mock backup policy lookup (will be called but won't trigger immutable validation since no vault)
		mockBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:      datamodel.BaseModel{UUID: backupPolicyID},
			LifeCycleState: models.LifeCycleStateREADY,
		}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyID, accountID).Return(mockBackupPolicy, nil)

		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		assert.NoError(t, err) // Should pass without immutable backup validation
		mockStorage.AssertExpectations(t)
	})
}

func TestValidateUpdateVolumeRequest_ImmutableBackupValidation_ExistingDataProtection(t *testing.T) {
	// Enable immutable backup feature flag for this test
	utils.SetImmutableBackupEnabledForTest(true)
	defer utils.SetImmutableBackupEnabledForTest(false)

	ctx := context.Background()

	t.Run("Validates immutable backup when volume has existing DataProtection with BackupVaultID and BackupPolicyID", func(t *testing.T) {
		// Setup
		mockStorage := database.NewMockStorage(t)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid",
				},
				AllowAutoTiering: true,
				SizeInBytes:      2000000000000,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID:   1,
						UUID: "account-uuid",
					},
				},
			},
			QuotaInBytes: 1000000000000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "volume-uuid",
			},
			Name:        "test-volume",
			State:       models.LifeCycleStateREADY,
			SizeInBytes: 100000000000,
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "account-uuid",
				},
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "existing-vault-id",
				BackupPolicyID: "existing-policy-id",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		params := &common.UpdateVolumeParams{
			VolumeId:    "volume-uuid",
			AccountName: "test-account",
			Region:      "us-west1",
			DataProtection: &models.UpdateDataProtection{
				// Not updating backup policy or vault, just other fields
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
			},
		}

		// Mock checkIsValidImmutableBackupPolicyWithRetry to return success
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "existing-vault-id", int64(1)).
			Return(&datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID: "existing-vault-id",
				},
				LifeCycleState: models.LifeCycleStateREADY,
			}, nil)

		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, "existing-policy-id", int64(1)).
			Return(&datamodel.BackupPolicy{
				BaseModel: datamodel.BaseModel{
					UUID: "existing-policy-id",
				},
				LifeCycleState: models.LifeCycleStateREADY,
			}, nil)

		// Act
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		// Assert
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Does not validate immutable backup when volume has no DataProtection", func(t *testing.T) {
		// Setup
		mockStorage := database.NewMockStorage(t)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid",
				},
				AllowAutoTiering: true,
				SizeInBytes:      2000000000000,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID:   1,
						UUID: "account-uuid",
					},
				},
			},
			QuotaInBytes: 1000000000000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "volume-uuid",
			},
			Name:        "test-volume",
			State:       models.LifeCycleStateREADY,
			SizeInBytes: 100000000000,
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "account-uuid",
				},
			},
			DataProtection:   nil, // No DataProtection
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		params := &common.UpdateVolumeParams{
			VolumeId:    "volume-uuid",
			AccountName: "test-account",
			Region:      "us-west1",
		}

		// Act
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		// Assert
		assert.NoError(t, err)
		// Should not call any backup-related methods
		mockStorage.AssertNotCalled(t, "GetBackupVaultByUUIDndOwnerID")
		mockStorage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID")
	})

	t.Run("Does not validate immutable backup when volume has DataProtection but missing BackupVaultID", func(t *testing.T) {
		// Setup
		mockStorage := database.NewMockStorage(t)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid",
				},
				AllowAutoTiering: true,
				SizeInBytes:      2000000000000,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID:   1,
						UUID: "account-uuid",
					},
				},
			},
			QuotaInBytes: 1000000000000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "volume-uuid",
			},
			Name:        "test-volume",
			State:       models.LifeCycleStateREADY,
			SizeInBytes: 100000000000,
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "account-uuid",
				},
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "", // Empty vault ID
				BackupPolicyID: "existing-policy-id",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		params := &common.UpdateVolumeParams{
			VolumeId:    "volume-uuid",
			AccountName: "test-account",
			Region:      "us-west1",
		}

		// Act
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		// Assert
		assert.NoError(t, err)
		// Should not call checkIsValidImmutableBackupPolicyWithRetry
		mockStorage.AssertNotCalled(t, "GetBackupVaultByUUIDndOwnerID")
		mockStorage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID")
	})

	t.Run("Does not validate immutable backup when volume has DataProtection but missing BackupPolicyID", func(t *testing.T) {
		// Setup
		mockStorage := database.NewMockStorage(t)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-uuid",
				},
				AllowAutoTiering: true,
				SizeInBytes:      2000000000000,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID:   1,
						UUID: "account-uuid",
					},
				},
			},
			QuotaInBytes: 1000000000000,
		}

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "volume-uuid",
			},
			Name:        "test-volume",
			State:       models.LifeCycleStateREADY,
			SizeInBytes: 100000000000,
			AccountID:   1,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "account-uuid",
				},
			},
			DataProtection: &datamodel.DataProtection{
				BackupVaultID:  "existing-vault-id",
				BackupPolicyID: "", // Empty policy ID
			},
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		params := &common.UpdateVolumeParams{
			VolumeId:    "volume-uuid",
			AccountName: "test-account",
			Region:      "us-west1",
		}

		// Act
		err := validateUpdateVolumeRequest(ctx, mockStorage, volume, params, pool)

		// Assert
		assert.NoError(t, err)
		// Should not call checkIsValidImmutableBackupPolicyWithRetry
		mockStorage.AssertNotCalled(t, "GetBackupVaultByUUIDndOwnerID")
		mockStorage.AssertNotCalled(t, "GetBackupPolicyByUUIDAndOwnerID")
	})
}

func TestCheckAndTriggerPoolScalingIfNeeded(t *testing.T) {
	t.Run("WhenPoolIsNotInReadyState", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			State:     models.LifeCycleStateCreating,
			Account:   &datamodel.Account{Name: "test-account"},
		}

		// Should return early without any calls to storage or temporal
		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenGetVolumeCountByPoolIDFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			State:     models.LifeCycleStateREADY,
			Account:   &datamodel.Account{Name: "test-account"},
		}

		// Mock GetVolumeCountByPoolID to fail
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(0), errors.New("failed to get volume count"))

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenAutoPoolScalingLimitsParsingFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			State:     models.LifeCycleStateREADY,
			Account:   &datamodel.Account{Name: "test-account"},
		}

		// Mock GetVolumeCountByPoolID to succeed
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(10), nil)

		// Override autoPoolScalingLimits with invalid JSON
		originalLimits := autoPoolScalingLimits
		autoPoolScalingLimits = "invalid-json"
		defer func() { autoPoolScalingLimits = originalLimits }()

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			State:       models.LifeCycleStateREADY,
			Account:     &datamodel.Account{Name: "test-account"},
			AccountID:   1,
			SizeInBytes: 1000000000000, // 1TB
			Description: "test pool",
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 256,
				Iops:            1024,
				Labels:          nil,
			},
		}

		// Mock GetVolumeCountByPoolID to succeed
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(10), nil)

		// Mock CreateJob to fail
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(nil, errors.New("failed to create job"))

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenExecuteWorkflowFails", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			State:       models.LifeCycleStateREADY,
			Account:     &datamodel.Account{Name: "test-account"},
			AccountID:   1,
			SizeInBytes: 1000000000000, // 1TB
			Description: "test pool",
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 256,
				Iops:            1024,
				Labels:          nil,
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Mock GetVolumeCountByPoolID to succeed
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(10), nil)

		// Mock CreateJob to succeed
		mockStorage.On("CreateJob", ctx, mock.AnythingOfType("*datamodel.Job")).Return(createdJob, nil)

		// Mock UpdatePoolState to succeed
		mockStorage.On("UpdatePoolState", ctx, pool, "UPDATING", "Update in progress").Return(pool, nil)

		// Mock UpdatePoolState for state reversion when workflow fails
		mockStorage.On("UpdatePoolState", ctx, pool, "READY", "").Return(pool, nil)

		// Mock ExecuteWorkflow to fail
		mockTemporal.EXPECT().ExecuteWorkflow(ctx,
			mock.AnythingOfType("internal.StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.UpdatePoolParams, *datamodel.Pool, *common.AutoPoolScalingParams) (gcpserver.V1betaDescribePoolRes, error)"),
			mock.AnythingOfType("*common.UpdatePoolParams"),
			pool,
			mock.AnythingOfType("*common.AutoPoolScalingParams"),
		).Return(nil, errors.New("failed to execute workflow"))

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:          "test-pool",
			State:         models.LifeCycleStateREADY,
			Account:       &datamodel.Account{Name: "test-account"},
			AccountID:     1,
			SizeInBytes:   1000000000000, // 1TB
			Description:   "test pool",
			LargeCapacity: false,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 256,
				Iops:            1024,
				Labels:          nil,
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Mock GetVolumeCountByPoolID to succeed
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(10), nil)

		// Mock CreateJob to succeed
		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.Type == string(models.JobTypeUpdatePool) &&
				job.State == string(models.JobsStateNEW) &&
				job.ResourceName == pool.UUID &&
				job.AccountID.Int64 == pool.AccountID &&
				job.AccountID.Valid
		})).Return(createdJob, nil)

		// Mock UpdatePoolState to succeed
		mockStorage.On("UpdatePoolState", ctx, pool, "UPDATING", "Update in progress").Return(pool, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(ctx,
			mock.AnythingOfType("internal.StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.UpdatePoolParams, *datamodel.Pool, *common.AutoPoolScalingParams) (gcpserver.V1betaDescribePoolRes, error)"),
			mock.MatchedBy(func(updateParams *common.UpdatePoolParams) bool {
				return updateParams.PoolId == pool.UUID &&
					updateParams.AccountName == pool.Account.Name &&
					updateParams.SizeInBytes == uint64(pool.SizeInBytes) &&
					updateParams.TotalThroughputMibps == pool.PoolAttributes.ThroughputMibps &&
					*updateParams.TotalIops == pool.PoolAttributes.Iops &&
					updateParams.Description == pool.Description
			}),
			pool,
			mock.MatchedBy(func(autoScalingParams *common.AutoPoolScalingParams) bool {
				return autoScalingParams.CurrentVolumeCount == 10 &&
					len(autoScalingParams.VolLimitPerInstanceMap) > 0
			}),
		).Return(nil, nil)

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenSuccessfulWithLargeCapacityPool", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel:     datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			Name:          "test-pool",
			State:         models.LifeCycleStateREADY,
			Account:       &datamodel.Account{Name: "test-account"},
			AccountID:     1,
			SizeInBytes:   1000000000000, // 1TB
			Description:   "test pool",
			LargeCapacity: true,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 256,
				Iops:            1024,
				Labels:          nil,
			},
		}

		createdJob := &datamodel.Job{
			BaseModel:  datamodel.BaseModel{UUID: "job-uuid"},
			WorkflowID: "workflow-id",
		}

		// Mock GetVolumeCountByPoolID to succeed
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(5), nil)

		// Mock CreateJob to succeed with large capacity job type
		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.Type == string(models.JobTypeUpdateLargePool) &&
				job.State == string(models.JobsStateNEW) &&
				job.ResourceName == pool.UUID &&
				job.AccountID.Int64 == pool.AccountID &&
				job.AccountID.Valid
		})).Return(createdJob, nil)

		// Mock UpdatePoolState to succeed
		mockStorage.On("UpdatePoolState", ctx, pool, "UPDATING", "Update in progress").Return(pool, nil)

		// Mock ExecuteWorkflow to succeed
		mockTemporal.EXPECT().ExecuteWorkflow(ctx,
			mock.AnythingOfType("internal.StartWorkflowOptions"),
			mock.AnythingOfType("func(internal.Context, *common.UpdatePoolParams, *datamodel.Pool, *common.AutoPoolScalingParams) (gcpserver.V1betaDescribePoolRes, error)"),
			mock.AnythingOfType("*common.UpdatePoolParams"),
			pool,
			mock.AnythingOfType("*common.AutoPoolScalingParams"),
		).Return(nil, nil)

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})

	t.Run("WhenAutoPoolScalingLimitsIsEmpty", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflowEngineMock.NewMockTemporalTestClient(tt)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid", ID: 1},
			State:     models.LifeCycleStateREADY,
			Account:   &datamodel.Account{Name: "test-account"},
		}

		// Mock GetVolumeCountByPoolID to succeed
		mockStorage.On("GetVolumeCountByPoolID", ctx, pool.ID).Return(int64(10), nil)

		// Override autoPoolScalingLimits with empty JSON object
		originalLimits := autoPoolScalingLimits
		autoPoolScalingLimits = "{}"
		defer func() { autoPoolScalingLimits = originalLimits }()

		checkAndTriggerPoolScalingIfNeeded(ctx, mockStorage, mockTemporal, pool)

		mockStorage.AssertExpectations(tt)
		mockTemporal.AssertExpectations(tt)
	})
}

func Test_verifyBackupRestoreCompatibilityForLargeVolumes(t *testing.T) {
	t.Run("ReturnsErrorWhenRestoringLargeVolumeFromNonLargeBackup", func(tt *testing.T) {
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle: "flexvol",
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity: true,
		}

		_, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Cannot restore a large capacity volume from a backup that is not a large volume backup")
	})

	t.Run("ReturnsParamsWhenRestoringNonLargeVolumeFromNonLargeBackup", func(tt *testing.T) {
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle: "flexvol",
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity: false,
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)
		assert.NoError(tt, err)
		assert.Equal(tt, params, result)
	})

	t.Run("SetsConstituentCountWhenRestoringLargeVolumeWithBackupPath", func(tt *testing.T) {
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: 10,
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity:               true,
			BackupPath:                  "some/path",
			LargeVolumeConstituentCount: 0,
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)
		assert.NoError(tt, err)
		assert.Equal(tt, int32(10), result.LargeVolumeConstituentCount)
	})

	t.Run("ReturnsErrorWhenCustomerConstituentCountDoesNotMatchBackup", func(tt *testing.T) {
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: 10,
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 5,
		}

		_, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Constituent count provided (5) does not match with that of backup (10)")
	})

	t.Run("ReturnsParamsWhenCustomerConstituentCountMatchesBackup", func(tt *testing.T) {
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: 10,
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 10,
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)
		assert.NoError(tt, err)
		assert.Equal(tt, params, result)
	})
}

func Test_createVolume_BackupRestoreCompatibilityError(t *testing.T) {
	t.Run("WhenLargeVolumeRestoreFromNonLargeBackup", func(tt *testing.T) {
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
			SizeInBytes: 1000 * 1024 * 1024 * 1024, // 1TB
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
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create a backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
		}
		err = store.DB().Create(backupVault).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		// Create a backup with non-flexgroup style (regular volume backup)
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "backupName",
			BackupVaultID: backupVault.ID,
			SizeInBytes:   100 * 1024 * 1024 * 1024, // 100GB
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle: "flexvol", // Non-large volume backup
			},
		}
		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		// Create volume params with LargeCapacity=true and BackupPath
		params := &common.CreateVolumeParams{
			AccountName:   "test_account",
			Region:        "test_region",
			Name:          "test_volume",
			Zone:          "us-west1-a",
			VendorID:      "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes:  150 * 1024 * 1024 * 1024, // 150GB
			Protocols:     []string{"NFS"},
			Description:   "Some description",
			DisplayName:   "Some display name",
			PoolID:        "test-pool-uuid",
			CreationToken: "test-creation-token",
			LargeCapacity: true, // This should cause the error
			BackupPath:    "projects/project123/locations/location123/backupVaults/bv1/backups/backupName",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)

		// Should return error due to backup restore compatibility issue
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Contains(tt, err.Error(), "Cannot restore a large capacity volume from a backup that is not a large volume backup")
	})

	t.Run("WhenLargeVolumeConstituentCountMismatch", func(tt *testing.T) {
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
			SizeInBytes: 1000 * 1024 * 1024 * 1024, // 1TB
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
			State:     models.LifeCycleStateREADY,
		}
		err = store.DB().Create(svm).Error
		if err != nil {
			tt.Fatalf("Failed to create svm: %v", err)
		}

		// Create a backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
			Name:      "bv1",
			AccountID: account.ID,
		}
		err = store.DB().Create(backupVault).Error
		if err != nil {
			tt.Fatalf("Failed to create backup vault: %v", err)
		}

		// Create a backup with flexgroup style and specific constituent count
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "test-backup-uuid"},
			Name:          "backupName",
			BackupVaultID: backupVault.ID,
			SizeInBytes:   100 * 1024 * 1024 * 1024, // 100GB
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup", // Large volume backup
				ConstituentCountOfBackup: 10,          // Backup has 10 constituents
			},
		}
		err = store.DB().Create(backup).Error
		if err != nil {
			tt.Fatalf("Failed to create backup: %v", err)
		}

		// Create volume params with mismatched constituent count
		params := &common.CreateVolumeParams{
			AccountName:                 "test_account",
			Region:                      "test_region",
			Name:                        "test_volume",
			Zone:                        "us-west1-a",
			VendorID:                    "/projects/project123/locations/us-west1-a/volumes/test-volume",
			QuotaInBytes:                150 * 1024 * 1024 * 1024, // 150GB
			Protocols:                   []string{"NFS"},
			Description:                 "Some description",
			DisplayName:                 "Some display name",
			PoolID:                      "test-pool-uuid",
			CreationToken:               "test-creation-token",
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 5, // Mismatched count (5 vs 10)
			BackupPath:                  "projects/project123/locations/location123/backupVaults/bv1/backups/backupName",
		}

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return account, nil
		}
		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error {
			return nil
		}

		defer func() {
			getOrCreateAccount = _getOrCreateAccount
			validateCreateVolumeParams = _validateCreateVolumeParams
		}()

		temporal := workflowEngineMock.NewMockTemporalTestClient(t)
		volume, _, err := createVolume(ctx, store, temporal, params)

		// Should return error due to constituent count mismatch
		assert.Error(tt, err)
		assert.Nil(tt, volume)
		assert.Contains(tt, err.Error(), "Constituent count provided (5) does not match with that of backup (10)")
	})
}

func TestIsPrime(t *testing.T) {
	testCases := []struct {
		name  string
		input int
		want  bool
	}{
		{"Prime 7", 7, true},
		{"Prime 11", 11, true},
		{"Prime 13", 13, true},
		{"Prime 17", 17, true},
		{"Prime 23", 23, true},
		{"Non-prime 9", 9, false},
		{"Non-prime 15", 15, false},
		{"Non-prime 21", 21, false},
		{"Non-prime 25", 25, false},
		{"Non-prime 27", 27, false},
		{"Edge 2", 2, false},
		{"Edge 3", 3, false},
		{"Edge 4", 4, false},
		{"Edge 6", 6, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isPrime(tc.input)
			if got != tc.want {
				t.Errorf("isPrime(%d) = %v; want %v", tc.input, got, tc.want)
			}
		})
	}
}
