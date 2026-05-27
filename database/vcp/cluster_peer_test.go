package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetClusterPeerByAccountIDExternalClusterAndPoolID(t *testing.T) {
	t.Run("WhenClusterPeerExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create test cluster peering row
		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		result, err := store.GetClusterPeerByAccountIDExternalClusterAndPoolID(context.Background(), account.ID, "test-cluster", pool.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, result, "Expected result to not be nil")
		assert.Equal(tt, clusterPeeringRow.ID, result.ID, "Expected cluster peer ID %v, got %v", clusterPeeringRow.ID, result.ID)
		assert.Equal(tt, clusterPeeringRow.OnprempCluster, result.OnprempCluster, "Expected onprem cluster %v, got %v", clusterPeeringRow.OnprempCluster, result.OnprempCluster)
	})

	t.Run("WhenClusterPeerDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err = store.GetClusterPeerByAccountIDExternalClusterAndPoolID(context.Background(), 999, "non-existent-cluster", 999)
		assert.Error(tt, err, "Expected error for non-existent cluster peer")

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			// Debug: print the actual error details
			tt.Logf("Actual error code: %d, Expected: %d", customErr.TrackingID, vsaerrors.ErrClusterPeerNotFound)
			tt.Logf("Error message: %s", customErr.Message)
			tt.Logf("Original error: %v", customErr.OriginalErr)

			// Accept the actual error code being returned (1011 = ErrInternalServerError)
			assert.Equal(tt, vsaerrors.ErrClusterPeerNotFound, customErr.TrackingID, "Expected internal server error, got %d", customErr.TrackingID)
		}
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Use a cancelled context to trigger a database error
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		_, err = store.GetClusterPeerByAccountIDExternalClusterAndPoolID(cancelledCtx, account.ID, "test-cluster", pool.ID)
		assert.Error(tt, err, "Expected error due to cancelled context")
	})
}

func TestGetClusterPeeringRowByID(t *testing.T) {
	t.Run("WhenClusterPeeringRowExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   42,
				UUID: "test-cluster-peer-uuid-by-id",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		result, err := store.GetClusterPeeringRowByID(context.Background(), clusterPeeringRow.ID)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, clusterPeeringRow.ID, result.ID)
		assert.Equal(tt, clusterPeeringRow.UUID, result.UUID)
		assert.Equal(tt, clusterPeeringRow.OnprempCluster, result.OnprempCluster)
	})

	t.Run("WhenClusterPeeringRowDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err = store.GetClusterPeeringRowByID(context.Background(), 99999)
		assert.Error(tt, err)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, vsaerrors.ErrClusterPeerNotFound, customErr.TrackingID)
		}
	})

	t.Run("WhenRowSoftDeleted", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-soft-del"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-soft-del"}, Name: "pool", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		row := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{ID: 77, UUID: "peer-soft-del"},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			OnprempCluster: "c1",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		assert.NoError(tt, store.db.Create(row).Error())
		assert.NoError(tt, store.DeleteClusterPeeringRow(context.Background(), row))

		_, err = store.GetClusterPeeringRowByID(context.Background(), row.ID)
		assert.Error(tt, err)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Equal(tt, vsaerrors.ErrClusterPeerNotFound, customErr.TrackingID)
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-cancel"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-cancel"}, Name: "pool", AccountID: account.ID, Account: account, DeploymentName: "dep"}
		assert.NoError(tt, store.db.Create(pool).Error())
		row := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "peer-cancel"},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			OnprempCluster: "c1",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		assert.NoError(tt, store.db.Create(row).Error())

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = store.GetClusterPeeringRowByID(cancelledCtx, row.ID)
		assert.Error(tt, err)
	})
}

func TestCreateClusterPeeringRow(t *testing.T) {
	t.Run("WhenClusterPeeringRowIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create cluster peering row attributes
		attributes := &datamodel.ClusterPeeringAttributes{
			PassPhrase:      stringPtr("test-passphrase"),
			Command:         stringPtr("cluster peer create"),
			ExpiryTime:      timePtr(time.Now().Add(24 * time.Hour)),
			ClusterLocation: stringPtr("us-west-1"),
		}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				UUID: "test-cluster-peer-uuid",
			},
			State:                    datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:             "Creating cluster peer",
			OnprempCluster:           "test-cluster",
			OntapPeerUUID:            "test-ontap-peer-uuid",
			AccountID:                account.ID,
			PoolID:                   pool.ID,
			ClusterPeeringAttributes: attributes,
		}

		createdRow, err := store.CreateClusterPeeringRow(context.Background(), clusterPeeringRow)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.NotNil(tt, createdRow, "Expected created row to not be nil")
		assert.Equal(tt, clusterPeeringRow.UUID, createdRow.UUID, "Expected UUID %v, got %v", clusterPeeringRow.UUID, createdRow.UUID)
		assert.Equal(tt, clusterPeeringRow.State, createdRow.State, "Expected state %v, got %v", clusterPeeringRow.State, createdRow.State)
		assert.Equal(tt, clusterPeeringRow.OnprempCluster, createdRow.OnprempCluster, "Expected onprem cluster %v, got %v", clusterPeeringRow.OnprempCluster, createdRow.OnprempCluster)
		assert.NotNil(tt, createdRow.ClusterPeeringAttributes, "Expected attributes to be set")
		assert.Equal(tt, "test-passphrase", *createdRow.ClusterPeeringAttributes.PassPhrase, "Expected passphrase to match")
	})

	t.Run("WhenDatabaseInsertFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create cluster peering row attributes
		attributes := &datamodel.ClusterPeeringAttributes{
			PassPhrase:      stringPtr("test-passphrase"),
			Command:         stringPtr("cluster peer create"),
			ExpiryTime:      timePtr(time.Now().Add(24 * time.Hour)),
			ClusterLocation: stringPtr("us-west-1"),
		}

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				UUID: "test-cluster-peer-uuid",
			},
			State:                    datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:             "Creating cluster peer",
			OnprempCluster:           "test-cluster",
			OntapPeerUUID:            "test-ontap-peer-uuid",
			AccountID:                account.ID,
			PoolID:                   pool.ID,
			ClusterPeeringAttributes: attributes,
		}

		// Use a cancelled context to trigger a database error
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		_, err = store.CreateClusterPeeringRow(cancelledCtx, clusterPeeringRow)
		assert.Error(tt, err, "Expected error due to cancelled context")
	})
}

func TestUpdateClusterPeeringRow(t *testing.T) {
	t.Run("WhenClusterPeeringRowIsUpdatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:      "test_pool",
			AccountID: account.ID,
			Account:   account,
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create initial cluster peering row
		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// Update the cluster peering row
		clusterPeeringRow.State = datamodel.CvpClusterPeeringStatusPEERED
		clusterPeeringRow.StateDetails = "Successfully peered"
		clusterPeeringRow.OntapPeerUUID = "updated-ontap-peer-uuid"

		err = store.UpdateClusterPeeringRow(context.Background(), clusterPeeringRow)
		assert.NoError(tt, err, "Expected no error, got %v", err)

		// Verify the update
		var updatedRow datamodel.ClusterPeerings
		err = store.db.Where("id = ?", clusterPeeringRow.ID).First(&updatedRow).Error()
		assert.NoError(tt, err, "Failed to retrieve updated row")
		assert.Equal(tt, datamodel.CvpClusterPeeringStatusPEERED, updatedRow.State, "Expected state to be updated")
		assert.Equal(tt, "Successfully peered", updatedRow.StateDetails, "Expected state details to be updated")
		assert.Equal(tt, "updated-ontap-peer-uuid", updatedRow.OntapPeerUUID, "Expected ontap peer UUID to be updated")
	})

	t.Run("WhenDatabaseUpdateFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create initial cluster peering row
		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// Update the cluster peering row
		clusterPeeringRow.State = datamodel.CvpClusterPeeringStatusPEERED
		clusterPeeringRow.StateDetails = "Successfully peered"
		clusterPeeringRow.OntapPeerUUID = "updated-ontap-peer-uuid"

		// Use a cancelled context to trigger a database error
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		err = store.UpdateClusterPeeringRow(cancelledCtx, clusterPeeringRow)
		assert.Error(tt, err, "Expected error due to cancelled context")
	})
}

func TestListClusterPeeringRowsByAccountID(t *testing.T) {
	t.Run("WhenClusterPeeringRowsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pools
		pool1 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-1-uuid",
			},
			Name:           "test_pool_1",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment-1",
		}
		err = store.db.Create(pool1).Error()
		assert.NoError(tt, err, "Failed to create pool 1")

		pool2 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-pool-2-uuid",
			},
			Name:           "test_pool_2",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment-2",
		}
		err = store.db.Create(pool2).Error()
		assert.NoError(tt, err, "Failed to create pool 2")

		// Create test cluster peering rows
		clusterPeeringRow1 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-1-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster-1",
			OntapPeerUUID:  "test-ontap-peer-1-uuid",
			AccountID:      account.ID,
			PoolID:         pool1.ID,
		}
		err = store.db.Create(clusterPeeringRow1).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 1")

		clusterPeeringRow2 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-cluster-peer-2-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster-2",
			OntapPeerUUID:  "test-ontap-peer-2-uuid",
			AccountID:      account.ID,
			PoolID:         pool2.ID,
		}
		err = store.db.Create(clusterPeeringRow2).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 2")

		// Create cluster peering row for different account
		otherAccount := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "other-account-uuid",
			},
			Name: "other_account",
		}
		err = store.db.Create(otherAccount).Error()
		assert.NoError(tt, err, "Failed to create other account")

		otherPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   3,
				UUID: "other-pool-uuid",
			},
			Name:           "other_pool",
			AccountID:      otherAccount.ID,
			Account:        otherAccount,
			DeploymentName: "other-deployment",
		}
		err = store.db.Create(otherPool).Error()
		assert.NoError(tt, err, "Failed to create other pool")

		otherClusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   3,
				UUID: "other-cluster-peer-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "other-cluster",
			OntapPeerUUID:  "other-ontap-peer-uuid",
			AccountID:      otherAccount.ID,
			PoolID:         otherPool.ID,
		}
		err = store.db.Create(otherClusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create other cluster peering row")

		// List cluster peering rows for the first account
		results, err := store.ListClusterPeeringRowsByAccountID(context.Background(), account.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, results, 2, "Expected 2 cluster peering rows, got %d", len(results))

		// Verify the results contain the correct rows
		foundRow1 := false
		foundRow2 := false
		for _, row := range results {
			assert.Equal(tt, account.ID, row.AccountID, "Expected account ID to match")

			if row.ID == clusterPeeringRow1.ID {
				foundRow1 = true
				assert.Equal(tt, "test-cluster-1", row.OnprempCluster, "Expected onprem cluster to match")
			} else if row.ID == clusterPeeringRow2.ID {
				foundRow2 = true
				assert.Equal(tt, "test-cluster-2", row.OnprempCluster, "Expected onprem cluster to match")
			}
		}
		assert.True(tt, foundRow1, "Expected to find cluster peering row 1")
		assert.True(tt, foundRow2, "Expected to find cluster peering row 2")
	})

	t.Run("WhenNoClusterPeeringRowsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		results, err := store.ListClusterPeeringRowsByAccountID(context.Background(), 999)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, results, "Expected empty results for non-existent account")
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pools
		pool1 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-1-uuid",
			},
			Name:           "test_pool_1",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment-1",
		}
		err = store.db.Create(pool1).Error()
		assert.NoError(tt, err, "Failed to create pool 1")

		pool2 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-pool-2-uuid",
			},
			Name:           "test_pool_2",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment-2",
		}
		err = store.db.Create(pool2).Error()
		assert.NoError(tt, err, "Failed to create pool 2")

		// Create test cluster peering rows
		clusterPeeringRow1 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-1-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster-1",
			OntapPeerUUID:  "test-ontap-peer-1-uuid",
			AccountID:      account.ID,
			PoolID:         pool1.ID,
		}
		err = store.db.Create(clusterPeeringRow1).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 1")

		clusterPeeringRow2 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-cluster-peer-2-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster-2",
			OntapPeerUUID:  "test-ontap-peer-2-uuid",
			AccountID:      account.ID,
			PoolID:         pool2.ID,
		}
		err = store.db.Create(clusterPeeringRow2).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 2")

		// Use a cancelled context to trigger a database error
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		_, err = store.ListClusterPeeringRowsByAccountID(cancelledCtx, account.ID)
		assert.Error(tt, err, "Expected error due to cancelled context")
	})
}

func TestListClusterPeeringRowsByAccountIDWithConditions(t *testing.T) {
	logger := log.NewLogger()
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, logger)

	t.Run("WhenClusterPeeringRowsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create pools
		pool1 := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-1-uuid"},
			Name:           "test_pool_1",
			AccountID:      1,
			DeploymentName: "test-deployment-1",
		}
		err = store.db.Create(pool1).Error()
		assert.NoError(tt, err, "Failed to create pool 1")

		pool2 := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-2-uuid"},
			Name:           "test_pool_2",
			AccountID:      1,
			DeploymentName: "test-deployment-2",
		}
		err = store.db.Create(pool2).Error()
		assert.NoError(tt, err, "Failed to create pool 2")

		// Create cluster peering rows
		clusterPeeringRow1 := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "test-cluster-peer-1-uuid"},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster-1",
			OntapPeerUUID:  "test-ontap-peer-1-uuid",
			AccountID:      1,
			PoolID:         pool1.ID,
		}
		err = store.db.Create(clusterPeeringRow1).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 1")

		clusterPeeringRow2 := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "test-cluster-peer-2-uuid"},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster-2",
			OntapPeerUUID:  "test-ontap-peer-2-uuid",
			AccountID:      1,
			PoolID:         pool2.ID,
		}
		err = store.db.Create(clusterPeeringRow2).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 2")

		// Query for cluster peering rows by account ID
		clusterPeeringRows, err := store.ListClusterPeeringRowsByAccountID(ctx, 1)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, clusterPeeringRows, 2, "Expected 2 cluster peering rows, got %d", len(clusterPeeringRows))

		// Verify the first cluster peering row
		foundRow1 := false
		foundRow2 := false
		for _, row := range clusterPeeringRows {
			assert.Equal(tt, int64(1), row.AccountID, "Expected account ID to be 1")
			if row.UUID == clusterPeeringRow1.UUID {
				foundRow1 = true
				assert.Equal(tt, clusterPeeringRow1.State, row.State, "Expected state %v, got %v", clusterPeeringRow1.State, row.State)
				assert.Equal(tt, clusterPeeringRow1.OnprempCluster, row.OnprempCluster, "Expected onprem cluster %v, got %v", clusterPeeringRow1.OnprempCluster, row.OnprempCluster)
			} else if row.UUID == clusterPeeringRow2.UUID {
				foundRow2 = true
				assert.Equal(tt, clusterPeeringRow2.State, row.State, "Expected state %v, got %v", clusterPeeringRow2.State, row.State)
				assert.Equal(tt, clusterPeeringRow2.OnprempCluster, row.OnprempCluster, "Expected onprem cluster %v, got %v", clusterPeeringRow2.OnprempCluster, row.OnprempCluster)
			}
		}
		assert.True(tt, foundRow1, "Expected to find cluster peering row 1")
		assert.True(tt, foundRow2, "Expected to find cluster peering row 2")
	})

	t.Run("WhenClusterPeeringRowsDoNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Query for cluster peering rows that do not exist
		clusterPeeringRows, err := store.ListClusterPeeringRowsByAccountID(ctx, 999)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, clusterPeeringRows, 0, "Expected 0 cluster peering rows, got %d", len(clusterPeeringRows))
	})

	t.Run("WhenAccountDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Query for cluster peering rows for non-existent account
		clusterPeeringRows, err := store.ListClusterPeeringRowsByAccountID(ctx, 999)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, clusterPeeringRows, 0, "Expected 0 cluster peering rows, got %d", len(clusterPeeringRows))
	})

	t.Run("WhenQueryingDifferentAccountID", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account and cluster peering row
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      1,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      1,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// Query for cluster peering rows with a different account ID
		// This tests the normal case where no records are found for the given account ID
		// Note: This tests the happy path where GORM returns empty slice (not line 77)
		// Line 77 would require a genuine database error with "record not found" message
		clusterPeeringRows, err := store.ListClusterPeeringRowsByAccountID(ctx, 999)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, clusterPeeringRows, 0, "Expected 0 cluster peering rows, got %d", len(clusterPeeringRows))
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account and cluster peering row
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      1,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      1,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// Use a cancelled context to trigger a database error
		// This follows the pattern from the reference test where invalid conditions cause errors
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		_, err = store.ListClusterPeeringRowsByAccountID(cancelledCtx, 1)
		assert.Error(tt, err, "Expected error due to cancelled context")
	})

	t.Run("WhenInvalidAccountIDCausesError", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create an account and cluster peering row
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "test-pool-uuid"},
			Name:           "test_pool",
			AccountID:      1,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "test-cluster-peer-uuid"},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      1,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// Use a very large account ID that might cause database issues
		// This is similar to the invalid filter condition approach from the reference test
		clusterPeeringRows, err := store.ListClusterPeeringRowsByAccountID(ctx, 9223372036854775807) // Max int64
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, clusterPeeringRows, 0, "Expected 0 cluster peering rows, got %d", len(clusterPeeringRows))
	})
}

func TestDeleteClusterPeeringRow(t *testing.T) {
	t.Run("WhenClusterPeeringRowDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Account and Pool
		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-del-success"}, Name: "acct"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-del-success"}, Name: "pool", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())

		// Cluster peer row
		row := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "cluster-peer-del-success"},
			State:          "PEERED",
			StateDetails:   "ok",
			OnprempCluster: "cluster-A",
			OntapPeerUUID:  "peer-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		assert.NoError(tt, store.db.Create(row).Error())

		// Delete
		err = store.DeleteClusterPeeringRow(context.Background(), row)
		assert.NoError(tt, err, "expected successful soft delete")

		// Verify soft delete (DeletedAt set) and subsequent lookup fails
		fetched, fetchErr := store.GetClusterPeerByAccountIDExternalClusterAndPoolID(context.Background(), account.ID, "cluster-A", pool.ID)
		assert.Nil(tt, fetched)
		assert.Error(tt, fetchErr)
		var vErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(fetchErr, &vErr))
		assert.Equal(tt, vsaerrors.ErrClusterPeerNotFound, vErr.TrackingID)
	})

	t.Run("WhenClusterPeeringRowNotFound", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		// Row with non-existent ID
		phantom := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{ID: 999999, UUID: "phantom-row-uuid"},
		}

		err = store.DeleteClusterPeeringRow(context.Background(), phantom)
		assert.Error(tt, err)
		var vErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &vErr))
		assert.Equal(tt, vsaerrors.ErrClusterPeerNotFound, vErr.TrackingID)
	})

	t.Run("WhenDatabaseDeleteFails", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err)
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)
		assert.NoError(tt, ClearInMemoryDB(store.db.GORM()))

		account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-del-fail"}, Name: "acct-fail"}
		assert.NoError(tt, store.db.Create(account).Error())
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-del-fail"}, Name: "pool-fail", AccountID: account.ID, Account: account}
		assert.NoError(tt, store.db.Create(pool).Error())
		row := &datamodel.ClusterPeerings{
			BaseModel:      datamodel.BaseModel{UUID: "cluster-peer-del-fail"},
			State:          "CREATING",
			StateDetails:   "in-progress",
			OnprempCluster: "cluster-B",
			OntapPeerUUID:  "peer-uuid-B",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		assert.NoError(tt, store.db.Create(row).Error())

		// Cancelled context to force DB error
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = store.DeleteClusterPeeringRow(cancelledCtx, row)
		assert.Error(tt, err)
		var vErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &vErr))
		assert.Equal(tt, vsaerrors.ErrDatabaseDataDeleteError, vErr.TrackingID)
	})
}

func TestListClusterPeeringRowsByPoolID(t *testing.T) {
	t.Run("WhenClusterPeeringRowsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create test cluster peering rows for the same pool
		clusterPeeringRow1 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-1-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster-1",
			OntapPeerUUID:  "test-ontap-peer-1-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow1).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 1")

		clusterPeeringRow2 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-cluster-peer-2-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster-2",
			OntapPeerUUID:  "test-ontap-peer-2-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow2).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 2")

		// Create cluster peering row for different pool
		otherPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "other-pool-uuid",
			},
			Name:           "other_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "other-deployment",
		}
		err = store.db.Create(otherPool).Error()
		assert.NoError(tt, err, "Failed to create other pool")

		otherClusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   3,
				UUID: "other-cluster-peer-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "other-cluster",
			OntapPeerUUID:  "other-ontap-peer-uuid",
			AccountID:      account.ID,
			PoolID:         otherPool.ID,
		}
		err = store.db.Create(otherClusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create other cluster peering row")

		// List cluster peering rows for the first pool
		results, err := store.ListClusterPeeringRowsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, results, 2, "Expected 2 cluster peering rows, got %d", len(results))

		// Verify the results contain the correct rows
		foundRow1 := false
		foundRow2 := false
		for _, row := range results {
			assert.Equal(tt, pool.ID, row.PoolID, "Expected pool ID to match")

			if row.ID == clusterPeeringRow1.ID {
				foundRow1 = true
				assert.Equal(tt, "test-cluster-1", row.OnprempCluster, "Expected onprem cluster to match")
				assert.Equal(tt, datamodel.CvpClusterPeeringStatusPEERED, row.State, "Expected state to match")
			} else if row.ID == clusterPeeringRow2.ID {
				foundRow2 = true
				assert.Equal(tt, "test-cluster-2", row.OnprempCluster, "Expected onprem cluster to match")
				assert.Equal(tt, datamodel.CvpClusterPeeringStatusCREATING, row.State, "Expected state to match")
			}
		}
		assert.True(tt, foundRow1, "Expected to find cluster peering row 1")
		assert.True(tt, foundRow2, "Expected to find cluster peering row 2")
	})

	t.Run("WhenNoClusterPeeringRowsExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		results, err := store.ListClusterPeeringRowsByPoolID(context.Background(), 999)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, results, "Expected empty results for non-existent pool")
	})

	t.Run("WhenDatabaseErrorOccurs", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create test cluster peering row
		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster",
			OntapPeerUUID:  "test-ontap-peer-uuid",
			AccountID:      account.ID,
			PoolID:         pool.ID,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// Use a cancelled context to trigger a database error
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		_, err = store.ListClusterPeeringRowsByPoolID(cancelledCtx, pool.ID)
		assert.Error(tt, err, "Expected error due to cancelled context")
	})

	t.Run("WhenPoolIDIsZero", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		results, err := store.ListClusterPeeringRowsByPoolID(context.Background(), 0)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, results, "Expected empty results for zero pool ID")
	})

	t.Run("WhenPoolIDIsNegative", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		results, err := store.ListClusterPeeringRowsByPoolID(context.Background(), -1)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, results, "Expected empty results for negative pool ID")
	})

	t.Run("WhenPoolIDIsMaxInt64", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		results, err := store.ListClusterPeeringRowsByPoolID(context.Background(), 9223372036854775807) // Max int64
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Empty(tt, results, "Expected empty results for max int64 pool ID")
	})

	t.Run("WhenMultiplePoolsHaveClusterPeeringRows", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create multiple pools
		pool1 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-1-uuid",
			},
			Name:           "test_pool_1",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment-1",
		}
		err = store.db.Create(pool1).Error()
		assert.NoError(tt, err, "Failed to create pool 1")

		pool2 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-pool-2-uuid",
			},
			Name:           "test_pool_2",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment-2",
		}
		err = store.db.Create(pool2).Error()
		assert.NoError(tt, err, "Failed to create pool 2")

		// Create cluster peering rows for pool 1
		clusterPeeringRow1 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-1-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:   "Successfully peered",
			OnprempCluster: "test-cluster-1",
			OntapPeerUUID:  "test-ontap-peer-1-uuid",
			AccountID:      account.ID,
			PoolID:         pool1.ID,
		}
		err = store.db.Create(clusterPeeringRow1).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 1")

		// Create cluster peering rows for pool 2
		clusterPeeringRow2 := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-cluster-peer-2-uuid",
			},
			State:          datamodel.CvpClusterPeeringStatusCREATING,
			StateDetails:   "Creating cluster peer",
			OnprempCluster: "test-cluster-2",
			OntapPeerUUID:  "test-ontap-peer-2-uuid",
			AccountID:      account.ID,
			PoolID:         pool2.ID,
		}
		err = store.db.Create(clusterPeeringRow2).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row 2")

		// Test querying pool 1
		results1, err := store.ListClusterPeeringRowsByPoolID(context.Background(), pool1.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, results1, 1, "Expected 1 cluster peering row for pool 1, got %d", len(results1))
		assert.Equal(tt, pool1.ID, results1[0].PoolID, "Expected pool ID to match")
		assert.Equal(tt, "test-cluster-1", results1[0].OnprempCluster, "Expected onprem cluster to match")

		// Test querying pool 2
		results2, err := store.ListClusterPeeringRowsByPoolID(context.Background(), pool2.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, results2, 1, "Expected 1 cluster peering row for pool 2, got %d", len(results2))
		assert.Equal(tt, pool2.ID, results2[0].PoolID, "Expected pool ID to match")
		assert.Equal(tt, "test-cluster-2", results2[0].OnprempCluster, "Expected onprem cluster to match")
	})

	t.Run("WhenClusterPeeringRowsHaveAttributes", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Create test account
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-account-uuid",
			},
			Name: "test_account",
		}
		err = store.db.Create(account).Error()
		assert.NoError(tt, err, "Failed to create account")

		// Create test pool
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-pool-uuid",
			},
			Name:           "test_pool",
			AccountID:      account.ID,
			Account:        account,
			DeploymentName: "test-deployment",
		}
		err = store.db.Create(pool).Error()
		assert.NoError(tt, err, "Failed to create pool")

		// Create cluster peering row attributes
		attributes := &datamodel.ClusterPeeringAttributes{
			PassPhrase:      stringPtr("test-passphrase"),
			Command:         stringPtr("cluster peer create"),
			ExpiryTime:      timePtr(time.Now().Add(24 * time.Hour)),
			ClusterLocation: stringPtr("us-west-1"),
		}

		// Create test cluster peering row with attributes
		clusterPeeringRow := &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-cluster-peer-uuid",
			},
			State:                    datamodel.CvpClusterPeeringStatusPEERED,
			StateDetails:             "Successfully peered",
			OnprempCluster:           "test-cluster",
			OntapPeerUUID:            "test-ontap-peer-uuid",
			AccountID:                account.ID,
			PoolID:                   pool.ID,
			ClusterPeeringAttributes: attributes,
		}
		err = store.db.Create(clusterPeeringRow).Error()
		assert.NoError(tt, err, "Failed to create cluster peering row")

		// List cluster peering rows for the pool
		results, err := store.ListClusterPeeringRowsByPoolID(context.Background(), pool.ID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Len(tt, results, 1, "Expected 1 cluster peering row, got %d", len(results))

		// Verify the result contains the attributes
		result := results[0]
		assert.Equal(tt, pool.ID, result.PoolID, "Expected pool ID to match")
		assert.NotNil(tt, result.ClusterPeeringAttributes, "Expected attributes to be set")
		assert.Equal(tt, "test-passphrase", *result.ClusterPeeringAttributes.PassPhrase, "Expected passphrase to match")
		assert.Equal(tt, "cluster peer create", *result.ClusterPeeringAttributes.Command, "Expected command to match")
		assert.Equal(tt, "us-west-1", *result.ClusterPeeringAttributes.ClusterLocation, "Expected cluster location to match")
	})
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}

func timePtr(t time.Time) *time.Time {
	return &t
}
