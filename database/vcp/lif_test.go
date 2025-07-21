package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

func TestGetLifByNodeID(t *testing.T) {
	t.Run("WhenLifExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		result, err := store.GetLifByNodeID(context.Background(), lif.NodeID, lif.AccountID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, result.Name, "Expected lif name %v, got %v", lif.Name, result.Name)
		assert.Equal(tt, lif.NodeID, result.NodeID, "Expected lif node id %v, got %v", lif.NodeID, result.NodeID)
		assert.Equal(tt, lif.AccountID, result.AccountID, "Expected lif account id %v, got %v", lif.AccountID, result.AccountID)
	})
	t.Run("WhenLifDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		_, err1 := store.GetLifByNodeID(context.Background(), 1, 1234)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err1, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif not found")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err1)
		}
	})
}

func TestCreateLif(t *testing.T) {
	t.Run("WhenLifIsCreatedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}

		createdLif, err := store.CreateLif(context.Background(), lif)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, createdLif.Name, "Expected lif name %v, got %v", lif.Name, createdLif.Name)
		assert.Equal(tt, lif.NodeID, createdLif.NodeID, "Expected lif node id %v, got %v", lif.NodeID, createdLif.NodeID)
		assert.Equal(tt, lif.AccountID, createdLif.AccountID, "Expected lif account id %v, got %v", lif.AccountID, createdLif.AccountID)
	})
	t.Run("WhenLifAlreadyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		_, err = store.CreateLif(context.Background(), lif)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif already exists")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err)
		}
	})
}

func TestDeleteLif(t *testing.T) {
	t.Run("WhenLifIsDeletedSuccessfully", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		err = store.DeleteLif(context.Background(), lif)
		if err != nil {
			tt.Errorf("Expected no error, got %v", err)
		}

		deletedLif := &datamodel.Lif{}
		err = store.db.GORM().First(deletedLif, "uuid = ?", lif.UUID).Error
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			tt.Errorf("Expected record not found error, got %v", err)
		}
	})
}

func TestGetLifForNode(t *testing.T) {
	t.Run("WhenLifExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-svm-uuid",
			},
			Name:      "test_lif",
			NodeID:    node.ID,
			AccountID: node.AccountID,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		result, err := store.GetLifForNode(context.Background(), lif.NodeID, lif.AccountID)
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, result.Name, "Expected lif name %v, got %v", lif.Name, result.Name)
		assert.Equal(tt, lif.NodeID, result.NodeID, "Expected lif node id %v, got %v", lif.NodeID, result.NodeID)
		assert.Equal(tt, lif.AccountID, result.AccountID, "Expected lif account id %v, got %v", lif.AccountID, result.AccountID)
	})
}

func TestGetLifByNodeIDAndProtocol(t *testing.T) {
	t.Run("WhenLifExistsWithProtocol", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lifDetails := &datamodel.LifDetails{
			ExternalUUID: "external-lif-uuid",
			ProtocolType: "nfs",
		}
		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-lif-uuid",
			},
			Name:       "test_lif_nfs",
			NodeID:     node.ID,
			AccountID:  node.AccountID,
			LifDetails: lifDetails,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		// Mock the function to avoid JSONB query issues in SQLite
		originalFunc := getLifWithProtocolDetails
		getLifWithProtocolDetails = func(db *gorm.DB, query *datamodel.Lif, protocol string) (*datamodel.Lif, error) {
			// For test, simulate the protocol filtering behavior
			if protocol == "nfs" {
				return lif, nil
			}
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("lif", nil))
		}
		defer func() { getLifWithProtocolDetails = originalFunc }()

		result, err := store.GetLifByNodeIDAndProtocol(context.Background(), lif.NodeID, lif.AccountID, "nfs")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, result.Name, "Expected lif name %v, got %v", lif.Name, result.Name)
		assert.Equal(tt, lif.NodeID, result.NodeID, "Expected lif node id %v, got %v", lif.NodeID, result.NodeID)
		assert.Equal(tt, lif.AccountID, result.AccountID, "Expected lif account id %v, got %v", lif.AccountID, result.AccountID)
		assert.Equal(tt, "nfs", result.LifDetails.ProtocolType, "Expected protocol type %v, got %v", "nfs", result.LifDetails.ProtocolType)
	})

	t.Run("WhenLifExistsWithEmptyProtocol", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		lifDetails := &datamodel.LifDetails{
			ExternalUUID: "external-lif-uuid",
			ProtocolType: "cifs",
		}
		lif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-lif-uuid",
			},
			Name:       "test_lif_any",
			NodeID:     node.ID,
			AccountID:  node.AccountID,
			LifDetails: lifDetails,
		}
		err = store.db.Create(lif).Error()
		if err != nil {
			tt.Fatalf("Failed to create lif: %v", err)
		}

		// Mock the function to handle empty protocol
		originalFunc := getLifWithProtocolDetails
		getLifWithProtocolDetails = func(db *gorm.DB, query *datamodel.Lif, protocol string) (*datamodel.Lif, error) {
			// When protocol is empty, return the lif regardless of protocol type
			if protocol == "" {
				return lif, nil
			}
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("lif", nil))
		}
		defer func() { getLifWithProtocolDetails = originalFunc }()

		// When protocol is empty, it should return the lif regardless of protocol type
		result, err := store.GetLifByNodeIDAndProtocol(context.Background(), lif.NodeID, lif.AccountID, "")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, lif.Name, result.Name, "Expected lif name %v, got %v", lif.Name, result.Name)
		assert.Equal(tt, lif.NodeID, result.NodeID, "Expected lif node id %v, got %v", lif.NodeID, result.NodeID)
		assert.Equal(tt, lif.AccountID, result.AccountID, "Expected lif account id %v, got %v", lif.AccountID, result.AccountID)
	})

	t.Run("WhenLifDoesNotExistWithSpecificProtocol", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Mock the function to simulate no match for specific protocol
		originalFunc := getLifWithProtocolDetails
		getLifWithProtocolDetails = func(db *gorm.DB, query *datamodel.Lif, protocol string) (*datamodel.Lif, error) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("lif", nil))
		}
		defer func() { getLifWithProtocolDetails = originalFunc }()

		// Try to get lif with specific protocol but none exists
		_, err1 := store.GetLifByNodeIDAndProtocol(context.Background(), 123, 456, "cifs")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err1, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif not found")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err1)
		}
	})

	t.Run("WhenLifDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		// Mock the function to simulate no lif found
		originalFunc := getLifWithProtocolDetails
		getLifWithProtocolDetails = func(db *gorm.DB, query *datamodel.Lif, protocol string) (*datamodel.Lif, error) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("lif", nil))
		}
		defer func() { getLifWithProtocolDetails = originalFunc }()

		_, err1 := store.GetLifByNodeIDAndProtocol(context.Background(), 999, 1234, "nfs")
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err1, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "lif not found")
		} else {
			tt.Fatalf("Expected a CustomError, got %v", err1)
		}
	})

	t.Run("WhenMultipleLifsExistButOnlyOneMatchesProtocol", func(tt *testing.T) {
		db, err := SetupTestDB()
		assert.NoError(tt, err, "Failed to set up test database")
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		assert.NoError(tt, err, "Failed to clean up test database")

		node := &datamodel.Node{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-node-uuid",
			},
			Name:      "test_node",
			AccountID: int64(1234),
			PoolID:    1234,
		}
		err = store.db.Create(node).Error()
		if err != nil {
			tt.Fatalf("Failed to create node: %v", err)
		}

		// Create first lif with NFS protocol
		nfsLifDetails := &datamodel.LifDetails{
			ExternalUUID: "external-nfs-lif-uuid",
			ProtocolType: "nfs",
		}
		nfsLif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-nfs-lif-uuid",
			},
			Name:       "test_lif_nfs",
			NodeID:     node.ID,
			AccountID:  node.AccountID,
			LifDetails: nfsLifDetails,
		}

		// Create second lif with CIFS protocol
		cifsLifDetails := &datamodel.LifDetails{
			ExternalUUID: "external-cifs-lif-uuid",
			ProtocolType: "cifs",
		}
		cifsLif := &datamodel.Lif{
			BaseModel: datamodel.BaseModel{
				ID:   2,
				UUID: "test-cifs-lif-uuid",
			},
			Name:       "test_lif_cifs",
			NodeID:     node.ID,
			AccountID:  node.AccountID,
			LifDetails: cifsLifDetails,
		}

		// Mock the function to return different lifs based on protocol
		originalFunc := getLifWithProtocolDetails
		getLifWithProtocolDetails = func(db *gorm.DB, query *datamodel.Lif, protocol string) (*datamodel.Lif, error) {
			switch protocol {
			case "nfs":
				return nfsLif, nil
			case "cifs":
				return cifsLif, nil
			default:
				return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("lif", nil))
			}
		}
		defer func() { getLifWithProtocolDetails = originalFunc }()

		// Get NFS lif specifically
		result, err := store.GetLifByNodeIDAndProtocol(context.Background(), node.ID, node.AccountID, "nfs")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, nfsLif.Name, result.Name, "Expected nfs lif name %v, got %v", nfsLif.Name, result.Name)
		assert.Equal(tt, "nfs", result.LifDetails.ProtocolType, "Expected protocol type %v, got %v", "nfs", result.LifDetails.ProtocolType)

		// Get CIFS lif specifically
		result, err = store.GetLifByNodeIDAndProtocol(context.Background(), node.ID, node.AccountID, "cifs")
		assert.NoError(tt, err, "Expected no error, got %v", err)
		assert.Equal(tt, cifsLif.Name, result.Name, "Expected cifs lif name %v, got %v", cifsLif.Name, result.Name)
		assert.Equal(tt, "cifs", result.LifDetails.ProtocolType, "Expected protocol type %v, got %v", "cifs", result.LifDetails.ProtocolType)
	})
}
