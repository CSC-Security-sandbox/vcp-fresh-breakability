package database

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

func setupNodeGroupTestRepo(t *testing.T) (*DataStoreRepository, *datamodel.NodeGroup) {
	db, err := SetupTestDB()
	assert.NoError(t, err)
	wrapper := gorm.New(db)
	repo := &DataStoreRepository{db: wrapper}
	err = ClearInMemoryDB(db)
	assert.NoError(t, err)
	group := &datamodel.NodeGroup{Name: "test-group"}
	return repo, group
}

func TestCreateNodeGroup(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "test-group", created.Name)
}

func TestCreateNodeGroup_UsesTxFromContext(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	ctx := context.Background()
	tx := repo.db.GORM().WithContext(ctx).Begin()
	require.NoError(t, tx.Error)
	defer func() { _ = tx.Rollback() }()

	ctxTx := utils.WithTx(ctx, tx)
	created, err := repo.CreateNodeGroup(ctxTx, &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{UUID: uuid.NewString()},
		Name:      "tx-group-" + uuid.NewString(),
	})
	require.NoError(t, err)
	require.NotZero(t, created.ID)

	var visible int64
	require.NoError(t, tx.Model(&datamodel.NodeGroup{}).Where("id = ?", created.ID).Count(&visible).Error)
	assert.Equal(t, int64(1), visible)
	require.NoError(t, tx.Commit().Error)

	got, err := repo.GetNodeGroup(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.Name, got.Name)
}

func TestGetNodeGroup(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	got, err := repo.GetNodeGroup(context.Background(), created.ID)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, created.ID, got.ID)
}

func TestUpdateNodeGroup(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	created.Name = "updated-group"
	updated, err := repo.UpdateNodeGroup(context.Background(), created)
	assert.NoError(t, err)
	assert.Equal(t, "updated-group", updated.Name)
}

func TestDeleteNodeGroup(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	err = repo.DeleteNodeGroup(context.Background(), created.ID)
	assert.NoError(t, err)
	// Normal query should not find the group
	deleted, err := repo.GetNodeGroup(context.Background(), created.ID)
	assert.Error(t, err)
	assert.Nil(t, deleted)
	// Unscoped query should find the soft-deleted group
	db := repo.db.GORM()
	var found datamodel.NodeGroup
	err = db.Unscoped().First(&found, created.ID).Error
	assert.NoError(t, err)
	assert.NotNil(t, found.DeletedAt)
	assert.True(t, found.DeletedAt.Valid)
}

func TestGetNodeGroup_NotFound(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	got, err := repo.GetNodeGroup(context.Background(), 9999)
	assert.Error(t, err)
	assert.Nil(t, got)
	var ce *vsaerrors.CustomError
	if err != nil && errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "node_group not found")
	}
}

func TestCreateNodeGroup_DuplicateName(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	_, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	dup := &datamodel.NodeGroup{Name: group.Name}
	_, err = repo.CreateNodeGroup(context.Background(), dup)
	assert.Error(t, err)
}

func TestUpdateNodeGroup_NotFound(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	fake := &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 9999}, Name: "does-not-exist"}
	_, err := repo.UpdateNodeGroup(context.Background(), fake)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if err != nil && errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "node_group not found")
	}
}

func TestDeleteNodeGroup_NotFound(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	err := repo.DeleteNodeGroup(context.Background(), 9999)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if err != nil && errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "record not found")
	}
}

func TestDeleteNodeGroup_DBError(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	// Simulate DB error by closing the DB connection
	db := repo.db.GORM()
	sqldb, _ := db.DB()
	cerr := sqldb.Close()
	assert.NoError(t, cerr)
	err = repo.DeleteNodeGroup(context.Background(), created.ID)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if err != nil && errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "sql: database is closed")
	}
}

func TestCreateNodeGroup_EmptyName(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	group := &datamodel.NodeGroup{Name: ""}
	_, err := repo.CreateNodeGroup(context.Background(), group)
	assert.Error(t, err)
}

func TestUpdateNodeGroup_EmptyName(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	created.Name = ""
	_, err = repo.UpdateNodeGroup(context.Background(), created)
	assert.Error(t, err)
	var ce *vsaerrors.CustomError
	if err != nil && errors.As(err, &ce) && ce.OriginalErr != nil {
		assert.Contains(t, ce.OriginalErr.Error(), "node_group name is empty")
	}
}

func TestCreateNodeGroup_Nil(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	_, err := repo.CreateNodeGroup(context.Background(), nil)
	assert.Error(t, err)
}

func TestUpdateNodeGroup_Nil(t *testing.T) {
	repo, _ := setupNodeGroupTestRepo(t)
	_, err := repo.UpdateNodeGroup(context.Background(), nil)
	assert.Error(t, err)
}

func TestUpdateNodeGroup_DBError(t *testing.T) {
	repo, group := setupNodeGroupTestRepo(t)
	created, err := repo.CreateNodeGroup(context.Background(), group)
	assert.NoError(t, err)
	// Simulate DB error by closing the DB connection
	db := repo.db.GORM()
	sqldb, _ := db.DB()
	cerr := sqldb.Close()
	assert.NoError(t, cerr)
	created.Name = "should-fail"
	_, err = repo.UpdateNodeGroup(context.Background(), created)
	assert.Error(t, err)
}
