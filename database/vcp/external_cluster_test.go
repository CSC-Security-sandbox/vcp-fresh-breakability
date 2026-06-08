package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupExternalClusterStore(t *testing.T) *DataStoreRepository {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&datamodel.Cluster{}))
	return NewDataStoreRepository(gormwrapper.New(db))
}

func TestCreateExternalCluster_PersistsEncryptedAdminPassword(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	const plaintext = "admin-pass-123"
	encrypted, err := utils.EncryptPassword(log.Secret(plaintext))
	require.NoError(t, err)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
		AdminPassword: *encrypted,
	})
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, created.AdminPassword)

	got, err := store.GetExternalCluster(ctx, created.UUID)
	require.NoError(t, err)
	decrypted, err := utils.DecryptPassword(log.Secret(got.AdminPassword))
	require.NoError(t, err)
	assert.Equal(t, plaintext, *decrypted)
}

func TestCreateExternalCluster_DuplicateHostNameReturnsConflictDetail(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	_, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.NoError(t, err)

	_, err = store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.Error(t, err)

	var customErr *vsaerrors.CustomError
	require.True(t, vsaerrors.As(err, &customErr))
	assert.True(t, customErr.IsError(vsaerrors.ErrResourceStateConflictError))
	assert.Equal(t, `external cluster "host-1" already onboarded in location "us-central1"`, customErr.GetDetailMessage())
}

func TestGetExternalCluster_Success(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.NoError(t, err)

	got, err := store.GetExternalCluster(ctx, created.UUID)
	require.NoError(t, err)
	assert.Equal(t, created.UUID, got.UUID)
	assert.Equal(t, ExternalClusterStateCreated, got.LifecycleState)
}

func TestGetExternalCluster_NotFoundWhenDeleted(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.NoError(t, err)

	_, err = store.DeleteExternalCluster(ctx, created.UUID)
	require.NoError(t, err)

	_, err = store.GetExternalCluster(ctx, created.UUID)
	assert.Error(t, err)
}

func TestDeleteExternalCluster_IdempotentWhenAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.NoError(t, err)

	first, err := store.DeleteExternalCluster(ctx, created.UUID)
	require.NoError(t, err)
	assert.Equal(t, ExternalClusterStateDeleted, first.LifecycleState)
	assert.NotNil(t, first.DeletedAt)
	assert.True(t, first.DeletedAt.Valid)

	second, err := store.DeleteExternalCluster(ctx, created.UUID)
	require.NoError(t, err)
	assert.Equal(t, created.UUID, second.UUID)
	assert.Equal(t, ExternalClusterStateDeleted, second.LifecycleState)
}

func TestCreateExternalCluster_PersistsDescriptionAndManagementIP(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "ontap-full-data.example.com",
		Description:   "Primary DR site",
		Label:         "type=SAPHANA",
		Protocol:      "HTTPS",
		Port:          443,
		AdminUsername: "admin",
		ClusterAttributes: &datamodel.ClusterAttributes{
			ManagementIP: "10.10.10.50",
		},
	})
	require.NoError(t, err)

	got, err := store.GetExternalCluster(ctx, created.UUID)
	require.NoError(t, err)
	assert.Equal(t, "Primary DR site", got.Description)
	assert.Equal(t, "type=SAPHANA", got.Label)
	assert.Equal(t, "HTTPS", got.Protocol)
	assert.Equal(t, 443, got.Port)
	require.NotNil(t, got.ClusterAttributes)
	assert.Equal(t, "10.10.10.50", got.ClusterAttributes.ManagementIP)
	assert.Empty(t, got.ClusterAttributes.OntapVersion)
}

func TestUpdateExternalCluster_UpdatesMutableFields(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "update-host.example.com",
		AdminUsername: "admin",
		AdminPassword: "V1:encrypted",
		Protocol:      "INSECURE_HTTPS",
		Port:          443,
		Label:         "old-label",
		ClusterAttributes: &datamodel.ClusterAttributes{
			ManagementIP: "10.0.0.5",
			OntapVersion: "9.14.1",
		},
	})
	require.NoError(t, err)

	created.Description = "updated"
	created.Label = "new-label"
	created.Protocol = "HTTP"
	created.Port = 80

	updated, err := store.UpdateExternalCluster(ctx, created)
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)
	assert.Equal(t, "new-label", updated.Label)
	assert.Equal(t, "HTTP", updated.Protocol)
	assert.Equal(t, 80, updated.Port)

	got, err := store.GetExternalCluster(ctx, created.UUID)
	require.NoError(t, err)
	assert.Equal(t, "updated", got.Description)
	assert.Equal(t, "9.14.1", got.ClusterAttributes.OntapVersion)
}

func TestCreateExternalCluster_CountReadError(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	sqlDB, err := store.db.GORM().DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.Error(t, err)
	var customErr *vsaerrors.CustomError
	require.True(t, vsaerrors.As(err, &customErr))
	assert.True(t, customErr.IsError(vsaerrors.ErrDatabaseDataReadError))
}

func TestGetExternalCluster_ReadError(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.NoError(t, err)

	sqlDB, err := store.db.GORM().DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = store.GetExternalCluster(ctx, created.UUID)
	require.Error(t, err)
	var customErr *vsaerrors.CustomError
	require.True(t, vsaerrors.As(err, &customErr))
	assert.True(t, customErr.IsError(vsaerrors.ErrDatabaseDataReadError))
}

func TestUpdateExternalCluster_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	_, err := store.UpdateExternalCluster(ctx, nil)
	require.Error(t, err)

	_, err = store.UpdateExternalCluster(ctx, &datamodel.Cluster{})
	require.Error(t, err)
}

func TestUpdateExternalCluster_SaveError(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	created, err := store.CreateExternalCluster(ctx, &datamodel.Cluster{
		LocationID:    "us-central1",
		HostName:      "host-1",
		AdminUsername: "admin",
	})
	require.NoError(t, err)

	sqlDB, err := store.db.GORM().DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	created.Description = "updated"
	_, err = store.UpdateExternalCluster(ctx, created)
	require.Error(t, err)
	var customErr *vsaerrors.CustomError
	require.True(t, vsaerrors.As(err, &customErr))
	assert.True(t, customErr.IsError(vsaerrors.ErrDatabaseDataUpdateError))
}

func TestDeleteExternalCluster_NotFoundWhenNeverCreated(t *testing.T) {
	ctx := context.Background()
	store := setupExternalClusterStore(t)

	_, err := store.DeleteExternalCluster(ctx, "00000000-0000-0000-0000-000000000000")
	assert.Error(t, err)
}
