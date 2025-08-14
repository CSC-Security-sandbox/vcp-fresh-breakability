package hydrationActivities

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"testing"
	"time"
)

func TestHydrateBatchSnapshotsToCCFE(t *testing.T) {
	t.Run("WhenTokenError", func(tt *testing.T) {
		var createdSnapshots []*datamodel.Snapshot
		var deletedSnapshots []*datamodel.Snapshot

		ctx := context.Background()
		expectedError := errors.New("some error")
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "", errors.New("some error")
		}
		defer func() { auth.GenerateCallbackToken = originalGetSignedCallbackToken }()
		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("AllCreatedSnapshotsHydratedSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		createdSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "created-snapshot-uuid-1"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "created-snapshot-uuid-2"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone2",
						},
					},
					Name: "Volume2",
				},
				Account: &datamodel.Account{Name: "Account2"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		var deletedSnapshots []*datamodel.Snapshot

		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalBatchHydrateCreatedSnapshots := batchHydrateCreatedSnapshots
		batchHydrateCreatedSnapshots = func(ctx context.Context, logger log.Logger, requestArr []models.Request, volumeName, region, accountName, token string) error {
			return nil
		}
		defer func() {
			batchHydrateCreatedSnapshots = originalBatchHydrateCreatedSnapshots
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()

		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})

	t.Run("WhenBatchHydrateCreatedSnapshotsThrowsErrorItShouldPassToNextBatch", func(tt *testing.T) {
		ctx := context.Background()
		createdSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "created-snapshot-uuid-1"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "created-snapshot-uuid-2"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone2",
						},
					},
					Name: "Volume2",
				},
				Account: &datamodel.Account{Name: "Account2"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		var deletedSnapshots []*datamodel.Snapshot

		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalBatchHydrateCreatedSnapshots := batchHydrateCreatedSnapshots
		batchHydrateCreatedSnapshots = func(ctx context.Context, logger log.Logger, requestArr []models.Request, volumeName, region, accountName, token string) error {
			return errors.New("some error")
		}
		defer func() {
			batchHydrateCreatedSnapshots = originalBatchHydrateCreatedSnapshots
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()
		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})

	t.Run("ValidationErrorOnCreatedSnapshotItShouldPassToNextBatch", func(tt *testing.T) {
		ctx := context.Background()
		createdSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: ""},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		var deletedSnapshots []*datamodel.Snapshot

		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()
		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})

	t.Run("AllDeletedSnapshotsHydratedSuccessfully", func(tt *testing.T) {
		ctx := context.Background()
		var createdSnapshots []*datamodel.Snapshot
		deletedSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "deleted-snapshot-uuid-1"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "deleted-snapshot-uuid-2"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone2",
						},
					},
					Name: "Volume2",
				},
				Account: &datamodel.Account{Name: "Account2"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalBatchHydrateDeletedSnapshots := batchHydrateDeletedSnapshots
		batchHydrateDeletedSnapshots = func(ctx context.Context, logger log.Logger, requestArr []models.Request, volumeName, region, accountName, token string) error {
			return nil
		}
		defer func() {
			batchHydrateDeletedSnapshots = originalBatchHydrateDeletedSnapshots
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()

		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})

	t.Run("WhenBatchHydrateDeletedSnapshotsThrowsErrortItShouldPassToNextBatch", func(tt *testing.T) {
		ctx := context.Background()
		var createdSnapshots []*datamodel.Snapshot
		deletedSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "deleted-snapshot-uuid-1"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "deleted-snapshot-uuid-2"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone2",
						},
					},
					Name: "Volume2",
				},
				Account: &datamodel.Account{Name: "Account2"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalBatchHydrateDeletedSnapshots := batchHydrateDeletedSnapshots
		batchHydrateDeletedSnapshots = func(ctx context.Context, logger log.Logger, requestArr []models.Request, volumeName, region, accountName, token string) error {
			return errors.New("some error")
		}
		defer func() {
			batchHydrateDeletedSnapshots = originalBatchHydrateDeletedSnapshots
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()

		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})

	t.Run("ValidationErrorOnDeletedSnapshotItShouldPassToNextBatch", func(tt *testing.T) {
		ctx := context.Background()
		var createdSnapshots []*datamodel.Snapshot
		deletedSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: ""},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		defer func() {
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()
		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})

	t.Run("ErrorDuringBatchHydrationItShouldPassToNextBatch", func(tt *testing.T) {
		ctx := context.Background()
		createdSnapshots := []*datamodel.Snapshot{
			{
				BaseModel: datamodel.BaseModel{UUID: "created-snapshot-uuid-1"},
				Volume: &datamodel.Volume{
					Pool: &datamodel.Pool{
						PoolAttributes: &datamodel.PoolAttributes{
							SecondaryZone: "zone1",
						},
					},
					Name: "Volume1",
				},
				Account: &datamodel.Account{Name: "Account1"},
				SnapshotAttributes: &datamodel.SnapshotAttributes{
					SizeInBytes: 1024,
				},
			},
		}
		var deletedSnapshots []*datamodel.Snapshot
		originalGetSignedCallbackToken := auth.GenerateCallbackToken
		auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
			return "mocked-token", nil
		}
		originalBatchHydrateCreatedSnapshots := batchHydrateCreatedSnapshots
		batchHydrateCreatedSnapshots = func(ctx context.Context, logger log.Logger, requestArr []models.Request, volumeName, region, accountName, token string) error {
			return errors.New("batch hydration error")
		}
		defer func() {
			batchHydrateCreatedSnapshots = originalBatchHydrateCreatedSnapshots
			auth.GenerateCallbackToken = originalGetSignedCallbackToken
		}()

		err := _hydrateBatchSnapshotstoCCFE(ctx, createdSnapshots, deletedSnapshots)
		assert.NoError(tt, err)
	})
}

func TestConvertBulkSnapshotToGCPSnapshotObject_AllCases(tt *testing.T) {
	tt.Run("ValidSnapshot_ReturnsCorrectObject", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID:      "validUUID",
				CreatedAt: time.Now(),
			},
			Name:         "snapshot-name",
			State:        "CREATED",
			StateDetails: "Snapshot created successfully",
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes: 1024,
			},
			Volume: &datamodel.Volume{
				Name: "volume-name",
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			Description: "valid-description",
		}

		expectedSnapshot := models.HydrateSnapshot{
			ResourceId:   utils.RenameSnapshotName(snapshot.Name),
			SnapshotId:   snapshot.UUID,
			State:        common.MapStateToGcpState(snapshot.State),
			StateDetails: snapshot.StateDetails,
			UsedBytes:    snapshot.SnapshotAttributes.SizeInBytes,
			CreateTime:   snapshot.CreatedAt,
			VolumeName:   snapshot.Volume.Name,
			AccountName:  snapshot.Account.Name,
			Description:  snapshot.Description,
		}

		result := _convertBulkSnapshotToGCPSnapshotObject(snapshot)
		assert.Equal(tt, expectedSnapshot, result)
	})

	tt.Run("EmptyDescription_ReturnsObjectWithoutDescription", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID:      "validUUID",
				CreatedAt: time.Now(),
			},
			Name:         "snapshot-name",
			State:        "CREATED",
			StateDetails: "Snapshot created successfully",
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes: 1024,
			},
			Volume: &datamodel.Volume{
				Name: "volume-name",
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			Description: "",
		}

		expectedSnapshot := models.HydrateSnapshot{
			ResourceId:   utils.RenameSnapshotName(snapshot.Name),
			SnapshotId:   snapshot.UUID,
			State:        common.MapStateToGcpState(snapshot.State),
			StateDetails: snapshot.StateDetails,
			UsedBytes:    snapshot.SnapshotAttributes.SizeInBytes,
			CreateTime:   snapshot.CreatedAt,
			VolumeName:   snapshot.Volume.Name,
			AccountName:  snapshot.Account.Name,
		}

		result := _convertBulkSnapshotToGCPSnapshotObject(snapshot)
		assert.Equal(tt, expectedSnapshot, result)
	})

	tt.Run("InvalidSnapshot_ReturnsError", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{
				UUID:      "",
				CreatedAt: time.Now(),
			},
			Name:         "",
			State:        "",
			StateDetails: "",
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				SizeInBytes: 0,
			},
			Volume: &datamodel.Volume{
				Name: "",
			},
			Account: &datamodel.Account{
				Name: "",
			},
			Description: "",
		}

		result := _convertBulkSnapshotToGCPSnapshotObject(snapshot)
		assert.NotNil(tt, result)
		assert.Empty(tt, result.ResourceId)
		assert.Empty(tt, result.SnapshotId)
		assert.Equal(tt, "STATE_UNSPECIFIED", result.State)
		assert.Empty(tt, result.StateDetails)
		assert.Zero(tt, result.UsedBytes)
		assert.Empty(tt, result.VolumeName)
		assert.Empty(tt, result.AccountName)
		assert.Empty(tt, result.Description)
	})
}

func TestValidateSnapshot(t *testing.T) {
	t.Run("SnapshotUUIDIsEmpty_ReturnsError", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: ""},
			Volume: &datamodel.Volume{
				Name: "volume-name",
				Pool: &datamodel.Pool{
					PoolAttributes: &datamodel.PoolAttributes{
						PrimaryZone:   "zone-name1",
						SecondaryZone: "zone-name2",
						IsRegionalHA:  true,
					},
				},
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			Description: "valid-description",
		}
		_, err := _validateSnapshot(snapshot)
		assert.Equal(tt, "errorEmptySnapShotSnapshotUUID", err.Error())
	})

	t.Run("VolumeNameIsEmpty_ReturnsError", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "validUUID"},
			Volume: &datamodel.Volume{
				Name: "",
				Pool: &datamodel.Pool{
					PoolAttributes: &datamodel.PoolAttributes{
						PrimaryZone:   "zone-name1",
						SecondaryZone: "zone-name",
						IsRegionalHA:  true,
					},
				},
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			Description: "valid-description",
		}
		_, err := _validateSnapshot(snapshot)
		assert.Equal(tt, "errorEmptySnapShotVolumeName", err.Error())
	})

	t.Run("AccountNameIsEmpty_ReturnsError", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "validUUID"},
			Volume: &datamodel.Volume{
				Name: "volume-name",
				Pool: &datamodel.Pool{
					PoolAttributes: &datamodel.PoolAttributes{
						PrimaryZone:   "zone-name1",
						SecondaryZone: "zone-name",
						IsRegionalHA:  true,
					},
				},
			},
			Account: &datamodel.Account{
				Name: "",
			},
			Description: "valid-description",
		}
		_, err := _validateSnapshot(snapshot)
		assert.Equal(tt, "errorEmptySnapshotAccountName", err.Error())
	})

	t.Run("DescriptionTooLong_ReturnsError", func(tt *testing.T) {
		longDescription := string(make([]byte, 1025))
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "validUUID"},
			Volume: &datamodel.Volume{
				Name: "volume-name",
				Pool: &datamodel.Pool{
					PoolAttributes: &datamodel.PoolAttributes{
						PrimaryZone:   "zone-name1",
						SecondaryZone: "zone-name",
						IsRegionalHA:  true,
					},
				},
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			Description: longDescription,
		}
		_, err := _validateSnapshot(snapshot)
		assert.Equal(tt, "errorTooLongDescription", err.Error())
	})

	t.Run("ValidSnapshot_ReturnsNil", func(tt *testing.T) {
		snapshot := datamodel.Snapshot{
			BaseModel: datamodel.BaseModel{UUID: "validUUID"},
			Volume: &datamodel.Volume{
				Name: "volume-name",
				Pool: &datamodel.Pool{
					PoolAttributes: &datamodel.PoolAttributes{
						PrimaryZone:   "zone-name1",
						SecondaryZone: "zone-name",
						IsRegionalHA:  true,
					},
				},
			},
			Account: &datamodel.Account{
				Name: "account-name",
			},
			Description: "valid-description",
		}
		_, err := _validateSnapshot(snapshot)
		assert.Nil(tt, err)
	})
}
