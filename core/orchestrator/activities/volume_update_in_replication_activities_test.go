package activities

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vcpError "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetReplicationFromDBVolume(t *testing.T) {
	t.Run("WhenListVolumeReplicationsError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-central1",
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test error"))
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "test error", err.Error())
	})
	t.Run("WhenNoReplicationsFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-central1",
		}
		dbRepl := []*datamodel.VolumeReplication{}
		expectedError := utilErrors.NewNonRetryableErr("no replication found for the volume")
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(dbRepl, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, err, expectedError)
	})
	t.Run("WhenUtilsParseProjectNumberFromURIError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-central1",
		}
		dbRepl := []*datamodel.VolumeReplication{
			{BaseModel: datamodel.BaseModel{UUID: "repl-uuid-123"}},
		}
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", errors.New("test error")
		}
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
		}()
		expectedError := vcpError.NewVCPError(vcpError.ErrProjectParsingError, errors.New("test error"))
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(dbRepl, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, err, expectedError)
	})
	t.Run("WhenInternalParseRegionAndZoneFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-central1",
		}
		dbRepl := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:        "us-central1",
					DestinationLocation:   "us-east4",
					SourcePoolUUID:        "pool-uuid-123",
					DestinationPoolUUID:   "pool-uuid-321",
					SourceVolumeUUID:      "vol-uuid-123",
					DestinationVolumeUUID: "vol-uuid-321",
				},
			},
		}
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) { return "", "", errors.New("test error") }
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalParseRegionAndZone = utils.ParseRegionAndZone
		}()
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(dbRepl, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.NotNil(tt, err)
	})
	t.Run("WhenInternalParseRegionAndZoneFailsForRemote", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-central1",
		}
		dbRepl := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:        "us-central1",
					DestinationLocation:   "us-east4",
					SourcePoolUUID:        "pool-uuid-123",
					DestinationPoolUUID:   "pool-uuid-321",
					SourceVolumeUUID:      "vol-uuid-123",
					DestinationVolumeUUID: "vol-uuid-321",
				},
			},
		}
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		count := 0
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if count == 0 {
				count = count + 1
				return "us-central1", "zone-1", nil
			}
			return "", "", errors.New("test error")
		}
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalParseRegionAndZone = utils.ParseRegionAndZone
		}()
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(dbRepl, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccessWithLocalRegion", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-central1",
		}
		dbRepl := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "src",
					SourceLocation:        "us-central1",
					DestinationLocation:   "us-east4",
					SourcePoolUUID:        "pool-uuid-123",
					DestinationPoolUUID:   "pool-uuid-321",
					SourceVolumeUUID:      "vol-uuid-123",
					DestinationVolumeUUID: "vol-uuid-321",
				},
			},
		}
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "zone-1", nil
		}
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalParseRegionAndZone = utils.ParseRegionAndZone
		}()
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(dbRepl, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.Nil(tt, err)
	})
	t.Run("WhenSuccessWithRemoteRegion", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume"}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}

		params := &common.UpdateVolumeParams{
			AccountName: "test-project",
			Region:      "us-east4",
		}
		dbRepl := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType:          "dst",
					SourceLocation:        "us-central1",
					DestinationLocation:   "us-east4",
					SourcePoolUUID:        "pool-uuid-123",
					DestinationPoolUUID:   "pool-uuid-321",
					SourceVolumeUUID:      "vol-uuid-123",
					DestinationVolumeUUID: "vol-uuid-321",
				},
			},
		}
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "zone-1", nil
		}
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalParseRegionAndZone = utils.ParseRegionAndZone
		}()
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(dbRepl, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}

		_, err := activity.GetReplicationFromDBVolume(ctx, volume, event, params)
		assert.Nil(tt, err)
	})
}

func TestGetLocalBasePathVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("mock error")
		}
		defer func() {
			replication.InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
		}()
		activity := UpdateVolumeInReplicationActivity{}
		_, err := activity.GetLocalBasePathVolume(ctx, event)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "projects/test-project/locations/us-central1", nil
		}
		defer func() {
			replication.InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
		}()
		activity := UpdateVolumeInReplicationActivity{}
		updatedEvent, err := activity.GetLocalBasePathVolume(ctx, event)
		assert.Nil(tt, err)
		assert.Equal(tt, "projects/test-project/locations/us-central1", updatedEvent.Local.BasePath)
	})
}

func TestGetRemoteBasePathVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
			Remote: common.ProjectInfo{
				Location: "remote-location",
			},
		}
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "", errors.New("mock error")
		}
		defer func() {
			replication.InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
		}()
		activity := UpdateVolumeInReplicationActivity{}
		_, err := activity.GetRemoteBasePathVolume(ctx, event)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
			Remote: common.ProjectInfo{
				Location: "remote-location",
			},
		}
		replication.InternalUtilGetPairedRegionURI = func(locationID string) (string, error) {
			return "projects/test-project/locations/us-east4", nil
		}
		defer func() {
			replication.InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
		}()
		activity := UpdateVolumeInReplicationActivity{}
		updatedEvent, err := activity.GetRemoteBasePathVolume(ctx, event)
		assert.Nil(tt, err)
		assert.Equal(tt, "projects/test-project/locations/us-east4", updatedEvent.Remote.BasePath)
	})
}

func TestGetSignedLocalTokenVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "", errors.New("mock error")
		}
		defer func() {
			replication.InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		activity := UpdateVolumeInReplicationActivity{}
		_, err := activity.GetSignedLocalTokenVolume(ctx, event)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location: "us-central1",
			},
		}
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "signed-token", nil
		}
		defer func() {
			replication.InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		activity := UpdateVolumeInReplicationActivity{}
		updatedEvent, err := activity.GetSignedLocalTokenVolume(ctx, event)
		assert.Nil(tt, err)
		assert.Equal(tt, "signed-token", updatedEvent.Local.JwtToken)
	})
}

func TestGetSignedRemoteTokenVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location:      "us-central1",
				ProjectNumber: "123456789",
			},
			Remote: common.ProjectInfo{
				ProjectNumber: "987654321",
			},
		}
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "", errors.New("mock error")
		}
		defer func() {
			replication.InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		activity := UpdateVolumeInReplicationActivity{}
		_, err := activity.GetSignedRemoteTokenVolume(ctx, event)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccessSameProjectNumber", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location:      "us-central1",
				ProjectNumber: "123456789",
				JwtToken:      "signed-token",
			},
			Remote: common.ProjectInfo{
				ProjectNumber: "123456789",
			},
		}
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "signed-token", nil
		}
		defer func() {
			replication.InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		activity := UpdateVolumeInReplicationActivity{}
		updatedEvent, err := activity.GetSignedRemoteTokenVolume(ctx, event)
		assert.Nil(tt, err)
		assert.Equal(tt, "signed-token", updatedEvent.Remote.JwtToken)
	})
	t.Run("WhenSuccessDifferentProjectNumber", func(tt *testing.T) {
		ctx := context.Background()
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				Location:      "us-central1",
				ProjectNumber: "123456789",
				JwtToken:      "signed-token",
			},
			Remote: common.ProjectInfo{
				ProjectNumber: "987654321",
			},
		}
		replication.InternalUtilGetSignedToken = func(locationID string) (string, error) {
			return "remote-signed-token", nil
		}
		defer func() {
			replication.InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		activity := UpdateVolumeInReplicationActivity{}
		updatedEvent, err := activity.GetSignedRemoteTokenVolume(ctx, event)
		assert.Nil(tt, err)
		assert.Equal(tt, "remote-signed-token", updatedEvent.Remote.JwtToken)
	})
}

func TestCreateJobForChildWorkflow(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume", AccountID: 1}

		mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.CreateJobForChildWorkflow(ctx, volume)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"}, Name: "test-volume", AccountID: 1}

		expectedJob := &datamodel.Job{
			Type:          string(coreModels.JobTypeUpdateVolume),
			State:         string(coreModels.JobsStateNEW),
			ResourceName:  volume.Name,
			AccountID:     sql.NullInt64{Int64: volume.AccountID, Valid: true},
			JobAttributes: &datamodel.JobAttributes{ResourceUUID: volume.UUID},
			CorrelationID: utils.GetCoRelationIDFromContext(ctx),
			RequestID:     utils.GetRequestIDFromContext(ctx),
		}
		mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(expectedJob, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		result, err := activity.CreateJobForChildWorkflow(ctx, volume)
		assert.Nil(tt, err)
		assert.Equal(tt, expectedJob.ResourceName, result.ResourceName)
		assert.Equal(tt, expectedJob.Type, result.Type)
		assert.Equal(tt, expectedJob.State, result.State)
	})
}

func TestGetReplicationMirrorState(t *testing.T) {
	t.Run("WhenListVolumeReplicationsError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("database error"))
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.NotNil(tt, err)
	})
	t.Run("WhenNoReplicationsFound", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.NotNil(tt, err)
	})
	t.Run("WhenDestinationReplicationUUIDEmpty", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.NotNil(tt, err)
	})
	t.Run("WhenV1betaGetMultipleReplicationsInternalError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some-error"))
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.NotNil(tt, err)
	})
	t.Run("WhenV1betaGetMultipleReplicationsInternalBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		badRequestResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest{
			Code:    400,
			Message: "Invalid request parameters",
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(badRequestResponse, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
	t.Run("WhenV1betaGetMultipleReplicationsInternalUnauthorized", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		unauthorizedResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized{
			Code:    401,
			Message: "Authentication failed",
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
	t.Run("WhenV1betaGetMultipleReplicationsInternalForbidden", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		forbiddenResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden{
			Code:    403,
			Message: "Access denied",
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(forbiddenResponse, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
	t.Run("WhenV1betaGetMultipleReplicationsInternalNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		notFoundResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound{
			Code:    404,
			Message: "Replication not found",
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(notFoundResponse, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
	t.Run("WhenV1betaGetMultipleReplicationsInternalInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		internalErrorResponse := &googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(internalErrorResponse, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, res)
		assert.Error(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
				},
			},
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, err)
		assert.Equal(tt, "MIRRORED", *res)
	})
	t.Run("WhenSuccessWithSrcEndpointType", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Local: common.ProjectInfo{
				BasePath:      "local-base-path",
				JwtToken:      "local-jwt-token",
				ProjectNumber: "111111111",
				Location:      "local-location",
			},
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "222222222",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
				DestinationLocation:        "remote-location",
				EndpointType:               "src",
			},
		}
		// Verify that when EndpointType is "src", the Remote basePath and token are used
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			assert.Equal(tt, "remote-base-path", basePath)
			assert.Equal(tt, "remote-jwt-token", jwt)
			return mc
		}
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.NewOptVolumeReplicationInternalV1betaMirrorState(googleproxyclient.VolumeReplicationInternalV1betaMirrorStateMIRRORED),
				},
			},
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		// Verify that the Remote projectNumber is used when EndpointType is "src"
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.MatchedBy(func(params googleproxyclient.V1betaGetMultipleReplicationsInternalParams) bool {
			return params.ProjectNumber == "222222222"
		})).Return(response, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.Nil(tt, err)
		assert.Equal(tt, "MIRRORED", *res)
	})
	t.Run("WhenNoReplicationsReturned", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{},
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.NotNil(tt, err)
	})
	t.Run("WhenMirrorStateNotSet", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mockStorage := database.NewMockStorage(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
			CorrelationID: "test-correlation-id",
		}
		dbVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: 1},
		}
		dbReplication := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "repl-uuid"},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationReplicationUUID: "dest-repl-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					MirrorState: googleproxyclient.OptVolumeReplicationInternalV1betaMirrorState{},
				},
			},
		}
		mockStorage.EXPECT().ListVolumeReplications(mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{dbReplication}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetReplicationMirrorState(ctx, event, dbVolume)
		assert.NotNil(tt, err)
	})
}

func TestGetRemotePoolDetailsVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		_, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.NotNil(tt, err)
	})
	t.Run("WhenV1betaDescribePoolBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		badRequestResponse := &googleproxyclient.V1betaDescribePoolBadRequest{
			Code:    400,
			Message: "Invalid request parameters",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(badRequestResponse, nil)
		res, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Equal(tt, "Failed to describe pool", err.Error())
	})
	t.Run("WhenV1betaDescribePoolUnauthorized", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		unauthorizedResponse := &googleproxyclient.V1betaDescribePoolUnauthorized{
			Code:    401,
			Message: "Authentication failed",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)
		res, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Equal(tt, "Failed to describe pool", err.Error())
	})
	t.Run("WhenV1betaDescribePoolForbidden", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		forbiddenResponse := &googleproxyclient.V1betaDescribePoolForbidden{
			Code:    403,
			Message: "Access denied",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(forbiddenResponse, nil)
		res, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Equal(tt, "Failed to describe pool", err.Error())
	})
	t.Run("WhenV1betaDescribePoolNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		notFoundResponse := &googleproxyclient.V1betaDescribePoolNotFound{
			Code:    404,
			Message: "Pool not found",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(notFoundResponse, nil)
		res, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Equal(tt, "Failed to describe pool", err.Error())
	})
	t.Run("WhenV1betaDescribePoolInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		internalErrorResponse := &googleproxyclient.V1betaDescribePoolInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(internalErrorResponse, nil)
		res, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.Nil(tt, res)
		assert.Error(tt, err)
		assert.Equal(tt, "Failed to describe pool", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		mockStorage := database.NewMockStorage(tt)
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
				PoolUUID:      "remote-pool-uuid",
			},
		}
		res := &googleproxyclient.PoolV1beta{}
		mockClient.EXPECT().V1betaDescribePool(mock.Anything, mock.Anything).Return(res, nil)
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		_, err := activity.GetRemotePoolDetailsVolume(ctx, event)
		assert.Nil(tt, err)
	})
}

func TestValidateRemoteVolumeUpdate(t *testing.T) {
	t.Run("WhenUpdateVolumeInvalid", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		pool := &googleproxyclient.PoolV1beta{
			SizeInBytes:    10,
			AllocatedBytes: googleproxyclient.NewOptNilFloat64(7),
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		dbVolume := &datamodel.Volume{
			SizeInBytes: 8,
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.ValidateRemoteVolumeUpdate(ctx, pool, params, dbVolume)
		assert.Nil(tt, err)
		assert.Equal(tt, false, res)
	})
	t.Run("WhenUpdateReplicationInvalidPoolSize", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		pool := &googleproxyclient.PoolV1beta{
			SizeInBytes:    10,
			AllocatedBytes: googleproxyclient.NewOptNilFloat64(7),
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 7,
		}
		dbVolume := &datamodel.Volume{
			SizeInBytes: 2,
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.ValidateRemoteVolumeUpdate(ctx, pool, params, dbVolume)
		assert.NotNil(tt, err)
		assert.Equal(tt, false, res)
		assert.Equal(tt, "Destination pool size is insufficient", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		pool := &googleproxyclient.PoolV1beta{
			SizeInBytes:    10,
			AllocatedBytes: googleproxyclient.NewOptNilFloat64(7),
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 3,
		}
		dbVolume := &datamodel.Volume{
			SizeInBytes: 2,
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		res, err := activity.ValidateRemoteVolumeUpdate(ctx, pool, params, dbVolume)
		assert.Nil(tt, err)
		assert.Equal(tt, true, res)
	})
}

func TestUpdateRemoteVolume(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		_, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
	})
	t.Run("WhenV1betaInternalUpdateVolumeBadRequest", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		badRequestResponse := &googleproxyclient.V1betaInternalUpdateVolumeBadRequest{
			Code:    400,
			Message: "Invalid request parameters",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(badRequestResponse, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to update volume internal", err.Error())
	})
	t.Run("WhenV1betaInternalUpdateVolumeUnauthorized", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		unauthorizedResponse := &googleproxyclient.V1betaInternalUpdateVolumeUnauthorized{
			Code:    401,
			Message: "Authentication failed",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(unauthorizedResponse, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to update volume internal", err.Error())
	})
	t.Run("WhenV1betaInternalUpdateVolumeNotFound", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		notFoundResponse := &googleproxyclient.V1betaInternalUpdateVolumeNotFound{
			Code:    404,
			Message: "Volume not found",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(notFoundResponse, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to update volume internal", err.Error())
	})
	t.Run("WhenV1betaInternalUpdateVolumeForbidden", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		forbiddenResponse := &googleproxyclient.V1betaInternalUpdateVolumeForbidden{
			Code:    403,
			Message: "Access denied",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(forbiddenResponse, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to update volume internal", err.Error())
	})
	t.Run("WhenV1betaInternalUpdateVolumeConflict", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		conflictResponse := &googleproxyclient.V1betaInternalUpdateVolumeConflict{
			Code:    409,
			Message: "conflict",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(conflictResponse, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to update volume internal", err.Error())
	})
	t.Run("WhenV1betaInternalUpdateVolumeInternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		internalErrorResponse := &googleproxyclient.V1betaInternalUpdateVolumeInternalServerError{
			Code:    500,
			Message: "Internal server error",
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(internalErrorResponse, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.NotNil(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "Failed to update volume internal", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		params := &common.UpdateVolumeParams{
			QuotaInBytes: 4,
		}
		response := &googleproxyclient.OperationV1beta{
			Name: googleproxyclient.NewOptString("operation-name/job-uuid"),
			Done: googleproxyclient.NewOptBool(false),
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		mockClient.EXPECT().V1betaInternalUpdateVolume(mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		res, err := activity.UpdateRemoteVolume(ctx, params, event)
		assert.Nil(tt, err)
		assert.Equal(tt, "job-uuid", *res)
	})
}

func TestDescribeRemoteJobVolumeUpdate(t *testing.T) {
	t.Run("WhenErrorJobNotFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			CorrelationID: "corr-id-123",
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		jobId := "remote-job-uuid"
		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    jobId,
			ProjectNumber:  event.Remote.ProjectNumber,
			LocationId:     event.Remote.Location,
			XCorrelationID: googleproxyclient.NewOptString(event.CorrelationID),
		}
		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(false)}, nil)
		err := activity.DescribeRemoteJobVolumeUpdate(ctx, event, jobId)
		assert.NotNil(tt, err)
	})
	t.Run("WhenSuccessJobFinished", func(tt *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(tt)
		mockClient := googleproxyclient.NewMockInvoker(tt)
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		event := &common.VolumeUpdateEventParams{
			CorrelationID: "corr-id-123",
			Remote: common.ProjectInfo{
				BasePath:      "remote-base-path",
				JwtToken:      "remote-jwt-token",
				ProjectNumber: "123456789",
				Location:      "remote-location",
			},
		}
		activity := UpdateVolumeInReplicationActivity{SE: mockStorage}
		jobId := "remote-job-uuid"
		describeOperationParams := googleproxyclient.V1betaInternalDescribeOperationParams{
			OperationId:    jobId,
			ProjectNumber:  event.Remote.ProjectNumber,
			LocationId:     event.Remote.Location,
			XCorrelationID: googleproxyclient.NewOptString(event.CorrelationID),
		}
		mockClient.EXPECT().V1betaInternalDescribeOperation(ctx, describeOperationParams).Return(&googleproxyclient.InternalOperationV1beta{Done: googleproxyclient.NewOptBool(true)}, nil)
		err := activity.DescribeRemoteJobVolumeUpdate(ctx, event, jobId)
		assert.Nil(tt, err)
	})
}
