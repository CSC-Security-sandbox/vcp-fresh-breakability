package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type FlexCachePrepopulateActivityTestSuite struct {
	suite.Suite
	mockStorage  *database.MockStorage
	mockProvider *vsa.MockProvider
	activity     *FlexCachePrepopulateActivity
	ctx          context.Context
}

func TestFlexCachePrepopulateActivityTestSuite(t *testing.T) {
	suite.Run(t, new(FlexCachePrepopulateActivityTestSuite))
}

func (s *FlexCachePrepopulateActivityTestSuite) SetupTest() {
	s.mockStorage = database.NewMockStorage(s.T())
	s.mockProvider = &vsa.MockProvider{}
	s.activity = &FlexCachePrepopulateActivity{SE: s.mockStorage}
	s.ctx = context.Background()
	mockLogger := log.NewLogger()
	s.ctx = context.WithValue(s.ctx, middleware.ContextSLoggerKey, mockLogger)
}

func (s *FlexCachePrepopulateActivityTestSuite) TearDownTest() {
	s.mockStorage.AssertExpectations(s.T())
}

func (s *FlexCachePrepopulateActivityTestSuite) TestGetActivePrepopulateJobs_Success_MultipleJobs() {
	expectedJobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: "job-uuid-1"},
			Type:         string(models.JobTypeFlexCachePrePopulate),
			State:        string(models.JobsStatePROCESSING),
			ResourceName: "volume-uuid-1",
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: "ontap-job-uuid-1",
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: "job-uuid-2"},
			Type:         string(models.JobTypeFlexCachePrePopulate),
			State:        string(models.JobsStateNEW),
			ResourceName: "volume-uuid-2",
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: "ontap-job-uuid-2",
			},
		},
	}

	s.mockStorage.On("GetActivePrepopulateJobs", s.ctx).Return(expectedJobs, nil)

	result, err := s.activity.GetActivePrepopulateJobs(s.ctx)

	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedJobs, result)
	assert.Len(s.T(), result, 2)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestGetActivePrepopulateJobs_Success_EmptyList() {
	s.mockStorage.On("GetActivePrepopulateJobs", s.ctx).Return([]*datamodel.Job{}, nil)

	result, err := s.activity.GetActivePrepopulateJobs(s.ctx)

	assert.NoError(s.T(), err)
	assert.Empty(s.T(), result)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestGetActivePrepopulateJobs_DatabaseError() {
	expectedError := errors.New("database connection error")
	s.mockStorage.On("GetActivePrepopulateJobs", s.ctx).Return(nil, expectedError)

	result, err := s.activity.GetActivePrepopulateJobs(s.ctx)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
	assert.Contains(s.T(), err.Error(), "failed to get active prepopulate jobs")
}

// GetVolumeByResourceName Tests
func (s *FlexCachePrepopulateActivityTestSuite) TestGetVolumeByResourceName_Success() {
	volumeUUID := "volume-uuid-123"

	expectedVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: &datamodel.CacheConfig{},
		},
	}

	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(expectedVolume, nil)

	result, err := s.activity.GetVolumeByResourceName(s.ctx, volumeUUID)

	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedVolume, result)
	assert.Equal(s.T(), volumeUUID, result.UUID)
	assert.Equal(s.T(), "test-volume", result.Name)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestGetVolumeByResourceName_VolumeNotFound() {
	volumeUUID := "non-existent-uuid"
	expectedError := errors.New("volume not found")
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(nil, expectedError)

	result, err := s.activity.GetVolumeByResourceName(s.ctx, volumeUUID)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
	assert.Contains(s.T(), err.Error(), "failed to get volume")
}

func (s *FlexCachePrepopulateActivityTestSuite) TestGetVolumeByResourceName_DatabaseError() {
	volumeUUID := "volume-uuid"
	expectedError := errors.New("database connection error")
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(nil, expectedError)

	result, err := s.activity.GetVolumeByResourceName(s.ctx, volumeUUID)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
	assert.Contains(s.T(), err.Error(), "failed to get volume")
}

// PollPrepopulateJobStatus Tests
func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_Success_RunningJob() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID},
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "test-password",
				SecretID:      "secret-id",
				CertificateID: "cert-id",
				AuthType:      1,
			},
		},
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	nodes := []*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-node",
		},
	}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nodes, nil)

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	expectedOntapJob := &vsa.OntapJob{State: "running"}
	mockProvider.On("JobGet", ontapJobUUID).Return(expectedOntapJob, nil)

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.Equal(s.T(), ontapJobUUID, result.JobUUID)
	assert.Equal(s.T(), "running", result.State)
	assert.Empty(s.T(), result.ErrorMessage)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_Success_SuccessState() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID},
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
			},
		},
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-node"}}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nodes, nil)

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	expectedOntapJob := &vsa.OntapJob{State: "success"}
	mockProvider.On("JobGet", ontapJobUUID).Return(expectedOntapJob, nil)

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.Equal(s.T(), ontapJobUUID, result.JobUUID)
	assert.Equal(s.T(), "success", result.State)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_Success_FailureWithError() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"
	errorMessage := "prepopulate failed due to network error"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID},
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
			},
		},
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-node"}}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nodes, nil)

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	expectedOntapJob := &vsa.OntapJob{
		State: "failure",
		Error: &vsa.OntapError{
			Message: errorMessage,
		},
	}
	mockProvider.On("JobGet", ontapJobUUID).Return(expectedOntapJob, nil)

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.Equal(s.T(), ontapJobUUID, result.JobUUID)
	assert.Equal(s.T(), "failure", result.State)
	assert.Equal(s.T(), errorMessage, result.ErrorMessage)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_Success_EmptyState() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID},
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
			},
		},
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-node"}}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nodes, nil)

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	expectedOntapJob := &vsa.OntapJob{State: ""}
	mockProvider.On("JobGet", ontapJobUUID).Return(expectedOntapJob, nil)

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), result)
	assert.Equal(s.T(), ontapJobUUID, result.JobUUID)
	assert.Empty(s.T(), result.State)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_NoJobAttributes() {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
	}

	job := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName:  "volume-uuid",
		JobAttributes: nil,
	}

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
	assert.Contains(s.T(), err.Error(), "has no ONTAP job UUID")
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_EmptyOntapJobUUID() {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: "",
		},
	}

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
	assert.Contains(s.T(), err.Error(), "has no ONTAP job UUID")
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_GetNodesError() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	expectedError := errors.New("failed to get nodes")
	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nil, expectedError)

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_NoNodesFound() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return([]*datamodel.Node{}, nil)

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_GetProviderError() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID},
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
			},
		},
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-node"}}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nodes, nil)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestPollPrepopulateJobStatus_JobGetError() {
	poolID := int64(123)
	ontapJobUUID := "ontap-job-uuid-123"

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		Name:      "test-volume",
		PoolID:    poolID,
		Pool: &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: poolID},
			DeploymentName: "test-deployment",
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
			},
		},
	}

	job := &datamodel.Job{
		BaseModel:    datamodel.BaseModel{UUID: "job-uuid"},
		ResourceName: "volume-uuid",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID: ontapJobUUID,
		},
	}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-node"}}

	s.mockStorage.On("GetNodesByPoolID", s.ctx, poolID).Return(nodes, nil)

	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	mockProvider.On("JobGet", ontapJobUUID).Return(nil, errors.New("job not found"))

	result, err := s.activity.PollPrepopulateJobStatus(s.ctx, volume, job)

	assert.Error(s.T(), err)
	assert.Nil(s.T(), result)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_SuccessState() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateSuccess,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: &datamodel.CacheConfig{},
		},
	}

	s.mockStorage.On("UpdateJob", s.ctx, jobUUID, string(models.JobsStateDONE), 0, "").Return(nil)
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		cacheParams, ok := updates["cache_parameters"].(*datamodel.CacheParameters)
		if !ok {
			return false
		}
		return cacheParams.CacheConfig != nil && cacheParams.CacheConfig.CachePrePopulateState == cvpModels.FlexCacheConfigV1betaCachePrePopulateStateCOMPLETE
	})).Return(nil)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_FailureState() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	errorMessage := "prepopulate failed"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID:      "ontap-job-uuid",
		State:        common.OntapJobStateFailure,
		ErrorMessage: errorMessage,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: &datamodel.CacheConfig{},
		},
	}

	s.mockStorage.On("UpdateJob", s.ctx, jobUUID, string(models.JobsStateERROR), 0, errorMessage).Return(nil)
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		cacheParams, ok := updates["cache_parameters"].(*datamodel.CacheParameters)
		if !ok {
			return false
		}
		return cacheParams.CacheConfig != nil && cacheParams.CacheConfig.CachePrePopulateState == cvpModels.FlexCacheConfigV1betaCachePrePopulateStateERROR
	})).Return(nil)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_ProcessingState_NoUpdate() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateRunning,
	}

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
	// No storage methods should be called
	s.mockStorage.AssertNotCalled(s.T(), "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	s.mockStorage.AssertNotCalled(s.T(), "GetVolume", mock.Anything, mock.Anything)
	s.mockStorage.AssertNotCalled(s.T(), "UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_QueuedState_NoUpdate() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   "queued",
	}

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_UpdateJobError() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateSuccess,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: &datamodel.CacheConfig{},
		},
	}

	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.Anything).Return(nil)

	expectedError := errors.New("update job failed")
	s.mockStorage.On("UpdateJob", s.ctx, jobUUID, string(models.JobsStateDONE), 0, "").Return(expectedError)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "failed to update job")
	s.mockStorage.AssertCalled(s.T(), "UpdateVolumeFields", s.ctx, volumeUUID, mock.Anything)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_GetVolumeError() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateSuccess,
	}

	expectedError := errors.New("volume not found")
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(nil, expectedError)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "failed to get volume")
	s.mockStorage.AssertNotCalled(s.T(), "UpdateJob")
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_NoCacheParameters() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateSuccess,
	}

	volume := &datamodel.Volume{
		BaseModel:       datamodel.BaseModel{UUID: volumeUUID},
		Name:            "test-volume",
		CacheParameters: nil,
	}

	s.mockStorage.On("UpdateJob", s.ctx, jobUUID, string(models.JobsStateDONE), 0, "").Return(nil)
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		cacheParams, ok := updates["cache_parameters"].(*datamodel.CacheParameters)
		if !ok {
			return false
		}
		return cacheParams.CacheConfig != nil && cacheParams.CacheConfig.CachePrePopulateState == cvpModels.FlexCacheConfigV1betaCachePrePopulateStateCOMPLETE
	})).Return(nil)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_NoCacheConfig() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateSuccess,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: nil,
		},
	}

	s.mockStorage.On("UpdateJob", s.ctx, jobUUID, string(models.JobsStateDONE), 0, "").Return(nil)
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		cacheParams, ok := updates["cache_parameters"].(*datamodel.CacheParameters)
		if !ok {
			return false
		}
		return cacheParams.CacheConfig != nil && cacheParams.CacheConfig.CachePrePopulateState == cvpModels.FlexCacheConfigV1betaCachePrePopulateStateCOMPLETE
	})).Return(nil)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_UpdateVolumeFieldsError() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: "ontap-job-uuid",
		State:   common.OntapJobStateSuccess,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: &datamodel.CacheConfig{},
		},
	}

	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	expectedError := errors.New("volume update failed")
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.Anything).Return(expectedError)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "failed to update volume")
	s.mockStorage.AssertNotCalled(s.T(), "UpdateJob")
}

func (s *FlexCachePrepopulateActivityTestSuite) TestUpdateJobAndVolumeStatus_Success_WithEmptyErrorMessage() {
	jobUUID := "job-uuid"
	volumeUUID := "volume-uuid"
	jobStatus := &common.PrepopulateJobStatus{
		JobUUID:      "ontap-job-uuid",
		State:        common.OntapJobStateFailure,
		ErrorMessage: "",
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		CacheParameters: &datamodel.CacheParameters{
			CacheConfig: &datamodel.CacheConfig{},
		},
	}

	s.mockStorage.On("UpdateJob", s.ctx, jobUUID, string(models.JobsStateERROR), 0, "").Return(nil)
	s.mockStorage.On("GetVolume", s.ctx, volumeUUID).Return(volume, nil)
	s.mockStorage.On("UpdateVolumeFields", s.ctx, volumeUUID, mock.Anything).Return(nil)

	err := s.activity.UpdateJobAndVolumeStatus(s.ctx, jobUUID, volumeUUID, jobStatus)

	assert.NoError(s.T(), err)
}
