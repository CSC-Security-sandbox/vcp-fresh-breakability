package backgroundactivities

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type CustomerAdoptionActivityUnitTestSuite struct {
	suite.Suite
	mockStorage *database.MockStorage
	activity    *CustomerAdoptionActivity
	ctx         context.Context
}

func TestCustomerAdoptionActivityUnitTestSuite(t *testing.T) {
	suite.Run(t, new(CustomerAdoptionActivityUnitTestSuite))
}

func (suite *CustomerAdoptionActivityUnitTestSuite) SetupTest() {
	suite.mockStorage = database.NewMockStorage(suite.T())
	suite.activity = &CustomerAdoptionActivity{SE: suite.mockStorage}
	suite.ctx = context.TODO()
}

func (suite *CustomerAdoptionActivityUnitTestSuite) TestGetActiveVolumesActivity_Success() {
	vols := []*datamodel.Volume{
		{Name: "vol1", State: "available"},
		{Name: "vol2", State: "deleted"},
		{Name: "vol3", State: "available"},
	}
	// First call returns vols, second call returns empty slice to break loop
	suite.mockStorage.On("ListAllVolumes", suite.ctx, mock.Anything, mock.Anything).Return(vols, nil).Once()
	suite.mockStorage.On("ListAllVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	result, err := suite.activity.GetActiveVolumesActivity(suite.ctx)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 2)
	assert.Equal(suite.T(), "vol1", result[0].Name)
	assert.Equal(suite.T(), "vol3", result[1].Name)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *CustomerAdoptionActivityUnitTestSuite) TestGetActiveVolumesActivity_Error() {
	suite.mockStorage.On("ListAllVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	result, err := suite.activity.GetActiveVolumesActivity(suite.ctx)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), result)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *CustomerAdoptionActivityUnitTestSuite) TestGetBackupDetailsActivity_Success() {
	timestamp := time.Now()

	// Sample backup data
	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			AccountIdentifier: "test-account",
			VolumeName:        "test-volume",
		},
		LatestLogicalBackupSize: 12345,
	}
	backups := []*datamodel.Backup{backup}

	// First call returns backups, second call returns empty slice to break loop
	suite.mockStorage.On("GetBackupMetrics", suite.ctx, mock.Anything, mock.Anything).Return(backups, nil).Once()
	suite.mockStorage.On("GetBackupMetrics", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Backup{}, nil).Once()

	result, err := suite.activity.GetBackupDetailsActivity(suite.ctx, timestamp)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 1, len(result.Details))
	assert.Equal(suite.T(), "test-volume", result.Details[0].VolName)
	assert.Equal(suite.T(), int64(12345), result.Details[0].Size)
	assert.Equal(suite.T(), "test-account", result.Details[0].AccountName)

	// Verify that the mock method was called
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *CustomerAdoptionActivityUnitTestSuite) TestGetBackupDetailsActivity_Error() {
	timestamp := time.Now()

	// Mock the GetBackupMetrics method to return an error
	suite.mockStorage.On("GetBackupMetrics", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError)

	result, err := suite.activity.GetBackupDetailsActivity(suite.ctx, timestamp)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), result)

	// Verify that the mock method was called
	suite.mockStorage.AssertExpectations(suite.T())
}
