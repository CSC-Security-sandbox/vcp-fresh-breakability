package backgroundactivities

import (
	"context"
	"testing"

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
