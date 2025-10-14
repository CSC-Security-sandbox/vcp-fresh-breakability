package backgroundactivities

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"testing"
)

type EligibilityStringActivityUnitTestSuite struct {
	suite.Suite
	mockStorage *database.MockStorage
	activity    *EligibilityStringActivity
	ctx         context.Context
}

func TestEligibilityStringActivityUnitTestSuite(t *testing.T) {
	suite.Run(t, new(EligibilityStringActivityUnitTestSuite))
}

func (suite *EligibilityStringActivityUnitTestSuite) SetupTest() {
	suite.mockStorage = database.NewMockStorage(suite.T())
	suite.activity = &EligibilityStringActivity{SE: suite.mockStorage}
	suite.ctx = context.TODO()
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ReturnsNonDeletedVolumes() {
	vols := []*datamodel.Volume{
		{Name: "vol1", State: "available"},
		{Name: "vol2", State: "deleted"},
		{Name: "vol3", State: "available"},
	}
	// First call returns vols, second call returns empty slice to break loop
	suite.mockStorage.On("ListAllVolumes", suite.ctx, mock.Anything, mock.Anything).Return(vols, nil).Once()
	suite.mockStorage.On("ListAllVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	result, err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 2)
	assert.Equal(suite.T(), "vol1", result[0].Name)
	assert.Equal(suite.T(), "vol3", result[1].Name)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ReturnsErrorOnListFailure() {
	suite.mockStorage.On("ListAllVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	result, err := suite.activity.GetEligibilityString(suite.ctx)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), result)
	suite.mockStorage.AssertExpectations(suite.T())
}
