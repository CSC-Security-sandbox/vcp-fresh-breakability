package backgroundactivities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
		{Name: "vol2", State: "available"},
		{Name: "vol3", State: "available"},
	}
	// First call returns vols, second call returns empty slice to break loop
	// GetEligibleVolumes already filters deleted_at IS NULL at DB level, so mock returns only eligible volumes
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(vols, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	// Activity emits metrics and returns nil on success
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ReturnsErrorOnListFailure() {
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.Error(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ContextCancellation() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Mock should not be called since context is cancelled
	err := suite.activity.GetEligibilityString(ctx)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), context.Canceled, err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_MultiplePaginationIterations() {
	// Test pagination with multiple pages (more than 2)
	page1 := []*datamodel.Volume{
		{Name: "vol1", State: "available"},
		{Name: "vol2", State: "available"},
	}
	page2 := []*datamodel.Volume{
		{Name: "vol3", State: "available"},
		{Name: "vol4", State: "available"},
	}
	page3 := []*datamodel.Volume{
		{Name: "vol5", State: "available"},
	}

	// First call returns page1, second returns page2, third returns page3, fourth returns empty to break
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page1, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page2, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page3, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_EmptyResultOnFirstCall() {
	// Test when first call returns empty (no volumes at all)
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ErrorOnSecondPaginationCall() {
	// Test error on second pagination call (not just first)
	page1 := []*datamodel.Volume{
		{Name: "vol1", State: "available"},
	}

	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page1, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.Error(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}
