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
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(vols, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ReturnsErrorOnListFailure() {
	// VCP fetch fails — expert mode fetch still runs, metrics emitted with nil VCP slice
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()
	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.Error(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ContextCancellation() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := suite.activity.GetEligibilityString(ctx)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), context.Canceled, err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_MultiplePaginationIterations() {
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

	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page1, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page2, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page3, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_EmptyResultOnFirstCall() {
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ErrorOnSecondPaginationCall() {
	// VCP second page fails — expert mode fetch still runs, error returned at the end
	page1 := []*datamodel.Volume{
		{Name: "vol1", State: "available"},
	}

	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(page1, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.Error(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ExpertModeVolumes() {
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	expertVols := []*datamodel.ExpertModeVolumes{
		{Name: "expert-vol1", State: "AVAILABLE"},
		{Name: "expert-vol2", State: "AVAILABLE"},
	}
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return(expertVols, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ExpertModeErrorOnFetch() {
	// Expert mode fetch fails — VCP data already collected, metrics emitted with nil expert mode slice, error returned
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return(nil, assert.AnError).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.Error(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ExpertModePagination() {
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	expertPage1 := []*datamodel.ExpertModeVolumes{
		{Name: "expert-vol1", State: "AVAILABLE"},
	}
	expertPage2 := []*datamodel.ExpertModeVolumes{
		{Name: "expert-vol2", State: "AVAILABLE"},
	}
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return(expertPage1, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return(expertPage2, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_ContextCancellationDuringExpertModeFetch() {
	ctx, cancel := context.WithCancel(context.Background())

	suite.mockStorage.On("GetEligibleVolumes", ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", ctx, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		cancel()
	}).Return([]*datamodel.ExpertModeVolumes{{Name: "vol1", State: "AVAILABLE"}}, nil).Once()

	suite.activity = &EligibilityStringActivity{SE: suite.mockStorage}

	err := suite.activity.GetEligibilityString(ctx)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), context.Canceled, err)
}

func (suite *EligibilityStringActivityUnitTestSuite) TestGetEligibilityString_BothVCPAndExpertModeVolumes() {
	vcpVols := []*datamodel.Volume{
		{Name: "vcp-vol1", State: "READY"},
	}
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return(vcpVols, nil).Once()
	suite.mockStorage.On("GetEligibleVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil).Once()

	expertVols := []*datamodel.ExpertModeVolumes{
		{Name: "expert-vol1", State: "AVAILABLE"},
	}
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return(expertVols, nil).Once()
	suite.mockStorage.On("GetEligibleExpertModeVolumes", suite.ctx, mock.Anything, mock.Anything).Return([]*datamodel.ExpertModeVolumes{}, nil).Once()

	err := suite.activity.GetEligibilityString(suite.ctx)
	assert.NoError(suite.T(), err)
	suite.mockStorage.AssertExpectations(suite.T())
}
