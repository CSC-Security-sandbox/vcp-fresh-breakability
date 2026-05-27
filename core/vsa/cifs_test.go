package vsa

import (
	stdErrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetCIFSService_Success(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	expectedCifs := &ontapRest.CifsService{
		CifsService: ontapModels.CifsService{
			Name: nillable.ToPointer("CIFS-SERVER"),
			AdDomain: &ontapModels.AdDomain{
				Fqdn: nillable.ToPointer("example.com"),
			},
		},
	}

	mockNAS.On("CifsServiceGet", mock.MatchedBy(func(params *ontapRest.CifsServiceGetParams) bool {
		return params.SvmUUID != nil && *params.SvmUUID == "svm-uuid-123" &&
			params.SvmName != nil && *params.SvmName == "svm-name" &&
			assert.ElementsMatch(t, []string{"ad_domain", "name"}, params.BaseParams.Fields)
	})).Return(expectedCifs, nil).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		result, err := provider.GetCIFSService("svm-name", "svm-uuid-123")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "CIFS-SERVER", *result.Name)
		assert.Equal(t, "example.com", *result.AdDomain.Fqdn)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestGetCIFSService_ClientCreationError(t *testing.T) {
	provider := newTestProvider()

	expectedErr := stdErrors.New("connection failed")
	withMockOntapClient(t, nil, expectedErr, func() {
		result, err := provider.GetCIFSService("svm-name", "svm-uuid-123")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get ONTAP client")
	})
}

func TestGetCIFSService_CifsServiceGetError(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	expectedErr := stdErrors.New("service not found")
	mockNAS.On("CifsServiceGet", mock.Anything).Return((*ontapRest.CifsService)(nil), expectedErr).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		result, err := provider.GetCIFSService("svm-name", "svm-uuid-123")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, expectedErr)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestGetCIFSService_NotFoundError(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	notFoundErr := vsaerrors.New("cifs service")
	mockNAS.On("CifsServiceGet", mock.Anything).Return((*ontapRest.CifsService)(nil), notFoundErr).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		result, err := provider.GetCIFSService("svm-name", "svm-uuid-123")
		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, notFoundErr)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestGetCIFSService_EmptyResponse(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	emptyCifs := &ontapRest.CifsService{}
	mockNAS.On("CifsServiceGet", mock.Anything).Return(emptyCifs, nil).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		result, err := provider.GetCIFSService("svm-name", "svm-uuid-123")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Nil(t, result.Name)
		assert.Nil(t, result.AdDomain)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}
