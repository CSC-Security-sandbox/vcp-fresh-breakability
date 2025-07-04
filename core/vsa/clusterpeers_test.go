package vsa

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestCreateClusterPeer(t *testing.T) {
	createClusterPeerParams := CreateClusterPeerParams{
		PeerAddresses:      []string{"1.2.3.4"},
		PeerName:           "theName",
		ExpiryTime:         nil,
		IPSpace:            "ipspace",
		GeneratePassphrase: true,
	}
	t.Run("WhenProviderFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		returnedError := fmt.Errorf("some Error")

		expectedParams := ontaprest.ClusterPeerCreateParams{
			IPAddresses:        createClusterPeerParams.PeerAddresses,
			Name:               createClusterPeerParams.PeerName,
			GeneratePassphrase: true,
			ExpiryTime:         nil,
			IPSpace:            "ipspace",
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerCreate", expectedParams).Return(nil, returnedError)

		_, err := ontapProvider.CreateClusterPeer(createClusterPeerParams)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenClusterPeerCreateSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		expectedParams := ontaprest.ClusterPeerCreateParams{
			IPAddresses:        createClusterPeerParams.PeerAddresses,
			Name:               createClusterPeerParams.PeerName,
			GeneratePassphrase: true,
			ExpiryTime:         nil,
			IPSpace:            "ipspace",
		}
		b := "true"
		expectedClusterPeer := &ontaprest.ClusterPeerCreateResponse{
			GeneratedPassphrase: &b,
			ClusterPeerUUID:     "1234",
			ExpiryTime:          nil,
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerCreate", expectedParams).Return(expectedClusterPeer, nil)

		result, err := ontapProvider.CreateClusterPeer(createClusterPeerParams)
		assert.NoError(tt, err)
		assert.Equal(tt, "1234", result.ExternalUUID)
		assert.Equal(tt, createClusterPeerParams.PeerName, result.PeerClusterName)
		assert.Equal(tt, createClusterPeerParams.PeerAddresses, result.PeerAddresses)
	})
}

func TestAcceptClusterPeer(t *testing.T) {
	acceptClusterPeerParams := CreateClusterPeerParams{
		PeerAddresses:      []string{"1.2.3.4"},
		PeerName:           "theName",
		ExpiryTime:         nil,
		GeneratePassphrase: false,
	}
	t.Run("WhenProviderFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		returnedError := fmt.Errorf("some Error")

		expectedParams := ontaprest.ClusterPeerCreateParams{
			IPAddresses:        acceptClusterPeerParams.PeerAddresses,
			Name:               acceptClusterPeerParams.PeerName,
			GeneratePassphrase: false,
			ExpiryTime:         nil,
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerAccept", expectedParams).Return(nil, returnedError)

		_, err := ontapProvider.AcceptClusterPeer(acceptClusterPeerParams)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenGetOntapClientFuncFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClient Error")
		}
		ontapProvider := &OntapRestProvider{}

		_, err := ontapProvider.AcceptClusterPeer(acceptClusterPeerParams)
		assert.Error(tt, err)
		assert.Equal(tt, err.Error(), "getOntapClient Error")
	})
	t.Run("WhenClusterPeerCreateSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		expectedParams := ontaprest.ClusterPeerCreateParams{
			IPAddresses:        acceptClusterPeerParams.PeerAddresses,
			Name:               acceptClusterPeerParams.PeerName,
			GeneratePassphrase: false,
			ExpiryTime:         nil,
		}
		b := "true"
		expectedClusterPeer := &ontaprest.ClusterPeerCreateResponse{
			GeneratedPassphrase: &b,
			ClusterPeerUUID:     "1234",
			ExpiryTime:          nil,
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerAccept", expectedParams).Return(expectedClusterPeer, nil)

		result, err := ontapProvider.AcceptClusterPeer(acceptClusterPeerParams)
		assert.NoError(tt, err)
		assert.Equal(tt, "1234", result.ExternalUUID)
		assert.Equal(tt, acceptClusterPeerParams.PeerName, result.PeerClusterName)
		assert.Equal(tt, acceptClusterPeerParams.PeerAddresses, result.PeerAddresses)
	})
}

func TestDeleteClusterPeer(t *testing.T) {
	t.Run("WhenClusterPeerDeleteSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerDelete", "validClusterPeerID").Return(nil)

		err := ontapProvider.DeleteClusterPeer("validClusterPeerID")
		assert.NoError(tt, err)
	})
	t.Run("WhenClusterPeerDeleteFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		expectedError := fmt.Errorf("delete failed")
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerDelete", "invalidClusterPeerID").Return(expectedError)

		err := ontapProvider.DeleteClusterPeer("invalidClusterPeerID")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenGetOntapClientFuncFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}

		err := ontapProvider.DeleteClusterPeer("invalidClusterPeerID")
		assert.Error(tt, err)
		assert.Equal(tt, "getOntapClientFunc error", err.Error())
	})
}

func TestGetClusterPeer(t *testing.T) {
	t.Run("WhenGetClusterPeerSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		expectedPeer := &ontaprest.ClusterPeerResponse{
			UUID:                "1234",
			PeerClusterName:     "peerCluster",
			IPAddresses:         []string{"1.2.3.4"},
			Availability:        "available",
			AuthenticationState: "authenticated",
			ExpiryTime:          "2025-01-01T00:00:00Z",
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerGet", "validClusterPeerID").Return(expectedPeer, nil)

		result, err := ontapProvider.GetClusterPeer("validClusterPeerID")
		assert.NoError(tt, err)
		assert.Equal(tt, "1234", result.ExternalUUID)
		assert.Equal(tt, "peerCluster", result.PeerClusterName)
		assert.Equal(tt, []string{"1.2.3.4"}, result.PeerAddresses)
		assert.Equal(tt, "available", result.Availability)
		assert.Equal(tt, "authenticated", result.AuthenticationState)
		assert.NotNil(tt, result.ExpiryTime)
	})
	t.Run("WhenGetClusterPeerFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := fmt.Errorf("peer not found")
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeerGet", "invalidClusterPeerID").Return(nil, expectedError)

		result, err := ontapProvider.GetClusterPeer("invalidClusterPeerID")
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.GetClusterPeer("invalidClusterPeerID")
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "getOntapClientFunc error", err.Error())
	})
}

func TestListClusterPeer(t *testing.T) {
	t.Run("WhenListClusterPeersSucceeds", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		expectedPeers := []*ontaprest.ClusterPeerResponse{
			{
				UUID:                "1234",
				PeerClusterName:     "peerCluster1",
				IPAddresses:         []string{"1.2.3.4"},
				Availability:        "available",
				AuthenticationState: "authenticated",
				ExpiryTime:          "2025-01-01T00:00:00Z",
			},
			{
				UUID:                "5678",
				PeerClusterName:     "peerCluster2",
				IPAddresses:         []string{"5.6.7.8"},
				Availability:        "unavailable",
				AuthenticationState: "unauthenticated",
				ExpiryTime:          "",
			},
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeersList").Return(expectedPeers, nil)

		result, err := ontapProvider.ListClusterPeers()
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, "1234", result[0].ExternalUUID)
		assert.Equal(tt, "peerCluster1", result[0].PeerClusterName)
		assert.Equal(tt, []string{"1.2.3.4"}, result[0].PeerAddresses)
		assert.Equal(tt, "available", result[0].Availability)
		assert.Equal(tt, "authenticated", result[0].AuthenticationState)
		assert.NotNil(tt, result[0].ExpiryTime)
		assert.Equal(tt, "5678", result[1].ExternalUUID)
		assert.Equal(tt, "peerCluster2", result[1].PeerClusterName)
		assert.Equal(tt, []string{"5.6.7.8"}, result[1].PeerAddresses)
		assert.Equal(tt, "unavailable", result[1].Availability)
		assert.Equal(tt, "unauthenticated", result[1].AuthenticationState)
		assert.Nil(tt, result[1].ExpiryTime)
	})
	t.Run("WhenListClusterPeersFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := fmt.Errorf("failed to list cluster peers")
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeersList").Return(nil, expectedError)

		result, err := ontapProvider.ListClusterPeers()
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenListClusterPeersReturnsEmptyList", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ClusterPeersList").Return([]*ontaprest.ClusterPeerResponse{}, nil)

		result, err := ontapProvider.ListClusterPeers()
		assert.NoError(tt, err)
		assert.Empty(tt, result)
	})
	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.ListClusterPeers()
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, errors.New("getOntapClientFunc error"), err)
	})
}

func TestConvertToOntapTime(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		location := time.FixedZone("", 3*60*60)
		expTime := strfmt.DateTime(time.Now().In(location))
		expTimeStr := (*time.Time)(&expTime).Format(time.RFC3339)

		result := convertToOntapTime(&expTime)
		assert.Equal(tt, expTimeStr, *result)
	})
	t.Run("WhenSuccessfulNil", func(tt *testing.T) {
		result := convertToOntapTime(nil)
		assert.Nil(tt, result)
	})
}

func TestConvertFromOntapTime(t *testing.T) {
	t.Run("WhenParseError", func(tt *testing.T) {
		timeStr := "ILLEGAL TIME STRING"

		result := convertFromOntapTime(timeStr)
		assert.Nil(tt, result)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		timeStr := "2024-08-12T22:56:05.000Z"

		result := convertFromOntapTime(timeStr)
		assert.Equal(tt, timeStr, result.String())
	})
	t.Run("WhenSuccessfulNil", func(tt *testing.T) {
		result := convertFromOntapTime("")
		assert.Nil(tt, result)
	})
}
