package vsa

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestAreAllNodeUpAndRunning(t *testing.T) {
	t.Run("WhenAllNodesAreUp_ThenReturnTrue", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}
		uuid1 := strfmt.UUID("12345678-1234-5678-1234-567812345678")
		uuid2 := strfmt.UUID("12345678-1234-5678-1234-567812345678")
		mockNodes := []*ontaprest.Node{
			{
				NodeResponseInlineRecordsInlineArrayItem: models.NodeResponseInlineRecordsInlineArrayItem{
					Name:  nillable.ToPointer("node2"),
					State: nillable.ToPointer("up"),
					UUID:  &uuid1,
				},
			},
			{
				NodeResponseInlineRecordsInlineArrayItem: models.NodeResponseInlineRecordsInlineArrayItem{
					Name:  nillable.ToPointer("node2"),
					State: nillable.ToPointer("up"),
					UUID:  &uuid2,
				},
			},
		}
		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.Node])
			_ = callback(mockNodes)
		}).Return(nil)

		result, err := rc.AreAllNodeUpAndRunning()

		assert.NoError(tt, err)
		assert.True(tt, result)

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenFetchingNodesFails_ThenReturnError", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}

		rc := &OntapRestProvider{
			Logger: log.NewLogger().(*log.Slogger),
		}

		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Return(errors.New("fetch error"))

		result, err := rc.AreAllNodeUpAndRunning()

		assert.Error(tt, err)
		assert.False(tt, result)
		assert.Equal(tt, "fetch error", err.Error())

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenAnyNodeIsNotUp_ThenReturnError", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		uuid1 := strfmt.UUID("12345678-1234-5678-1234-567812345678")
		mockNodes := []*ontaprest.Node{
			{
				NodeResponseInlineRecordsInlineArrayItem: models.NodeResponseInlineRecordsInlineArrayItem{
					Name:  nillable.ToPointer("node1"),
					State: nillable.ToPointer("down"), // Simulate a node not being "up"
					UUID:  &uuid1,
				},
			},
			{
				NodeResponseInlineRecordsInlineArrayItem: models.NodeResponseInlineRecordsInlineArrayItem{
					Name:  nillable.ToPointer("node2"),
					State: nillable.ToPointer("down"), // Simulate a node not being "up"
					UUID:  &uuid1,
				},
			},
		}

		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.Node])
			_ = callback(mockNodes)
		}).Return(nil)

		result, err := rc.AreAllNodeUpAndRunning()

		assert.Error(tt, err)
		assert.False(tt, result)
		assert.Equal(tt, "node node1 is not up", err.Error())

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenNodeCountIsLessThanExpected_ThenReturnError", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		uuid1 := strfmt.UUID("12345678-1234-5678-1234-567812345678")
		mockNodes := []*ontaprest.Node{
			{
				NodeResponseInlineRecordsInlineArrayItem: models.NodeResponseInlineRecordsInlineArrayItem{
					Name:  nillable.ToPointer("node1"),
					State: nillable.ToPointer("up"),
					UUID:  &uuid1,
				},
			},
		}

		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.Node])
			_ = callback(mockNodes)
		}).Return(nil)

		result, err := rc.AreAllNodeUpAndRunning()

		assert.Error(tt, err)
		assert.False(tt, result)
		assert.Equal(tt, "expected 2 nodes, got 1", err.Error())

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestGetNodeByName(t *testing.T) {
	t.Run("WhenNodeExists_ThenReturnNodeDetails", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		uuid := strfmt.UUID("12345678-1234-5678-1234-567812345678")
		mockNodes := []*ontaprest.Node{
			{
				NodeResponseInlineRecordsInlineArrayItem: models.NodeResponseInlineRecordsInlineArrayItem{
					Name: nillable.ToPointer("node1"),
					UUID: &uuid,
				},
			},
		}

		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.Node])
			_ = callback(mockNodes)
		}).Return(nil)

		node, err := rc.GetNodeByName("node1")

		assert.NoError(tt, err)
		assert.NotNil(tt, node)
		assert.Equal(tt, "node1", node.Name)
		assert.Equal(tt, uuid.String(), node.ExternalUUID)

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenNodeDoesNotExist_ThenReturnError", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.Node])
			_ = callback([]*ontaprest.Node{})
		}).Return(nil)

		node, err := rc.GetNodeByName("node1")

		assert.Error(tt, err)
		assert.Nil(tt, node)
		assert.Equal(tt, "node with name node1 not found", err.Error())

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenFetchingNodesFails_ThenReturnError", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockCluster.On("NodesGet", mock.Anything, mock.Anything).Return(errors.New("fetch error"))

		node, err := rc.GetNodeByName("node1")

		assert.Error(tt, err)
		assert.Nil(tt, node)
		assert.Equal(tt, "fetch error", err.Error())

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestGetONTAPVersion(t *testing.T) {
	t.Run("WhenVersionIsRetrievedSuccessfully_ThenReturnVersion", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		version := "9.10.1"
		mockCluster.On("GetONTAPVersion").Return(&version, nil)

		result, err := rc.GetONTAPVersion()

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, version, *result)

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenVersionRetrievalFails_ThenReturnError", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockCluster.On("GetONTAPVersion").Return(nil, errors.New("version fetch error"))

		result, err := rc.GetONTAPVersion()

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, "version fetch error", err.Error())

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenVersionIsNil_ThenReturnNil", func(tt *testing.T) {
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockCluster.On("GetONTAPVersion").Return(nil, nil)

		result, err := rc.GetONTAPVersion()

		assert.NoError(tt, err)
		assert.Nil(tt, result)

		mockCluster.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}
