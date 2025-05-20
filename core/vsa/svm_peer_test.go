package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetSvmPeer(t *testing.T) {
	localSVMName := "local-svm"
	remoteSVMName := "remote-svm"
	t.Run("WhenSvmPeerCollectionGetReturnsEmptyResponse", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		emptySvmPeerCollectionResponse := make([]*ontaprest.SvmPeer, 0)
		expectedError := errors.NewNotFoundErr("SVM peer not found", nil)
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(emptySvmPeerCollectionResponse, nil).Times(1)
		svmPeer, err := ontapProvider.GetSVMPeer(&localSVMName, &remoteSVMName)
		assert.Nil(tt, svmPeer)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSvmPeerCollectionGetReturnsEmptyResponse", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeer1 := &ontaprest.SvmPeer{
			SvmPeer: models.SvmPeer{
				UUID: nillable.ToPointer("uuid-1"),
			},
		}
		svmPeer2 := &ontaprest.SvmPeer{
			SvmPeer: models.SvmPeer{
				UUID: nillable.ToPointer("uuid-2"),
			},
		}
		resp := make([]*ontaprest.SvmPeer, 0)
		resp = append(resp, svmPeer1, svmPeer2)
		expectedError := errors.New("Multiple SVM peers found")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(resp, nil).Times(1)
		svmPeer, err := ontapProvider.GetSVMPeer(&localSVMName, &remoteSVMName)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		assert.Nil(tt, svmPeer)
	})
	t.Run("WhenSvmPeerUUIDIsEmpty", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					UUID: nillable.ToPointer(""),
				},
			},
		}
		expectedError := errors.New("SVM peer UUID is nil or empty")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		svmPeer, err := ontapProvider.GetSVMPeer(&localSVMName, &remoteSVMName)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		assert.Nil(tt, svmPeer)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		localSVMUUID := "local-svm-uuid"
		remoteSVMUUID := "remote-svm-uuid"
		clusterName := "cluster-name"
		peerUUID := "peer-uuid"
		application := models.SvmPeerApplications("snapmirror")
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		svmPeer, err := ontapProvider.GetSVMPeer(&localSVMName, &remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, svmPeer)
		assert.Equal(tt, *svmPeerCollectionResponse[0].UUID, svmPeer.UUID)
	})
}

func TestCreateSVMPeer(t *testing.T) {
	localSVMName := "dst-svm"
	peerSVMName := "src-svm"
	peerClusterName := "src-cluster"
	var snapmirrorApplication = models.SvmPeerApplicationsSnapmirror
	t.Run("WhenSvmPeerCollectionGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.createSVMPeer(localSVMName, peerSVMName, peerClusterName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)
		err := ontapProvider.createSVMPeer(localSVMName, peerSVMName, peerClusterName, snapmirrorApplication)
		assert.NoError(tt, err)
	})
}

func TestAcceptSvmPeer(t *testing.T) {
	svmPeerUUID := "peer-uuid"
	t.Run("WhenSvmPeerCollectionGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerModify", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.acceptSVMPeer(svmPeerUUID)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerModify", mock.Anything).Return(nil).Times(1)
		err := ontapProvider.acceptSVMPeer(svmPeerUUID)
		assert.NoError(tt, err)
	})
}

func TestDeleteSvmPeer(t *testing.T) {
	svmPeerUUID := "peer-uuid"
	t.Run("WhenSvmPeerCollectionGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerDelete", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.DeleteSVMPeer(svmPeerUUID)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerDelete", mock.Anything).Return(nil).Times(1)
		err := ontapProvider.DeleteSVMPeer(svmPeerUUID)
		assert.NoError(tt, err)
	})
}

func TestCreateSvmPeering(t *testing.T) {
	srcSvmName := "peer-svm"
	srcClusterName := "peer-cluster"
	dstSvmName := "local-svm"
	peerUUID := "peer-uuid"
	localSVMUUID := "local-svm-uuid"
	remoteSVMUUID := "remote-svm-uuid"
	localSVMName := "local-svm"
	remoteSVMName := "remote-svm"
	clusterName := "cluster-name"
	application := models.SvmPeerApplications("snapmirror")
	var snapmirrorApplication = models.SvmPeerApplicationsSnapmirror

	t.Run("WhenGetSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expectedError).Times(1)
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenCreateSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.NewNotFoundErr("not found", nil)
		expectedError1 := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expectedError).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(expectedError1).Times(1)
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError1, err)
	})
	t.Run("WhenSvmPeerStatusIsPeered", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.NoError(tt, err)
	})
	t.Run("WhenDeleteSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("Error deleting svm peer")
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateRejected),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerDelete", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenDeleteSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("Error deleting svm peer")
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateRejected),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerDelete", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenRecreatingSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateRejected),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerDelete", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(expectedError).Times(1)

		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenGetSvmPeerReturnsErrorAfterCreation", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("Error getting svm peer this time for some reason")
		expected := errors.NewNotFoundErr("not found", nil)
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expected).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expectedError).Times(1)
		mm.On("SvmPeerDelete", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)

		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSvmPeerDeleteReturnsErrorAfterCreation", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateRejected),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		expectedError := errors.New("Some error")
		expected := errors.NewNotFoundErr("not found", nil)
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expected).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerDelete", mock.Anything).Return(expectedError).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)

		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSvmPeerDeleteReturnsErrorAfterCreation", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateRejected),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		expectedError := errors.New("Error setting up peering infrastructure")
		expected := errors.NewNotFoundErr("not found", nil)
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expected).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerDelete", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)

		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenTimeoutDuringPeering", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		oldSvmPeerTimeoutMinutes := svmPeerTimeoutMinutes
		svmPeerTimeoutMinutes = 0
		defer func() {
			svmPeerTimeoutMinutes = oldSvmPeerTimeoutMinutes
		}()
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateInitializing),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		expectedError := errors.New("Timeout during peering infrastructure setup")

		cleanupError := errors.New("Error cleaning up peering infrastructure")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(2)
		mm.On("SvmPeerDelete", mock.Anything).Return(cleanupError).Times(1)
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
}

func TestAcceptSvmPeering(t *testing.T) {
	srcSvmName := "peer-svm"
	dstSvmName := "local-svm"
	peerUUID := "peer-uuid"
	localSVMUUID := "local-svm-uuid"
	remoteSVMUUID := "remote-svm-uuid"
	localSVMName := "local-svm"
	remoteSVMName := "remote-svm"
	clusterName := "cluster-name"
	application := models.SvmPeerApplications("snapmirror")
	t.Run("WhenGetSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}

		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expectedError).Times(1)
		err := ontapProvider.AcceptSvmPeering(srcSvmName, dstSvmName)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenTimeoutDuringPeering", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("Timeout during peering infrastructure setup")
		oldSvmPeerTimeoutMinutes := svmPeerTimeoutMinutes
		svmPeerTimeoutMinutes = 0
		defer func() { svmPeerTimeoutMinutes = oldSvmPeerTimeoutMinutes }()
		mockClient.On("SVM").Return(mm)
		err := ontapProvider.AcceptSvmPeering(srcSvmName, dstSvmName)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSvmPeerStateReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateRejected),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		expectedError := errors.New("Error setting up peering infrastructure")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		err := ontapProvider.AcceptSvmPeering(srcSvmName, dstSvmName)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSvmPeerAcceptReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePending),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		expectedError := errors.New("Error accepting SVM peer")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerModify", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.AcceptSvmPeering(srcSvmName, dstSvmName)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenStatusIsPeered", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		err := ontapProvider.AcceptSvmPeering(srcSvmName, dstSvmName)
		assert.NoError(tt, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		oldSvmPeerPollIntervalSeconds := svmPeerPollIntervalSeconds
		svmPeerPollIntervalSeconds = 0
		defer func() {
			svmPeerPollIntervalSeconds = oldSvmPeerPollIntervalSeconds
		}()
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStateInitiated),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		svmPeerCollectionResponse1 := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePending),
					UUID:  &peerUUID,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)},
					Peer: &models.SvmPeerInlinePeer{
						Svm:     &models.SvmPeerInlinePeerInlineSvm{UUID: nillable.ToPointer(remoteSVMUUID), Name: nillable.ToPointer(remoteSVMName)},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{Name: nillable.ToPointer(clusterName)},
					},
				},
			},
		}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse1, nil).Times(1)
		mm.On("SvmPeerModify", mock.Anything).Return(nil).Times(1)
		err := ontapProvider.AcceptSvmPeering(srcSvmName, dstSvmName)
		assert.NoError(tt, err)
	})
}
