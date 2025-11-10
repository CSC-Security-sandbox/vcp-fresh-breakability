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

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		emptySvmPeerCollectionResponse := make([]*ontaprest.SvmPeer, 0)
		expectedError := errors.NewNotFoundErr("SVM peer", nil)
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

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
	t.Run("OntapClientFuncError", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFuncError")
		}
		ontapProvider := &OntapRestProvider{}
		svmPeer, err := ontapProvider.GetSVMPeer(&localSVMName, &remoteSVMName)
		assert.Error(tt, err)
		assert.Nil(tt, svmPeer)
		assert.Equal(tt, errors.New("OntapClientFuncError"), err)
	})
}

func TestAcceptSvmPeer(t *testing.T) {
	svmPeerUUID := "peer-uuid"
	t.Run("WhenSvmPeerCollectionGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerModify", mock.Anything).Return(nil).Times(1)
		err := ontapProvider.acceptSVMPeer(svmPeerUUID)
		assert.NoError(tt, err)
	})
	t.Run("OntapClientFuncError", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}
		err := ontapProvider.acceptSVMPeer(svmPeerUUID)
		assert.Error(tt, err)
		assert.Equal(tt, errors.New("OntapClientFunc error"), err)
	})
}

func TestDeleteSvmPeer(t *testing.T) {
	svmPeerUUID := "peer-uuid"
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	t.Run("WhenSvmPeerCollectionGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerDelete", mock.Anything).Return(expectedError).Times(1)
		err := ontapProvider.DeleteSVMPeer(svmPeerUUID, false)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerDelete", mock.Anything).Return(nil).Times(1)
		err := ontapProvider.DeleteSVMPeer(svmPeerUUID, false)
		assert.NoError(tt, err)
	})
	t.Run("OntapClientFuncError", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}
		err := ontapProvider.DeleteSVMPeer(svmPeerUUID, false)
		assert.Error(tt, err)
		assert.Equal(tt, errors.New("OntapClientFunc error"), err)
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

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenGetSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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

		expectedError := errors.New("Error getting svm peer this time for some reason")
		expected := errors.NewNotFoundErr("not found", nil)
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expected).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(2)
		mm.On("SvmPeerDelete", mock.Anything).Return(expectedError).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)

		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSvmPeerDeleteReturnsErrorAfterCreation", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(2)
		mm.On("SvmPeerDelete", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)

		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenTimeoutDuringPeering", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
	t.Run("OntapClientFuncError", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}
		err := ontapProvider.CreateSvmPeering(srcClusterName, srcSvmName, dstSvmName, snapmirrorApplication)
		assert.Error(tt, err)
		assert.Equal(tt, errors.New("OntapClientFunc error"), err)
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

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	t.Run("WhenGetSvmPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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

func TestCreateSVMPeer(t *testing.T) {
	localSVMName := "local-svm"
	peerSVMName := "peer-svm"
	peerClusterName := "peer-cluster"
	localSVMUUID := "local-svm-uuid"
	peerUUID := "peer-uuid"
	application := models.SvmPeerApplications("snapmirror")

	params := CreateSVMPeerParams{
		LocalSVMName:    localSVMName,
		PeerSVMName:     peerSVMName,
		PeerClusterName: peerClusterName,
		Applications:    []models.SvmPeerApplications{application},
	}

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("OntapClientFuncError", func(tt *testing.T) {
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		provider := &OntapRestProvider{}
		resp, err := provider.CreateSVMPeer(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, errors.New("OntapClientFunc error"), err)
	})

	t.Run("WhenSvmPeerCreateReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) { return mockClient, nil }
		provider := &OntapRestProvider{}
		expectedError := errors.New("create error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(expectedError).Times(1)
		resp, err := provider.CreateSVMPeer(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("WhenGetSVMPeerReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) { return mockClient, nil }
		provider := &OntapRestProvider{}
		expectedError := errors.New("some error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expectedError).Times(1)
		resp, err := provider.CreateSVMPeer(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("WhenGetSVMPeerReturnsNotFound", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) { return mockClient, nil }
		provider := &OntapRestProvider{}
		expectedError := errors.NewNotFoundErr("SVM peer", nil)
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return([]*ontaprest.SvmPeer{}, nil).Times(1)
		resp, err := provider.CreateSVMPeer(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("WhenGetSVMPeerReturnsMultiple", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) { return mockClient, nil }
		provider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)
		peer1 := &ontaprest.SvmPeer{SvmPeer: models.SvmPeer{UUID: nillable.ToPointer("uuid-1")}}
		peer2 := &ontaprest.SvmPeer{SvmPeer: models.SvmPeer{UUID: nillable.ToPointer("uuid-2")}}
		mm.On("SvmPeerCollectionGet", mock.Anything).Return([]*ontaprest.SvmPeer{peer1, peer2}, nil).Times(1)
		resp, err := provider.CreateSVMPeer(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, errors.New("Multiple SVM peers found"), err)
	})

	t.Run("WhenGetSVMPeerReturnsEmptyUUID", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) { return mockClient, nil }
		provider := &OntapRestProvider{}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)
		peer := &ontaprest.SvmPeer{SvmPeer: models.SvmPeer{UUID: nillable.ToPointer("")}}
		mm.On("SvmPeerCollectionGet", mock.Anything).Return([]*ontaprest.SvmPeer{peer}, nil).Times(1)
		resp, err := provider.CreateSVMPeer(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, errors.New("SVM peer UUID is nil or empty"), err)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)
		getOntapClientFunc = func(p ontaprest.RESTClientParams) (ontaprest.RESTClient, error) { return mockClient, nil }
		provider := &OntapRestProvider{}
		peer := &ontaprest.SvmPeer{SvmPeer: models.SvmPeer{State: nillable.ToPointer(models.SvmPeerStatePeered), UUID: nillable.ToPointer(peerUUID), SvmPeerInlineApplications: []*models.SvmPeerApplications{&application}, Svm: &models.SvmPeerInlineSvm{UUID: nillable.ToPointer(localSVMUUID), Name: nillable.ToPointer(localSVMName)}}}
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCreate", mock.Anything).Return(nil).Times(1)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return([]*ontaprest.SvmPeer{peer}, nil).Times(1)
		resp, err := provider.CreateSVMPeer(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, peerUUID, resp.UUID)
	})
}

func TestListSVMPeers(t *testing.T) {
	remoteSVMName := "remote-svm"
	localSVMName := "local-svm"
	localSVMUUID := "local-svm-uuid"
	peerSVMName := "peer-svm"
	peerSVMUUID := "peer-svm-uuid"
	peerClusterName := "peer-cluster"
	peerUUID1 := "peer-uuid-1"
	peerUUID2 := "peer-uuid-2"
	application := models.SvmPeerApplications("snapmirror")

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	t.Run("WhenSuccessfulWithMultiplePeers", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID1,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: &models.SvmPeerInlinePeerInlineSvm{
							UUID: nillable.ToPointer(peerSVMUUID),
							Name: nillable.ToPointer(peerSVMName),
						},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{
							Name: nillable.ToPointer(peerClusterName),
						},
					},
				},
			},
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePending),
					UUID:  &peerUUID2,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: &models.SvmPeerInlinePeerInlineSvm{
							UUID: nillable.ToPointer(peerSVMUUID),
							Name: nillable.ToPointer(peerSVMName),
						},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{
							Name: nillable.ToPointer(peerClusterName),
						},
					},
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 2)
		assert.Equal(tt, peerUUID1, result[0].UUID)
		assert.Equal(tt, "peered", result[0].State)
		assert.Equal(tt, localSVMName, result[0].LocalSvmName)
		assert.Equal(tt, localSVMUUID, result[0].LocalSvmUUID)
		assert.Equal(tt, peerSVMName, result[0].PeerSvmName)
		assert.Equal(tt, peerSVMUUID, result[0].PeerSvmUUID)
		assert.Equal(tt, peerClusterName, result[0].PeerClusterName)
		assert.Len(tt, result[0].Applications, 1)
		assert.Equal(tt, "snapmirror", result[0].Applications[0])

		assert.Equal(tt, peerUUID2, result[1].UUID)
		assert.Equal(tt, "pending", result[1].State)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenSuccessfulWithEmptyList", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.Empty(tt, result)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenSuccessfulWithNilRemoteSVMName", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID1,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: &models.SvmPeerInlinePeerInlineSvm{
							UUID: nillable.ToPointer(peerSVMUUID),
							Name: nillable.ToPointer(peerSVMName),
						},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{
							Name: nillable.ToPointer(peerClusterName),
						},
					},
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenPeersWithInvalidUUIDAreSkipped", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		emptyUUID := ""
		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &emptyUUID, // Empty UUID should be skipped
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
				},
			},
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  nil, // Nil UUID should be skipped
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
				},
			},
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID1, // Valid UUID should be included
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: &models.SvmPeerInlinePeerInlineSvm{
							UUID: nillable.ToPointer(peerSVMUUID),
							Name: nillable.ToPointer(peerSVMName),
						},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{
							Name: nillable.ToPointer(peerClusterName),
						},
					},
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1) // Only one valid peer should be returned
		assert.Equal(tt, peerUUID1, result[0].UUID)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenPeerInfoIsNil", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID1,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: nil, // Peer is nil
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, peerUUID1, result[0].UUID)
		assert.Equal(tt, "", result[0].PeerSvmName)
		assert.Equal(tt, "", result[0].PeerSvmUUID)
		assert.Equal(tt, "", result[0].PeerClusterName)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenPeerSvmIsNil", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID1,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: nil, // Peer.Svm is nil
						Cluster: &models.SvmPeerInlinePeerInlineCluster{
							Name: nillable.ToPointer(peerClusterName),
						},
					},
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, peerUUID1, result[0].UUID)
		assert.Equal(tt, "", result[0].PeerSvmName)
		assert.Equal(tt, "", result[0].PeerSvmUUID)
		assert.Equal(tt, peerClusterName, result[0].PeerClusterName)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenPeerClusterIsNil", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State: nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:  &peerUUID1,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{
						&application,
					},
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: &models.SvmPeerInlinePeerInlineSvm{
							UUID: nillable.ToPointer(peerSVMUUID),
							Name: nillable.ToPointer(peerSVMName),
						},
						Cluster: nil, // Peer.Cluster is nil
					},
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, peerUUID1, result[0].UUID)
		assert.Equal(tt, peerSVMName, result[0].PeerSvmName)
		assert.Equal(tt, peerSVMUUID, result[0].PeerSvmUUID)
		assert.Equal(tt, "", result[0].PeerClusterName)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenSvmPeerCollectionGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		expectedError := errors.New("SvmPeerCollectionGet error")
		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(nil, expectedError).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, expectedError, err)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFuncFails", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Equal(tt, errors.New("getOntapClientFunc error"), err)
	})

	t.Run("WhenSuccessfulWithNoApplications", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}

		svmPeerCollectionResponse := []*ontaprest.SvmPeer{
			{
				SvmPeer: models.SvmPeer{
					State:                     nillable.ToPointer(models.SvmPeerStatePeered),
					UUID:                      &peerUUID1,
					SvmPeerInlineApplications: []*models.SvmPeerApplications{}, // No applications
					Svm: &models.SvmPeerInlineSvm{
						UUID: nillable.ToPointer(localSVMUUID),
						Name: nillable.ToPointer(localSVMName),
					},
					Peer: &models.SvmPeerInlinePeer{
						Svm: &models.SvmPeerInlinePeerInlineSvm{
							UUID: nillable.ToPointer(peerSVMUUID),
							Name: nillable.ToPointer(peerSVMName),
						},
						Cluster: &models.SvmPeerInlinePeerInlineCluster{
							Name: nillable.ToPointer(peerClusterName),
						},
					},
				},
			},
		}

		mockClient.On("SVM").Return(mm)
		mm.On("SvmPeerCollectionGet", mock.Anything).Return(svmPeerCollectionResponse, nil).Times(1)

		result, err := ontapProvider.ListSVMPeersByRemoteSVMName(&remoteSVMName)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, peerUUID1, result[0].UUID)
		assert.Len(tt, result[0].Applications, 0)
		mockClient.AssertExpectations(tt)
		mm.AssertExpectations(tt)
	})
}
