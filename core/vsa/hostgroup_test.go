package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestIGroupCreate_Success(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}
	iGroupName := "testIGroup"
	params := IgroupCreateParams{
		IgroupName: iGroupName,
		SvmName:    "testSVM",
		OsType:     "linux",
		Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
	}

	mockSAN.On("IGroupCreate", mock.Anything).Return(iGroupName, nil)

	resp, err := rc.IgroupCreate(params)

	assert.NoError(t, err)
	assert.Equal(t, iGroupName, resp)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestIGroupCreate_Error(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	iGroupName := "testIGroup"
	params := IgroupCreateParams{
		IgroupName: iGroupName,
		SvmName:    "testSVM",
		OsType:     "linux",
		Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
	}

	mockSAN.On("IGroupCreate", mock.Anything).Return("", errors.New("creation error"))

	resp, err := rc.IgroupCreate(params)

	assert.Error(t, err)
	assert.Empty(t, resp)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestIgroupGet(t *testing.T) {
	t.Run("WhenIgroupExists_ThenReturnIgroupDetails", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockIgroup := &ontaprest.Igroup{
			Igroup: models.Igroup{
				Name: nillable.ToPointer("testIgroup"),
				UUID: nillable.ToPointer("testUUID"),
			},
		}

		mockSAN.On("IGroupGet", mock.Anything).Return(mockIgroup, nil)

		igroup, err := rc.IgroupGet(nillable.GetStringPtr("testIgroup"), nillable.GetStringPtr("testSVM"))

		assert.NoError(tt, err)
		assert.NotNil(tt, igroup)
		assert.Equal(tt, "testIgroup", *igroup.Name)
		assert.Equal(tt, "testUUID", *igroup.UUID)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenIgroupDoesNotExist_ThenReturnNil", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IGroupGet", mock.Anything).Return(nil, nil)

		igroup, err := rc.IgroupGet(nillable.GetStringPtr("testIgroup"), nillable.GetStringPtr("testSVM"))

		assert.NoError(tt, err)
		assert.Nil(tt, igroup)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenFetchingIgroupFails_ThenReturnError", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IGroupGet", mock.Anything).Return(nil, errors.New("fetch error"))

		igroup, err := rc.IgroupGet(nillable.GetStringPtr("testIgroup"), nillable.GetStringPtr("testSVM"))

		assert.Error(tt, err)
		assert.Nil(tt, igroup)
		assert.Equal(tt, "fetch error", err.Error())

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestIgroupExists(t *testing.T) {
	t.Run("WhenIgroupExists_ThenReturnTrue", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockIgroup := &ontaprest.Igroup{
			Igroup: models.Igroup{
				Name: nillable.ToPointer("testIgroup"),
			},
		}

		mockSAN.On("IGroupGet", mock.Anything).Return(mockIgroup, nil)

		exists, igroup, err := rc.IgroupExists("testIgroup", nillable.GetStringPtr("testSVM"))

		assert.NoError(tt, err)
		assert.True(tt, exists)
		assert.Equal(tt, igroup.Name, nillable.GetStringPtr("testIgroup"))

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenIgroupDoesNotExist_ThenReturnFalse", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IGroupGet", mock.Anything).Return(nil, nil)

		exists, igroup, err := rc.IgroupExists("testIgroup", nillable.GetStringPtr("testSVM"))

		assert.NoError(tt, err)
		assert.False(tt, exists)
		assert.Nil(tt, igroup)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenFetchingIgroupFailsWithNotFoundError_ThenReturnFalse", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IGroupGet", mock.Anything).Return(nil, errors.NewNotFoundErr("Igroup", nil))

		exists, igroup, err := rc.IgroupExists("testIgroup", nillable.GetStringPtr("testSVM"))

		assert.NoError(tt, err)
		assert.False(tt, exists)
		assert.Nil(tt, igroup)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenFetchingIgroupFailsWithOtherError_ThenReturnError", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IGroupGet", mock.Anything).Return(nil, errors.New("Igroup not found"))

		exists, igroup, err := rc.IgroupExists("testIgroup", nillable.GetStringPtr("testSVM"))

		assert.Error(tt, err)
		assert.False(tt, exists)
		assert.Equal(tt, "An internal error occurred.", err.Error())
		assert.Nil(tt, igroup)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.OriginalErr, "Igroup not found")
		}
		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestIgroupAddInitiator(t *testing.T) {
	t.Run("WhenIgroupAddInitiatorSuccess", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		params := IgroupAddInitiator{
			Initiator: []string{"iqn.1993-08.org.debian:01:123456789"},
		}
		mockSAN.On("IGroupAddInitiator", mock.Anything).Return(nil)

		err := rc.IgroupAddInitiator(params)

		assert.NoError(tt, err)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenIgroupAddInitiatorFails", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		params := IgroupAddInitiator{
			Initiator: []string{"iqn.1993-08.org.debian:01:123456789"},
		}
		mockSAN.On("IGroupAddInitiator", mock.Anything).Return(errors.New("dummy error"))

		err := rc.IgroupAddInitiator(params)

		assert.EqualError(tt, err, "dummy error")

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenIgroupAddInitiatorFailsWithAlready", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		params := IgroupAddInitiator{
			Initiator: []string{"iqn.1993-08.org.debian:01:123456789"},
		}
		mockSAN.On("IGroupAddInitiator", mock.Anything).Return(errors.New("already contains initiator"))

		err := rc.IgroupAddInitiator(params)

		assert.NoError(tt, err)
		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestIgroupDeleteInitiator(t *testing.T) {
	t.Run("WhenIgroupDeleteInitiatorSuccess", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		params := IgroupDeleteInitiator{
			InitiatorName: "sfd",
			IgroupUUID:    "abcd",
		}
		mockSAN.On("IGroupDeleteInitiator", mock.Anything).Return(nil)

		err := rc.IgroupDeleteInitiator(params)

		assert.NoError(tt, err)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenIgroupDeleteInitiatorFails", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		params := IgroupDeleteInitiator{
			InitiatorName: "sfd",
			IgroupUUID:    "abcd",
		}
		mockSAN.On("IGroupDeleteInitiator", mock.Anything).Return(errors.New("dummy error"))

		err := rc.IgroupDeleteInitiator(params)

		assert.EqualError(tt, err, "dummy error")

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
	t.Run("WhenIgroupDeleteInitiatorFailsWithAlready", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		params := IgroupDeleteInitiator{
			InitiatorName: "sfd",
			IgroupUUID:    "abcd",
		}
		mockSAN.On("IGroupDeleteInitiator", mock.Anything).Return(errors.New("does not contain initiator"))

		err := rc.IgroupDeleteInitiator(params)

		assert.NoError(tt, err)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestIgroupDelete(t *testing.T) {
	t.Run("WhenIgroupDeleteSucceeds", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		uuid := "test-igroup-uuid"
		mockSAN.On("IGroupDelete", mock.Anything).Return(nil)

		err := rc.IgroupDelete(uuid)

		assert.NoError(tt, err)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenIgroupDeleteFails", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		uuid := "test-igroup-uuid"
		expectedError := errors.New("delete failed")
		mockSAN.On("IGroupDelete", mock.Anything).Return(expectedError)

		err := rc.IgroupDelete(uuid)

		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFuncFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		rc := &OntapRestProvider{}

		uuid := "test-igroup-uuid"

		err := rc.IgroupDelete(uuid)

		assert.Error(tt, err)
		assert.Equal(tt, "getOntapClientFunc error", err.Error())
	})
}
