package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestLunCreate_Success(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	lunName := "testLun"
	params := LunCreateParams{
		LunName:    lunName,
		SvmName:    "testSVM",
		OsType:     "linux",
		VolumeName: "testVolume",
		Size:       int64(1024),
	}

	mockLun := &ontaprest.Lun{
		Lun: models.Lun{
			Name:            nillable.ToPointer(lunName),
			UUID:            nillable.ToPointer("testUUID"),
			SerialNumberHex: nillable.ToPointer("6c5738423724595454686164"),
			OsType:          nillable.ToPointer("LINUX"),
			Space: &models.LunInlineSpace{
				Size: nillable.ToPointer(int64(1024)),
			},
		},
	}

	mockSAN.On("LunCreate", mock.Anything).Return(mockLun, nil)

	resp, err := rc.LunCreate(params)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, lunName, resp.Name)
	assert.Equal(t, "testUUID", resp.ExternalUUID)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestLunCreate_Error(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	lunName := "testLun"
	params := LunCreateParams{
		LunName:    lunName,
		SvmName:    "testSVM",
		OsType:     "linux",
		VolumeName: "testVolume",
		Size:       int64(1024),
	}

	mockSAN.On("LunCreate", mock.Anything).Return(nil, errors.New("creation error"))

	resp, err := rc.LunCreate(params)

	assert.Error(t, err)
	assert.Nil(t, resp)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestLunMapCreate_Success(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	params := LunMapCreateParams{
		LunName:    "testLun",
		SvmName:    "testSVM",
		IGroupName: []string{"iGroupName1", "iGroupName2"},
	}

	mockSAN.On("LunMapCreate", mock.Anything).Return(nil)

	err := rc.LunMapCreate(params)

	assert.NoError(t, err)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestLunMapCreate_Error(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	params := LunMapCreateParams{
		LunName:    "testLun",
		SvmName:    "testSVM",
		IGroupName: []string{"iGroupName1", "iGroupName2"},
	}

	mockSAN.On("LunMapCreate", mock.Anything).Return(errors.New("mapping error"))

	err := rc.LunMapCreate(params)

	assert.Error(t, err)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestLunMapCreate_ConflictError(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	params := LunMapCreateParams{
		LunName:    "testLun",
		SvmName:    "testSVM",
		IGroupName: []string{"iGroupName1", "iGroupName2"},
	}

	mockSAN.On("LunMapCreate", mock.Anything).Return(errors.New("LUN already mapped to this group"))

	err := rc.LunMapCreate(params)

	assert.NoError(t, err)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestLunMapCreate_OntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("OntapClientFunc error")
	}
	rc := &OntapRestProvider{}

	params := LunMapCreateParams{
		LunName:    "testLun",
		SvmName:    "testSVM",
		IGroupName: []string{"iGroupName1", "iGroupName2"},
	}

	err := rc.LunMapCreate(params)
	assert.Error(t, err)
	assert.Equal(t, "OntapClientFunc error", err.Error())
}

func TestIsAggregateOnline_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	aggregateName := "testAggregate"
	mockAggregate := &ontaprest.Aggregate{
		Aggregate: models.Aggregate{
			Name:  &aggregateName,
			State: nillable.ToPointer("online"),
		},
	}

	mockStorage.On("AggregateFindByName", mock.Anything).Return(mockAggregate, nil)

	isOnline, err := rc.IsAggregateOnline(aggregateName)

	assert.NoError(t, err)
	assert.True(t, isOnline)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestIsAggregateOnline_NotFound(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	aggregateName := "testAggregate"

	mockStorage.On("AggregateFindByName", mock.Anything).Return(nil, nil)

	isOnline, err := rc.IsAggregateOnline(aggregateName)

	assert.NoError(t, err)
	assert.False(t, isOnline)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestIsAggregateOnline_Error(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	aggregateName := "testAggregate"

	mockStorage.On("AggregateFindByName", mock.Anything).Return(nil, errors.New("API error"))

	isOnline, err := rc.IsAggregateOnline(aggregateName)

	assert.Error(t, err)
	assert.False(t, isOnline)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestIsAggregateOnline_OntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("OntapClientFunc error")
	}
	rc := &OntapRestProvider{}
	aggregateName := "testAggregate"
	aggregate, err := rc.IsAggregateOnline(aggregateName)

	assert.Error(t, err)
	assert.False(t, aggregate)
	assert.Equal(t, "OntapClientFunc error", err.Error())
}

func TestGetAggregateByName_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	aggregateName := "testAggregate"
	mockAggregate := &ontaprest.Aggregate{
		Aggregate: models.Aggregate{
			Name:  &aggregateName,
			State: nillable.ToPointer("online"),
		},
	}

	mockStorage.On("AggregateFindByName", mock.Anything).Return(mockAggregate, nil)

	aggregate, err := rc.GetAggregateByName(aggregateName)

	assert.NoError(t, err)
	assert.NotNil(t, aggregate)
	assert.Equal(t, aggregateName, aggregate.Name)
	assert.Equal(t, "online", aggregate.State)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetAggregateByName_NotFound(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	aggregateName := "testAggregate"

	mockStorage.On("AggregateFindByName", mock.Anything).Return(nil, nil)

	aggregate, err := rc.GetAggregateByName(aggregateName)

	assert.Error(t, err)
	assert.Nil(t, aggregate)
	assert.Equal(t, "aggregate not found", err.Error())

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetAggregateByName_Error(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	aggregateName := "testAggregate"

	mockStorage.On("AggregateFindByName", mock.Anything).Return(nil, errors.New("API error"))

	aggregate, err := rc.GetAggregateByName(aggregateName)

	assert.Error(t, err)
	assert.Nil(t, aggregate)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetAggregateByName_OntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("OntapClientFunc error")
	}
	rc := &OntapRestProvider{}
	aggregateName := "testAggregate"
	aggregate, err := rc.GetAggregateByName(aggregateName)

	assert.Error(t, err)
	assert.Nil(t, aggregate)
	assert.Equal(t, "OntapClientFunc error", err.Error())
}

func TestIscsiServiceCreate(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	t.Run("WhenIscsiServiceIsCreatedSuccessfully_ThenReturnNil", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IscsiServiceCreate", mock.Anything).Return(nil)

		err := rc.IscsiServiceCreate("testSvmUUID")

		assert.NoError(tt, err)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenIscsiServiceCreationFails_ThenReturnError", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("IscsiServiceCreate", mock.Anything).Return(errors.New("creation error"))

		err := rc.IscsiServiceCreate("testSvmUUID")

		assert.Error(tt, err)
		assert.Equal(tt, "creation error", err.Error())

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenOntapClientFunc_ThenReturnError", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		rc := &OntapRestProvider{}

		err := rc.IscsiServiceCreate("testSvmUUID")

		assert.Error(tt, err)
		assert.Equal(tt, "OntapClientFunc error", err.Error())
	})
}

func TestGetOntapClient(t *testing.T) {
	t.Run("WhenValidClientParamsProvided_ThenReturnOntapRestClient", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "192.168.1.0"
		clientParams := ontaprest.RESTClientParams{
			Host:     "192.168.1.0",
			Hosts:    hostMap,
			Password: "test-password",
			Trace:    log.NewLogger().(*log.Slogger),
		}
		orginalTestConnection := ontaprest.TestConnection
		defer func() {
			ontaprest.TestConnection = orginalTestConnection // Reset to original after test
		}()

		ontaprest.TestConnection = func(params *ontaprest.OntapRestClient) error {
			return nil
		}

		client, err := getOntapClient(clientParams)

		assert.NoError(tt, err)
		assert.NotNil(tt, client)
		assert.Equal(tt, clientParams.Host, client.Host())
	})
}

func TestLunGet(t *testing.T) {
	t.Run("WhenLunIsFound_ThenReturnLunResponse", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockLun := &ontaprest.Lun{
			Lun: models.Lun{
				Name:            nillable.ToPointer("testLun"),
				UUID:            nillable.ToPointer("uuid-123"),
				SerialNumberHex: nillable.ToPointer("6c5738423724595454686164"),
				OsType:          nillable.ToPointer("LINUX"),
				Space: &models.LunInlineSpace{
					Size: nillable.ToPointer(int64(1024)),
				},
			},
		}

		mockSAN.On("LunGet", mock.Anything).Return(mockLun, nil)

		params := LunGetParams{
			SvmName:    "testSVM",
			VolumeName: "testVol",
			LunName:    "testLun",
		}
		resp, err := rc.LunGet(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "testLun", resp.Name)
		assert.Equal(tt, "uuid-123", resp.ExternalUUID)
		assert.Equal(tt, "6c5738423724595454686164", resp.SerialNumber)

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenLunIsNotFound_ThenReturnError", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("LunGet", mock.Anything).Return(nil, nil)

		params := LunGetParams{
			SvmName:    "testSVM",
			VolumeName: "testVol",
			LunName:    "testLun",
		}
		resp, err := rc.LunGet(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), "lun not found")

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenLunGetReturnsError_ThenReturnError", func(tt *testing.T) {
		mockSAN := new(ontaprest.MockSANClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SAN").Return(mockSAN)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockSAN.On("LunGet", mock.Anything).Return(nil, errors.New("fetch error"))

		params := LunGetParams{
			SvmName:    "testSVM",
			VolumeName: "testVol",
			LunName:    "testLun",
		}
		resp, err := rc.LunGet(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, "fetch error", err.Error())

		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenLunGetReturnsError_OntapClientFuncError", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		rc := &OntapRestProvider{}

		params := LunGetParams{
			SvmName:    "testSVM",
			VolumeName: "testVol",
			LunName:    "testLun",
		}
		resp, err := rc.LunGet(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, "OntapClientFunc error", err.Error())
	})
}

func TestLunUpdate(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	params := LunUpdateParams{
		UUID:       "uuid-1",
		LunName:    "lun-1",
		SvmName:    "svm-1",
		VolumeName: "vol-1",
		Size:       int64(1024),
	}

	t.Run("WhenLunUpdateReturnsError", func(tt *testing.T) {
		mockSAN.On("LunUpdate", mock.Anything).Return(false, nil, errors.New("update error")).Once()
		err := rc.LunUpdate(params)
		assert.Error(tt, err)
		assert.Equal(tt, "update error", err.Error())
		mockSAN.AssertExpectations(tt)
	})

	t.Run("WhenLunUpdateSuccessTrue", func(tt *testing.T) {
		mockSAN.On("LunUpdate", mock.Anything).Return(true, nil, nil).Once()
		err := rc.LunUpdate(params)
		assert.NoError(tt, err)
		mockSAN.AssertExpectations(tt)
	})

	t.Run("WhenLunUpdateSuccessFalseAndPollSucceeds", func(tt *testing.T) {
		mockSAN.On("LunUpdate", mock.Anything).Return(false, &ontaprest.JobAccepted{JobUUID: "job-1"}, nil).Once()
		mockClient.On("Poll", "job-1").Return(nil).Once()
		err := rc.LunUpdate(params)
		assert.NoError(tt, err)
		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenLunUpdateSuccessFalseAndPollFails", func(tt *testing.T) {
		mockSAN.On("LunUpdate", mock.Anything).Return(false, &ontaprest.JobAccepted{JobUUID: "job-2"}, nil).Once()
		mockClient.On("Poll", "job-2").Return(errors.New("poll error")).Once()
		err := rc.LunUpdate(params)
		assert.Error(tt, err)
		assert.Equal(tt, "poll error", err.Error())
		mockSAN.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenLunUpdateReturnsConflictError", func(tt *testing.T) {
		mockSAN.On("LunUpdate", mock.Anything).Return(false, nil, errors.New("New LUN size is the same as the old LUN size")).Once()
		err := rc.LunUpdate(params)
		assert.Error(tt, err)
		assert.True(tt, errors.IsConflictErr(err))
		assert.Contains(tt, err.Error(), "already has the specified size")
		mockSAN.AssertExpectations(tt)
	})

	t.Run("WhenLunUpdateReturnsOntapClientFuncError", func(tt *testing.T) {
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("OntapClientFunc error")
		}
		err := rc.LunUpdate(params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "OntapClientFunc error")
	})
}

func TestSnapshotGet(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	snapshotUUID := "testSnapshotUUID"
	volumeUUID := "testVolumeUUID"
	snapshotName := "testSnapshot"
	mockSnapshot := &ontaprest.Snapshot{Snapshot: models.Snapshot{Name: &snapshotName, UUID: &snapshotUUID}}

	mockStorage.On("SnapshotGet", mock.Anything).Return(mockSnapshot, nil)

	snapshot, err := rc.SnapshotGet(snapshotUUID, volumeUUID, snapshotName)

	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshotName, *snapshot.Name)
	assert.Equal(t, snapshotUUID, *snapshot.UUID)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestSnapshotGet_OntapClientFuncErr(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("OntapClientFunc error")
	}
	rc := &OntapRestProvider{}
	snapshot, err := rc.SnapshotGet("testSnapshotUUID", "testVolumeUUID", "testSnapshot")

	assert.Error(t, err)
	assert.Nil(t, snapshot)
	assert.Contains(t, err.Error(), "OntapClientFunc error")
}
