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

func TestLunCreate_Success(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)

	rc := &OntapRestProvider{client: mockClient}

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
			Name: nillable.ToPointer(lunName),
			UUID: nillable.ToPointer("testUUID"),
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

	rc := &OntapRestProvider{client: mockClient}

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

func TestIGroupCreate_Success(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)

	rc := &OntapRestProvider{client: mockClient}

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

	rc := &OntapRestProvider{client: mockClient}

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

func TestLunMapCreate_Success(t *testing.T) {
	mockSAN := new(ontaprest.MockSANClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("SAN").Return(mockSAN)

	rc := &OntapRestProvider{client: mockClient}

	params := LunMapCreateParams{
		LunName:    "testLun",
		SvmName:    "testSVM",
		IGroupName: []string{"iGroupName1", "iGroupName2"},
		LunNumber:  1,
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

	rc := &OntapRestProvider{client: mockClient}

	params := LunMapCreateParams{
		LunName:    "testLun",
		SvmName:    "testSVM",
		IGroupName: []string{"iGroupName1", "iGroupName2"},
		LunNumber:  1,
	}

	mockSAN.On("LunMapCreate", mock.Anything).Return(errors.New("mapping error"))

	err := rc.LunMapCreate(params)

	assert.Error(t, err)

	mockSAN.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestIsAggregateOnline_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	rc := &OntapRestProvider{client: mockClient}

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

	rc := &OntapRestProvider{client: mockClient}

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

	rc := &OntapRestProvider{client: mockClient}

	aggregateName := "testAggregate"

	mockStorage.On("AggregateFindByName", mock.Anything).Return(nil, errors.New("API error"))

	isOnline, err := rc.IsAggregateOnline(aggregateName)

	assert.Error(t, err)
	assert.False(t, isOnline)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetAggregateByName_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	rc := &OntapRestProvider{client: mockClient}

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

	rc := &OntapRestProvider{client: mockClient}

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

	rc := &OntapRestProvider{client: mockClient}

	aggregateName := "testAggregate"

	mockStorage.On("AggregateFindByName", mock.Anything).Return(nil, errors.New("API error"))

	aggregate, err := rc.GetAggregateByName(aggregateName)

	assert.Error(t, err)
	assert.Nil(t, aggregate)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}
