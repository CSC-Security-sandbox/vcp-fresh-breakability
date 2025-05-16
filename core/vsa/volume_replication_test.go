package vsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestCreateVolumeReplicationSchedule(t *testing.T) {
	t.Run("WhenGetScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationSchedule10Minutely
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(returnedError)

		err := ontapProvider.CreateVolumeReplicationSchedule(schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenCreate10minutelyScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationSchedule10Minutely
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(returnedError)

		err := ontapProvider.CreateVolumeReplicationSchedule(schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenCreateHourlyScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationScheduleHourly
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(returnedError)

		err := ontapProvider.CreateVolumeReplicationSchedule(schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenCreateDailyScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationScheduleDaily
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(returnedError)

		err := ontapProvider.CreateVolumeReplicationSchedule(schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenInvalidSchedule", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		schedule := "InvalidSchedule"
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		expectedError := errors.NewUserInputValidationErr("Unknown replication schedule")
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)

		err := ontapProvider.CreateVolumeReplicationSchedule(schedule)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenScheduleExists", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		scheduleName := VolumeReplicationScheduleDaily
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: scheduleName,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(nil)
		err := ontapProvider.CreateVolumeReplicationSchedule(scheduleName)
		assert.NoError(tt, err)
	})
}
