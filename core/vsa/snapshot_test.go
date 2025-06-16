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

func TestCreateSnapshot(t *testing.T) {
	t.Run("CreateSnapshotSuccess", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{
			Snapshot: models.Snapshot{
				Comment:     nillable.ToPointer("testComment"),
				Name:        nillable.ToPointer("testSnapshot"),
				Size:        nillable.ToPointer(int64(1024)),
				UUID:        nillable.ToPointer("testUUID"),
				LogicalSize: nillable.ToPointer(int64(1024)),
			},
		}
		mockJob := &ontaprest.JobAccepted{
			JobUUID: "testJobUUID",
		}

		mockStorage.On("SnapshotGet", mock.Anything).Return(mockSnapshot, nil)
		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, mockJob, nil)
		mockClient.On("Poll", mockJob.JobUUID).Return(mockJob)

		resp, err := rc.CreateSnapshot(params)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "testSnapshot", resp.Name)
		assert.Equal(t, "testUUID", resp.ExternalUUID)
		assert.Equal(t, int64(1024), resp.SizeInBytes)
	})

	t.Run("CreateSnapshotErrorOnCreate", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockStorage.On("SnapshotCreate", mock.Anything).Return(nil, nil, errors.New("creation error"))

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("CreateSnapshotErrorOnPoll", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{
			Snapshot: models.Snapshot{
				Comment: nillable.ToPointer("testComment"),
				Name:    nillable.ToPointer("testSnapshot"),
				Size:    nillable.ToPointer(int64(1024)),
			},
		}
		mockJob := &ontaprest.JobAccepted{
			JobUUID: "testJobUUID",
		}

		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, mockJob, nil)
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("polling error"))

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("CreateSnapshotInvalidResponse", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{}

		mockStorage.On("SnapshotGet", mock.Anything).Return(nil, nil)
		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, nil, nil)

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.ErrorContains(t, err, "invalid Snapshot create response from API")
	})

	t.Run("CreateSnapshotVolumeNotFound", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "nonExistentVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockStorage.On("SnapshotCreate", mock.Anything).Return(nil, nil, errors.NewNotFoundErr("Volume", nil))

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.ErrorContains(t, err, "Volume not found")
		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("CreateSnapshot_GetError", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{
			Snapshot: models.Snapshot{
				Comment:     nillable.ToPointer("testComment"),
				Name:        nillable.ToPointer("testSnapshot"),
				Size:        nillable.ToPointer(int64(1024)),
				UUID:        nillable.ToPointer("testUUID"),
				LogicalSize: nillable.ToPointer(int64(1024)),
			},
		}
		mockJob := &ontaprest.JobAccepted{
			JobUUID: "testJobUUID",
		}

		mockStorage.On("SnapshotGet", mock.Anything).Return(mockSnapshot, errors.New("get error"))
		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, mockJob, nil)
		mockClient.On("Poll", mockJob.JobUUID).Return(mockJob)

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("CreateSnapshot_NilResponseError", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{
			Snapshot: models.Snapshot{
				UUID: nillable.ToPointer("testUUID"),
			},
		}
		mockJob := &ontaprest.JobAccepted{
			JobUUID: "testJobUUID",
		}

		mockStorage.On("SnapshotGet", mock.Anything).Return(mockSnapshot, nil)
		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, mockJob, nil)
		mockClient.On("Poll", mockJob.JobUUID).Return(mockJob)

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestCreateSnapshotPolicy(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		schedule := &Schedule{
			Name:        "sched1",
			Months:      []int{1},
			DaysOfMonth: []int{1},
			DaysOfWeek:  []int{1},
			Hours:       []int{1},
			Minutes:     []int{0},
		}
		policy := &SnapshotPolicy{
			Name:      "policy1",
			Comment:   "comment",
			IsEnabled: true,
			Schedules: []*SnapshotPolicySchedule{
				{
					SnapmirrorLabel: "label1",
					Count:           1,
					Schedule:        schedule,
				},
			},
		}

		mockStorage.On("SnapshotPolicyCreate", mock.Anything).Return(nil)

		err := rc.CreateSnapshotPolicy(policy)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ErrorTooManySchedules", func(t *testing.T) {
		rc := &OntapRestProvider{}
		policy := &SnapshotPolicy{
			Name:      "policy1",
			Comment:   "comment",
			IsEnabled: true,
			Schedules: []*SnapshotPolicySchedule{
				{}, {}, {}, {}, {}, // 5 schedules
			},
		}
		err := rc.CreateSnapshotPolicy(policy)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too many snapshot policy schedules")
	})

	t.Run("ErrorNoSchedules", func(t *testing.T) {
		rc := &OntapRestProvider{}
		policy := &SnapshotPolicy{
			Name:      "policy1",
			Comment:   "comment",
			IsEnabled: true,
			Schedules: []*SnapshotPolicySchedule{},
		}
		err := rc.CreateSnapshotPolicy(policy)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must have at least one snapshot policy schedule")
	})

	t.Run("ErrorOnCreate", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		schedule := &Schedule{
			Name:        "sched1",
			Months:      []int{1},
			DaysOfMonth: []int{1},
			DaysOfWeek:  []int{1},
			Hours:       []int{1},
			Minutes:     []int{0},
		}
		policy := &SnapshotPolicy{
			Name:      "policy1",
			Comment:   "comment",
			IsEnabled: true,
			Schedules: []*SnapshotPolicySchedule{
				{
					SnapmirrorLabel: "label1",
					Count:           1,
					Schedule:        schedule,
				},
			},
		}

		mockStorage.On("SnapshotPolicyCreate", mock.Anything).Return(errors.New("some error"))

		err := rc.CreateSnapshotPolicy(policy)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "some error")
	})

	t.Run("ErrorOnScheduleCreate", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		schedule := &Schedule{
			Name:        "sched1",
			Months:      []int{1},
			DaysOfMonth: []int{1},
			DaysOfWeek:  []int{1},
			Hours:       []int{1},
			Minutes:     []int{0},
		}
		policy := &SnapshotPolicy{
			Name:      "policy1",
			Comment:   "comment",
			IsEnabled: true,
			Schedules: []*SnapshotPolicySchedule{
				{
					SnapmirrorLabel: "label1",
					Count:           1,
					Schedule:        schedule,
				},
			},
		}

		// First call returns "not found" error with "Schedule" prefix, triggering schedule creation
		mockStorage.On("SnapshotPolicyCreate", mock.Anything).Return(errors.New("Schedule not found"))
		// ScheduleCreate returns an error that is not "exists" or "duplicate entry"
		mockCluster.On("ScheduleCreate", mock.Anything).Return(errors.New("unexpected error"))

		err := rc.CreateSnapshotPolicy(policy)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected error")
	})

	t.Run("ScheduleAlreadyExists", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockCluster := new(ontaprest.MockClusterClient)
		mockClient.On("Storage").Return(mockStorage)
		mockClient.On("Cluster").Return(mockCluster)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		schedule := &Schedule{
			Name:        "sched1",
			Months:      []int{1},
			DaysOfMonth: []int{1},
			DaysOfWeek:  []int{1},
			Hours:       []int{1},
			Minutes:     []int{0},
		}
		policy := &SnapshotPolicy{
			Name:      "policy1",
			Comment:   "comment",
			IsEnabled: true,
			Schedules: []*SnapshotPolicySchedule{
				{
					SnapmirrorLabel: "label1",
					Count:           1,
					Schedule:        schedule,
				},
			},
		}

		mockStorage.On("SnapshotPolicyCreate", mock.Anything).Return(errors.New("Schedule not found")).Once()
		mockCluster.On("ScheduleCreate", mock.Anything).Return(errors.New("already exists")).Once()
		mockStorage.On("SnapshotPolicyCreate", mock.Anything).Return(nil).Once()

		err := rc.CreateSnapshotPolicy(policy)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockCluster.AssertExpectations(t)
	})
}

func TestDeleteSnapshot(t *testing.T) {
	t.Run("DeleteSnapshotSuccess", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		snapshotUUID := "testUUID"
		snapshotName := "testSnapshot"

		mockStorage.On("SnapshotDelete", mock.Anything).Return(nil)

		err := rc.DeleteSnapshot(snapshotUUID, snapshotName)

		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("DeleteSnapshotError", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		snapshotUUID := "testUUID"
		snapshotName := "testSnapshot"

		mockStorage.On("SnapshotDelete", mock.Anything).Return(errors.New("deletion error"))

		err := rc.DeleteSnapshot(snapshotUUID, snapshotName)

		assert.Error(t, err)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})
}

func TestGenerateNameForSchedule(t *testing.T) {
	t.Run("MonthlySchedule", func(t *testing.T) {
		schedule := &Schedule{
			DaysOfMonth: []int{1, 15},
			Minutes:     []int{0},
			Hours:       []int{2},
		}
		name := generateNameForSchedule(schedule)
		assert.Contains(t, name, "monthly-on-day-1+15")
		assert.Contains(t, name, "0-min-past")
		assert.Contains(t, name, "2am")
	})

	t.Run("WeeklySchedule", func(t *testing.T) {
		schedule := &Schedule{
			DaysOfWeek: []int{1, 2},
			Minutes:    []int{30},
			Hours:      []int{5},
		}
		name := generateNameForSchedule(schedule)
		assert.Contains(t, name, "weekly-on-monday+tuesday")
		assert.Contains(t, name, "30-min-past")
		assert.Contains(t, name, "5am")
	})

	t.Run("DailySchedule", func(t *testing.T) {
		schedule := &Schedule{
			Hours:   []int{7},
			Minutes: []int{45},
		}
		name := generateNameForSchedule(schedule)
		assert.Contains(t, name, "daily-45-min-past")
		assert.Contains(t, name, "7am")
	})

	t.Run("HourlySchedule", func(t *testing.T) {
		schedule := &Schedule{
			Minutes: []int{15},
		}
		name := generateNameForSchedule(schedule)
		assert.Contains(t, name, "hourly-15-min-past-hour")
	})

	t.Run("EmptySchedule", func(t *testing.T) {
		schedule := &Schedule{}
		name := generateNameForSchedule(schedule)
		assert.Contains(t, name, "hourly-0-min-past-hour")
	})
}
