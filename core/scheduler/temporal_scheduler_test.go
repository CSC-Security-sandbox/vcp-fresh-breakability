package scheduler

import (
	"context"
	"errors"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/mocks"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

type mockScheduleHandle struct {
	client.ScheduleHandle
	id         string
	updateFunc func(ctx context.Context, opts client.ScheduleUpdateOptions) error
	deleteFunc func(ctx context.Context) error
}

func (m *mockScheduleHandle) GetID() string { return m.id }

func (m *mockScheduleHandle) Update(ctx context.Context, opts client.ScheduleUpdateOptions) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, opts)
	}
	return nil
}
func (m *mockScheduleHandle) Delete(ctx context.Context) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx)
	}
	return nil
}

func TestTemporalScheduler_Create(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{id: scheduleID}

	mockClient.On("Create", ctx, mock.Anything).Return(mockHandle, nil).Once()

	params := CreateScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
			Args:       []interface{}{"arg1"},
		},
		TemporalScheduleOptions: TemporalCreateScheduleParams{
			WorkflowID: "wf-id",
			Workflow:   "wf",
			Spec:       client.ScheduleSpec{},
		},
	}

	resp, err := scheduler.Create(ctx, params)
	require.NoError(t, err)
	require.Equal(t, scheduleID, resp.ID)
	require.Equal(t, ScheduleStateActive, resp.Status)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Update(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{id: scheduleID}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Once()

	params := UpdateScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
		TemporalScheduleOptions: TemporalUpdateScheduleParams{
			Spec: client.ScheduleSpec{},
		},
	}

	resp, err := scheduler.Update(ctx, params)
	require.NoError(t, err)
	require.Equal(t, scheduleID, resp.ID)
	require.Equal(t, ScheduleStateActive, resp.Status)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Delete(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{id: scheduleID}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Once()

	params := DeleteScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
	}

	resp, err := scheduler.Delete(ctx, params)
	require.NoError(t, err)
	require.Equal(t, scheduleID, resp.ID)
	require.Equal(t, ScheduleStateDeleted, resp.Status)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Create_Error(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockClient.On("Create", ctx, mock.Anything).Return(nil, errors.New("create error")).Times(3)

	params := CreateScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
			Args:       []interface{}{"arg1"},
		},
		TemporalScheduleOptions: TemporalCreateScheduleParams{
			WorkflowID: "wf-id",
			Workflow:   "wf",
			Spec:       client.ScheduleSpec{},
		},
	}

	resp, err := scheduler.Create(ctx, params)
	require.Error(t, err)
	require.Nil(t, resp)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Update_Error(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{
		id: scheduleID,
		updateFunc: func(ctx context.Context, opts client.ScheduleUpdateOptions) error {
			return errors.New("update error")
		},
	}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Times(3)

	params := UpdateScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
		TemporalScheduleOptions: TemporalUpdateScheduleParams{
			Spec: client.ScheduleSpec{},
		},
	}

	resp, err := scheduler.Update(ctx, params)
	require.Error(t, err)
	require.Nil(t, resp)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Delete_Error(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{
		id: scheduleID,
		deleteFunc: func(ctx context.Context) error {
			return errors.New("delete error")
		},
	}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Times(3)

	params := DeleteScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
	}

	resp, err := scheduler.Delete(ctx, params)
	require.Error(t, err)
	require.Nil(t, resp)
	mockClient.AssertExpectations(t)
}
