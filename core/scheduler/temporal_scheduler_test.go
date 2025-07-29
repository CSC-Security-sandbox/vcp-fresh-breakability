package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

type mockScheduleHandle struct {
	client.ScheduleHandle
	id          string
	updateFunc  func(ctx context.Context, opts client.ScheduleUpdateOptions) error
	deleteFunc  func(ctx context.Context) error
	pauseFunc   func(ctx context.Context, opts client.SchedulePauseOptions) error
	unpauseFunc func(ctx context.Context, opts client.ScheduleUnpauseOptions) error
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

func (m *mockScheduleHandle) Pause(ctx context.Context, opts client.SchedulePauseOptions) error {
	if m.pauseFunc != nil {
		return m.pauseFunc(ctx, opts)
	}
	return nil
}

func (m *mockScheduleHandle) Unpause(ctx context.Context, opts client.ScheduleUnpauseOptions) error {
	if m.unpauseFunc != nil {
		return m.unpauseFunc(ctx, opts)
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

func TestTemporalScheduler_Pause(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{id: scheduleID}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Once()

	params := PauseScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
	}

	resp, err := scheduler.Pause(ctx, params)
	require.NoError(t, err)
	require.Equal(t, scheduleID, resp.ID)
	require.Equal(t, ScheduleStatePaused, resp.Status)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Pause_Error(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{
		id: scheduleID,
		pauseFunc: func(ctx context.Context, opts client.SchedulePauseOptions) error {
			return errors.New("pause error")
		},
	}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Times(3)

	params := PauseScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
	}

	resp, err := scheduler.Pause(ctx, params)
	require.Error(t, err)
	require.Nil(t, resp)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Unpause(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{id: scheduleID}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Once()

	params := UnpauseScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
	}

	resp, err := scheduler.Unpause(ctx, params)
	require.NoError(t, err)
	require.Equal(t, scheduleID, resp.ID)
	require.Equal(t, ScheduleStateActive, resp.Status)
	mockClient.AssertExpectations(t)
}

func TestTemporalScheduler_Unpause_Error(t *testing.T) {
	mockClient := &mocks.ScheduleClient{}
	scheduler := NewTemporalScheduler(mockClient)

	ctx := context.Background()
	scheduleID := "test-schedule"
	mockHandle := &mockScheduleHandle{
		id: scheduleID,
		unpauseFunc: func(ctx context.Context, opts client.ScheduleUnpauseOptions) error {
			return errors.New("unpause error")
		},
	}

	mockClient.On("GetHandle", ctx, scheduleID).Return(mockHandle).Times(3)

	params := UnpauseScheduleParams{
		ScheduleParams: ScheduleParams{
			ScheduleID: scheduleID,
		},
	}

	resp, err := scheduler.Unpause(ctx, params)
	require.Error(t, err)
	require.Nil(t, resp)
	mockClient.AssertExpectations(t)
}
