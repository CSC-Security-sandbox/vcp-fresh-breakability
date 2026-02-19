package backgroundworkflows

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"go.temporal.io/sdk/testsuite"
)

func TestSyncFlexCachePrepopulateWorkflow_Success_NoJobs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return([]*datamodel.Job{}, nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_Success_SingleJob_Completed(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	volumeUUID := "volume-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      resourceName,
	}

	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID,
		State:   "success",
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(volume, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume, jobs[0]).Return(jobStatus, nil)
	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID, volumeUUID, jobStatus).Return(nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_Success_SingleJob_InProgress(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	volumeUUID := "volume-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      resourceName,
	}

	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID,
		State:   "running",
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(volume, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume, jobs[0]).Return(jobStatus, nil)
	// UpdateJobAndVolumeStatus should NOT be called for in-progress jobs

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_Success_MultipleJobs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID1 := "job-uuid-1"
	jobUUID2 := "job-uuid-2"
	jobUUID3 := "job-uuid-3"
	volumeUUID1 := "volume-uuid-1"
	volumeUUID2 := "volume-uuid-2"
	volumeUUID3 := "volume-uuid-3"
	resourceName1 := "test-volume-1"
	resourceName2 := "test-volume-2"
	resourceName3 := "test-volume-3"
	ontapJobUUID1 := "ontap-job-uuid-1"
	ontapJobUUID2 := "ontap-job-uuid-2"
	ontapJobUUID3 := "ontap-job-uuid-3"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID1},
			ResourceName: resourceName1,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID1,
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID2},
			ResourceName: resourceName2,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID2,
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID3},
			ResourceName: resourceName3,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID3,
			},
		},
	}

	volume1 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID1},
		Name:      resourceName1,
	}
	volume2 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID2},
		Name:      resourceName2,
	}
	volume3 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID3},
		Name:      resourceName3,
	}

	jobStatus1 := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID1,
		State:   "success",
	}

	jobStatus2 := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID2,
		State:   "running",
	}

	jobStatus3 := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID3,
		State:   "failure",
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)

	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName1).Return(volume1, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName2).Return(volume2, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName3).Return(volume3, nil)

	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume1, jobs[0]).Return(jobStatus1, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume2, jobs[1]).Return(jobStatus2, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume3, jobs[2]).Return(jobStatus3, nil)

	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID1, volumeUUID1, jobStatus1).Return(nil)
	// jobStatus2 is running, so no update
	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID3, volumeUUID3, jobStatus3).Return(nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_Success_WithFailedJob(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	volumeUUID := "volume-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"
	errorMessage := "prepopulate failed due to network error"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      resourceName,
	}

	jobStatus := &common.PrepopulateJobStatus{
		JobUUID:      ontapJobUUID,
		State:        "failure",
		ErrorMessage: errorMessage,
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(volume, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume, jobs[0]).Return(jobStatus, nil)
	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID, volumeUUID, jobStatus).Return(nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_GetJobsFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(nil, errors.New("database error"))

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "database error")
}

func TestSyncFlexCachePrepopulateWorkflow_GetVolumeFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(nil, errors.New("volume not found"))

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	// Workflow should continue despite individual job errors
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_PollJobStatusFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	volumeUUID := "volume-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      resourceName,
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(volume, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume, jobs[0]).Return(nil, errors.New("ONTAP error"))

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	// Workflow should continue despite individual job errors
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_UpdateJobAndVolumeFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	volumeUUID := "volume-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      resourceName,
	}

	jobStatus := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID,
		State:   "success",
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(volume, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume, jobs[0]).Return(jobStatus, nil)
	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID, volumeUUID, jobStatus).Return(errors.New("update failed"))

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	// Workflow should continue despite individual job errors
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_PartialFailures(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID1 := "job-uuid-1"
	jobUUID2 := "job-uuid-2"
	jobUUID3 := "job-uuid-3"
	jobUUID4 := "job-uuid-4"
	volumeUUID1 := "volume-uuid-1"
	volumeUUID3 := "volume-uuid-3"
	volumeUUID4 := "volume-uuid-4"
	resourceName1 := "test-volume-1"
	resourceName2 := "test-volume-2"
	resourceName3 := "test-volume-3"
	resourceName4 := "test-volume-4"
	ontapJobUUID1 := "ontap-job-uuid-1"
	ontapJobUUID2 := "ontap-job-uuid-2"
	ontapJobUUID3 := "ontap-job-uuid-3"
	ontapJobUUID4 := "ontap-job-uuid-4"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID1},
			ResourceName: resourceName1,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID1,
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID2},
			ResourceName: resourceName2,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID2,
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID3},
			ResourceName: resourceName3,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID3,
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID4},
			ResourceName: resourceName4,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID4,
			},
		},
	}

	volume1 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID1},
		Name:      resourceName1,
	}
	volume3 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID3},
		Name:      resourceName3,
	}
	volume4 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID4},
		Name:      resourceName4,
	}

	jobStatus1 := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID1,
		State:   "success",
	}

	jobStatus4 := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID4,
		State:   "success",
	}

	// Job 1 succeeds, job 2 fails to get volume, job 3 fails to poll, job 4 succeeds
	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)

	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName1).Return(volume1, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName2).Return(nil, errors.New("volume not found"))
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName3).Return(volume3, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName4).Return(volume4, nil)

	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume1, jobs[0]).Return(jobStatus1, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume3, jobs[2]).Return(nil, errors.New("ONTAP error"))
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume4, jobs[3]).Return(jobStatus4, nil)

	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID1, volumeUUID1, jobStatus1).Return(nil)
	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID4, volumeUUID4, jobStatus4).Return(nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	// Workflow should complete successfully even with partial failures
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_StatusQuery_Completed(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return([]*datamodel.Job{}, nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Query the workflow status
	qr, err := env.QueryWorkflow(workflows.StatusQueryName)
	assert.NoError(t, err)
	var status workflows.WorkflowStatus
	assert.NoError(t, qr.Get(&status))
	assert.NotEmpty(t, status.ID)
	assert.Equal(t, workflows.WorkflowStatusCompleted, status.Status)
}

func TestSyncFlexCachePrepopulateWorkflow_StatusQuery_Failed(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(nil, errors.New("database error"))

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	qr, err := env.QueryWorkflow(workflows.StatusQueryName)
	assert.NoError(t, err)
	var status workflows.WorkflowStatus
	assert.NoError(t, qr.Get(&status))
	assert.NotEmpty(t, status.ID)
	assert.Equal(t, workflows.WorkflowStatusFailed, status.Status)
}

func TestSyncFlexCachePrepopulateWorkflow_Success_LimitsToMaxJobs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	// Create 25 jobs to exceed maxJobsPerRun of 20
	numJobs := 25
	maxProcessed := 20
	jobs := make([]*datamodel.Job, numJobs)

	for i := 0; i < numJobs; i++ {
		jobUUID := fmt.Sprintf("job-uuid-%d", i)
		ontapJobUUID := fmt.Sprintf("ontap-job-uuid-%d", i)
		resourceName := fmt.Sprintf("test-volume-%d", i)

		jobs[i] = &datamodel.Job{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		}
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)

	// Only mock for the jobs that will actually be processed (first maxProcessed)
	for i := 0; i < maxProcessed; i++ {
		jobUUID := fmt.Sprintf("job-uuid-%d", i)
		volumeUUID := fmt.Sprintf("volume-uuid-%d", i)
		ontapJobUUID := fmt.Sprintf("ontap-job-uuid-%d", i)
		resourceName := fmt.Sprintf("test-volume-%d", i)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: volumeUUID},
			Name:      resourceName,
		}

		jobStatus := &common.PrepopulateJobStatus{
			JobUUID: ontapJobUUID,
			State:   "success",
		}

		env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(volume, nil).Once()
		env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.UUID == jobUUID
		})).Return(jobStatus, nil).Once()
		env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID, volumeUUID, jobStatus).Return(nil).Once()
	}

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_OrphanedJob_MarkSuccess(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	// Activity returns (nil, nil) for not-found volume → triggers orphan detection
	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(nil, nil)
	env.OnActivity(activity.MarkOrphanedPrepopulateJob, mock.Anything, jobUUID, resourceName).Return(nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_OrphanedJob_MarkFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	// Volume not found (orphan) — activity returns (nil, nil), but marking also fails
	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(nil, nil)
	env.OnActivity(activity.MarkOrphanedPrepopulateJob, mock.Anything, jobUUID, resourceName).Return(errors.New("database error"))

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	// Workflow should continue despite marking failure
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_OrphanedJob_ContinuesWithOtherJobs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID1 := "job-uuid-1"
	jobUUID2 := "job-uuid-2"
	volumeUUID2 := "volume-uuid-2"
	resourceName1 := "deleted-volume"
	resourceName2 := "existing-volume"
	ontapJobUUID1 := "ontap-job-uuid-1"
	ontapJobUUID2 := "ontap-job-uuid-2"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID1},
			ResourceName: resourceName1,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID1,
			},
		},
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID2},
			ResourceName: resourceName2,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID2,
			},
		},
	}

	volume2 := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID2},
		Name:      resourceName2,
	}

	jobStatus2 := &common.PrepopulateJobStatus{
		JobUUID: ontapJobUUID2,
		State:   "success",
	}

	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)

	// Job 1: orphaned (volume deleted) - activity returns (nil, nil) for not-found
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName1).Return(nil, nil)
	env.OnActivity(activity.MarkOrphanedPrepopulateJob, mock.Anything, jobUUID1, resourceName1).Return(nil)

	// Job 2: normal flow, completes successfully
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName2).Return(volume2, nil)
	env.OnActivity(activity.PollPrepopulateJobStatus, mock.Anything, volume2, jobs[1]).Return(jobStatus2, nil)
	env.OnActivity(activity.UpdateJobAndVolumeStatus, mock.Anything, jobUUID2, volumeUUID2, jobStatus2).Return(nil)

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
}

func TestSyncFlexCachePrepopulateWorkflow_GetVolumeFails_NonNotFoundError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	activity := &backgroundactivities.FlexCachePrepopulateActivity{}
	env.RegisterActivity(activity)

	jobUUID := "job-uuid-1"
	resourceName := "test-volume-1"
	ontapJobUUID := "ontap-job-uuid-1"

	jobs := []*datamodel.Job{
		{
			BaseModel:    datamodel.BaseModel{UUID: jobUUID},
			ResourceName: resourceName,
			JobAttributes: &datamodel.JobAttributes{
				ResourceUUID: ontapJobUUID,
			},
		},
	}

	// Non-NotFound error (e.g. database error) — should NOT trigger orphan marking
	env.OnActivity(activity.GetActivePrepopulateJobs, mock.Anything).Return(jobs, nil)
	env.OnActivity(activity.GetVolumeByResourceName, mock.Anything, resourceName).Return(nil, errors.New("database connection error"))
	// MarkOrphanedPrepopulateJob should NOT be called

	env.ExecuteWorkflow(SyncFlexCachePrepopulateWorkflow)

	assert.True(t, env.IsWorkflowCompleted())
	// Workflow should continue despite individual job errors
	assert.NoError(t, env.GetWorkflowError())
}
