package scheduler

const (
	JobStatusCreating  = "CREATING"
	JobStatusUpdating  = "UPDATING"
	JobStatusDeleting  = "DELETING"
	JobStatusScheduled = "SCHEDULED"
	JobStatusDeleted   = "DELETED"

	JobManagerWorkflowID           = "job-manager-workflow"
	UpdateBackupScheduleWorkflowID = "update-backup-schedule-workflow"
)
