package models

import (
	"time"
)

const (
	LifeCycleStateCreating   = "CREATING"
	LifeCycleStateOngoing    = "ONGOING"
	LifeCycleStateReverting  = "REVERTING"
	LifeCycleStateUndeleting = "UNDELETING"
	LifeCycleStateCompleted  = "COMPLETED"
	LifeCycleStateRestoring  = "RESTORING"
	LifeCycleStateSplitting  = "SPLITTING"
	LifeCycleStateAvailable  = "AVAILABLE"
	LifeCycleStateREADY      = "READY"
	LifeCycleStateInUse      = "IN_USE"
	LifeCycleStateDisabled   = "DISABLED"
	LifeCycleStateDisabling  = "DISABLING"
	LifeCycleStateEnabling   = "ENABLING"
	LifeCycleStateUpdating   = "UPDATING"
	LifeCycleStateDeleting   = "DELETING"
	LifeCycleStateDeleted    = "DELETED"
	LifeCycleStateError      = "ERROR"
	LifeCycleStateRetained   = "RETAINED"
	LifeCycleStateCreated    = "CREATED"

	LifeCycleStateCreatingDetails      = "Creation in progress"
	LifeCycleStateRevertingDetails     = "Revert in progress"
	LifeCycleStateUndeletingDetails    = "Undelete in progress"
	LifeCycleStateRestoringDetails     = "Restore in progress"
	LifeCycleStateAvailableDetails     = "Available for use"
	LifeCycleStateDisabledDetails      = "Disabled"
	LifeCycleStateUpdatingDetails      = "Update in progress"
	LifeCycleStateDeletingDetails      = "Deletion in progress"
	LifeCycleStateSplittingDetails     = "Splitting in progress"
	LifeCycleStateDeletedDetails       = "Deleted"
	LifeCycleStateCompletedDetails     = "Completed"
	LifeCycleStateRetainedDetails      = "Retained"
	LifeCycleStateOngoingDetails       = "Ongoing"
	LifeCycleStateCreationErrorDetails = "Error in creating"
	LifeCycleStateUpdateErrorDetails   = "Error in updating"
	LifeCycleStateDeletionErrorDetails = "Error in deleting"
	LifeCycleStateReadyDetails         = "Ready for use"
	LifeCycleStateCreatedDetails       = "Created successfully"

	AccountStateDisabled = "DISABLED"
	AccountStateEnabled  = "ENABLED"
)

// SVM represents a single SVM resource
type SVM struct {
	BaseModel
	Name         string
	Description  string
	State        string
	StateDetails string
}

type Account struct {
	BaseModel
	Name  string
	State string
	Tags  string
}

// BaseModel describes the base model shared by all other models
type BaseModel struct {
	ID        int64
	UUID      string
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

type UserCache struct {
	Time     time.Time
	SecretID string
	Password string
}
