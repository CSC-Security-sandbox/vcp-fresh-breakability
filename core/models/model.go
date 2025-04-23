package models

import (
	"time"
)

const (
	LifeCycleStateCreating   = "creating"
	LifeCycleStateOngoing    = "ongoing"
	LifeCycleStateReverting  = "reverting"
	LifeCycleStateUndeleting = "undeleting"
	LifeCycleStateCompleted  = "completed"
	LifeCycleStateRestoring  = "restoring"
	LifeCycleStateSplitting  = "splitting"
	LifeCycleStateAvailable  = "available"
	LifeCycleStateDisabled   = "disabled"
	LifeCycleStateDisabling  = "disabling"
	LifeCycleStateEnabling   = "enabling"
	LifeCycleStateUpdating   = "updating"
	LifeCycleStateDeleting   = "deleting"
	LifeCycleStateDeleted    = "deleted"
	LifeCycleStateError      = "error"
	LifeCycleStateRetained   = "retained"

	LifeCycleStateCreatingDetails   = "Creation in progress"
	LifeCycleStateRevertingDetails  = "Revert in progress"
	LifeCycleStateUndeletingDetails = "Undelete in progress"
	LifeCycleStateRestoringDetails  = "Restore in progress"
	LifeCycleStateAvailableDetails  = "Available for use"
	LifeCycleStateDisabledDetails   = "Disabled"
	LifeCycleStateUpdatingDetails   = "Update in progress"
	LifeCycleStateDeletingDetails   = "Deletion in progress"
	LifeCycleStateSplittingDetails  = "Splitting in progress"
	LifeCycleStateDeletedDetails    = "Deleted"
	LifeCycleStateCompletedDetails  = "Completed"
	LifeCycleStateRetainedDetails   = "Retained"
	LifeCycleStateOngoingDetails    = "Ongoing"

	AccountStateDisabled = "DISABLED"
	AccountStateEnabled  = "ENABLED"
)

// Volume represents a single volume resource
type Volume struct {
	BaseModel
	Name         string
	Description  string
	State        string
	StateDetails string
}

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
