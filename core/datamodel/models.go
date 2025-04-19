package datamodel

import "time"

type Pool struct {
	UUID string `gorm:"type:varchar(36)"`
	Name string `gorm:"type:varchar(255)"`
	Size int64  `gorm:"type:bigint"`
}

type Volume struct {
	UUID string `gorm:"type:varchar(36)"`
}

type Svm struct {
	UUID string `gorm:"type:varchar(36)"`
}

// Job is a struct that represents the job data model.
type Job struct {
	ID string `json:"uuid" gorm:"unique"`
	//workflowID string    `db:"workflow_id" bson:"workflow_id"`
	CustomerID string    `gorm:"type:varchar"`
	Status     string    `gorm:"type:varchar"`
	CreatedAt  time.Time `json:"createdAt"`
}
