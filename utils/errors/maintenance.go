// Copyright (c) 2022 NetApp, Inc. All rights reserved.

package errors

import (
	"fmt"
)

// MaintenanceErr defines an error when an operation is blocked because of an SVM migration
type MaintenanceErr struct {
	operation  string
	trackingID int
}

// Error returns a string version of the error
func (me *MaintenanceErr) Error() string {
	return fmt.Sprintf("Cannot %s because of ongoing maintenance (code %d). If the problem persists contact Support", me.operation, me.trackingID)
}

// NewMaintenanceErr returns a MaintenanceErr
func NewMaintenanceErr(operation string, tID int) error {
	return &MaintenanceErr{operation: operation, trackingID: tID}
}

// IsMaintenanceErr checks whether the specified error is a MaintenanceErr
func IsMaintenanceErr(err error) bool {
	_, is := err.(*MaintenanceErr)
	return is
}

// GetTrackingID returns the tracking id for the error
func (me *MaintenanceErr) GetTrackingID() int {
	return me.trackingID
}
