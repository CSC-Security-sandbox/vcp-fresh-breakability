// Copyright (c) 2022 NetApp, Inc. All rights reserved.

package errors

import (
	"testing"
)

func TestNewMaintenanceErr(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		err := NewMaintenanceErr("delete volume", 1)
		if err == nil {
			tt.Fail()
		} else {
			if err.Error() != "Cannot delete volume because of ongoing maintenance (code 1). If the problem persists contact Support" {
				tt.Errorf("Error message '%s' does not match expected one", err.Error())
			}
			if _, ok := err.(*MaintenanceErr); !ok {
				tt.Error("Error type does not match expected one")
			}
		}
	})
}

func TestIsMaintenanceErr(t *testing.T) {
	t.Run("WhenSpecifyingNil", func(tt *testing.T) {
		if IsMaintenanceErr(nil) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotNotFoundErr", func(tt *testing.T) {
		if IsMaintenanceErr(New("entry doesn't exist")) {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNotFoundErr", func(tt *testing.T) {
		if !IsMaintenanceErr(NewMaintenanceErr("operation", 1)) {
			tt.Fail()
		}
	})
}
