package models

import "testing"

func TestVolumePerformanceGroupAllocationHelpers(t *testing.T) {
	t.Run("IsShared", func(tt *testing.T) {
		vpg := &VolumePerformanceGroup{AllocationType: AllocationTypeShared}
		if !vpg.IsShared() {
			tt.Fatalf("expected IsShared to return true for SHARED allocation type")
		}
		if vpg.IsPerVolume() {
			tt.Fatalf("expected IsPerVolume to return false for SHARED allocation type")
		}
	})

	t.Run("IsPerVolume", func(tt *testing.T) {
		vpg := &VolumePerformanceGroup{AllocationType: AllocationTypePerVolume}
		if !vpg.IsPerVolume() {
			tt.Fatalf("expected IsPerVolume to return true for PER_VOLUME allocation type")
		}
		if vpg.IsShared() {
			tt.Fatalf("expected IsShared to return false for PER_VOLUME allocation type")
		}
	})

	t.Run("NilReceiver", func(tt *testing.T) {
		var vpg *VolumePerformanceGroup
		if vpg.IsShared() {
			tt.Fatalf("expected IsShared to return false for nil receiver")
		}
		if vpg.IsPerVolume() {
			tt.Fatalf("expected IsPerVolume to return false for nil receiver")
		}
	})
}
