// Package leakedresources provides an extensible framework for detecting and
// reporting leaked resources (pools, volumes, snapshots, backup vaults). It is triggered every
// 6 hours via the cron scheduler in vcp-core/cmd/main.go. Flow: Scan → Detect → Report.
package leakedresources

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"

// Re-export model types for backward compatibility and external use.
type (
	ResourceType = model.ResourceType
	LeakRecord   = model.LeakRecord
)

const (
	ResourceTypePool               = model.ResourceTypePool
	ResourceTypeVolume             = model.ResourceTypeVolume
	ResourceTypeSnapshot           = model.ResourceTypeSnapshot
	ResourceTypeInternalReservedIP = model.ResourceTypeInternalReservedIP
	ResourceTypeBackupVault        = model.ResourceTypeBackupVault
)
