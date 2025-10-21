// Package models contains data models for the VSA Control Plane
package models

import "time"

// ClusterUpgradeResponse represents a single cluster upgrade response
type ClusterUpgradeResponse struct {
	ClusterID string        `json:"clusterId"`
	Status    UpgradeStatus `json:"status"`
	JobID     string        `json:"jobId"`
	CreatedAt time.Time     `json:"createdAt"`
	UpdatedAt time.Time     `json:"updatedAt"`
}

// BatchClusterUpgradeResponse represents a batch cluster upgrade response
type BatchClusterUpgradeResponse struct {
	BatchUpgradeID   string            `json:"batchUpgradeId"`
	TotalClusters    int               `json:"totalClusters"`
	SelectedClusters []SelectedCluster `json:"selectedClusters"`
	Status           UpgradeStatus     `json:"status"`
	JobID            string            `json:"jobId"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

// SelectionCriteria represents criteria for selecting clusters for batch upgrade
type SelectionCriteria struct {
	Type        SelectionType   `json:"type"`                 // "percentage" or "poolList"
	Filters     *ClusterFilters `json:"filters"`              // Always applicable for filtering
	Percentage  int             `json:"percentage,omitempty"` // Required when type is "percentage"
	PoolIDs     []string        `json:"poolIds,omitempty"`    // Required when type is "poolList"
	MaxClusters int             `json:"maxClusters,omitempty"`
}

// SelectionType represents the type of cluster selection
type SelectionType string

const (
	SelectionTypePercentage SelectionType = "percentage"
	SelectionTypePoolList   SelectionType = "poolList"
)

// ClusterFilters represents filters for cluster selection
type ClusterFilters struct {
	Zones         []string          `json:"zones,omitempty"`
	OntapVersions []string          `json:"ontapVersions,omitempty"`
	AccountIDs    []string          `json:"accountIds,omitempty"`
	ClusterStates []string          `json:"clusterStates,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
	CreatedAfter  *time.Time        `json:"createdAfter,omitempty"`
	CreatedBefore *time.Time        `json:"createdBefore,omitempty"`
}

// SelectedCluster represents a cluster selected for batch upgrade
type SelectedCluster struct {
	ClusterID      string `json:"clusterId"`
	PoolID         string `json:"poolId"`
	CurrentVersion string `json:"currentVersion"`
	Priority       int    `json:"priority"`
}

// UpgradeStatus represents the status of an upgrade operation
type UpgradeStatus string

const (
	UpgradeStatusPending    UpgradeStatus = "PENDING"
	UpgradeStatusInProgress UpgradeStatus = "IN_PROGRESS"
	UpgradeStatusCompleted  UpgradeStatus = "COMPLETED"
	UpgradeStatusFailed     UpgradeStatus = "FAILED"
	UpgradeStatusCancelled  UpgradeStatus = "CANCELLED"
)

// UpgradeProgress represents the progress of an upgrade operation
type UpgradeProgress struct {
	JobID    string                 `json:"jobId"`
	Status   UpgradeStatus          `json:"status"`
	Clusters []ClusterUpgradeStatus `json:"clusters,omitempty"`
	Errors   []UpgradeError         `json:"errors,omitempty"`
	Warnings []string               `json:"warnings,omitempty"`
}

// ClusterUpgradeStatus represents the status of a single cluster in an upgrade operation
type ClusterUpgradeStatus struct {
	ClusterID   string     `json:"clusterId"`
	Status      string     `json:"status"`
	StartTime   *time.Time `json:"startTime,omitempty"`
	EndTime     *time.Time `json:"endTime,omitempty"`
	CurrentStep string     `json:"currentStep,omitempty"`
}

// UpgradeError represents an error that occurred during upgrade
type UpgradeError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	Retryable bool   `json:"retryable"`
	ClusterID string `json:"clusterId,omitempty"`
}

// AvailableVersion represents an available ONTAP version for upgrade
type AvailableVersion struct {
	OntapVersion string `json:"ontapVersion"`
	VSAImagePath string `json:"vsaImagePath"`
	VSAName      string `json:"vsaName"`
	MediatorName string `json:"mediatorName"`
	IsCurrent    bool   `json:"isCurrent"` // Computed field - true if this is VCP's current version
	IsActive     bool   `json:"isActive"`
}

// ListAvailableVersionsResponse represents the response for listing available versions
type ListAvailableVersionsResponse struct {
	Versions []AvailableVersion `json:"versions"`
	Current  string             `json:"current"` // VCP's current version from environment
}
