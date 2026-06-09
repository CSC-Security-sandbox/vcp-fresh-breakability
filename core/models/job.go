package models

import (
	"database/sql"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

// ResourceOperation represents the type of operation being performed on a resource
type ResourceOperation string

// ResourceType represents the type of resource being operated on
type ResourceType string

const (
	WaitForTemporalJobMaxRetryCount = 5
)

const (
	// Resource operation types
	ResourceOperationCreate ResourceOperation = "CREATE"
	ResourceOperationUpdate ResourceOperation = "UPDATE"
	ResourceOperationDelete ResourceOperation = "DELETE"
)

const (
	// Resource types
	ResourceTypePool         ResourceType = "POOL"
	ResourceTypeSubnet       ResourceType = "SUBNET"
	ResourceTypeStringBucket              = "BUCKET"
)

type PoolCategory string

const (
	// Pool categories for extensible classification
	PoolCategoryStandard      PoolCategory = "standardPool"      // Standard/regular pools
	PoolCategoryLargeCapacity PoolCategory = "largeCapacityPool" // Large capacity pools
	PoolCategoryDefault       PoolCategory = "default"           // Default fallback (maps to standard)
)

// GetResourceJobType returns the appropriate job type based on the resource type, operation, and pool category
func GetResourceJobType(resourceType ResourceType, operation ResourceOperation, poolCategory PoolCategory) datamodel.JobType {
	// Handle default category by mapping to standard pool
	if poolCategory == PoolCategoryDefault {
		poolCategory = PoolCategoryStandard
	}

	// Define the job type mapping based on resource type, operation, and pool category
	// This extensible design allows adding new pool categories without breaking existing code
	jobTypeMap := map[ResourceType]map[ResourceOperation]map[PoolCategory]datamodel.JobType{
		ResourceTypePool: {
			ResourceOperationCreate: {
				PoolCategoryStandard:      datamodel.JobTypeCreatePool,      // Standard pool create
				PoolCategoryLargeCapacity: datamodel.JobTypeCreateLargePool, // Large capacity pool create
			},
			ResourceOperationUpdate: {
				PoolCategoryStandard:      datamodel.JobTypeUpdatePool,      // Standard pool update
				PoolCategoryLargeCapacity: datamodel.JobTypeUpdateLargePool, // Large capacity pool update
			},
			ResourceOperationDelete: {
				PoolCategoryStandard:      datamodel.JobTypeDeletePool,      // Standard pool delete
				PoolCategoryLargeCapacity: datamodel.JobTypeDeleteLargePool, // Large capacity pool delete
			},
		},
		ResourceTypeSubnet: {
			ResourceOperationCreate: {
				PoolCategoryStandard:      datamodel.JobTypeCreateSubnet,      // Standard subnet create
				PoolCategoryLargeCapacity: datamodel.JobTypeCreateLargeSubnet, // Large capacity subnet create
			},
			ResourceOperationDelete: {
				PoolCategoryStandard: datamodel.JobTypeDeleteSubnet, // Standard subnet delete
				// TODO: adding subnet delete support for Large volumes . PoolCategoryLargeCapacity: datamodel.JobTypeDeleteLargeSubnet, // Large capacity subnet delete
			},
			// Note: Subnets only support CREATE operations currently
			// Future operations can be added here as needed
		},
	}

	// Get the job type from the mapping
	if resourceMap, exists := jobTypeMap[resourceType]; exists {
		if operationMap, exists := resourceMap[operation]; exists {
			if jobType, exists := operationMap[poolCategory]; exists {
				return jobType
			}
		}
	}

	// Default fallback (should not reach here with valid inputs)
	return datamodel.JobTypeCreatePool
}

// GetPoolCategory is a concise helper function that maps boolean capacity to PoolCategory
func GetPoolCategory(isLargeCapacity bool) PoolCategory {
	if isLargeCapacity {
		return PoolCategoryLargeCapacity
	}
	return PoolCategoryStandard
}

// Job describes a job DB model
type Job struct {
	BaseModel
	CorrelationID string
	RequestID     string
	Type          datamodel.JobType
	State         datamodel.JobState
	StateDetails  string
	TrackingID    int
	ErrorDetails  []byte
	AccountID     sql.NullInt64
	IsAdminJob    bool
	JobAttributes *JobAttributes
	WorkflowID    string
	ScheduledAt   time.Time
	ResourceName  string
}
type JobAttributes struct {
	ResourceUUID string
	PoolUUID     string
	Location     string
}
