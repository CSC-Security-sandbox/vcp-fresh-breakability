// This file defines the types and interfaces that are used by various decision making implementations/strategies.

package vmrs

import (
	"math"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
)

type VMSelectionStrategy string

const (
	// DecisionMakerTypeLeastCostSingleVM is the type of the decision maker that selects the least cost single VM that satisfies the customer request.
	LeastCostSingleVM VMSelectionStrategy = "least_cost_single_vm"
)

// VMRS configuration object that holds performance limits for different hyperscalers.
type VMRSConfig struct {
	// The list of performance limits - one element in the list for each hyperscaler.
	HyperscalerPerfLimits HyperscalerPerfLimits `yaml:"vmrs" validate:"required"`
}

// HyperscalerPerfLimits represents the performance limits for a specific hyperscaler.
type HyperscalerPerfLimits struct {
	// DecisionMaker to use.
	VMSelectionStrategy VMSelectionStrategy `yaml:"selection_strategy" validate:"required"`
	// The max. number of HA-pairs that can be created in a single storage pool.
	MaxNumHAPairs int64 `yaml:"max_num_ha_pairs" validate:"required"`
	// Ontap constants provided by the perf team.
	OntapOverheads OntapOverheads `yaml:"ontap_overheads" validate:"required"`
	// The list of performance limits for different disk types.
	DiskPerfLimits []DiskTypePerfLimit `yaml:"disk_limits" validate:"required,min=1"`
	// The max. multiplier by which disks can be overprovisioned.
	MaxDiskOverprovisioningFactors PerfAmplificationFactors `yaml:"max_disk_overprovisioning_factors" validate:"required"`
}

// OntapOverheads contains the overheads that need to be accounted for when provisioning volumes.
type OntapOverheads struct {
	// The amplification factors for IOPs, throughput, and capacity.
	AmplificationFactors AmplificationFactors `yaml:"amplification_factors" validate:"required"`
	// The number of disks that can be attached to a VM, per zone.
	NumDisksPerZone int64 `yaml:"num_disks_per_zone" validate:"required"`
	// Workload specific overheads for the Ontap system.
	WorkloadHeadroom []WorkloadHeadroom `yaml:"workload_headroom"`
	// Hotspot prevention factors for IOPS and throught.
	HotspotPreventionFactors PerfAmplificationFactors `yaml:"hotspot_prevention_factors" validate:"required"`
}

type AmplificationFactors struct {
	// The amplification factors for IOPs, and throughput.
	// Fields are promoted to the parent struct.
	PerfAmplificationFactors `yaml:",inline"`
	// The amplification factor for capacity.
	Capacity float64 `yaml:"capacity" validate:"required"`
}

// PerfAmplificationFactors contains the amplification factors for IOPs, and throughput.
type PerfAmplificationFactors struct {
	// The amplification factor for IOPs.
	IOPS float64 `yaml:"iops" validate:"required"`
	// The amplification factor for throughput.
	Throughput float64 `yaml:"throughput" validate:"required"`
}

// WorkloadHeadroom contains the overheads for different workloads.
type WorkloadHeadroom struct {
	// The name of the workload.
	WorkloadName string `yaml:"workload" validate:"required"`
	// The amplification factor for IOPs and throughput for this workload.
	Headroom PerfAmplificationFactors `yaml:"headroom" validate:"required"`
}

// DiskTypePerfLimit represents the performance limits for a specific disk type provided by the hyperscaler.
type DiskTypePerfLimit struct {
	// The type of the disk provided by the hyperscaler.
	DiskType string `yaml:"disk_type" validate:"required"`
	// Perf limits for qualified VMs.
	QualifiedVMs []VMPerfLimit `yaml:"qualified_vms" validate:"required,min=1"`
}

// The performance limits for a specific VM.
type VMPerfLimit struct {
	// The VM/instance type for which these limits apply.
	VMType string `yaml:"vm_type" validate:"required"`
	// The Ontap performance limits for this disk type.
	OntapLimits OntapPerfLimit `yaml:"ontap_perf" validate:"required"`
	// The performance limits specified by the hyperscaler for this disk type.
	DiskLimits DiskPerfLimit `yaml:"disk_perf" validate:"required"`
	// The relative cost of this VM type compared to the cheapest VM type.
	RelativeCost float64 `yaml:"relative_cost" validate:"required"`
}

// OntapPerfLimit represents the performance limits for a specific VM type.
// This information is provided by our perf team.
type OntapPerfLimit struct {
	// The IOPs limit for this VM type.
	IOPS int64 `yaml:"iops" validate:"required"`
	// The throughput limit in MiB/s for this VM type.
	ThroughputInMiBs int64 `yaml:"throughput_in_mibs" validate:"required"`
	// The capacity limit in GiB for this VM type.
	CapacityInGiB int64 `yaml:"capacity_in_gib" validate:"required"`
}

// DiskPerfLimit represents the performance limits for a specific VM type.
// This information is provided by the hyperscaler.
type DiskPerfLimit struct {
	// The IOPS limit for this disk/disk type.
	IOPS int64 `yaml:"iops" validate:"required"`
	// The throughput limit in MiB/s for this disk/disk type.
	ThroughputInMiBs int64 `yaml:"throughput_in_mibs" validate:"required"`
	// The capacity limit in GiB for this disk/disk type.
	CapacityInGiB int64 `yaml:"capacity_in_gib" validate:"required"`
}

// CustomerRequestedPerformance represents the customer specified request for performance limits (IOPs, throughput, and capacity).
type CustomerRequestedPerformance struct {
	DesiredIOPS             int64 // IOPS requested by the customer
	DesiredThroughputInMiBs int64 // Throughput requested by the customer
	DesiredCapacityInGiB    int64 // Capacity requested by the customer
}

// Decision represents the list of VM identifiers that satisfy the CustomerRequestedPerformance based on the VMRS configuration that was input to the decision-making logic.
type Decision struct {
	ChosenVMs []string // The list of VMs that satisfy the customer request
	// Storage pool performance limits to request from VLM.
	StoragePoolRequirements CustomerRequestedPerformance
}

// The DecisionMaker interface defines the method to make decisions based on the VMRS configuration and customer requested performance.
type DecisionMaker interface {
	// FindOptimalVMs takes the VMRS configuration and customer requested performance.
	// It returns the list of VM identifiers that together satisfy the customer request performance thresholds, while optimizing for some cost function. The cost function that is optimized for depends on the implementation.
	FindOptimalVMs(config *VMRSConfig, customerRequest CustomerRequestedPerformance, currentConfig *vlm.VLMConfig) (*Decision, error)
}

// A DecisionMakerFactory is responsible for creating instances of DecisionMaker based on the provided VMRSConfig.
type DecisionMakerFactory interface {
	NewDecisionMaker(config *VMRSConfig) (DecisionMaker, error)
}

// Given CustomerRequestedPerformance, this function computes the result of the overheads specified in the VMRSConfig, and returns the scaled performance limits.
func (config *VMRSConfig) ScaleCustomerRequestedPerformance(customerRequest CustomerRequestedPerformance, workloadOverheads PerfAmplificationFactors) CustomerRequestedPerformance {
	iopsToProvision := int64(math.Ceil(float64(customerRequest.DesiredIOPS) * config.HyperscalerPerfLimits.OntapOverheads.AmplificationFactors.IOPS * workloadOverheads.IOPS * float64(config.HyperscalerPerfLimits.OntapOverheads.HotspotPreventionFactors.IOPS)))
	throughputInMibpsToProvision := int64(math.Ceil(float64(customerRequest.DesiredThroughputInMiBs) * config.HyperscalerPerfLimits.OntapOverheads.AmplificationFactors.Throughput * workloadOverheads.Throughput * float64(config.HyperscalerPerfLimits.OntapOverheads.HotspotPreventionFactors.Throughput)))
	capacityInGiBToProvision := int64(math.Ceil(float64(customerRequest.DesiredCapacityInGiB) * config.HyperscalerPerfLimits.OntapOverheads.AmplificationFactors.Capacity))
	return CustomerRequestedPerformance{
		DesiredIOPS:             iopsToProvision,
		DesiredThroughputInMiBs: throughputInMibpsToProvision,
		DesiredCapacityInGiB:    capacityInGiBToProvision,
	}
}
