// This file implements the DecisionMaker interface to make decisions based on a single VM's performance characteristics.

package decision

import (
	"math"
	"sort"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

// LeastCostSingleVMDecisionMaker implements the DecisionMaker interface for a single VM.
type LeastCostSingleVMDecisionMaker struct {
	config *vmrs.VMRSConfig
	// Sorted list of VMs based on their relative cost.
	vmsSortedByCost []vmrs.VMPerfLimit
	// Overall workload overheads computed from the VMRSConfig.
	overallWorkloadHeadroom vmrs.PerfAmplificationFactors
}

// Given a VMRSConfig, this function returns a new SingleVMDecisionMaker instance that is ready to make decisions based on the provided configuration.
func NewLeastCostSingleVMDecisionMaker(config *vmrs.VMRSConfig) *LeastCostSingleVMDecisionMaker {
	totalWorkloadHeadroom := computeWorkloadHeadroom(config)
	// When the customer requests for X IOPS/Y MiB/s, we need to over provision the disk so that the customer receives the requested performance, after accounting for the various overheads (ontap/workload).
	workloadHeadroomForCustomerScaling := vmrs.PerfAmplificationFactors{
		IOPS:       1.0 + totalWorkloadHeadroom.IOPS,
		Throughput: 1.0 + totalWorkloadHeadroom.Throughput,
	}

	// In order to determine if the VM can satisfy the customer request, we need to scale down the performance limits by the workload headroom.
	workloadHeadroomForDeterminingVM := vmrs.PerfAmplificationFactors{
		IOPS:       1.0 - totalWorkloadHeadroom.IOPS,
		Throughput: 1.0 - totalWorkloadHeadroom.Throughput,
	}
	return &LeastCostSingleVMDecisionMaker{
		config:                  config,
		vmsSortedByCost:         sortVMsByCost(config, workloadHeadroomForDeterminingVM),
		overallWorkloadHeadroom: workloadHeadroomForCustomerScaling,
	}
}

// This implementation of DecisionMaker just loops over the VMRSConfig, looking for the lowest cost VM that satisfies the customer request. It can only ever return one VM identifier.
func (d *LeastCostSingleVMDecisionMaker) FindOptimalVMs(config *vmrs.VMRSConfig, customerRequest vmrs.CustomerRequestedPerformance, currentConfig *vlmconfig.VLMConfig) (*vmrs.Decision, error) {
	// Scale up the customer requested performance to account for the various overheads (ontap/workload/hotspotting) specified in the VMRSConfig.
	// We add a 1.0 to the overall workload headroom to account for the base performance that is always available.
	scaledCustomerRequest := d.config.ScaleCustomerRequestedPerformance(customerRequest, d.overallWorkloadHeadroom)

	for _, vm := range d.vmsSortedByCost {
		if scaledCustomerRequest.DesiredIOPS <= vm.OntapLimits.IOPS && scaledCustomerRequest.DesiredThroughputInMiBs <= vm.OntapLimits.ThroughputInMiBs && scaledCustomerRequest.DesiredCapacityInGiB <= vm.OntapLimits.CapacityInGiB {
			// The VM satisfies the customer request limits. When provisioning the VM, we need to upscale the customer requested performance by the overheads specified in the VMRSConfig.
			return &vmrs.Decision{
				ChosenVMs:               []string{vm.VMType},
				StoragePoolRequirements: scaledCustomerRequest,
			}, nil
		}
	}

	// No VM satisfies the request
	return nil, &vmrs.NoSuitableVMError{
		Message:         "no suitable VM found for the customer request",
		CustomerRequest: customerRequest,
	}
}

// Compute the overall workload headroom as specified in the VMRSConfig.
func computeWorkloadHeadroom(config *vmrs.VMRSConfig) vmrs.PerfAmplificationFactors {
	totalHeadroom := vmrs.PerfAmplificationFactors{
		IOPS:       0.0,
		Throughput: 0.0,
	}

	// Iterate through each workload overhead and accumulate the amplification factors.
	for _, headroom := range config.HyperscalerPerfLimits.OntapOverheads.WorkloadHeadroom {
		totalHeadroom.IOPS += headroom.Headroom.IOPS
		totalHeadroom.Throughput += headroom.Headroom.Throughput
	}

	return totalHeadroom
}

// sortVMsByCost sorts the VMs in the VMRSConfig based on their relative cost.
func sortVMsByCost(config *vmrs.VMRSConfig, workloadHeadroom vmrs.PerfAmplificationFactors) []vmrs.VMPerfLimit {
	sortedVMs := make([]vmrs.VMPerfLimit, len(config.HyperscalerPerfLimits.DiskPerfLimits[0].QualifiedVMs))
	copy(sortedVMs, config.HyperscalerPerfLimits.DiskPerfLimits[0].QualifiedVMs)

	// Sort the VMs based on their relative cost
	sort.Slice(sortedVMs, func(i, j int) bool {
		return sortedVMs[i].RelativeCost < sortedVMs[j].RelativeCost
	})

	for _, vm := range sortedVMs {
		vm.OntapLimits.IOPS = int64(math.Ceil(float64(vm.OntapLimits.IOPS) * workloadHeadroom.IOPS))
		vm.OntapLimits.ThroughputInMiBs = int64(math.Ceil(float64(vm.OntapLimits.ThroughputInMiBs) * workloadHeadroom.Throughput))
	}

	return sortedVMs
}
