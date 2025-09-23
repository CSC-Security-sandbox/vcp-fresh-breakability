package activities

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const AggregateStateOnline = "online"

var maxConstituentsPerAggregate = env.GetInt64("MAX_CONSTITUENTS_PER_AGGREGATE", 1000)

var availableAggregateStates = []string{AggregateStateOnline}

// CalculateAggregatesForConstituentVolumesWithSpaceLimits calculates the optimal distribution of constituent volumes
// across available aggregates using an optimized greedy approach with first and second maxima tracking.
// Returns the aggregate distribution result containing the list of aggregates and multiplier.
func CalculateAggregatesForConstituentVolumesWithSpaceLimits(ctx context.Context, aggregates []*vsa.Aggregate, largeVolumeConstituentCount, size int64, totalNodes int) (*models.AggregateDistributionResult, error) {
	logger := util.GetLogger(ctx)
	expectedAggregateCount := totalNodes / 2

	if largeVolumeConstituentCount <= 0 {
		return nil, fmt.Errorf("constituent volume count must be greater than zero")
	}

	// Validate total aggregates is same as number of HA pairs
	if len(aggregates) != expectedAggregateCount {
		return nil, fmt.Errorf("expected exactly %d aggregates, got %d", expectedAggregateCount, len(aggregates))
	}

	// Build map of aggregate name to available CVs based on size
	aggregateNameToAvailableCvMap := make(map[string]int64)
	availableAggregates := make([]string, 0, len(aggregates))
	totalAvailableCapacity := int64(0)

	// Get the size of each CV based on total size and constituent count
	cvSizeInBytes := size / largeVolumeConstituentCount
	for _, agg := range aggregates {
		// Check if any aggregate is not available - return error immediately
		if !utils.ContainsString(availableAggregateStates, agg.State) {
			return nil, fmt.Errorf("aggregate %s is not online (state: %s), all aggregates must be online", agg.Name, agg.State)
		}

		if agg.VolumeCount < maxConstituentsPerAggregate {
			agg.AvailableSize = agg.Size - agg.TotalProvisionedSize
			if agg.AvailableSize <= 0 {
				continue // No available space
			}

			// Based on a cv size find out how many more cvs can fit in this aggregate
			availableCvs := (agg.AvailableSize) / cvSizeInBytes
			if availableCvs > maxConstituentsPerAggregate-agg.VolumeCount {
				availableCvs = maxConstituentsPerAggregate - agg.VolumeCount
			}
			aggregateNameToAvailableCvMap[agg.Name] = availableCvs
			availableAggregates = append(availableAggregates, agg.Name)
			totalAvailableCapacity += availableCvs
		}
	}

	if len(availableAggregates) == 0 {
		return nil, fmt.Errorf("no aggregates with available capacity (all have reached max %d constituents)", maxConstituentsPerAggregate)
	}

	// Check if we can serve the customer request
	if largeVolumeConstituentCount > totalAvailableCapacity {
		return nil, fmt.Errorf("insufficient total aggregate capacity: requested %d CVs, but only %d capacity available across all aggregates",
			largeVolumeConstituentCount, totalAvailableCapacity)
	}

	// Create map of aggregate names to CVs placed
	aggregateDistribution := make(map[string]int64)

	// Optimized approach: maintain first and second maxima
	remaining := largeVolumeConstituentCount
	for remaining > 0 {
		// Find first and second maxima
		firstMax, secondMax := findFirstAndSecondMaxima(availableAggregates, aggregateNameToAvailableCvMap, maxConstituentsPerAggregate)
		logger.Debugf("Aggregate with available CVs: %v", aggregateNameToAvailableCvMap)
		logger.Debugf("First maxima: %s (%d CVs), Second maxima: %s (%d CVs)", firstMax, aggregateNameToAvailableCvMap[firstMax], secondMax, aggregateNameToAvailableCvMap[secondMax])

		if firstMax == "" {
			break // No more capacity available
		}

		// Calculate how many CVs we can place in first minima aggregates
		var firstMaxAggregates []string
		for _, aggName := range availableAggregates {
			if aggregateNameToAvailableCvMap[aggName] == aggregateNameToAvailableCvMap[firstMax] && aggregateNameToAvailableCvMap[aggName] > 0 {
				firstMaxAggregates = append(firstMaxAggregates, aggName)
			}
		}

		// Place CVs until firstMaxima reaches secondMaxima
		targetLevel := int64(0)
		if secondMax != "" {
			targetLevel = aggregateNameToAvailableCvMap[secondMax]
		}

		cvsToPlace := (aggregateNameToAvailableCvMap[firstMax] - targetLevel) * int64(len(firstMaxAggregates))
		if cvsToPlace > remaining {
			cvsToPlace = remaining
		}

		// Distribute CVs evenly among first minima aggregates
		cvsPerAggregate := cvsToPlace / int64(len(firstMaxAggregates))
		extraCvs := cvsToPlace % int64(len(firstMaxAggregates))

		for i, aggName := range firstMaxAggregates {
			cvs := cvsPerAggregate
			if int64(i) < extraCvs {
				cvs++
			}

			aggregateNameToAvailableCvMap[aggName] -= cvs
			remaining -= cvs
			// Update distribution map to know how many cvs have been placed in each aggregate
			aggregateDistribution[aggName] += cvs
		}
	}

	logger.Debugf("Final aggregate distribution: %v", aggregateDistribution)

	// Calculate HCF of all CV counts
	hcf := calculateHCF(aggregateDistribution)

	// Create flattened result based on HCF
	var result []string
	for aggName, cvCount := range aggregateDistribution {
		occurrences := cvCount / hcf
		for i := int64(0); i < occurrences; i++ {
			result = append(result, aggName)
		}
	}

	return &models.AggregateDistributionResult{
		Aggregates:     result,
		AggrMultiplier: hcf,
	}, nil
}

// CalculateAggregatesForConstituentVolumesWithCVLimits calculates the optimal distribution of constituent volumes
// across available aggregates using an optimized greedy approach with first and second minima tracking.
// Returns the aggregate distribution result containing the list of aggregates and multiplier.
func CalculateAggregatesForConstituentVolumesWithCVLimits(ctx context.Context, aggregates []*vsa.Aggregate, largeVolumeConstituentCount int64, totalNodes int) (*models.AggregateDistributionResult, error) {
	logger := util.GetLogger(ctx)
	const maxConstituentsPerAggregate int64 = 200
	expectedAggregateCount := totalNodes / 2

	// Validate that we have exactly 12 aggregates
	if len(aggregates) != expectedAggregateCount {
		return nil, fmt.Errorf("expected exactly %d aggregates, got %d", expectedAggregateCount, len(aggregates))
	}
	// Build aggregate state: map of aggregate name to current CV count
	aggregateState := make(map[string]int64)
	availableAggregates := make([]string, 0, len(aggregates))
	totalAvailableCapacity := int64(0)

	for _, agg := range aggregates {
		// Check if any aggregate is not available - return error immediately
		if !utils.ContainsString(availableAggregateStates, agg.State) {
			return nil, fmt.Errorf("aggregate %s is not online (state: %s), all aggregates must be online", agg.Name, agg.State)
		}

		if agg.VolumeCount < maxConstituentsPerAggregate {
			aggregateState[agg.Name] = agg.VolumeCount
			availableAggregates = append(availableAggregates, agg.Name)
			totalAvailableCapacity += maxConstituentsPerAggregate - agg.VolumeCount
		}
	}

	if len(availableAggregates) == 0 {
		return nil, fmt.Errorf("no aggregates with available capacity (all have reached max %d constituents)", maxConstituentsPerAggregate)
	}

	// Check if we can serve the customer request
	if largeVolumeConstituentCount > totalAvailableCapacity {
		return nil, fmt.Errorf("insufficient total aggregate capacity: requested %d CVs, but only %d capacity available across all aggregates",
			largeVolumeConstituentCount, totalAvailableCapacity)
	}

	// Create map of aggregate names to CVs placed
	aggregateDistribution := make(map[string]int64)

	// Optimized approach: maintain first and second minima
	remaining := largeVolumeConstituentCount

	for remaining > 0 {
		// Find first and second minima
		firstMin, secondMin := findFirstAndSecondMinima(availableAggregates, aggregateState, maxConstituentsPerAggregate)
		logger.Debugf("Current Aggregate capacities: %v", aggregateState)
		logger.Debugf("First minima: %s (%d CVs), Second minima: %s (%d CVs)", firstMin, aggregateState[firstMin], secondMin, aggregateState[secondMin])

		if firstMin == "" {
			break // No more capacity available
		}

		// Calculate how many CVs we can place in first minima aggregates
		var firstMinAggregates []string
		for _, aggName := range availableAggregates {
			if aggregateState[aggName] == aggregateState[firstMin] && aggregateState[aggName] < maxConstituentsPerAggregate {
				firstMinAggregates = append(firstMinAggregates, aggName)
			}
		}

		// Place CVs until firstMinima reaches secondMinima or we run out of CVs
		targetLevel := maxConstituentsPerAggregate
		if secondMin != "" {
			targetLevel = aggregateState[secondMin]
		}

		cvsToPlace := (targetLevel - aggregateState[firstMin]) * int64(len(firstMinAggregates))
		if cvsToPlace > remaining {
			cvsToPlace = remaining
		}

		// Distribute CVs evenly among first minima aggregates
		cvsPerAggregate := cvsToPlace / int64(len(firstMinAggregates))
		extraCvs := cvsToPlace % int64(len(firstMinAggregates))

		for i, aggName := range firstMinAggregates {
			cvs := cvsPerAggregate
			if int64(i) < extraCvs {
				cvs++
			}
			aggregateState[aggName] += cvs
			remaining -= cvs
			// Update distribution map to know how many cvs have been placed in each aggregate
			aggregateDistribution[aggName] += cvs
		}
	}

	logger.Debugf("Final aggregate distribution: %v", aggregateDistribution)

	// Calculate HCF of all CV counts
	hcf := calculateHCF(aggregateDistribution)

	// Create flattened result based on HCF
	var result []string
	for aggName, cvCount := range aggregateDistribution {
		occurrences := cvCount / hcf
		for i := int64(0); i < occurrences; i++ {
			result = append(result, aggName)
		}
	}

	return &models.AggregateDistributionResult{
		Aggregates:     result,
		AggrMultiplier: hcf,
	}, nil
}

// findFirstAndSecondMinima finds aggregates with minimum and second minimum volume counts
func findFirstAndSecondMinima(aggregates []string, aggregateState map[string]int64, maxCapacity int64) (string, string) {
	var firstMin, secondMin string
	var firstMinVal, secondMinVal int64 = maxCapacity, maxCapacity

	for _, aggName := range aggregates {
		if aggregateState[aggName] >= maxCapacity {
			continue
		}

		count := aggregateState[aggName]
		if count < firstMinVal {
			secondMin = firstMin
			secondMinVal = firstMinVal
			firstMin = aggName
			firstMinVal = count
		} else if count < secondMinVal && count > firstMinVal {
			secondMin = aggName
			secondMinVal = count
		}
	}

	return firstMin, secondMin
}

func findFirstAndSecondMaxima(aggregates []string, aggregateNameToAvailableCvMap map[string]int64, maxCapacity int64) (string, string) {
	var firstMax, secondMax string
	var firstMaxVal, secondMaxVal int64

	for _, aggName := range aggregates {
		if aggregateNameToAvailableCvMap[aggName] <= 0 {
			continue
		}

		count := aggregateNameToAvailableCvMap[aggName]
		if count > firstMaxVal {
			secondMax = firstMax
			secondMaxVal = firstMaxVal
			firstMax = aggName
			firstMaxVal = count
		} else if count > secondMaxVal && count < firstMaxVal {
			secondMax = aggName
			secondMaxVal = count
		}
	}

	return firstMax, secondMax
}

// calculateHCF calculates the highest common factor (GCD) of all values in the distribution map
func calculateHCF(distribution map[string]int64) int64 {
	if len(distribution) == 0 {
		return 1
	}

	var values []int64
	for _, count := range distribution {
		values = append(values, count)
	}

	result := values[0]
	for i := 1; i < len(values); i++ {
		result = gcd(result, values[i])
		if result == 1 {
			break // Early exit if GCD becomes 1
		}
	}

	return result
}

// gcd calculates the greatest common divisor of two numbers
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
