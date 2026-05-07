// Package ipscan holds the input/output contract and Temporal workflow name
// shared by the internal-reserved-IP detector (caller, runs in core pod) and
// the scan-regional-addresses workflow + activity (callee, runs in
// vcp-background-worker pod).
//
// This package intentionally has no heavy dependencies so both sides can
// import it without pulling in the full orchestrator graph and without
// risking an import cycle between detectors and background workflows.
package ipscan

import (
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
)

// WorkflowName is the Temporal workflow type registered on the worker side
// and submitted by the detector. Using a constant string lets the detector
// submit the workflow without importing the workflow function symbol.
const WorkflowName = "ScanRegionalAddressesWorkflow"

// ProjectRegion is one (project, region) pair that the activity should hit
// with Compute API's regional Addresses.List endpoint.
type ProjectRegion struct {
	Project string `json:"project"`
	Region  string `json:"region"`
}

// ScanInput is the request payload for ScanRegionalAddressesWorkflow.
// Targets is the deduplicated list of (project, region) pairs the detector
// derived from active pool subnets — addresses are listed per-region, not
// globally, so the detector enumerates the pairs up front.
type ScanInput struct {
	Targets []ProjectRegion `json:"targets"`
}

// ScanResult bundles the addresses returned for a single (project, region)
// pair. Keeping the grouping lets the detector reuse its existing per-pair
// policy loop instead of re-grouping a flat slice.
type ScanResult struct {
	Project   string                                       `json:"project"`
	Region    string                                       `json:"region"`
	Addresses []hyperscalerleakedresources.RegionalAddress `json:"addresses,omitempty"`
}

// ScanOutput is the response payload from ScanRegionalAddressesWorkflow.
// Failures on individual (project, region) pairs are recorded in
// PartialFailures and do not abort the whole scan.
type ScanOutput struct {
	Results         []ScanResult           `json:"results"`
	PartialFailures []ProjectRegionFailure `json:"partialFailures,omitempty"`
}

// ProjectRegionFailure records a per-(project,region) error from the
// Compute API so the detector can surface partial scans in logs/metrics.
type ProjectRegionFailure struct {
	Project string `json:"project"`
	Region  string `json:"region"`
	Error   string `json:"error"`
}
