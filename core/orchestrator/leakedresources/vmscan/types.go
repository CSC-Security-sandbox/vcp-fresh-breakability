// Package vmscan holds the input/output contract and Temporal workflow name
// shared by the VM-leak detector (caller, runs in core pod) and the
// scan-GCE-instances workflow + activity (callee, runs in vcp-background-worker pod).
//
// This package intentionally has no heavy dependencies so both sides can
// import it without pulling in the full orchestrator graph and without
// risking an import cycle between detectors and background workflows.
package vmscan

// WorkflowName is the Temporal workflow type registered on the worker side
// and submitted by the detector. Using a constant string lets the detector
// submit the workflow without importing the workflow function symbol.
const WorkflowName = "ScanGCEInstancesWorkflow"

// ScanInput is the request payload for ScanGCEInstancesWorkflow.
// ProjectIDs is the deduplicated list of GCP tenant projects to scan
// (collected from pools — including soft-deleted — by the detector).
type ScanInput struct {
	ProjectIDs []string `json:"projectIds"`
}

// ScanOutput is the response payload from ScanGCEInstancesWorkflow.
// Items contains one entry per VM observed in the scanned projects.
// PartialFailures lists projects that returned an error from the Compute API;
// the workflow does not fail the whole run when a subset of projects errors,
// it returns whatever it could collect plus the failure list so the detector
// can decide how to react.
type ScanOutput struct {
	Items           []GCEInstanceItem `json:"items"`
	PartialFailures []ProjectFailure  `json:"partialFailures,omitempty"`
}

// GCEInstanceItem is the minimal VM projection returned to the detector.
// Only fields the detector actually needs to make a leak decision are
// included to keep the Temporal payload small. Labels is returned verbatim
// so the detector can apply policy by reading whichever label key it cares
// about (pool_uuid, deployment_id, etc.) without the activity needing to know.
type GCEInstanceItem struct {
	Project           string            `json:"project"`
	Zone              string            `json:"zone"`
	Name              string            `json:"name"`
	SelfLink          string            `json:"selfLink"`
	Status            string            `json:"status"`
	MachineType       string            `json:"machineType,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
}

// ProjectFailure records a per-project error from the Compute API so the
// detector can surface partial scans in logs/metrics.
type ProjectFailure struct {
	Project string `json:"project"`
	Error   string `json:"error"`
}
