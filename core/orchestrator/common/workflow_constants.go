package common

// Control workflow ID format constants

const (
	// PoolDataSubnetCreateDelete is a placeholder used for sequence workflow instance that runs all
	// subnet create and delete operations for a specific account and VPC sequentially.
	PoolDataSubnetCreateDelete = "Account_%d_VPC_%s_Ops_PoolDataSubnet-CD"

	// PoolOperationsSeq is a placeholder used for sequence workflow instance that runs all
	// pool-level operations (certificate rotation, password rotation, updates, deletes, etc.) sequentially.
	// This ensures only one workflow operates on a pool at a time, preventing race conditions.
	PoolOperationsSeq = "Pool_%s_Ops_All"

	// Signal is the name of the signal used to call sequential workflows.
	Signal = "req"

	// Workflow name constants for use with control workflows
	// These are used to reference workflows by name when executing them sequentially
	RotatePoolCertificateWorkflowName = "RotatePoolCertificateWorkflow"
	RotatePoolPasswordWorkflowName    = "RotatePoolPasswordWorkflow"
)

