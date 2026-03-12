package handlers

import (
	"fmt"
)

const (
	// SnaplockPrivilegeLevel is the privilege level required for snaplock operations
	SnaplockPrivilegeLevel = "admin"

	// SnaplockUsername is the username for vserver context in snaplock operations
	SnaplockUsername = "snaplock-user"
)

// BuildSnaplockDeleteCommand constructs the ONTAP CLI command for privileged delete.
// Command format: vserver context -vserver <vserver_name> -username snaplock-user;vol file privileged-delete -file <filepath>
// Note: Once in vserver context, the -vserver flag is not needed for the privileged-delete command
//
// Used by the ogen handler in endpoints/endpoints.go.
func BuildSnaplockDeleteCommand(filePath, vserverName string) string {
	return fmt.Sprintf(
		"vserver context -vserver %s -username %s;vol file privileged-delete -file %s",
		vserverName,
		SnaplockUsername,
		filePath,
	)
}

// BuildSnaplockLegalHoldBeginCommand constructs the ONTAP CLI command for snaplock legal-hold begin.
// Command format: snaplock legal-hold begin -litigation-name <name> -volume <volume_name> -path <path> -vserver <vserver_name>
func BuildSnaplockLegalHoldBeginCommand(litigationName, volumeName, path, vserverName string) string {
	return fmt.Sprintf(
		"vserver context -vserver %s -username %s;snaplock legal-hold begin -litigation-name %s -volume %s -path %s",
		vserverName, SnaplockUsername, litigationName, volumeName, path,
	)
}

// BuildSnaplockLegalHoldEndPathCommand constructs the ONTAP CLI command for snaplock legal-hold end on a path.
// Command format: snaplock legal-hold end -litigation-name <name> -volume <volume_name> -path <path> -vserver <vserver_name>
func BuildSnaplockLegalHoldEndPathCommand(litigationName, volumeName, path, vserverName string) string {
	return fmt.Sprintf(
		"vserver context -vserver %s -username %s;snaplock legal-hold end -litigation-name %s -volume %s -path %s",
		vserverName, SnaplockUsername, litigationName, volumeName, path,
	)
}

// BuildSnaplockLegalHoldAbortCommand constructs the ONTAP CLI command for snaplock legal-hold abort.
// Command format: vserver context ... ; snaplock legal-hold abort -operation-id <id>
func BuildSnaplockLegalHoldAbortCommand(operationID, vserverName string) string {
	return fmt.Sprintf(
		"vserver context -vserver %s -username %s;snaplock legal-hold abort -operation-id %s",
		vserverName, SnaplockUsername, operationID,
	)
}

// BuildSnaplockLegalHoldShowCommand constructs the ONTAP CLI command to list legal-hold operations/litigations.
// Command format: snaplock legal-hold show -vserver <vserver_name> -volume <volume_name> -instance
func BuildSnaplockLegalHoldShowCommand(vserverName, volumeName string) string {
	return fmt.Sprintf(
		"vserver context -vserver %s -username %s;snaplock legal-hold show -volume %s -instance",
		vserverName, SnaplockUsername, volumeName,
	)
}

// BuildSnaplockLegalHoldShowForLitigationCommand constructs the ONTAP CLI command to show a single litigation.
// Command format: snaplock legal-hold show -vserver <vserver> -volume <volume> -litigation-name <name> -instance
func BuildSnaplockLegalHoldShowForLitigationCommand(vserverName, volumeName, litigationName string) string {
	return fmt.Sprintf(
		"vserver context -vserver %s -username %s;snaplock legal-hold show -volume %s -litigation-name %s -instance",
		vserverName, SnaplockUsername, volumeName, litigationName,
	)
}

// BuildSnaplockLegalHoldShowOperationCommand constructs the ONTAP CLI command to show a single legal-hold operation status.
// Command format: snaplock legal-hold show -operation-id <id> -instance
func BuildSnaplockLegalHoldShowOperationCommand(operationID string) string {
	return fmt.Sprintf("vserver context -username %s;snaplock legal-hold show -operation-id %s -instance", SnaplockUsername, operationID)
}
