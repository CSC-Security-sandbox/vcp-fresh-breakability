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
