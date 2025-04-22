package jira

import (
	"github.com/spf13/cobra"
)

var allowMergeCmd = &cobra.Command{
	Use:   "allow_merge",
	Short: "Command to check if a PR is allowed to be merged with basic jira validations",
}

func init() {
	allowMergeCmd.AddCommand(allowMainCmd)
}
