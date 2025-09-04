package slack

import (
	"github.com/spf13/cobra"
)

var SlackCmd = &cobra.Command{
	Use:   "slack",
	Short: "A command to handle all slack functionalities",
}

func init() {
	SlackCmd.AddCommand(NotifyBuildReportCommand)
	SlackCmd.AddCommand(NotifyCommand)
}
