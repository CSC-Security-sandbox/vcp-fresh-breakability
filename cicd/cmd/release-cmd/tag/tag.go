package tag

import (
	"github.com/spf13/cobra"
)

var TagCmd = &cobra.Command{
	Use:   "tag",
	Short: "Entry point for tag related commands",
}

func init() {
	TagCmd.AddCommand(promotionCmd)
	TagCmd.AddCommand(releaseCmd)
}
