package release_cmd

import (
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"main/release-cmd/tag"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "vsactl",
	Short: "A cli used to control vsa cicd controller",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	_ = godotenv.Load()
	rootCmd.AddCommand(tag.TagCmd)
}
