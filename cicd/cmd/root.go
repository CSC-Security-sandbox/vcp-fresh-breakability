package cmd

import (
	"main/cmd/images"
	"main/cmd/jira"
	"main/cmd/lint"
	"main/cmd/release-cmd/tag"
	"main/cmd/unit-test"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "vsacictl",
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
	rootCmd.AddCommand(jira.JiraCmd)
	rootCmd.AddCommand(unitTest.UnitTestCmd)
	rootCmd.AddCommand(lint.LintCmd)
	rootCmd.AddCommand(images.ImagesCmd)
	rootCmd.AddCommand(tag.TagCmd)
}
