package cmd

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "tatctl",
	Short: "sanity checks CLI application",
	Long:  `sanity checks CLI application`,
}
var Log = logrus.New()

// var destinaPubSubTopic string = "buddy-cli-url-consumer"
// var destinaPubSubTopicProject string = "netapp-us-e4-autopush-sde-sqa"

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		Log.Errorf("Error is executing the root command: %v", err)
		os.Exit(1)
	}
}

func init() {
	Log.SetLevel(logrus.InfoLevel)

	// Set the output to stdout
	Log.SetOutput(os.Stdout)

	// Set the log format to JSON (optional)
	Log.SetFormatter(&logrus.JSONFormatter{})

	// Add subcommands to the root command
	rootCmd.AddCommand(sanityCmd)
}
