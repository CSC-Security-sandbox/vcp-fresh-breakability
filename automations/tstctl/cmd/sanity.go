package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/tstctl/common"
)

// Production-grade support utilities and constants for billing command.
// These helpers perform early validation and provide safer execution patterns
// without expanding external dependencies.

const (
	defaultRepoURL = "github.com/VCP-VSA-control-Plane/vsa-cp-cd.git"
	// Increased timeout to 3 hours (10800 seconds) to allow long-running test script to complete.
	timeOut = 10800
)

// preflight validates required flags and environment prior to execution.
// It is safe to call multiple times.
func preflight() error {
	if pat := os.Getenv("GITHUB_PAT"); pat == "" {
		return fmt.Errorf("GITHUB_PAT environment variable not set")
	}
	return nil
}

// withTimeout returns a cancellable context bounded by scriptTimeout seconds.
// withTimeout returns a cancellable context bounded by the script timeout (10800 seconds = 3 hours).
func withTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeOut*time.Second)
}

// cloneRepo wraps common.CloneRepo with clearer error surface.
func cloneRepo(branch string) error {
	if err := common.CloneRepo(defaultRepoURL, branch, os.Getenv("GITHUB_PAT")); err != nil {
		return fmt.Errorf("clone failed: %w", err)
	}
	return nil
}

// runTests executes the test script; it never returns a fatal error,
// leaving decision-making to caller.
func runTests(ctx context.Context) error {
	return common.RunShellScriptCtx(ctx, "./run-tests.sh", testSuitePath, cfgPath)
}

// processResults reads XML and posts Slack updates.
// Errors are logged and aggregated minimally in a single returned error.
// multiError aggregates multiple errors.
type multiError []error

func (m multiError) Error() string {
	if len(m) == 1 {
		return m[0].Error()
	}
	s := "multiple errors:"
	for _, e := range m {
		s += "\n - " + e.Error()
	}
	return s
}

func processResults() error {
	const reportPath = "vsa-cp-cd/out.xml"

	msg, ok, xmlErr := common.ReadOutputXML(reportPath)
	var errs []error

	if xmlErr != nil {
		if os.IsNotExist(xmlErr) {
			Log.Warnf("report not found: %v", xmlErr)
			msg = "Tests ended prematurely; no report generated."
			ok = false
		} else {
			Log.Warnf("report read error: %v", xmlErr)
			errs = append(errs, fmt.Errorf("read xml report: %w", xmlErr))
			if msg == "" {
				msg = "Test results unavailable due to report read error."
				ok = false
			}
		}
	}

	if slackErr := common.SendSlackCard(msg, testSuitePath, slackChannel, environment, ok); slackErr != nil {
		Log.Warnf("slack send error (channel=%s env=%s): %v", slackChannel, environment, slackErr)
		errs = append(errs, fmt.Errorf("send slack: %w", slackErr))
	}

	if len(errs) == 0 {
		return nil
	}
	return multiError(errs)
}

var (
	cfgPath       string
	testSuitePath string
	branch        string
	slackChannel  string
	environment   string
	poolId        string
	jsonInput     string
)

var cfg common.PoolConfig

func init() {
	rootCmd.AddCommand(sanityCmd)

	sanityCmd.Flags().StringVarP(&cfgPath, "config", "c", "", "Config file path")
	sanityCmd.Flags().StringVarP(&testSuitePath, "tests", "t", "", "Test suite path")
	sanityCmd.Flags().StringVarP(&branch, "branch", "b", "main", "Git branch to clone")
	sanityCmd.Flags().StringVarP(&slackChannel, "slack-channel", "s", "C09HUEEJ3NE", "Slack channel ID to post results to")
	sanityCmd.Flags().StringVarP(&environment, "environment", "e", "", "Target environment for tests (default: CLOUD_RUN_ENVIRONMENT or ap-tst-us-c1)")
	sanityCmd.Flags().StringVarP(&poolId, "pool-id", "p", "", "Pool ID for test resources")
	sanityCmd.Flags().StringVarP(&jsonInput, "jsonInput", "j", "", "JSON string input")
}

var sanityCmd = &cobra.Command{
	Use:   "sanity",
	Short: "Runs sanity checks",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := preflight(); err != nil {
			return err
		}
		// Resolve environment: flag > CLOUD_RUN_ENVIRONMENT > default
		if environment == "" {
			environment = os.Getenv("CLOUD_RUN_ENVIRONMENT")
			if environment == "" {
				environment = "ap-tst-us-c1"
			}
		}

		if jsonInput != "" {
			// Normalize and safely unquote JSON input if the runner wrapped it in quotes.
			jsonInput = strings.TrimSpace(jsonInput)

			// Try Go-style Unquote (handles double-quoted and escaped strings).
			// Only remove a single layer of wrapping quotes.
			if unquoted, err := strconv.Unquote(jsonInput); err == nil {
				jsonInput = unquoted
			} else if strings.HasPrefix(jsonInput, "'") && strings.HasSuffix(jsonInput, "'") && len(jsonInput) >= 2 {
				// Handle single-quoted payloads commonly produced by some runners.
				jsonInput = jsonInput[1 : len(jsonInput)-1]
			}

			// Extract only the JSON object substring (between the first '{' and last '}'),
			// to ignore any extra text around the payload (e.g., logs or error messages).
			if start := strings.Index(jsonInput, "{"); start != -1 {
				if end := strings.LastIndex(jsonInput, "}"); end != -1 && end >= start {
					jsonInput = jsonInput[start : end+1]
				}
			}

			// Validate non-empty JSON after normalization.
			if strings.TrimSpace(jsonInput) == "" {
				return fmt.Errorf("invalid JSON: empty input after normalization")
			}

			Log.Infof("Received JSON input: %s", jsonInput)

			if err := json.Unmarshal([]byte(jsonInput), &cfg); err != nil {
				return fmt.Errorf("invalid JSON: %w", err)
			}

			Log.Infof("Parsed config: %+v", cfg)
			common.SetPoolConfigEnv(cfg)
			// Here you can add code to parse and utilize the JSON input as needed.
		}

		Log.Infof("Config: %s", cfgPath)
		Log.Infof("Test suite: %s", testSuitePath)

		if err := cloneRepo(branch); err != nil {
			return err
		}
		Log.Info("Repository cloned successfully")

		ctx, cancel := withTimeout(context.Background())
		defer cancel()
		if poolId != "" {
			os.Setenv("POOL_ID", poolId)
		}
		if err := runTests(ctx); err != nil {
			Log.Warnf("script failed (continuing): %v", err)
		} else {
			Log.Info("script completed")
		}

		if err := processResults(); err != nil {
			Log.Warnf("result processing encountered issues: %v", err)
			return err
		}

		return nil
	},
}
