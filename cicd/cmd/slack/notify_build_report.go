package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"main/cmd/jira"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

var NotifyBuildReportCommand = &cobra.Command{
	Use:   "notify_build_report",
	Short: "A command to handle all jira functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		err := notifyBuildReport()
		if err != nil {
			return err
		}
		return nil
	},
}

var (
	webhookURL                      string
	GHCR_vsa_control_plane_HELM_URL string
	GCP_vsa_control_plane_HELM_URL  string
	GHCR_vcp_worker_HELM_URL        string
	GCP_vcp_worker_HELM_URL         string
)

// Implement the logic to notify build report here
func notifyBuildReport() error {
	// Construct the payload
	query := fmt.Sprintf("project in (28302) AND cf[15918] = %s", jira.NewTag)
	jiraBuildInfoLink, err := jira.GetJiraInfo(query)
	if err != nil {
		log.Printf("Error generating JIRA build info link: %v", err)
	}
	if err != nil {
		log.Printf("error getting Jira issue link: %v", err)
		os.Exit(1)
	}
	payload := map[string]interface{}{
		"blocks": []map[string]interface{}{
			{
				"type": "context",
				"elements": []map[string]string{
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf(":white_check_mark: *Generated VSA Control Plane Build: %s*\n", jira.NewTag),
					},
					{
						"type": "mrkdwn",
						"text": "\n\n:package: *HELM_Charts*\n" +
							fmt.Sprintf("%-60s\n`%s`\n", "*VCP Primary Helm Repo*", GHCR_vsa_control_plane_HELM_URL) +
							fmt.Sprintf("%-60s\n`%s`\n", "*VCP Worker Primary Helm Repo*", GHCR_vcp_worker_HELM_URL) +
							fmt.Sprintf("%-60s\n`%s`\n", "*VCP Secondary Helm Repo*", GCP_vsa_control_plane_HELM_URL) +
							fmt.Sprintf("%-60s\n`%s`\n", "*VCP Worker Secondary Helm Repo*", GCP_vcp_worker_HELM_URL) +
							"\n\n",
					},
					{
						"type": "mrkdwn",
						"text": "*Components Built:* Core, Google-Proxy, VCP-DB-Migrate, VCP-Worker",
					},
					{
						"type": "mrkdwn",
						"text": ":link: *Associated JIRA with the Build:*" +
							fmt.Sprintf(" <%s|JIRA QUERY>\n", jiraBuildInfoLink),
					},
				},
			},
		},
	}
	// Convert payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %v", err)
	}

	// Send POST request to Slack webhook
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("error sending Slack message: %v", err)
	}
	defer func(resp *http.Response) {
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				log.Printf("Error closing response body: %v", err)
			}
		}
	}(resp)

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send Slack message. Status: %s", resp.Status)
	}

	log.Println("Slack message sent successfully.")
	return nil
}

func init() {
	webhookURL = os.Getenv("VSABUILDREPORTWEBHOOK")
	GHCR_vsa_control_plane_HELM_URL = os.Getenv("GHCR_vsa_control_plane_HELM_URL")
	GCP_vsa_control_plane_HELM_URL = os.Getenv("GCP_vsa_control_plane_HELM_URL")
	GHCR_vcp_worker_HELM_URL = os.Getenv("GHCR_vcp_worker_HELM_URL")
	GCP_vcp_worker_HELM_URL = os.Getenv("GCP_vcp_worker_HELM_URL")
}
