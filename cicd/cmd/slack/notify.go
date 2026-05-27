package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

var NotifyCommand = &cobra.Command{
	Use:   "notify",
	Short: "A command to notify on slack",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		err := notify()
		if err != nil {
			return err
		}
		return nil
	},
}

var (
	WEBHOOK string
	JSON    string
	URL     string
	MESSAGE string
)

// Implement the logic to notify build report here
func notify() error {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(JSON), &obj); err != nil {
		panic(err)
	}
	// Construct the payload
	payload := map[string]interface{}{
		"blocks": []map[string]interface{}{
			{
				"type": "context",
				"elements": []map[string]string{
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf("*Message:* %s\n", MESSAGE),
					},
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf(":link: *URL:* %s\n", URL),
					},
					{
						"type": "mrkdwn",
						"text": utils.PrintObject(obj),
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
	resp, err := http.Post(WEBHOOK, "application/json", bytes.NewBuffer(payloadBytes))
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
	WEBHOOK = os.Getenv("WEBHOOK")
	URL = os.Getenv("URL")
	MESSAGE = os.Getenv("MESSAGE")
	JSON = os.Getenv("JSON")
}
