package common

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func SendSlackCard(message string, testName string, slackChannel, environemnt string, success bool) error {
	// Guard: if upstream forgot to set message (shadowed variable), avoid empty Slack card.
	if strings.TrimSpace(message) == "" {
		message = "No test suite summary (message was empty)."
	}

	token := os.Getenv("SLACK_TOKEN")
	channel := slackChannel
	if token == "" || channel == "" {
		return errors.New("missing SLACK_TOKEN or SLACK_CHANNEL_ID env")
	}
	titlePrefix := "BILLING ONBOARDING SANITY CHECKS RESULTS"
	if strings.Contains(strings.ToLower(testName), "crr") {
		titlePrefix = "CRR BILLING SANITY CHECKS RESULTS"
	} else if strings.Contains(strings.ToLower(testName), "backup") {
		titlePrefix = "BACKUP BILLING SANITY CHECKS RESULTS"
	} else if strings.Contains(strings.ToLower(testName), "pool") {
		titlePrefix = "POOL BILLING SANITY CHECKS RESULTS"
	} else if strings.Contains(strings.ToLower(testName), "volume") {
		titlePrefix = "VOLUME BILLING SANITY CHECKS RESULTS"
	} else if strings.Contains(strings.ToLower(testName), "at") {
		titlePrefix = "AT BILLING SANITY CHECKS RESULTS"
	}

	color := "#2eb886"
	title := fmt.Sprintf("%s - ✅ SUCCESS", titlePrefix)
	if !success {
		color = "#e01e5a"
		title = fmt.Sprintf("%s - ❌ FAILED", titlePrefix)
	}

	// Default URL
	tdsDocURL := os.Getenv("TDS_DOC_URL")
	if strings.TrimSpace(tdsDocURL) == "" {
		tdsDocURL = "https://confluence.example.com/display/TDS/Billing+Sanity+Checks"
	}

	// Try to find the link, fallback if not found
	link, ok := DocsMap[titlePrefix]
	if !ok || strings.TrimSpace(link) == "" {
		link = tdsDocURL // fallback to dummy/default link if not found
	}

	tdsDocTitle := strings.ToLower(titlePrefix)
	tdsDocTitle = strings.ReplaceAll(tdsDocTitle, "results", "")
	tdsDocTitle = tdsDocTitle + " - doc"

	payload := map[string]any{
		"channel": channel,
		"attachments": []map[string]any{
			{
				"color":     color,
				"title":     title,
				"text":      message,
				"footer":    "Sanity Tests",
				"ts":        time.Now().Unix(),
				"mrkdwn_in": []string{"fields", "text"},
				"fields": []map[string]string{
					{"title": "ENVIRONMENT", "value": environemnt, "short": "true"},
					{"title": "HOST", "value": "cloud run job", "short": "true"},
					{"title": "TEST PATH", "value": testName, "short": "false"},
					{"title": "TDS DOC", "value": fmt.Sprintf("<%s|%s>", link, tdsDocTitle), "short": "false"},
				},
			},
		},
	}

	b, err := jsonMarshal(payload)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", "https://slack.com/api/chat.postMessage", bytesNewReader(b))
	if err != nil {
		return fmt.Errorf("request build failed: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("slack post failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("slack api status %d: %s", resp.StatusCode, string(body))
	}
	if !bytes.Contains(body, []byte(`"ok":true`)) {
		return fmt.Errorf("slack api error: %s", string(body))
	}
	return nil
}

// Helpers (avoid new imports shadowing).
func hostnameSafe() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// Wrappers to use already imported packages without adding new import lines.
func jsonMarshal(v any) ([]byte, error) {
	type jsonPkg struct{}
	// Need encoding/json; emulate minimal marshal via stdlib reflection not feasible,
	// so rely on encoding/json using blank import assumption.
	return encodingJSONMarshal(v)
}

// Below indirections allow using encoding/json & bytes if added to imports.
var (
	encodingJSONMarshal = func(v any) ([]byte, error) { return json.Marshal(v) }
	bytesNewReader      = func(b []byte) *bytes.Reader { return bytes.NewReader(b) }
)

// NOTE: Add to import block if missing: "encoding/json", "bytes", "net/http"
