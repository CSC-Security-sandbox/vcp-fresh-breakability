package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func runVerifyGitHub() error {
	if cfg.GitHub.Token == "" {
		return fmt.Errorf("GitHub token not configured. Set GITHUB_TOKEN environment variable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get authenticated user
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+cfg.GitHub.Token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to get user info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to authenticate: status %d", resp.StatusCode)
	}

	var user struct {
		Login string `json:"login"`
		Email string `json:"email"`
		Name  string `json:"name"`
		Type  string `json:"type"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return fmt.Errorf("failed to decode user info: %w", err)
	}

	// Get user's GPG keys
	req2, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user/gpg_keys", nil)
	if err != nil {
		return fmt.Errorf("failed to create GPG keys request: %w", err)
	}

	req2.Header.Set("Authorization", "Bearer "+cfg.GitHub.Token)
	req2.Header.Set("Accept", "application/vnd.github.v3+json")

	resp2, err := client.Do(req2)
	if err != nil {
		return fmt.Errorf("failed to get GPG keys: %w", err)
	}
	defer resp2.Body.Close()

	var gpgKeys []struct {
		ID     int    `json:"id"`
		KeyID  string `json:"key_id"`
		Emails []struct {
			Email    string `json:"email"`
			Verified bool   `json:"verified"`
		} `json:"emails"`
		CanSign   bool `json:"can_sign"`
		CanVerify bool `json:"can_verify"`
	}

	if err := json.NewDecoder(resp2.Body).Decode(&gpgKeys); err != nil {
		return fmt.Errorf("failed to decode GPG keys: %w", err)
	}

	printBox("GITHUB TOKEN VERIFICATION")
	logger.Info("")
	logger.Info(fmt.Sprintf("  User: %s (%s)\n", user.Login, user.Name))
	logger.Info(fmt.Sprintf("  Type: %s\n", user.Type))
	logger.Info(fmt.Sprintf("  Email: %s\n", user.Email))
	logger.Info("")
	logger.Info(fmt.Sprintf("  GPG Keys: %d configured\n", len(gpgKeys)))
	logger.Info("")

	if len(gpgKeys) == 0 {
		logger.Info("  [WARNING] No GPG keys found\n")
		logger.Info("  Commits will be signed by GitHub's web-flow key\n")
		logger.Info("")
		logger.Info("  To add a GPG key:\n")
		logger.Info("    1. Generate: gpg --full-generate-key\n")
		logger.Info("    2. Export: gpg --armor --export YOUR_KEY_ID\n")
		logger.Info("    3. Add at: https://github.com/settings/keys\n")
	} else {
		for i, key := range gpgKeys {
			logger.Info(fmt.Sprintf("  Key %d:\n", i+1))
			logger.Info(fmt.Sprintf("    ID: %s\n", key.KeyID))
			logger.Info(fmt.Sprintf("    Can Sign: %v\n", key.CanSign))
			logger.Info(fmt.Sprintf("    Can Verify: %v\n", key.CanVerify))
			logger.Info("    Emails:\n")
			for _, email := range key.Emails {
				verifiedStatus := "verified"
				if !email.Verified {
					verifiedStatus = "unverified"
				}
				logger.Info(fmt.Sprintf("      - %s (%s)\n", email.Email, verifiedStatus))
			}
			logger.Info("")
		}

		logger.Info("  [OK] GPG keys configured\n")
		logger.Info("  Commits via SafeSQL will be signed\n")
	}

	logger.Info("")
	logger.Info(fmt.Sprintf("  Repository: %s\n", cfg.GitHub.Repository))
	logger.Info("")

	if user.Type != "User" {
		logger.Info("  [WARNING] Token type is not 'User'\n")
		logger.Info("  GPG signing may not work with App tokens\n")
		logger.Info("  Use a Personal Access Token for GPG signing\n")
	}

	return nil
}
