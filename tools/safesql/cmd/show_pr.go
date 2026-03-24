package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/github"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
)

func runShowForPR(prNumber int, asJSON bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Parse repository from config
	parts := strings.SplitN(cfg.GitHub.Repository, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid repository format in config: %s", cfg.GitHub.Repository)
	}
	owner, repo := parts[0], parts[1]

	// Create GitHub client
	ghClient := github.New(cfg.GitHub.Token)

	// Get PR details
	pr, err := ghClient.GetPR(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to get PR: %w", err)
	}

	// List files in PR
	files, err := ghClient.ListPRFiles(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to list PR files: %w", err)
	}

	// Find plan file
	var planFile *github.PRFile
	for _, f := range files {
		if strings.HasSuffix(f.Filename, "-plan.json") && f.Status != "removed" {
			planFile = f
			break
		}
	}

	if planFile == nil {
		return fmt.Errorf("no plan file found in PR #%d. Run 'safesql plan --pr %d --ticket <ticket>' to generate one", prNumber, prNumber)
	}

	// Fetch plan file
	var planContent *github.FileContent
	if pr.State == "merged" {
		// Fetch from base branch (where it was merged to)
		planContent, err = ghClient.GetPRFile(ctx, owner, repo, pr.BaseBranch, planFile.Filename)
	} else {
		// Fetch from PR branch
		planContent, err = ghClient.GetPRFile(ctx, owner, repo, pr.HeadBranch, planFile.Filename)
	}

	if err != nil {
		return fmt.Errorf("failed to fetch plan file: %w", err)
	}

	// Parse plan
	var plan planner.Plan
	if err := json.Unmarshal([]byte(planContent.Content), &plan); err != nil {
		return fmt.Errorf("failed to parse plan: %w", err)
	}

	if asJSON {
		data, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal plan: %w", err)
		}
		logger.Info(string(data))
		return nil
	}

	// Print formatted plan details
	printDetailedPlan(&plan)

	// Add PR context
	logger.Info("")
	printBox("PR CONTEXT")
	logger.Info("")
	logger.Info(fmt.Sprintf("  PR: #%d - %s\n", pr.Number, pr.Title))
	logger.Info(fmt.Sprintf("  State: %s\n", pr.State))
	logger.Info(fmt.Sprintf("  Author: %s\n", pr.Author))
	logger.Info(fmt.Sprintf("  URL: %s\n", pr.URL))
	if pr.State == "merged" && pr.MergedAt != nil {
		logger.Info(fmt.Sprintf("  Merged: %s\n", pr.MergedAt.Format(time.RFC3339)))
	}
	logger.Info("")

	if pr.State == "open" {
		logger.Info("Next steps:\n")
		logger.Info("  1. Review the plan above\n")
		logger.Info("  2. Get the PR reviewed and approved\n")
		logger.Info("  3. Merge the PR\n")
		logger.Info(fmt.Sprintf("  4. Run: safesql apply --pr %d\n", prNumber))
	} else if pr.State == "merged" {
		planAge := time.Since(plan.CreatedAt)
		if planAge > 1*time.Hour {
			logger.Info("[WARNING] Plan is expired (> 1 hour old)\n")
			logger.Info("  Create a new PR with fresh plan:\n")
			logger.Info(fmt.Sprintf("    1. Create new branch from %s\n", pr.BaseBranch))
			logger.Info("    2. Copy SQL file to new branch\n")
			logger.Info("    3. Create new PR and run: safesql plan --pr <new-pr> --ticket <ticket>\n")
		} else {
			logger.Info("Ready to apply:\n")
			logger.Info(fmt.Sprintf("  safesql apply --pr %d\n", prNumber))
		}
	}

	return nil
}
