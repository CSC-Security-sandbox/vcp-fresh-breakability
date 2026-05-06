package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/api/option"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func ensureIAMDBUsers(cfg config) error {
	if cfg.instanceConnName == "" {
		slog.Info("INSTANCE_CONNECTION_NAME not set, skipping IAM DB user provisioning")
		return nil
	}

	project, instance, err := parseInstanceConnName(cfg.instanceConnName)
	if err != nil {
		return err
	}

	ctx := context.Background()
	service, err := sqladmin.NewService(ctx, option.WithScopes(sqladmin.SqlserviceAdminScope))
	if err != nil {
		return fmt.Errorf("create sqladmin client: %w", err)
	}

	existing, err := listCloudSQLUsers(ctx, service, project, instance)
	if err != nil {
		return fmt.Errorf("list users: %w", err)
	}

	for _, iamUser := range allIAMUsers(cfg) {
		if existing[iamUser] {
			slog.Info("IAM DB user already exists", "user", iamUser)
			continue
		}
		slog.Info("creating IAM DB user", "user", iamUser)
		if err := createCloudSQLIAMUserWithRetry(ctx, service, project, instance, iamUser, 3); err != nil {
			return fmt.Errorf("create user %s: %w", iamUser, err)
		}
		slog.Info("IAM DB user created", "user", iamUser)
	}
	return nil
}

func parseInstanceConnName(connName string) (project, instance string, err error) {
	parts := strings.SplitN(connName, ":", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("invalid instance connection name %q (expected project:region:instance)", connName)
	}
	return parts[0], parts[2], nil
}

func listCloudSQLUsers(ctx context.Context, service *sqladmin.Service, project, instance string) (map[string]bool, error) {
	resp, err := service.Users.List(project, instance).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("list users API call: %w", err)
	}

	users := make(map[string]bool)
	for _, u := range resp.Items {
		users[u.Name] = true
	}
	return users, nil
}

func createCloudSQLIAMUserWithRetry(ctx context.Context, service *sqladmin.Service, project, instance, iamUser string, maxRetries int) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := createCloudSQLIAMUser(ctx, service, project, instance, iamUser)
		if err == nil {
			return nil
		}

		// Check if error is retryable
		if isRetryableAPIError(err) {
			lastErr = err
			if attempt < maxRetries {
				backoff := time.Duration(attempt*attempt) * time.Second // 1s, 4s, 9s
				slog.Warn("retryable error creating IAM user, retrying", 
					"user", iamUser, 
					"attempt", attempt, 
					"max_retries", maxRetries, 
					"backoff", backoff,
					"error", err)
				time.Sleep(backoff)
				continue
			}
		}
		// Non-retryable error or user already exists
		return err
	}
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func createCloudSQLIAMUser(ctx context.Context, service *sqladmin.Service, project, instance, iamUser string) error {
	user := &sqladmin.User{
		Name: iamUser,
		Type: "CLOUD_IAM_SERVICE_ACCOUNT",
	}

	_, err := service.Users.Insert(project, instance, user).Context(ctx).Do()
	if err != nil {
		// Check if user already exists (409 Conflict)
		if strings.Contains(err.Error(), "409") || strings.Contains(err.Error(), "already exists") {
			slog.Info("IAM DB user already exists (conflict)", "user", iamUser)
			return nil
		}
		return fmt.Errorf("insert user API call: %w", err)
	}
	return nil
}

func isRetryableAPIError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	
	// Retryable HTTP status codes
	retryableErrors := []string{
		"429", // Too Many Requests (rate limit)
		"500", // Internal Server Error
		"502", // Bad Gateway
		"503", // Service Unavailable
		"504", // Gateway Timeout
		"UNAVAILABLE",
		"DEADLINE_EXCEEDED",
		"RESOURCE_EXHAUSTED",
		"temporarily unavailable",
		"timeout",
	}
	
	for _, retryable := range retryableErrors {
		if strings.Contains(errMsg, retryable) {
			return true
		}
	}
	return false
}
