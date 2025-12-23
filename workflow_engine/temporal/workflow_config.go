package temporal

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
)

var (
	CMEKWorkflowGlobalTimeoutMinutes     = env.GetString("CMEK_WORKFLOW_GLOBAL_TIMEOUT_MINUTES", "14")
	WorkflowGlobalTimeoutMinutes         = env.GetString("WORKFLOW_GLOBAL_TIMEOUT_MINUTES", "60")
	ExpertModeSyncWorkflowTimeoutMinutes = env.GetString("EXPERT_MODE_SYNC_WORKFLOW_TIMEOUT_MINUTES", "10")
	CreateBackupWorkflowTimeoutMinutes   = env.GetString("CREATE_BACKUP_WORKFLOW_TIMEOUT_MINUTES", "8640")
	DeleteBackupWorkflowTimeoutMinutes   = env.GetString("DELETE_BACKUP_WORKFLOW_TIMEOUT_MINUTES", "6480")
	SFRWorkflowTimeoutMinutes            = env.GetString("SFR_WORKFLOW_TIMEOUT_MINUTES", "13680")
)

// Struct for RetryPolicy configuration
type RetryPolicyConfig struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
	NonRetryableErrors []string
}

// Struct for StartWorkflowOptions configuration
type StartWorkflowOptionsConfig struct {
	TaskQueue             string
	WorkflowID            string
	WorkflowIDReusePolicy enums.WorkflowIdReusePolicy
	RetryPolicy           *RetryPolicyConfig
}

func GetRetryPolicy(config *RetryPolicyConfig) *temporal.RetryPolicy {
	// Define a retry policy with exponential backoff
	// Initial interval of 1 second, doubling each time, up to a maximum of 100 seconds
	retryConfig := RetryPolicyConfig{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Second * 100,
		MaximumAttempts:    0, // Unlimited
		NonRetryableErrors: []string{},
	}

	// Override defaults with provided config values if set
	if config.InitialInterval != 0 {
		retryConfig.InitialInterval = config.InitialInterval
	}
	if config.BackoffCoefficient != 0 {
		retryConfig.BackoffCoefficient = config.BackoffCoefficient
	}
	if config.MaximumInterval != 0 {
		retryConfig.MaximumInterval = config.MaximumInterval
	}
	if config.MaximumAttempts != 0 {
		retryConfig.MaximumAttempts = config.MaximumAttempts
	}
	if config.NonRetryableErrors != nil {
		retryConfig.NonRetryableErrors = config.NonRetryableErrors
	}

	// Return the temporal.RetryPolicy
	return &temporal.RetryPolicy{
		InitialInterval:        retryConfig.InitialInterval,
		BackoffCoefficient:     retryConfig.BackoffCoefficient,
		MaximumInterval:        retryConfig.MaximumInterval,
		MaximumAttempts:        retryConfig.MaximumAttempts,
		NonRetryableErrorTypes: retryConfig.NonRetryableErrors,
	}
}

func GetWorkflowGlobalTimeout() time.Duration {
	timeout, err := time.ParseDuration(WorkflowGlobalTimeoutMinutes + "m")
	if err != nil {
		// If parsing fails, default to 60 minutes
		return 60 * time.Minute
	}
	return timeout
}

func GetCMEKWorkFlowGlobalTimeout() time.Duration {
	timeout, err := time.ParseDuration(CMEKWorkflowGlobalTimeoutMinutes + "m")
	if err != nil {
		// If parsing fails, default to 14 minutes
		return 14 * time.Minute
	}
	return timeout
}

func GetExpertModeSyncWorkflowTimeout() time.Duration {
	timeout, err := time.ParseDuration(ExpertModeSyncWorkflowTimeoutMinutes + "m")
	if err != nil {
		return 10 * time.Minute
	}
	return timeout
}

func getWorkflowTimeoutWithDefault(configValue string, defaultMinutes int) *time.Duration {
	timeout, err := time.ParseDuration(configValue + "m")
	if err != nil {
		defaultTimeout := time.Duration(defaultMinutes) * time.Minute
		return &defaultTimeout
	}
	return &timeout
}

func GetCreateBackupWorkflowTimeout() *time.Duration {
	return getWorkflowTimeoutWithDefault(CreateBackupWorkflowTimeoutMinutes, 8640)
}

func GetDeleteBackupWorkflowTimeout() *time.Duration {
	return getWorkflowTimeoutWithDefault(DeleteBackupWorkflowTimeoutMinutes, 6480)
}

func GetSFRWorkflowTimeout() *time.Duration {
	return getWorkflowTimeoutWithDefault(SFRWorkflowTimeoutMinutes, 13680)
}
