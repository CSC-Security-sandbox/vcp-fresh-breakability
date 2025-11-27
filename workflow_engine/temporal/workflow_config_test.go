package temporal

import (
	"testing"
	"time"
)

func TestGetWorkflowGlobalTimeout_InvalidEnv(t *testing.T) {
	t.Setenv("WORKFLOW_GLOBAL_TIMEOUT_MINUTES", "invalid")
	got := GetWorkflowGlobalTimeout()
	want := 60 * time.Minute
	if got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestGetWorkflowGlobalTimeout_DefaultEnv(t *testing.T) {
	t.Setenv("WORKFLOW_GLOBAL_TIMEOUT_MINUTES", "")
	got := GetWorkflowGlobalTimeout()
	want := 60 * time.Minute
	if got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestGetCMEKWorkFlowGlobalTimeout_Default(t *testing.T) {
	t.Setenv("CMEK_WORKFLOW_GLOBAL_TIMEOUT_MINUTES", "")
	got := GetCMEKWorkFlowGlobalTimeout()
	if got != 14*time.Minute {
		t.Errorf("expected default 14m, got %v", got)
	}
}

func TestGetCMEKWorkFlowGlobalTimeout_Invalid(t *testing.T) {
	t.Setenv("CMEK_WORKFLOW_GLOBAL_TIMEOUT_MINUTES", "oops")
	got := GetCMEKWorkFlowGlobalTimeout()
	if got != 14*time.Minute {
		t.Errorf("expected fallback 14m, got %v", got)
	}
}
