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
