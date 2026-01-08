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

func TestGetExpertModeSyncWorkflowTimeout_InvalidEnv(t *testing.T) {
	original := ExpertModeSyncWorkflowTimeoutMinutes
	defer func() { ExpertModeSyncWorkflowTimeoutMinutes = original }()

	ExpertModeSyncWorkflowTimeoutMinutes = "invalid"
	got := GetExpertModeSyncWorkflowTimeout()
	want := 10 * time.Minute
	if got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestGetExpertModeSyncWorkflowTimeout_DefaultsToCustomEnv(t *testing.T) {
	original := ExpertModeSyncWorkflowTimeoutMinutes
	defer func() { ExpertModeSyncWorkflowTimeoutMinutes = original }()

	ExpertModeSyncWorkflowTimeoutMinutes = "15"
	got := GetExpertModeSyncWorkflowTimeout()
	want := 15 * time.Minute
	if got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestGetCreatePoolWorkflowTimeout_Defaults(t *testing.T) {
	origLV := CreatePoolWorkflowTimeoutMinutesLV
	defer func() {
		CreatePoolWorkflowTimeoutMinutesLV = origLV
	}()

	CreatePoolWorkflowTimeoutMinutesLV = "120"

	lv := GetCreatePoolWorkflowTimeout(true)
	if lv == nil || *lv != 120*time.Minute {
		t.Fatalf("expected 120m, got %v", lv)
	}
}

func TestGetCreatePoolWorkflowTimeout_InvalidEnvFallsBack(t *testing.T) {
	origLV := CreatePoolWorkflowTimeoutMinutesLV
	defer func() {
		CreatePoolWorkflowTimeoutMinutesLV = origLV
	}()

	CreatePoolWorkflowTimeoutMinutesLV = "invalid"

	lv := GetCreatePoolWorkflowTimeout(true)
	if lv == nil || *lv != 120*time.Minute {
		t.Fatalf("expected fallback 120m, got %v", lv)
	}
}

func TestGetCreatePoolWorkflowTimeout_StandardReturnsNil(t *testing.T) {
	got := GetCreatePoolWorkflowTimeout(false)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestGetCreatePoolWorkflowRunTimeout_StandardUsesGlobal(t *testing.T) {
	origGlobal := WorkflowGlobalTimeoutMinutes
	defer func() { WorkflowGlobalTimeoutMinutes = origGlobal }()

	WorkflowGlobalTimeoutMinutes = "75"
	got := GetCreatePoolWorkflowRunTimeout(false)
	if got == nil || *got != 75*time.Minute {
		t.Fatalf("expected 75m, got %v", got)
	}
}

func TestGetCreatePoolWorkflowRunTimeout_LVUsesLVTimeout(t *testing.T) {
	origGlobal := WorkflowGlobalTimeoutMinutes
	origLV := CreatePoolWorkflowTimeoutMinutesLV
	defer func() {
		WorkflowGlobalTimeoutMinutes = origGlobal
		CreatePoolWorkflowTimeoutMinutesLV = origLV
	}()

	WorkflowGlobalTimeoutMinutes = "75"
	CreatePoolWorkflowTimeoutMinutesLV = "30"

	got := GetCreatePoolWorkflowRunTimeout(true)
	if got == nil || *got != 30*time.Minute {
		t.Fatalf("expected 30m, got %v", got)
	}
}

func TestGetCreateBackupWorkflowTimeout_InvalidEnv(t *testing.T) {
	original := CreateBackupWorkflowTimeoutMinutes
	defer func() { CreateBackupWorkflowTimeoutMinutes = original }()

	CreateBackupWorkflowTimeoutMinutes = "invalid"
	got := GetCreateBackupWorkflowTimeout()
	want := 8640 * time.Minute
	if got == nil {
		t.Fatal("expected non-nil timeout, got nil")
	}
	if *got != want {
		t.Errorf("expected %v, got %v", want, *got)
	}
}

func TestGetCreateBackupWorkflowTimeout_ValidEnv(t *testing.T) {
	original := CreateBackupWorkflowTimeoutMinutes
	defer func() { CreateBackupWorkflowTimeoutMinutes = original }()

	CreateBackupWorkflowTimeoutMinutes = "4320"
	got := GetCreateBackupWorkflowTimeout()
	want := 4320 * time.Minute
	if got == nil {
		t.Fatal("expected non-nil timeout, got nil")
	}
	if *got != want {
		t.Errorf("expected %v, got %v", want, *got)
	}
}

func TestGetDeleteBackupWorkflowTimeout_InvalidEnv(t *testing.T) {
	original := DeleteBackupWorkflowTimeoutMinutes
	defer func() { DeleteBackupWorkflowTimeoutMinutes = original }()

	DeleteBackupWorkflowTimeoutMinutes = "invalid"
	got := GetDeleteBackupWorkflowTimeout()
	want := 6480 * time.Minute
	if got == nil {
		t.Fatal("expected non-nil timeout, got nil")
	}
	if *got != want {
		t.Errorf("expected %v, got %v", want, *got)
	}
}

func TestGetDeleteBackupWorkflowTimeout_ValidEnv(t *testing.T) {
	original := DeleteBackupWorkflowTimeoutMinutes
	defer func() { DeleteBackupWorkflowTimeoutMinutes = original }()

	DeleteBackupWorkflowTimeoutMinutes = "3240"
	got := GetDeleteBackupWorkflowTimeout()
	want := 3240 * time.Minute
	if got == nil {
		t.Fatal("expected non-nil timeout, got nil")
	}
	if *got != want {
		t.Errorf("expected %v, got %v", want, *got)
	}
}

func TestGetSFRWorkflowTimeout_InvalidEnv(t *testing.T) {
	original := SFRWorkflowTimeoutMinutes
	defer func() { SFRWorkflowTimeoutMinutes = original }()

	SFRWorkflowTimeoutMinutes = "invalid"
	got := GetSFRWorkflowTimeout()
	want := 13680 * time.Minute
	if got == nil {
		t.Fatal("expected non-nil timeout, got nil")
	}
	if *got != want {
		t.Errorf("expected %v, got %v", want, *got)
	}
}

func TestGetSFRWorkflowTimeout_ValidEnv(t *testing.T) {
	original := SFRWorkflowTimeoutMinutes
	defer func() { SFRWorkflowTimeoutMinutes = original }()

	SFRWorkflowTimeoutMinutes = "6840"
	got := GetSFRWorkflowTimeout()
	want := 6840 * time.Minute
	if got == nil {
		t.Fatal("expected non-nil timeout, got nil")
	}
	if *got != want {
		t.Errorf("expected %v, got %v", want, *got)
	}
}
