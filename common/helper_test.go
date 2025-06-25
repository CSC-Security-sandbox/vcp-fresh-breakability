package common

import (
	"context"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/cloudresourcemanager/v1"
)

func TestHelperReturnsHello(t *testing.T) {
	expected := "Hello"
	actual := Helper()
	if actual != expected {
		t.Errorf("expected %s but got %s", expected, actual)
	}
}

func TestHelperReturnsNonEmptyString(t *testing.T) {
	actual := Helper()
	if actual == "" {
		t.Errorf("expected non-empty string but got empty string")
	}
}

func TestAddRoleToMember(t *testing.T) {
	t.Run("AddRoleToMemberAddsRoleWhenNotPresent", func(tt *testing.T) {
		policy := &cloudresourcemanager.Policy{
			Bindings: []*cloudresourcemanager.Binding{},
		}
		role := "roles/viewer"
		member := "serviceAccount:test@example.com"
		updated := AddRoleToMember(policy, role, member)
		found := false
		for _, b := range updated.Bindings {
			if b.Role == role && len(b.Members) == 1 && b.Members[0] == member {
				found = true
			}
		}
		if !found {
			t.Errorf("expected role %s with member %s to be added", role, member)
		}
	})
}

func TestFilterPolicyForMember(t *testing.T) {
	t.Run("filterPolicyForMemberReturnsFalseWhenRoleNotPresent", func(tt *testing.T) {
		policy := &cloudresourcemanager.Policy{
			Bindings: []*cloudresourcemanager.Binding{},
		}
		if filterPolicyForMember(policy, "roles/owner", "serviceAccount:test@example.com") {
			t.Errorf("expected false when role is not present")
		}
	})
	t.Run("filterPolicyForMemberReturnsTrueWhenMemberHasRole", func(tt *testing.T) {
		policy := &cloudresourcemanager.Policy{
			Bindings: []*cloudresourcemanager.Binding{
				{Role: "roles/owner", Members: []string{"serviceAccount:test@example.com"}},
			},
		}
		if !filterPolicyForMember(policy, "roles/owner", "serviceAccount:test@example.com") {
			t.Errorf("expected true when member has the role")
		}
	})
}

func TestCreateCloudResourceManagerService(t *testing.T) {
	t.Run("CreateCloudResourceManagerServiceReturnsServiceOnSuccess", func(tt *testing.T) {
		ctx := context.Background()
		MockMetaDataHost = "sample-server.com"
		defer func() {
			MockMetaDataHost = env.GetString("GCP_MOCK_METADATA_HOST", "")
		}()
		service, err := CreateCloudResourceManagerService(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if service == nil {
			t.Error("expected non-nil service, got nil")
		}
	})
}

func TestGrantRoleToServiceAccountReturnsNilWhenRoleAlreadyGranted(t *testing.T) {
	ctx := context.Background()
	called := false
	createCloudResourceManagerService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
		return &cloudresourcemanager.Service{}, nil
	}
	getIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string) (*cloudresourcemanager.Policy, error) {
		return &cloudresourcemanager.Policy{
			Bindings: []*cloudresourcemanager.Binding{
				{Role: "roles/editor", Members: []string{"serviceAccount:test@example.com"}},
			},
		}, nil
	}
	filterPolicyForMember = func(policy *cloudresourcemanager.Policy, role, member string) bool {
		return true
	}
	setIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string, policy *cloudresourcemanager.Policy) error {
		called = true
		return nil
	}
	err := GrantRoleToServiceAccount(ctx, "project", "test@example.com", "roles/editor")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if called {
		t.Error("expected setIamPolicy not to be called when role already granted")
	}
}

func TestGrantRoleToServiceAccountReturnsErrorWhenCreateServiceFails(t *testing.T) {
	ctx := context.Background()
	createCloudResourceManagerService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
		return nil, errors.New("service error")
	}
	err := GrantRoleToServiceAccount(ctx, "project", "test@example.com", "roles/editor")
	if err == nil || err.Error() != "service error" {
		t.Errorf("expected service error, got %v", err)
	}
}

func TestGrantRoleToServiceAccountReturnsErrorWhenGetIamPolicyFails(t *testing.T) {
	ctx := context.Background()
	createCloudResourceManagerService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
		return &cloudresourcemanager.Service{}, nil
	}
	getIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string) (*cloudresourcemanager.Policy, error) {
		return nil, errors.New("policy error")
	}
	err := GrantRoleToServiceAccount(ctx, "project", "test@example.com", "roles/editor")
	if err == nil || err.Error() != "policy error" {
		t.Errorf("expected policy error, got %v", err)
	}
}

func TestGrantRoleToServiceAccountReturnsErrorWhenSetIamPolicyFails(t *testing.T) {
	ctx := context.Background()
	createCloudResourceManagerService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
		return &cloudresourcemanager.Service{}, nil
	}
	getIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string) (*cloudresourcemanager.Policy, error) {
		return &cloudresourcemanager.Policy{}, nil
	}
	filterPolicyForMember = func(policy *cloudresourcemanager.Policy, role, member string) bool {
		return false
	}
	addRoleToMember = func(policy *cloudresourcemanager.Policy, role, member string) *cloudresourcemanager.Policy {
		return policy
	}
	setIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string, policy *cloudresourcemanager.Policy) error {
		return errors.New("set error")
	}
	err := GrantRoleToServiceAccount(ctx, "project", "test@example.com", "roles/editor")
	if err == nil || err.Error() != "set error" {
		t.Errorf("expected set error, got %v", err)
	}
}

func TestGrantRoleToServiceAccountGrantsRoleWhenNotPresent(t *testing.T) {
	ctx := context.Background()
	setCalled := false
	createCloudResourceManagerService = func(ctx context.Context) (*cloudresourcemanager.Service, error) {
		return &cloudresourcemanager.Service{}, nil
	}
	getIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string) (*cloudresourcemanager.Policy, error) {
		return &cloudresourcemanager.Policy{}, nil
	}
	filterPolicyForMember = func(policy *cloudresourcemanager.Policy, role, member string) bool {
		return false
	}
	addRoleToMember = func(policy *cloudresourcemanager.Policy, role, member string) *cloudresourcemanager.Policy {
		return policy
	}
	setIamPolicy = func(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string, policy *cloudresourcemanager.Policy) error {
		setCalled = true
		return nil
	}
	err := GrantRoleToServiceAccount(ctx, "project", "test@example.com", "roles/editor")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if !setCalled {
		t.Error("expected setIamPolicy to be called")
	}
}
