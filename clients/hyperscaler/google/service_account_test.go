package google

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iam/v1"
)

func TestGrantServiceAccountRole(t *testing.T) {
	t.Run("TestGrantServiceAccountRoleAddsNewBindingWhenRoleNotPresent", func(t *testing.T) {
		ctx := context.Background()
		mockGcp := &GcpServices{}
		calledSet := false

		getServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string) (*iam.Policy, error) {
			return &iam.Policy{Bindings: []*iam.Binding{}}, nil
		}
		setServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string, policy *iam.Policy) (*iam.Policy, error) {
			calledSet = true
			if len(policy.Bindings) != 1 {
				t.Errorf("expected 1 binding, got %d", len(policy.Bindings))
			}
			return policy, nil
		}

		err := mockGcp.GrantServiceAccountRole(ctx, "target@project.iam.gserviceaccount.com", "member@project.iam.gserviceaccount.com", "roles/viewer")
		assert.NoError(t, err)
		assert.True(t, calledSet)
	})
	t.Run("TestGrantServiceAccountRoleDoesNotDuplicateMember", func(t *testing.T) {
		ctx := context.Background()
		mockGcp := &GcpServices{}
		calledSet := false

		getServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string) (*iam.Policy, error) {
			return &iam.Policy{
				Bindings: []*iam.Binding{
					{Role: "roles/viewer", Members: []string{"serviceAccount:member@project.iam.gserviceaccount.com"}},
				},
			}, nil
		}
		setServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string, policy *iam.Policy) (*iam.Policy, error) {
			calledSet = true
			if len(policy.Bindings[0].Members) != 1 {
				t.Errorf("expected 1 member, got %d", len(policy.Bindings[0].Members))
			}
			return policy, nil
		}

		err := mockGcp.GrantServiceAccountRole(ctx, "target@project.iam.gserviceaccount.com", "member@project.iam.gserviceaccount.com", "roles/viewer")
		assert.NoError(t, err)
		assert.True(t, calledSet)
	})
	t.Run("TestGrantServiceAccountRoleReturnsErrorWhenGetPolicyFails", func(t *testing.T) {
		ctx := context.Background()
		mockGcp := &GcpServices{}

		getServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string) (*iam.Policy, error) {
			return nil, fmt.Errorf("get policy error")
		}

		err := mockGcp.GrantServiceAccountRole(ctx, "target@project.iam.gserviceaccount.com", "member@project.iam.gserviceaccount.com", "roles/viewer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "get policy error")
	})
	t.Run("TestGrantServiceAccountRoleReturnsErrorWhenSetPolicyFails", func(t *testing.T) {
		ctx := context.Background()
		mockGcp := &GcpServices{}

		getServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string) (*iam.Policy, error) {
			return &iam.Policy{Bindings: []*iam.Binding{}}, nil
		}
		setServiceAccountIamPolicy = func(ctx context.Context, gcpService *GcpServices, resource string, policy *iam.Policy) (*iam.Policy, error) {
			return nil, fmt.Errorf("set policy error")
		}

		err := mockGcp.GrantServiceAccountRole(ctx, "target@project.iam.gserviceaccount.com", "member@project.iam.gserviceaccount.com", "roles/viewer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "set policy error")
	})
}
