package google

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
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

// Unit tests for _deleteAllServiceAccountKeys
func Test__deleteAllServiceAccountKeys(t *testing.T) {
	ctx := context.Background()
	email := "test@example.com"

	t.Run("WhenListServiceAccountsKeysFails", func(tt *testing.T) {
		origList := listServiceAccountsKeysWithRetry
		listServiceAccountsKeysWithRetry = func(ctx context.Context, c *GcpServices, email string) (keyResponse *iam.ListServiceAccountKeysResponse, err error) {
			return nil, fmt.Errorf("list error")
		}
		defer func() { listServiceAccountsKeysWithRetry = origList }()
		err := _deleteAllServiceAccountKeys(ctx, &GcpServices{}, email)
		if err == nil || !strings.Contains(err.Error(), "list error") {
			tt.Errorf("expected list error, got %v", err)
		}
	})

	t.Run("WhenDeleteServiceAccountKeyFails", func(tt *testing.T) {
		origList := listServiceAccountsKeysWithRetry
		origDelete := deleteServiceAccountKeyWithRetry
		keys := []*iam.ServiceAccountKey{&iam.ServiceAccountKey{Name: "key1", KeyType: "USER_MANAGED"}}
		listServiceAccountsKeysWithRetry = func(ctx context.Context, c *GcpServices, email string) (keyResponse *iam.ListServiceAccountKeysResponse, err error) {
			return &iam.ListServiceAccountKeysResponse{Keys: keys}, nil
		}

		deleteServiceAccountKeyWithRetry = func(_ context.Context, _ *GcpServices, _ string) error {
			return fmt.Errorf("delete error")
		}
		defer func() {
			listServiceAccountsKeysWithRetry = origList
			deleteServiceAccountKeyWithRetry = origDelete
		}()
		err := _deleteAllServiceAccountKeys(ctx, &GcpServices{}, email)
		if err == nil || !strings.Contains(err.Error(), "delete error") {
			tt.Errorf("expected delete error, got %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		origList := listServiceAccountsKeysWithRetry
		origDelete := deleteServiceAccountKeyWithRetry
		keys := []*iam.ServiceAccountKey{&iam.ServiceAccountKey{Name: "key1", KeyType: "USER_MANAGED"}, &iam.ServiceAccountKey{Name: "key2", KeyType: "SYSTEM_MANAGED"}}

		listServiceAccountsKeysWithRetry = func(ctx context.Context, c *GcpServices, email string) (keyResponse *iam.ListServiceAccountKeysResponse, err error) {
			return &iam.ListServiceAccountKeysResponse{Keys: keys}, nil
		}
		deleteServiceAccountKeyWithRetry = func(_ context.Context, _ *GcpServices, _ string) error {
			return nil
		}
		defer func() {
			listServiceAccountsKeysWithRetry = origList
			deleteServiceAccountKeyWithRetry = origDelete
		}()
		err := _deleteAllServiceAccountKeys(ctx, &GcpServices{}, email)
		if err != nil {
			tt.Errorf("expected no error, got %v", err)
		}
	})
}

func Test_ListServiceAccountsKeysWithRetry(t *testing.T) {
	ctx := context.Background()
	email := "test@example.com"

	t.Run("WhenListFails", func(tt *testing.T) {
		svc3, err := iam.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint("url"))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		c := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: svc3,
			},
		}
		_, err = ListServiceAccountsKeysWithRetry(ctx, c, email)
		assert.Error(tt, err)
		if err == nil {
			tt.Errorf("expected error, got null")
		}
	})
}

func Test_ListDeleteAccountsKeysWithRetry(t *testing.T) {
	ctx := context.Background()
	email := "test@example.com"

	t.Run("WhenDeleteFails", func(tt *testing.T) {
		svc3, err := iam.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint("url"))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		c := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: svc3,
			},
		}
		err = DeleteServiceAccountKeyWithRetry(ctx, c, email)
		assert.Error(tt, err)
		if err == nil {
			tt.Errorf("expected error, got null")
		}
	})
}
