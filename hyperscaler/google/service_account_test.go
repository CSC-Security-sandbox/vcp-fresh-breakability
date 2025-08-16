package google

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func Test_CreateServiceAccountKey(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		svc3, err := iam.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint("url"))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		ctx := context.Background()
		gcp := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: svc3,
			},
		}
		key, err := _createServiceAccountKey(gcp, ctx, "test@project.iam.gserviceaccount.com")
		assert.Error(tt, err)
		assert.Nil(tt, key)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		keyResp := &iam.ServiceAccountKey{
			Name:            "key-name",
			KeyAlgorithm:    "KEY_ALG_RSA_2048",
			KeyOrigin:       "USER_PROVIDED",
			PrivateKeyType:  "TYPE_GOOGLE_CREDENTIALS_FILE",
			PrivateKeyData:  "private",
			PublicKeyData:   "public",
			ValidAfterTime:  "2023-01-01T00:00:00Z",
			ValidBeforeTime: "2024-01-01T00:00:00Z",
		}
		url := "/v1/projects/-/serviceAccounts/test@project.iam.gserviceaccount.com/keys"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, err := json.Marshal(keyResp)
				if err != nil {
					rw.WriteHeader(http.StatusInternalServerError)
					return
				}
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		iamSvc, err := iam.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}
		gcp := &GcpServices{
			AdminGCPService: &AdminGCPService{
				iamService: iamSvc,
			},
		}
		key, err := _createServiceAccountKey(gcp, ctx, "test@project.iam.gserviceaccount.com")
		assert.NoError(tt, err)
		assert.NotNil(tt, key)
		assert.Equal(tt, "key-name", key.Name)
		assert.Equal(tt, "KEY_ALG_RSA_2048", key.KeyAlgorithm)
		assert.Equal(tt, "USER_PROVIDED", key.KeyOrigin)
		assert.Equal(tt, "TYPE_GOOGLE_CREDENTIALS_FILE", key.PrivateKeyType)
		assert.Equal(tt, "private", key.PrivateKeyData)
		assert.Equal(tt, "public", key.PublicKeyData)
		assert.Equal(tt, "2023-01-01T00:00:00Z", key.ValidAfterTime)
		assert.Equal(tt, "2024-01-01T00:00:00Z", key.ValidBeforeTime)
	})
}

func Test_DeleteServiceAccountKeysExcludingKey(t *testing.T) {
	ctx := context.Background()
	email := "test@example.com"
	keyToExclude := "key2"

	t.Run("WhenListServiceAccountsKeysFails", func(tt *testing.T) {
		origList := listServiceAccountsKeysWithRetry
		listServiceAccountsKeysWithRetry = func(ctx context.Context, c *GcpServices, email string) (*iam.ListServiceAccountKeysResponse, error) {
			return nil, fmt.Errorf("list error")
		}
		defer func() { listServiceAccountsKeysWithRetry = origList }()
		err := (&GcpServices{}).DeleteServiceAccountKeysExcludingKey(ctx, email, keyToExclude)
		if err == nil || err.Error() != "Projects.ServiceAccounts.Keys.List: list error" {
			tt.Errorf("expected list error, got %v", err)
		}
	})

	t.Run("WhenDeleteServiceAccountKeyFails", func(tt *testing.T) {
		origList := listServiceAccountsKeysWithRetry
		origDelete := deleteServiceAccountKeyWithRetry
		keys := []*iam.ServiceAccountKey{
			{Name: "key1", KeyType: "USER_MANAGED"},
			{Name: "key2", KeyType: "USER_MANAGED"},
		}
		listServiceAccountsKeysWithRetry = func(ctx context.Context, c *GcpServices, email string) (*iam.ListServiceAccountKeysResponse, error) {
			return &iam.ListServiceAccountKeysResponse{Keys: keys}, nil
		}
		deleteServiceAccountKeyWithRetry = func(_ context.Context, _ *GcpServices, keyName string) error {
			if keyName == "key1" {
				return fmt.Errorf("delete error")
			}
			return nil
		}
		defer func() {
			listServiceAccountsKeysWithRetry = origList
			deleteServiceAccountKeyWithRetry = origDelete
		}()
		err := (&GcpServices{}).DeleteServiceAccountKeysExcludingKey(ctx, email, keyToExclude)
		if err == nil || err.Error() != "Projects.ServiceAccounts.Keys.Delete: delete error" {
			tt.Errorf("expected delete error, got %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		origList := listServiceAccountsKeysWithRetry
		origDelete := deleteServiceAccountKeyWithRetry
		keys := []*iam.ServiceAccountKey{
			{Name: "key1", KeyType: "USER_MANAGED"},
			{Name: "key2", KeyType: "USER_MANAGED"},
			{Name: "key3", KeyType: "SYSTEM_MANAGED"},
		}
		var deletedKeys []string
		listServiceAccountsKeysWithRetry = func(ctx context.Context, c *GcpServices, email string) (*iam.ListServiceAccountKeysResponse, error) {
			return &iam.ListServiceAccountKeysResponse{Keys: keys}, nil
		}
		deleteServiceAccountKeyWithRetry = func(_ context.Context, _ *GcpServices, keyName string) error {
			deletedKeys = append(deletedKeys, keyName)
			return nil
		}
		defer func() {
			listServiceAccountsKeysWithRetry = origList
			deleteServiceAccountKeyWithRetry = origDelete
		}()
		err := (&GcpServices{}).DeleteServiceAccountKeysExcludingKey(ctx, email, keyToExclude)
		if err != nil {
			tt.Errorf("expected no error, got %v", err)
		}
		if len(deletedKeys) != 1 || deletedKeys[0] != "key1" {
			tt.Errorf("expected only key1 to be deleted, got %v", deletedKeys)
		}
	})
}
