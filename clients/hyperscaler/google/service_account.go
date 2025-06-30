package google

import (
	"context"
	"fmt"
	"time"

	retry2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/retry"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/iam/v1"
)

var (
	createServiceAccountKey          = _createServiceAccountKey
	getServiceAccountIamPolicy       = _getServiceAccountIamPolicy
	setServiceAccountIamPolicy       = _setServiceAccountIamPolicy
	retryTimeout                     = 1 * time.Minute
	retryInterval                    = 5 * time.Second
	listServiceAccountsKeysWithRetry = ListServiceAccountsKeysWithRetry
	deleteServiceAccountKeyWithRetry = DeleteServiceAccountKeyWithRetry
	deleteAllServiceAccountKeys      = _deleteAllServiceAccountKeys
	retryDo                          = retry2.RetryDoWithTimeout
	keyResourceUrlPrefix             = "projects/-/serviceAccounts/"
)

func (gcpService *GcpServices) CreateServiceAccountKey(ctx context.Context, email string) (*iam.ServiceAccountKey, error) {
	return createServiceAccountKey(gcpService, ctx, email)
}

func _createServiceAccountKey(gcpService *GcpServices, ctx context.Context, serviceAccountEmail string) (*iam.ServiceAccountKey, error) {
	request := &iam.CreateServiceAccountKeyRequest{}
	return gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.Keys.Create(fmt.Sprintf("projects/-/serviceAccounts/%s", serviceAccountEmail), request).Context(ctx).Do()
}

func _getServiceAccountIamPolicy(ctx context.Context, gcpService *GcpServices, resource string) (*iam.Policy, error) {
	return gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.GetIamPolicy(resource).Context(ctx).Do()
}

func _setServiceAccountIamPolicy(ctx context.Context, gcpService *GcpServices, resource string, policy *iam.Policy) (*iam.Policy, error) {
	return gcpService.AdminGCPService.iamService.Projects.ServiceAccounts.SetIamPolicy(resource, &iam.SetIamPolicyRequest{
		Policy: policy,
	}).Context(ctx).Do()
}

func (gcpService *GcpServices) GrantServiceAccountRole(ctx context.Context, email string, memberEmail string, role string) error {
	// Get the current IAM policy for the target service account
	log := util.GetLogger(ctx)
	resource := fmt.Sprintf("projects/-/serviceAccounts/%s", email)
	policy, err := getServiceAccountIamPolicy(ctx, gcpService, resource)
	if err != nil {
		log.Errorf("Failed to get IAM policy: %v", err)
		return err
	}

	// Add the new binding for the initial service account
	member := fmt.Sprintf("serviceAccount:%s", memberEmail)
	policy.Bindings = append(policy.Bindings, &iam.Binding{
		Role:    role,
		Members: []string{member},
	})

	// Set the updated IAM policy
	_, err = setServiceAccountIamPolicy(ctx, gcpService, resource, policy)
	if err != nil {
		log.Errorf("Failed to set IAM policy: %v", err)
		return err
	}

	log.Infof("Successfully granted role %s to %s", role, email)
	return nil
}

func (gcpService *GcpServices) DeleteAllServiceAccountKeys(ctx context.Context, email string) error {
	return deleteAllServiceAccountKeys(ctx, gcpService, email)
}

// _deleteAllServiceAccountKeys deletes keys for a service account using email
func _deleteAllServiceAccountKeys(ctx context.Context, c *GcpServices, email string) error {
	keyList, err := listServiceAccountsKeysWithRetry(ctx, c, keyResourceUrlPrefix+email)
	if err != nil {
		return fmt.Errorf("Projects.ServiceAccounts.Keys.List: %v", err)
	}
	if keyList.Keys != nil {
		for _, key := range keyList.Keys {
			if key.KeyType == "USER_MANAGED" {
				if err := deleteServiceAccountKeyWithRetry(ctx, c, key.Name); err != nil {
					return fmt.Errorf("Projects.ServiceAccounts.Keys.Delete: %v", err)
				}
			}
		}
	}
	return nil
}

// ListServiceAccountsKeysWithRetry list keys for a particular service account using email
func ListServiceAccountsKeysWithRetry(ctx context.Context, c *GcpServices, email string) (keyResponse *iam.ListServiceAccountKeysResponse, err error) {
	err = retryDo(ctx, retryTimeout, retryInterval, "ListServiceAccountsKeysWithRetry", func(attempt int) (bool, error) {
		keyResponse, err = c.AdminGCPService.iamService.Projects.ServiceAccounts.Keys.List(email).Do()
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return keyResponse, nil
}

// DeleteServiceAccountKeyWithRetry deletes keys using keyName
func DeleteServiceAccountKeyWithRetry(ctx context.Context, c *GcpServices, keyName string) (err error) {
	err = retryDo(ctx, retryTimeout, retryInterval, "DeleteServiceAccountKeyWithRetry", func(attempt int) (bool, error) {
		_, err = c.AdminGCPService.iamService.Projects.ServiceAccounts.Keys.Delete(keyName).Do()
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	return nil
}
