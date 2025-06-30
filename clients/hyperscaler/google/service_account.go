package google

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/iam/v1"
)

var (
	createServiceAccountKey    = _createServiceAccountKey
	getServiceAccountIamPolicy = _getServiceAccountIamPolicy
	setServiceAccountIamPolicy = _setServiceAccountIamPolicy
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
