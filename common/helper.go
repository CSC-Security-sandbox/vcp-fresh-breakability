package common

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	logger "golang.org/x/exp/slog"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/option"
	scopesHttp "google.golang.org/api/transport/http"
)

var (
	addRoleToMember                   = AddRoleToMember
	createCloudResourceManagerService = CreateCloudResourceManagerService
	getIamPolicy                      = _getIamPolicy
	setIamPolicy                      = SetIamPolicy
	filterPolicyForMember             = _filterPolicyForMember
	MockMetaDataHost                  = env.GetString("GCE_METADATA_HOST", "")
)

func Helper() string {
	return "Hello"
}

// CreateCloudResourceManagerService initializes the Cloud Resource Manager service.
func CreateCloudResourceManagerService(ctx context.Context) (*cloudresourcemanager.Service, error) {
	scopesOption := option.WithScopes(cloudresourcemanager.CloudPlatformScope)
	opts := []option.ClientOption{scopesOption}
	logger.Debug(fmt.Sprintf("opts: %#v", opts))

	if MockMetaDataHost != "" {
		opts = append(opts, option.WithTokenSource(google.ComputeTokenSource("", cloudresourcemanager.CloudPlatformScope)))
	}
	logger.Debug("creating newClient")
	client, _, err := scopesHttp.NewClient(ctx, opts...)
	if err != nil {
		logger.Error("error while creating new client for _initializeNetworkingService", err)
		return nil, err
	}
	crmService, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Resource Manager service: %v", err)
	}
	return crmService, nil
}

// getIamPolicy retrieves the current IAM policy for the given project.
func _getIamPolicy(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string) (*cloudresourcemanager.Policy, error) {
	policy, err := crmService.Projects.GetIamPolicy(projectID, &cloudresourcemanager.GetIamPolicyRequest{}).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get IAM policy: %v", err)
	}
	return policy, nil
}

// filterPolicyForMember filters the policy to check if the specific member has the specified role.
func _filterPolicyForMember(policy *cloudresourcemanager.Policy, role, member string) bool {
	for _, binding := range policy.Bindings {
		if binding.Role == role {
			for _, m := range binding.Members {
				if m == member {
					return true
				}
			}
		}
	}
	return false
}

// AddRoleToMember adds the specified role to the member if it doesn't already exist.
func AddRoleToMember(policy *cloudresourcemanager.Policy, role, member string) *cloudresourcemanager.Policy {
	for _, binding := range policy.Bindings {
		if binding.Role == role {
			binding.Members = append(binding.Members, member)
			return policy
		}
	}

	// If the role does not exist, add a new binding
	policy.Bindings = append(policy.Bindings, &cloudresourcemanager.Binding{
		Role:    role,
		Members: []string{member},
	})
	return policy
}

// SetIamPolicy sets the updated IAM policy for the project.
func SetIamPolicy(ctx context.Context, crmService *cloudresourcemanager.Service, projectID string, policy *cloudresourcemanager.Policy) error {
	_, err := crmService.Projects.SetIamPolicy(projectID, &cloudresourcemanager.SetIamPolicyRequest{
		Policy: policy,
	}).Do()
	if err != nil {
		return fmt.Errorf("failed to set IAM policy: %v", err)
	}
	return nil
}

func GrantRoleToServiceAccount(ctx context.Context, projectID, serviceAccountEmail, role string) error {
	crmService, err := createCloudResourceManagerService(ctx)
	if err != nil {
		return err
	}

	policy, err := getIamPolicy(ctx, crmService, projectID)
	if err != nil {
		return err
	}

	member := fmt.Sprintf("serviceAccount:%s", serviceAccountEmail)

	if filterPolicyForMember(policy, role, member) {
		logger.Info("%s already has %s on project %s", serviceAccountEmail, role, projectID)
		return nil
	}

	policy = addRoleToMember(policy, role, member)

	if err := setIamPolicy(ctx, crmService, projectID, policy); err != nil {
		return err
	}

	logger.Info("Granted %s to %s on project %s", role, serviceAccountEmail, projectID)
	return nil
}
