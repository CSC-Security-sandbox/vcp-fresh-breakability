package activities_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/testsuite"
)

func TestDeployADCCloudRunService(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		cloudRunConfig := &hyperscalermodels.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: fmt.Sprintf("adc-svc-%s", "timestamp"),
			Image:       "adcImage",
			Description: fmt.Sprintf("ADC Cloud Run service for %s", "backup uuid"),
			Labels: map[string]string{
				"app":        "adc",
				"component":  "backup",
				"managed-by": "vsa-control-plane",
			},
			Annotations: map[string]string{
				"description": "ADC service for backup and restore operations",
			},
			Ingress: "INGRESS_TRAFFIC_INTERNAL_ONLY", // Equivalent to "internal" annotation
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"ENABLE_COPY":        "1",
				"LOG_TO_CONSOLE":     "1",
				"CA_FILE":            "adc-cert.crt",
				"CERT_PATH":          "/home/ADC/cert/",
			},
			VolumeMounts: []hyperscalermodels.VolumeMount{
				{
					Name:      "adc-cert",
					MountPath: "/home/ADC/cert",
				},
			},
			Volumes: []hyperscalermodels.Volume{
				{
					Name:       "adc-cert",
					VolumeType: "secret",
					Source: hyperscalermodels.VolumeSource{
						SecretName: "adc-cert",
						Items: []hyperscalermodels.SecretItem{
							{
								Path:    "adc-cert.crt",
								Version: "latest",
							},
						},
					},
				},
			},
		}
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateCloudRunService").Return(&hyperscalermodels.CloudRunOperationResponse{
			OperationName: "DeployADCCloudRunService",
			Status:        "success",
		}, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		mockGCPService.On("CreateCloudRunService", ctx, cloudRunConfig).Return(&hyperscalermodels.CloudRunOperationResponse{}, nil)
		activity := activities.ADCActivity{}
		response, err := activity.DeployADCCloudRunService(ctx, cloudRunConfig)
		assert.Nil(t, err)
		assert.NotNil(t, response)
	})
	t.Run("withInternalIngress", func(t *testing.T) {
		cloudRunConfig := &hyperscalermodels.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: fmt.Sprintf("adc-svc-%s", "timestamp"),
			Image:       "adcImage",
			Description: fmt.Sprintf("ADC Cloud Run service for %s", "backup uuid"),
			Labels: map[string]string{
				"app":        "adc",
				"component":  "backup",
				"managed-by": "vsa-control-plane",
			},
			Annotations: map[string]string{
				"description": "ADC service for backup and restore operations",
			},
			Ingress: "INGRESS_TRAFFIC_INTERNAL_ONLY",
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"ENABLE_COPY":        "1",
				"LOG_TO_CONSOLE":     "1",
				"CA_FILE":            "adc-cert.crt",
				"CERT_PATH":          "/home/ADC/cert/",
			},
		}
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateCloudRunService", ctx, cloudRunConfig).Return(&hyperscalermodels.CloudRunOperationResponse{
			OperationName: "DeployADCCloudRunService",
			Status:        "success",
		}, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		activity := activities.ADCActivity{}
		response, err := activity.DeployADCCloudRunService(ctx, cloudRunConfig)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "DeployADCCloudRunService", response.OperationName)
	})
	t.Run("withAllTrafficIngress", func(t *testing.T) {
		cloudRunConfig := &hyperscalermodels.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: fmt.Sprintf("adc-svc-%s", "timestamp"),
			Image:       "adcImage",
			Description: fmt.Sprintf("ADC Cloud Run service for %s", "backup uuid"),
			Labels: map[string]string{
				"app":        "adc",
				"component":  "backup",
				"managed-by": "vsa-control-plane",
			},
			Annotations: map[string]string{
				"description": "ADC service for backup and restore operations",
			},
			Ingress: "INGRESS_TRAFFIC_ALL",
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"ENABLE_COPY":        "1",
				"LOG_TO_CONSOLE":     "1",
				"CA_FILE":            "adc-cert.crt",
				"CERT_PATH":          "/home/ADC/cert/",
			},
		}
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateCloudRunService", ctx, cloudRunConfig).Return(&hyperscalermodels.CloudRunOperationResponse{
			OperationName: "DeployADCCloudRunService",
			Status:        "success",
		}, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		activity := activities.ADCActivity{}
		response, err := activity.DeployADCCloudRunService(ctx, cloudRunConfig)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "DeployADCCloudRunService", response.OperationName)
	})
	t.Run("withNoIngressSpecified", func(t *testing.T) {
		cloudRunConfig := &hyperscalermodels.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: fmt.Sprintf("adc-svc-%s", "timestamp"),
			Image:       "adcImage",
			Description: fmt.Sprintf("ADC Cloud Run service for %s", "backup uuid"),
			Labels: map[string]string{
				"app":        "adc",
				"component":  "backup",
				"managed-by": "vsa-control-plane",
			},
			Annotations: map[string]string{
				"description": "ADC service for backup and restore operations",
			},
			// No Ingress field specified - should use default
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"ENABLE_COPY":        "1",
				"LOG_TO_CONSOLE":     "1",
				"CA_FILE":            "adc-cert.crt",
				"CERT_PATH":          "/home/ADC/cert/",
			},
		}
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateCloudRunService", ctx, cloudRunConfig).Return(&hyperscalermodels.CloudRunOperationResponse{
			OperationName: "DeployADCCloudRunService",
			Status:        "success",
		}, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		activity := activities.ADCActivity{}
		response, err := activity.DeployADCCloudRunService(ctx, cloudRunConfig)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, "DeployADCCloudRunService", response.OperationName)
	})
	t.Run("onCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}
		activity := activities.ADCActivity{}
		response, err := activity.DeployADCCloudRunService(ctx, nil)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})
	t.Run("onCreateCloudRunServiceFailure", func(t *testing.T) {
		cloudRunConfig := &hyperscalermodels.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: fmt.Sprintf("adc-svc-%s", "timestamp"),
			Image:       "adcImage",
			Description: fmt.Sprintf("ADC Cloud Run service for %s", "backup uuid"),
			Labels: map[string]string{
				"app":        "adc",
				"component":  "backup",
				"managed-by": "vsa-control-plane",
			},
			Annotations: map[string]string{
				"description": "ADC service for backup and restore operations",
			},
			Ingress: "INGRESS_TRAFFIC_INTERNAL_ONLY", // Equivalent to "internal" annotation
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"ENABLE_COPY":        "1",
				"LOG_TO_CONSOLE":     "1",
				"CA_FILE":            "adc-cert.crt",
				"CERT_PATH":          "/home/ADC/cert/",
			},
			VolumeMounts: []hyperscalermodels.VolumeMount{
				{
					Name:      "adc-cert",
					MountPath: "/home/ADC/cert",
				},
			},
			Volumes: []hyperscalermodels.Volume{
				{
					Name:       "adc-cert",
					VolumeType: "secret",
					Source: hyperscalermodels.VolumeSource{
						SecretName: "adc-cert",
						Items: []hyperscalermodels.SecretItem{
							{
								Path:    "adc-cert.crt",
								Version: "latest",
							},
						},
					},
				},
			},
		}
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateCloudRunService", ctx, cloudRunConfig).Return(nil, fmt.Errorf("failed to create cloud run service"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		activity := activities.ADCActivity{}
		response, err := activity.DeployADCCloudRunService(ctx, cloudRunConfig)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})
}

func TestGetADCServiceURL(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		region := "us-central1"
		serviceName := "adc-svc-test"
		expectedURL := "https://adc-svc-test-abc123.run.app"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetCloudRunServiceURL", ctx, projectID, region, serviceName).Return(expectedURL, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		url, err := activity.GetADCServiceURL(ctx, projectID, region, serviceName)
		assert.Nil(t, err)
		assert.Equal(t, expectedURL, url)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		url, err := activity.GetADCServiceURL(ctx, "project", "region", "service")
		assert.NotNil(t, err)
		assert.Empty(t, url)
	})

	t.Run("OnGetServiceURLFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetCloudRunServiceURL", ctx, "project", "region", "service").Return("", fmt.Errorf("failed to get service URL"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		url, err := activity.GetADCServiceURL(ctx, "project", "region", "service")
		assert.NotNil(t, err)
		assert.Empty(t, url)
	})
}

func TestCleanupADCCloudRunService(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		projectID := "test-project"
		region := "us-central1"
		serviceName := "adc-svc-test"

		expectedResponse := &hyperscalermodels.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteCloudRunService", mock.Anything, projectID, region, serviceName).Return(expectedResponse, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		encodedValue, err := env.ExecuteActivity(activity.CleanupADCCloudRunService, projectID, region, serviceName)
		assert.NoError(t, err)
		var response *hyperscalermodels.CloudRunOperationResponse
		err = encodedValue.Get(&response)
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, response)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CleanupADCCloudRunService, "project", "region", "service")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get cloud service")
	})

	t.Run("OnDeleteServiceFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteCloudRunService", mock.Anything, "project", "region", "service").Return(nil, fmt.Errorf("failed to delete service"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CleanupADCCloudRunService, "project", "region", "service")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete service")
		mockGCPService.AssertExpectations(t)
	})
}

func TestCreateServiceAccount(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		saAccountID := "adc-sa-test"
		saDisplayName := "ADC Service Account"

		expectedSA := &hyperscalermodels.ServiceAccount{
			Email:       "adc-sa-test@test-project.iam.gserviceaccount.com",
			DisplayName: saDisplayName,
		}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateServiceAccount", mock.Anything, projectID, saDisplayName).Return(expectedSA, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		sa, err := activity.CreateServiceAccount(ctx, projectID, saAccountID, saDisplayName)
		assert.Nil(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		sa, err := activity.CreateServiceAccount(ctx, "project", "sa-id", "display-name")
		assert.NotNil(t, err)
		assert.Nil(t, sa)
	})

	t.Run("OnCreateServiceAccountFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateServiceAccount", mock.Anything, "project", "display-name").Return(nil, fmt.Errorf("failed to create service account"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		sa, err := activity.CreateServiceAccount(ctx, "project", "sa-id", "display-name")
		assert.NotNil(t, err)
		assert.Nil(t, sa)
	})
}

func TestAttachRolesToServiceAccount(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		serviceAccountEmail := "adc-sa@test-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.admin", "roles/storage.objectAdmin"}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("AttachOrUpdateRolesForServiceAccounts", roles, serviceAccountEmail, projectID).Return(nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		err := activity.AttachRolesToServiceAccount(ctx, projectID, serviceAccountEmail, roles)
		assert.Nil(t, err)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		err := activity.AttachRolesToServiceAccount(ctx, "project", "email", []string{"role"})
		assert.NotNil(t, err)
	})

	t.Run("OnAttachRolesFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("AttachOrUpdateRolesForServiceAccounts", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("failed to attach roles"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		err := activity.AttachRolesToServiceAccount(ctx, "project", "email", []string{"role"})
		assert.NotNil(t, err)
	})
}

func TestIsServiceAccountCreated(t *testing.T) {
	t.Run("OnSuccess_AccountExists", func(t *testing.T) {
		ctx := context.Background()
		saEmail := "adc-sa@test-project.iam.gserviceaccount.com"
		expectedSA := &hyperscalermodels.ServiceAccount{Email: saEmail}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetServiceAccountByEmail", saEmail).Return(expectedSA, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		exists, err := activity.IsServiceAccountCreated(ctx, saEmail)
		assert.Nil(t, err)
		assert.True(t, exists)
	})

	t.Run("OnSuccess_AccountNotExists", func(t *testing.T) {
		ctx := context.Background()
		saEmail := "adc-sa@test-project.iam.gserviceaccount.com"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetServiceAccountByEmail", saEmail).Return(nil, fmt.Errorf("not found"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		exists, err := activity.IsServiceAccountCreated(ctx, saEmail)
		assert.NotNil(t, err)
		assert.False(t, exists)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		exists, err := activity.IsServiceAccountCreated(ctx, "email")
		assert.NotNil(t, err)
		assert.False(t, exists)
	})
}

func TestDeleteSA(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		saAccountID := "adc-sa-test"
		saEmail := "adc-sa-test@test-project.iam.gserviceaccount.com"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("DeleteServiceAccount", projectID, saEmail).Return(nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		err := activity.DeleteSA(ctx, projectID, saAccountID)
		assert.Nil(t, err)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		err := activity.DeleteSA(ctx, "project", "sa-id")
		assert.NotNil(t, err)
	})

	t.Run("OnDeleteServiceAccountFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("DeleteServiceAccount", mock.Anything, mock.Anything).Return(fmt.Errorf("failed to delete service account"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		err := activity.DeleteSA(ctx, "project", "sa-id")
		assert.NotNil(t, err)
	})
}

func TestRemoveRolesFromServiceAccount(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		saAccountID := "adc-sa-test"
		roles := []string{"roles/storage.admin", "roles/storage.objectAdmin"}
		saEmail := "adc-sa-test@test-project.iam.gserviceaccount.com"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("RemoveRolesFromServiceAccounts", roles, saEmail, projectID).Return(nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		err := activity.RemoveRolesFromServiceAccount(ctx, projectID, saAccountID, roles)
		assert.Nil(t, err)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		err := activity.RemoveRolesFromServiceAccount(ctx, "project", "sa-id", []string{"role"})
		assert.NotNil(t, err)
	})

	t.Run("OnRemoveRolesFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("RemoveRolesFromServiceAccounts", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("failed to remove roles"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		err := activity.RemoveRolesFromServiceAccount(ctx, "project", "sa-id", []string{"role"})
		assert.NotNil(t, err)
	})
}

func TestCheckOperationStatus(t *testing.T) {
	t.Run("OnSuccess_OperationComplete", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		operationName := "operations/test-operation"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CheckOperationStatus", mock.Anything, operationName).Return(true, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		encodedValue, err := env.ExecuteActivity(activity.CheckOperationStatus, operationName)
		assert.NoError(t, err)
		var isReady bool
		err = encodedValue.Get(&isReady)
		assert.NoError(t, err)
		assert.True(t, isReady)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("OnSuccess_OperationInProgress", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		operationName := "operations/test-operation"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CheckOperationStatus", mock.Anything, operationName).Return(false, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		encodedValue, err := env.ExecuteActivity(activity.CheckOperationStatus, operationName)
		assert.NoError(t, err)
		var isReady bool
		err = encodedValue.Get(&isReady)
		assert.NoError(t, err)
		assert.False(t, isReady)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CheckOperationStatus, "operation")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get cloud service")
	})

	t.Run("OnCheckStatusFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CheckOperationStatus", mock.Anything, "operation").Return(false, fmt.Errorf("failed to check status"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CheckOperationStatus, "operation")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check status")
		mockGCPService.AssertExpectations(t)
	})
}

func TestCreateHmacKeys(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		params := &commonparams.HmacKeyCreateParams{
			ProjectNumber:  "123456789",
			ServiceAccount: "adc-sa@test-project.iam.gserviceaccount.com",
		}
		accessKey := "test-access-key"
		secretKey := "test-secret-key"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateHmacKey", params.ProjectNumber, params.ServiceAccount).Return(&accessKey, &secretKey, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		keys, err := activity.CreateHmacKeys(ctx, params)
		assert.Nil(t, err)
		assert.NotNil(t, keys)
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(accessKey)), keys.AccessKey)
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(secretKey)), keys.SecretKey)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		keys, err := activity.CreateHmacKeys(ctx, &commonparams.HmacKeyCreateParams{})
		assert.NotNil(t, err)
		assert.Nil(t, keys)
	})

	t.Run("OnCreateHmacKeyFailure", func(t *testing.T) {
		ctx := context.Background()
		params := &commonparams.HmacKeyCreateParams{
			ProjectNumber:  "123456789",
			ServiceAccount: "adc-sa@test-project.iam.gserviceaccount.com",
		}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateHmacKey", params.ProjectNumber, params.ServiceAccount).Return(nil, nil, fmt.Errorf("failed to create HMAC key"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		keys, err := activity.CreateHmacKeys(ctx, params)
		assert.NotNil(t, err)
		assert.Nil(t, keys)
	})

	t.Run("OnNilKeys", func(t *testing.T) {
		ctx := context.Background()
		params := &commonparams.HmacKeyCreateParams{
			ProjectNumber:  "123456789",
			ServiceAccount: "adc-sa@test-project.iam.gserviceaccount.com",
		}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CreateHmacKey", params.ProjectNumber, params.ServiceAccount).Return(nil, nil, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		keys, err := activity.CreateHmacKeys(ctx, params)
		assert.NotNil(t, err)
		assert.Nil(t, keys)
	})
}

func TestGenerateResourceTimestamp(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		activity := activities.ADCActivity{}
		timestamp, err := activity.GenerateResourceTimestamp(ctx)
		assert.Nil(t, err)
		assert.NotEmpty(t, timestamp)
		// Verify it's in the expected format (YYYYMMDDHHMMSS)
		assert.Len(t, timestamp, 18)
	})
}

func TestInitialDeleteRequestWithCloudRun(t *testing.T) {
	t.Run("OnSuccess_WithRedirect", func(t *testing.T) {
		// Create a test server that returns 307 redirect
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "DELETE", r.Method)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "application/hal+json", r.Header.Get("Accept"))
			assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")

			w.Header().Set("Location", "/api/status/123")
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusTemporaryRedirect, response.StatusCode)
		assert.Equal(t, "/api/status/123", response.RedirectURL)
	})

	t.Run("OnSuccess_ImmediateCompletion", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})

	t.Run("OnInvalidADCParams", func(t *testing.T) {
		ctx := setupTestContext()
		adcParams := &commonparams.ADCParams{
			AccessKey: "invalid-base64",
			SecretKey: "invalid-base64",
		}
		serviceURL := "http://test.com"

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})

	t.Run("OnIdentityTokenFailure", func(t *testing.T) {
		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := "http://test.com"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "", fmt.Errorf("failed to get token")
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})

	t.Run("OnHTTPError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})
	t.Run("On404_NotFound", func(t *testing.T) {
		// Create a test server that returns 404 Not Found
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "DELETE", r.Method)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "application/hal+json", r.Header.Get("Accept"))
			assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")

			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusNotFound, response.StatusCode)
		assert.Equal(t, "", response.RedirectURL) // Should be empty for 404
	})
}

func TestCheckDeleteStatusWithCloudRun(t *testing.T) {
	t.Run("OnSuccess_WithNewRedirect", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "/api/status/456")
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		defer server.Close()

		params := createTestADCParams()
		serviceURL := server.URL
		redirectURL := "/api/status/123"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		encodedValue, err := env.ExecuteActivity(activity.CheckDeleteStatusWithCloudRun, params, serviceURL, redirectURL)
		assert.NoError(t, err)
		var response *commonparams.ADCResponse
		err = encodedValue.Get(&response)
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusTemporaryRedirect, response.StatusCode)
		assert.Equal(t, "/api/status/456", response.RedirectURL)
	})

	t.Run("OnSuccess_Completion", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		params := createTestADCParams()
		serviceURL := server.URL
		redirectURL := "/api/status/123"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		encodedValue, err := env.ExecuteActivity(activity.CheckDeleteStatusWithCloudRun, params, serviceURL, redirectURL)
		assert.NoError(t, err)
		var response *commonparams.ADCResponse
		err = encodedValue.Get(&response)
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})

	t.Run("OnEmptyRedirectURL", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		params := createTestADCParams()
		serviceURL := "http://test.com"
		redirectURL := ""

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CheckDeleteStatusWithCloudRun, params, serviceURL, redirectURL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing redirect URL")
	})

	t.Run("OnInvalidADCParams", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		params := &commonparams.ADCParams{
			AccessKey: "invalid-base64",
			SecretKey: "invalid-base64",
		}
		serviceURL := "http://test.com"
		redirectURL := "/api/status/123"

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CheckDeleteStatusWithCloudRun, params, serviceURL, redirectURL)
		assert.Error(t, err)
	})

	t.Run("OnIdentityTokenFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		params := createTestADCParams()
		serviceURL := "http://test.com"
		redirectURL := "/api/status/123"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "", fmt.Errorf("failed to get token")
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		env.RegisterActivity(&activity)

		_, err := env.ExecuteActivity(activity.CheckDeleteStatusWithCloudRun, params, serviceURL, redirectURL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get token")
	})
}

func TestGetStandardAuthToken(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := setupTestContext()
		expectedToken := "test-identity-token"

		originalGetCloudService := activities.GetCloudService
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetIdentityToken", mock.Anything, mock.Anything).Return(expectedToken, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		defer func() { activities.GetCloudService = originalGetCloudService }()

		token, err := activities.GetStandardAuthToken(ctx, "https://test-service-url")
		assert.Nil(t, err)
		assert.Equal(t, expectedToken, token)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := setupTestContext()

		originalGetCloudService := activities.GetCloudService
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}
		defer func() { activities.GetCloudService = originalGetCloudService }()

		token, err := activities.GetStandardAuthToken(ctx, "https://test-service-url")
		assert.NotNil(t, err)
		assert.Empty(t, token)
	})

	t.Run("OnGetIdentityTokenFailure", func(t *testing.T) {
		ctx := setupTestContext()

		originalGetCloudService := activities.GetCloudService
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetIdentityToken", mock.Anything, mock.Anything).Return("", fmt.Errorf("failed to get identity token"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		defer func() { activities.GetCloudService = originalGetCloudService }()

		token, err := activities.GetStandardAuthToken(ctx, "https://test-service-url")
		assert.NotNil(t, err)
		assert.Empty(t, token)
	})
}

func TestConvertADCParamsToRequest(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		adcParams := createTestADCParams()
		req, err := activities.ConvertADCParamsToRequest(adcParams)
		assert.Nil(t, err)
		assert.NotNil(t, req)
		assert.Equal(t, adcParams.BucketName, req.ObjectStore.Container)
		assert.Equal(t, adcParams.Port, req.ObjectStore.Port)
		assert.Equal(t, "test-access-key", req.ObjectStore.AccessKey)
		assert.Equal(t, "test-secret-key", req.ObjectStore.SecretPassword)
		assert.Equal(t, adcParams.ServerURL, req.ObjectStore.Server)
		assert.Equal(t, adcParams.ProvideType, req.ObjectStore.ProviderType)
	})
}

// Helper functions
func setupTestContext() context.Context {
	return context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
}

func createTestADCParams() *commonparams.ADCParams {
	return &commonparams.ADCParams{
		ADCName:          "test-adc",
		DestEndpointUUID: "endpoint-uuid",
		SnapshotUUID:     "snapshot-uuid",
		BucketName:       "test-bucket",
		AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
		SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
		ProvideType:      "GoogleCloud",
		ServerURL:        "storage.googleapis.com",
		AccountName:      "test-account",
		Port:             443,
	}
}

func TestCalculateLogicalBytesAndOptimizedBytes(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		// Create a test server that returns successful response
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "GET", r.Method)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "application/hal+json", r.Header.Get("Accept"))
			assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")
			assert.Equal(t, "test-access-key", r.Header.Get("access_key"))
			assert.Equal(t, "test-secret-key", r.Header.Get("secret_password"))
			assert.Equal(t, "443", r.Header.Get("port"))
			assert.Equal(t, "test-bucket", r.Header.Get("container"))
			assert.Equal(t, "storage.googleapis.com", r.Header.Get("server"))
			assert.Equal(t, "GoogleCloud", r.Header.Get("provider_type"))
			assert.Contains(t, r.URL.Path, "/api/endpoints/endpoint-uuid")

			response := activities.LogicalBytesResp{
				EndpointMetrics: activities.EndpointMetrics{
					LogicalSize:                1024000,
					CompressedBytesTransferred: 512000,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.Nil(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, uint64(1024000), result.LogicalSize)
		assert.Equal(t, uint64(512000), result.OptimizedSize)
	})

	t.Run("OnErrorResponse", func(t *testing.T) {
		// Create a test server that returns error response
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			errorResp := activities.LogicalBytesRespErr{
				Code:    500,
				Message: "Internal server error",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(errorResp)
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "An internal error occurred.")
	})

	t.Run("OnIdentityTokenFailure", func(t *testing.T) {
		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := "http://test.com"

		// Mock the identity token generation to fail
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "", fmt.Errorf("failed to get token")
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, result)
	})

	t.Run("OnInvalidADCParams", func(t *testing.T) {
		ctx := setupTestContext()
		adcParams := &commonparams.ADCParams{
			AccessKey: "invalid-base64",
			SecretKey: "invalid-base64",
		}
		serviceURL := "http://test.com"

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "An internal error occurred.")
	})

	t.Run("OnHTTPRequestFailure", func(t *testing.T) {
		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := "http://invalid-url-that-does-not-exist"

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "An internal error occurred.")
	})

	t.Run("OnInvalidJSONResponse", func(t *testing.T) {
		// Create a test server that returns invalid JSON
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("invalid json"))
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "An internal error occurred.")
	})

	t.Run("OnInvalidErrorJSONResponse", func(t *testing.T) {
		// Create a test server that returns invalid JSON for error response
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("invalid json"))
		}))
		defer server.Close()

		ctx := setupTestContext()
		adcParams := createTestADCParams()
		serviceURL := server.URL

		// Mock the identity token generation
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		result, err := activity.CalculateLogicalBytesAndOptimizedBytes(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "An internal error occurred.")
	})
}

func TestCreateADCGetRequestForLogicalSize(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		adcParams := createTestADCParams()
		serviceURL := "https://test-service.com"

		activity := activities.ADCActivity{}
		req, err := activity.CreateADCGetRequestForLogicalSize(adcParams, serviceURL)
		assert.Nil(t, err)
		assert.NotNil(t, req)

		// Verify URL
		expectedURL := fmt.Sprintf("%s/api/endpoints/%s", serviceURL, adcParams.DestEndpointUUID)
		assert.Equal(t, expectedURL, req.URL.String())

		// Verify method
		assert.Equal(t, "GET", req.Method)

		// Verify headers
		assert.Equal(t, "test-access-key", req.Header.Get("access_key"))
		assert.Equal(t, "test-secret-key", req.Header.Get("secret_password"))
		assert.Equal(t, "443", req.Header.Get("port"))
		assert.Equal(t, "test-bucket", req.Header.Get("container"))
		assert.Equal(t, "storage.googleapis.com", req.Header.Get("server"))
		assert.Equal(t, "GoogleCloud", req.Header.Get("provider_type"))
		assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", req.Header.Get("Accept"))
	})

	t.Run("OnInvalidAccessKey", func(t *testing.T) {
		adcParams := &commonparams.ADCParams{
			AccessKey:        "invalid-base64",
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			DestEndpointUUID: "endpoint-uuid",
			Port:             443,
			BucketName:       "test-bucket",
			ServerURL:        "storage.googleapis.com",
			ProvideType:      "GoogleCloud",
		}
		serviceURL := "https://test-service.com"

		activity := activities.ADCActivity{}
		req, err := activity.CreateADCGetRequestForLogicalSize(adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, req)
		assert.Contains(t, err.Error(), "failed to decode access key")
	})

	t.Run("OnInvalidSecretKey", func(t *testing.T) {
		adcParams := &commonparams.ADCParams{
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        "invalid-base64",
			DestEndpointUUID: "endpoint-uuid",
			Port:             443,
			BucketName:       "test-bucket",
			ServerURL:        "storage.googleapis.com",
			ProvideType:      "GoogleCloud",
		}
		serviceURL := "https://test-service.com"

		activity := activities.ADCActivity{}
		req, err := activity.CreateADCGetRequestForLogicalSize(adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, req)
		assert.Contains(t, err.Error(), "failed to decode secret key")
	})

	t.Run("OnInvalidURL", func(t *testing.T) {
		adcParams := createTestADCParams()
		serviceURL := "://invalid-url"

		activity := activities.ADCActivity{}
		req, err := activity.CreateADCGetRequestForLogicalSize(adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, req)
		assert.Contains(t, err.Error(), "failed to create HTTP request")
	})
}

func TestFetchLogicalSizeAndUpdateActivity_Success(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "volume-uuid"
	expectedLogicalSize := uint64(1024000)

	// Create test ADC params
	adcParams := &commonparams.ADCParams{
		ADCName:          "test-adc",
		DestEndpointUUID: "endpoint-uuid",
		SnapshotUUID:     "snapshot-uuid",
		BucketName:       "test-bucket",
		AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
		SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
		ProvideType:      "GoogleCloud",
		ServerURL:        "storage.googleapis.com",
		AccountName:      "test-account",
		Port:             443,
	}

	// Create mock storage
	mockStorage := database.NewMockStorage(t)
	mockStorage.On("UpdateLatestBackupLogicalSize", ctx, volumeUUID, int64(expectedLogicalSize)).Return(nil)

	// Create a test server that returns successful response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := activities.LogicalBytesResp{
			EndpointMetrics: activities.EndpointMetrics{
				LogicalSize:                expectedLogicalSize,
				CompressedBytesTransferred: 512000,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// Mock the identity token generation
	originalGetStandardAuthToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
		return "test-token", nil
	}
	defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

	activity := activities.ADCActivity{SE: mockStorage}

	// Execute the function
	err := activity.FetchLogicalSizeAndUpdateActivity(ctx, volumeUUID, adcParams, server.URL)

	// Assertions
	assert.Nil(t, err)
	mockStorage.AssertExpectations(t)
}

func TestFetchLogicalSizeAndUpdateActivity_ADCError(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "volume-uuid"
	serviceURL := "https://test-service.com"

	// Create test ADC params with invalid data to trigger ADC error
	adcParams := &commonparams.ADCParams{
		AccessKey: "invalid-base64",
		SecretKey: "invalid-base64",
	}

	// Create mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.ADCActivity{SE: mockStorage}

	// Execute the function
	err := activity.FetchLogicalSizeAndUpdateActivity(ctx, volumeUUID, adcParams, serviceURL)

	// Assertions
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "An internal error occurred.")
	// Verify that UpdateLatestBackupLogicalSize was not called
	mockStorage.AssertNotCalled(t, "UpdateLatestBackupLogicalSize")
}

func TestFetchLogicalSizeAndUpdateActivity_UpdateDatabaseError(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "volume-uuid"
	expectedLogicalSize := uint64(1024000)
	expectedError := fmt.Errorf("database update failed")

	// Create test ADC params
	adcParams := &commonparams.ADCParams{
		ADCName:          "test-adc",
		DestEndpointUUID: "endpoint-uuid",
		SnapshotUUID:     "snapshot-uuid",
		BucketName:       "test-bucket",
		AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
		SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
		ProvideType:      "GoogleCloud",
		ServerURL:        "storage.googleapis.com",
		AccountName:      "test-account",
		Port:             443,
	}

	// Create mock storage
	mockStorage := database.NewMockStorage(t)
	mockStorage.On("UpdateLatestBackupLogicalSize", ctx, volumeUUID, int64(expectedLogicalSize)).Return(expectedError)

	// Create a test server that returns successful response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := activities.LogicalBytesResp{
			EndpointMetrics: activities.EndpointMetrics{
				LogicalSize:                expectedLogicalSize,
				CompressedBytesTransferred: 512000,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// Mock the identity token generation
	originalGetStandardAuthToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
		return "test-token", nil
	}
	defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

	activity := activities.ADCActivity{SE: mockStorage}

	// Execute the function
	err := activity.FetchLogicalSizeAndUpdateActivity(ctx, volumeUUID, adcParams, server.URL)

	// Assertions
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "database update failed")
	mockStorage.AssertExpectations(t)
}

func TestFetchLogicalSizeAndUpdateActivity_HTTPError(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "volume-uuid"

	// Create test ADC params
	adcParams := &commonparams.ADCParams{
		ADCName:          "test-adc",
		DestEndpointUUID: "endpoint-uuid",
		SnapshotUUID:     "snapshot-uuid",
		BucketName:       "test-bucket",
		AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
		SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
		ProvideType:      "GoogleCloud",
		ServerURL:        "storage.googleapis.com",
		AccountName:      "test-account",
		Port:             443,
	}

	// Create mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.ADCActivity{SE: mockStorage}

	// Mock the identity token generation
	originalGetStandardAuthToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
		return "test-token", nil
	}
	defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

	// Use an invalid URL to trigger HTTP error
	invalidURL := "https://invalid-url-that-will-fail.com"

	// Execute the function
	err := activity.FetchLogicalSizeAndUpdateActivity(ctx, volumeUUID, adcParams, invalidURL)

	// Assertions
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "An internal error occurred.")
	// Verify that UpdateLatestBackupLogicalSize was not called
	mockStorage.AssertNotCalled(t, "UpdateLatestBackupLogicalSize")
}

func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_Success(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "volume-uuid"
	deletedBackupVaultID := int64(1)
	expectedLogicalSize := uint64(1024000)
	latestBackupUUID := "latest-backup-uuid"

	adcParams := &commonparams.ADCParams{
		DestEndpointUUID: "endpoint-uuid",
		BucketName:       "test-bucket",
		AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
		SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
		ProvideType:      "GoogleCloud",
		ServerURL:        "storage.googleapis.com",
		Port:             443,
	}

	// Single vault (deleted vault): per-vault latest is the same as deleted
	backupsPerVault := []*datamodel.Backup{
		{
			BaseModel:     datamodel.BaseModel{UUID: "backup-1"},
			VolumeUUID:    volumeUUID,
			BackupVaultID: deletedBackupVaultID,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		},
	}
	latestBackup := &datamodel.Backup{
		BaseModel:  datamodel.BaseModel{UUID: latestBackupUUID},
		VolumeUUID: volumeUUID,
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestBackupUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		v, ok := updates["latest_logical_backup_size"].(int64)
		return ok && v == int64(expectedLogicalSize)
	})).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestBackupUUID).Return(nil)
	mockStorage.On("UpdateBackupChainHistory", ctx, volumeUUID, int64(expectedLogicalSize)).Return(nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := activities.LogicalBytesResp{
			EndpointMetrics: activities.EndpointMetrics{
				LogicalSize:                expectedLogicalSize,
				CompressedBytesTransferred: 512000,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	originalGetStandardAuthToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
		return "test-token", nil
	}
	defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, server.URL, deletedBackupVaultID)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_GetBucketDetailsFails_Continues covers the path
// where fetchLogicalSizeForOtherVault fails (e.g. getBucketDetailsForBucket returns error); activity continues and updates with 0.
func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_GetBucketDetailsFails_Continues(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "volume-uuid"
	deletedBackupVaultID := int64(1)
	latestBackupUUID := "latest-backup-uuid"

	adcParams := &commonparams.ADCParams{
		DestEndpointUUID: "ep-uuid",
		BucketName:       "test-bucket",
		AccessKey:        base64.StdEncoding.EncodeToString([]byte("key")),
		SecretKey:        base64.StdEncoding.EncodeToString([]byte("secret")),
		ProvideType:      "GoogleCloud",
		ServerURL:        "storage.googleapis.com",
		Port:             443,
	}

	// Other vault backup has bucket name that won't match vault's BucketDetails (vault has "other-bucket")
	backupsPerVault := []*datamodel.Backup{
		{
			BaseModel:     datamodel.BaseModel{UUID: "backup-other"},
			VolumeUUID:    volumeUUID,
			BackupVaultID: 2,
			Attributes:    &datamodel.BackupAttributes{BucketName: "wrong-bucket", EndpointUUID: "ep"},
		},
	}
	vault2 := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 2},
		Name:      "vault2",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "other-bucket", ServiceAccountName: "sa@proj.iam.gserviceaccount.com"},
		},
	}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestBackupUUID}, VolumeUUID: volumeUUID}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetBackupVaultById", ctx, int64(2)).Return(vault2, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestBackupUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		v, ok := updates["latest_logical_backup_size"].(int64)
		return ok && v == int64(0)
	})).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestBackupUUID).Return(nil)
	mockStorage.On("UpdateBackupChainHistory", ctx, volumeUUID, int64(0)).Return(nil)

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, "http://localhost:9999", deletedBackupVaultID)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_GetLatestBackupByVolumeUUIDError(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(nil, errors.New("db error"))

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, "", 1)

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_UpdateBackupFieldsError(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	latestUUID := "latest-uuid"
	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestUUID}, VolumeUUID: volumeUUID}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestUUID, mock.Anything).Return(errors.New("update failed"))

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, "", 1)

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_UpdateBackupChainHistoryFails_WarnsOnly(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	latestUUID := "latest-uuid"
	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestUUID}, VolumeUUID: volumeUUID}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestUUID, mock.Anything).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestUUID).Return(nil)
	mockStorage.On("UpdateBackupChainHistory", ctx, volumeUUID, int64(0)).Return(errors.New("history update failed"))

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, "", 1)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_GetBackupVaultByIdReturnsNilVault_Continues
// covers getBucketDetailsForBucket(backupVault==nil) when GetBackupVaultById returns (nil, nil) for another vault.
func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_GetBackupVaultByIdReturnsNilVault_Continues(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	deletedBackupVaultID := int64(1)
	latestBackupUUID := "latest-uuid"

	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "other"}, VolumeUUID: volumeUUID, BackupVaultID: 2, Attributes: &datamodel.BackupAttributes{BucketName: "b2"}},
	}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestBackupUUID}, VolumeUUID: volumeUUID}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetBackupVaultById", ctx, int64(2)).Return((*datamodel.BackupVault)(nil), nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestBackupUUID, mock.Anything).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestBackupUUID).Return(nil)
	mockStorage.On("UpdateBackupChainHistory", ctx, volumeUUID, int64(0)).Return(nil)

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, "", deletedBackupVaultID)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_NilBackupInList_Continues covers the loop continue when b == nil.
func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_NilBackupInList_Continues(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	deletedBackupVaultID := int64(1)
	latestBackupUUID := "latest-uuid"

	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{
		nil,
		{BaseModel: datamodel.BaseModel{UUID: "b1"}, VolumeUUID: volumeUUID, BackupVaultID: deletedBackupVaultID, Attributes: &datamodel.BackupAttributes{BucketName: "b", EndpointUUID: "ep"}},
	}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestBackupUUID}, VolumeUUID: volumeUUID}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(activities.LogicalBytesResp{EndpointMetrics: activities.EndpointMetrics{LogicalSize: 1024}})
	}))
	defer server.Close()

	origToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) { return "token", nil }
	defer func() { activities.GetStandardAuthToken = origToken }()

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestBackupUUID, mock.Anything).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestBackupUUID).Return(nil)
	mockStorage.On("UpdateBackupChainHistory", ctx, volumeUUID, int64(1024)).Return(nil)

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, server.URL, deletedBackupVaultID)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_CalculateLogicalBytesFails_Continues covers continue when deleted-vault size fetch fails.
func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_CalculateLogicalBytesFails_Continues(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	deletedBackupVaultID := int64(1)
	latestBackupUUID := "latest-uuid"

	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "b1"}, VolumeUUID: volumeUUID, BackupVaultID: deletedBackupVaultID, Attributes: &datamodel.BackupAttributes{BucketName: "b", EndpointUUID: "ep"}},
	}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestBackupUUID}, VolumeUUID: volumeUUID}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	origToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) { return "token", nil }
	defer func() { activities.GetStandardAuthToken = origToken }()

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestBackupUUID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		v, ok := updates["latest_logical_backup_size"].(int64)
		return ok && v == 0
	})).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestBackupUUID).Return(nil)
	mockStorage.On("UpdateBackupChainHistory", ctx, volumeUUID, int64(0)).Return(nil)

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, server.URL, deletedBackupVaultID)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_UpdateBackupLatestLogicalBackupSizeByVolumeError
func TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_UpdateBackupLatestLogicalBackupSizeByVolumeError(t *testing.T) {
	ctx := context.Background()
	volumeUUID := "vol-uuid"
	latestUUID := "latest-uuid"
	adcParams := &commonparams.ADCParams{BucketName: "b", ServerURL: "http://x", Port: 443}
	backupsPerVault := []*datamodel.Backup{}
	latestBackup := &datamodel.Backup{BaseModel: datamodel.BaseModel{UUID: latestUUID}, VolumeUUID: volumeUUID}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(backupsPerVault, nil)
	mockStorage.On("GetLatestBackupByVolumeUUID", ctx, volumeUUID).Return(latestBackup, nil)
	mockStorage.On("UpdateBackupFields", ctx, latestUUID, mock.Anything).Return(nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, volumeUUID, latestUUID).Return(errors.New("update latest size failed"))

	activity := activities.ADCActivity{SE: mockStorage}
	err := activity.FetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity(ctx, volumeUUID, adcParams, "", 1)

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_ActiveVault_EndpointInfoError_Continues(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	volumeUUID := "vol-uuid"
	vaultUUID := "v1"
	vaultID := int64(1)
	accountID := int64(10)

	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		AccountID:      accountID,
		DataProtection: &datamodel.DataProtection{BackupVaultID: vaultUUID},
	}
	vault := &datamodel.BackupVault{BaseModel: datamodel.BaseModel{UUID: vaultUUID, ID: vaultID}, Name: "v1"}
	latestPerVault := []*datamodel.Backup{
		{
			BaseModel:     datamodel.BaseModel{UUID: "b1"},
			BackupVaultID: vaultID,
			Attributes: &datamodel.BackupAttributes{
				BucketName:      "b1",
				ObjectStoreUUID: "obj-uuid",
				EndpointUUID:    "ep-uuid",
			},
		},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", mock.Anything, volumeUUID).Return(vol, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", mock.Anything, vaultUUID, accountID).Return(vault, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", mock.Anything, volumeUUID).Return(latestPerVault, nil)

	mockProvider := new(vsa.MockProvider)
	mockProvider.On("ObjectStoreEndpointInfoGet", "obj-uuid", "ep-uuid").Return(nil, errors.New("endpoint unreachable"))
	origGetProvider := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = origGetProvider }()

	activity := activities.ADCActivity{SE: mockStorage}
	env.RegisterActivity(&activity)
	encoded, err := env.ExecuteActivity(activity.GetSummedLogicalBackupSizeAllVaultsActivity, volumeUUID, node, "")
	assert.NoError(t, err)
	var sum int64
	assert.NoError(t, encoded.Get(&sum))
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_ADCError_Continues(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vaultID := int64(2)
	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: vaultID},
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "bucket2", ServiceAccountName: "sa@proj.iam.gserviceaccount.com", TenantProjectNumber: "123"},
		},
	}
	latestPerVault := []*datamodel.Backup{
		{
			BaseModel:     datamodel.BaseModel{UUID: "detached-b"},
			BackupVaultID: vaultID,
			Attributes:    &datamodel.BackupAttributes{BucketName: "bucket2", EndpointUUID: "ep"},
		},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", mock.Anything, volumeUUID).Return(&datamodel.Volume{DataProtection: &datamodel.DataProtection{}}, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", mock.Anything, volumeUUID).Return(latestPerVault, nil)
	mockStorage.On("GetBackupVaultById", mock.Anything, vaultID).Return(vault, nil)
	origGetCloud := activities.GetCloudService
	activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
		return nil, errors.New("no cloud service")
	}
	defer func() { activities.GetCloudService = origGetCloud }()

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "http://localhost:9999")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

// TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_EmptyServiceAccount_Continues covers fetchLogicalSizeForOtherVault when serviceAccountEmail returns ""
func TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_EmptyServiceAccount_Continues(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vaultID := int64(2)
	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: vaultID},
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "bucket2", ServiceAccountName: "", TenantProjectNumber: ""},
		},
	}
	latestPerVault := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "detached-b"}, BackupVaultID: vaultID, Attributes: &datamodel.BackupAttributes{BucketName: "bucket2", EndpointUUID: "ep"}},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", mock.Anything, volumeUUID).Return(&datamodel.Volume{DataProtection: &datamodel.DataProtection{}}, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", mock.Anything, volumeUUID).Return(latestPerVault, nil)
	mockStorage.On("GetBackupVaultById", mock.Anything, vaultID).Return(vault, nil)

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "http://localhost:9999")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

// TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_CreateHmacKeysError_Continues covers fetchLogicalSizeForOtherVault when CreateHmacKeys fails
func TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_CreateHmacKeysError_Continues(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vaultID := int64(2)
	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: vaultID},
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "bucket2", ServiceAccountName: "mysa", TenantProjectNumber: "123"},
		},
	}
	latestPerVault := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "detached-b"}, BackupVaultID: vaultID, Attributes: &datamodel.BackupAttributes{BucketName: "bucket2", EndpointUUID: "ep"}},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", mock.Anything, volumeUUID).Return(&datamodel.Volume{DataProtection: &datamodel.DataProtection{}}, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", mock.Anything, volumeUUID).Return(latestPerVault, nil)
	mockStorage.On("GetBackupVaultById", mock.Anything, vaultID).Return(vault, nil)
	mockGCPService := new(hyperscaler.MockGoogleServices)
	mockGCPService.On("CreateHmacKey", "123", "mysa@123.iam.gserviceaccount.com").Return(nil, nil, errors.New("hmac key failed"))
	origGetCloud := activities.GetCloudService
	activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) { return mockGCPService, nil }
	defer func() { activities.GetCloudService = origGetCloud }()

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "http://localhost:9999")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
	mockGCPService.AssertExpectations(t)
}

// TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_CalculateLogicalBytesFails_Continues covers fetchLogicalSizeForOtherVault when CalculateLogicalBytesAndOptimizedBytes fails
func TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_CalculateLogicalBytesFails_Continues(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vaultID := int64(2)
	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: vaultID},
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "bucket2", ServiceAccountName: "sa@proj.iam.gserviceaccount.com", TenantProjectNumber: "123"},
		},
	}
	latestPerVault := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "detached-b"}, BackupVaultID: vaultID, Attributes: &datamodel.BackupAttributes{BucketName: "bucket2", EndpointUUID: "ep"}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", mock.Anything, volumeUUID).Return(&datamodel.Volume{DataProtection: &datamodel.DataProtection{}}, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", mock.Anything, volumeUUID).Return(latestPerVault, nil)
	mockStorage.On("GetBackupVaultById", mock.Anything, vaultID).Return(vault, nil)
	origToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) { return "token", nil }
	defer func() { activities.GetStandardAuthToken = origToken }()
	accessKey, secretKey := "ak", "sk"
	mockGCPService := new(hyperscaler.MockGoogleServices)
	mockGCPService.On("CreateHmacKey", "123", "sa@proj.iam.gserviceaccount.com").Return(&accessKey, &secretKey, nil)
	origGetCloud := activities.GetCloudService
	activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) { return mockGCPService, nil }
	defer func() { activities.GetCloudService = origGetCloud }()

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, server.URL)

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
	mockGCPService.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_DescribeVolumeError(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(nil, errors.New("describe failed"))

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.Error(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_VolumeNil(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(nil, nil)

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.Error(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_DataProtectionNil(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vol := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}, DataProtection: nil}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(vol, nil)

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.Error(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_GetLatestBackupsPerVaultError(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{BackupVaultID: ""},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(vol, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(nil, errors.New("per-vault failed"))

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.Error(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_Success_EmptyBackups(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{BackupVaultID: ""},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(vol, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return([]*datamodel.Backup{}, nil)

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_SkipsNilBackup(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{BackupVaultID: ""},
	}
	latestPerVault := []*datamodel.Backup{nil, {BaseModel: datamodel.BaseModel{UUID: "b"}, Attributes: nil}}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(vol, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(latestPerVault, nil)

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

func TestGetSummedLogicalBackupSizeAllVaultsActivity_SkipsBackupWithEmptyBucketName(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{BackupVaultID: ""},
	}
	latestPerVault := []*datamodel.Backup{
		{BaseModel: datamodel.BaseModel{UUID: "b1"}, BackupVaultID: 1, Attributes: &datamodel.BackupAttributes{BucketName: ""}},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(vol, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(latestPerVault, nil)

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, "")

	assert.NoError(t, err)
	assert.Equal(t, int64(0), sum)
	mockStorage.AssertExpectations(t)
}

// TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_WithHTTPServer tests the detached-vault path
// using a real HTTP mock server (httptest), matching other ADC tests (e.g. TestFetchSummedLogicalSizeFromAllVaultsViaADCAndUpdateActivity_Success).
func TestGetSummedLogicalBackupSizeAllVaultsActivity_DetachedVault_WithHTTPServer(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "vol-uuid"
	detachedVaultID := int64(1)
	expectedLogicalSize := uint64(2048)

	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{BackupVaultID: ""}, // activeVaultID stays 0
	}
	latestPerVault := []*datamodel.Backup{
		{
			BaseModel:     datamodel.BaseModel{UUID: "detached-backup"},
			Name:          "detached",
			BackupVaultID: detachedVaultID,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "ep-uuid",
			},
		},
	}
	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: detachedVaultID},
		BucketDetails: datamodel.BucketDetailsArray{
			{
				BucketName:          "test-bucket",
				ServiceAccountName:  "adc-sa@test.iam.gserviceaccount.com",
				TenantProjectNumber: "123456789",
			},
		},
	}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", ctx, volumeUUID).Return(vol, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", ctx, volumeUUID).Return(latestPerVault, nil)
	mockStorage.On("GetBackupVaultById", ctx, detachedVaultID).Return(vault, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := activities.LogicalBytesResp{
			EndpointMetrics: activities.EndpointMetrics{
				LogicalSize:                expectedLogicalSize,
				CompressedBytesTransferred: 1000,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	accessKey := "test-access-key"
	secretKey := "test-secret-key"
	mockGCPService := new(hyperscaler.MockGoogleServices)
	mockGCPService.On("CreateHmacKey", "123456789", "adc-sa@test.iam.gserviceaccount.com").Return(&accessKey, &secretKey, nil)
	origGetCloudService := activities.GetCloudService
	activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
		return mockGCPService, nil
	}
	defer func() { activities.GetCloudService = origGetCloudService }()

	origGetStandardAuthToken := activities.GetStandardAuthToken
	activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
		return "test-token", nil
	}
	defer func() { activities.GetStandardAuthToken = origGetStandardAuthToken }()

	activity := activities.ADCActivity{SE: mockStorage}
	sum, err := activity.GetSummedLogicalBackupSizeAllVaultsActivity(ctx, volumeUUID, nil, server.URL)

	assert.NoError(t, err)
	assert.Equal(t, int64(expectedLogicalSize), sum)
	mockStorage.AssertExpectations(t)
	mockGCPService.AssertExpectations(t)
}

// TestGetSummedLogicalBackupSizeAllVaultsActivity_ActiveVault_EndpointInfo covers the active-vault path when node and
// backup have ObjectStoreUUID/EndpointUUID; size is read via GetObjectStoreEndpointInfo. Runs in activity env so context is valid.
func TestGetSummedLogicalBackupSizeAllVaultsActivity_ActiveVault_EndpointInfo(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	volumeUUID := "vol-uuid"
	vaultUUID := "vault-uuid-1"
	vaultID := int64(1)
	accountID := int64(10)
	expectedSize := int64(4096)

	vol := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		AccountID:      accountID,
		DataProtection: &datamodel.DataProtection{BackupVaultID: vaultUUID},
	}
	vault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: vaultUUID, ID: vaultID},
		Name:      "vault1",
	}
	latestPerVault := []*datamodel.Backup{
		{
			BaseModel:     datamodel.BaseModel{UUID: "b1"},
			BackupVaultID: vaultID,
			Attributes: &datamodel.BackupAttributes{
				BucketName:      "bucket1",
				ObjectStoreUUID: "obj-uuid",
				EndpointUUID:    "ep-uuid",
			},
		},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}

	mockStorage := database.NewMockStorage(t)
	mockStorage.On("DescribeVolume", mock.Anything, volumeUUID).Return(vol, nil)
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", mock.Anything, vaultUUID, accountID).Return(vault, nil)
	mockStorage.On("GetLatestBackupsPerVaultByVolumeUUID", mock.Anything, volumeUUID).Return(latestPerVault, nil)

	mockProvider := new(vsa.MockProvider)
	mockProvider.On("ObjectStoreEndpointInfoGet", "obj-uuid", "ep-uuid").Return(&vsa.SmObjectStoreEndpointt{LogicalSize: &expectedSize}, nil)
	origGetProvider := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, n *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = origGetProvider }()

	activity := activities.ADCActivity{SE: mockStorage}
	env.RegisterActivity(&activity)

	encoded, err := env.ExecuteActivity(activity.GetSummedLogicalBackupSizeAllVaultsActivity, volumeUUID, node, "")
	assert.NoError(t, err)
	var sum int64
	assert.NoError(t, encoded.Get(&sum))
	assert.Equal(t, expectedSize, sum)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestGetFileInodeNumbers(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "GET", r.Method)
			assert.Contains(t, r.URL.Path, "/file1.txt")
			assert.Equal(t, "test-access-key", r.Header.Get("access_key"))
			assert.Equal(t, "test-secret-key", r.Header.Get("secret_password"))
			assert.Equal(t, "Bearer test-identity-token", r.Header.Get("Authorization"))

			response := map[string]interface{}{
				"records": []map[string]interface{}{
					{
						"inode":    12345,
						"size":     1024,
						"filename": "file1.txt",
					},
				},
				"end-of-list": true,
				"num-records": 1,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "12345", result["/file1.txt"].Inode)
		assert.Equal(t, int64(1024), result["/file1.txt"].Size)
	})

	t.Run("MultipleFiles", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt", "/file2.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		callCount := 0
		// Create mock HTTP server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			response := map[string]interface{}{
				"records": []map[string]interface{}{
					{
						"inode":    10000 + callCount,
						"size":     1024 * callCount,
						"filename": fmt.Sprintf("file%d.txt", callCount),
					},
				},
				"end-of-list": true,
				"num-records": 1,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 2, len(result))
	})

	t.Run("GetStandardAuthTokenError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken to return error
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "", errors.New("failed to get identity token")
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		serviceURL := "https://adc-service.run.app"
		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("InvalidAccessKey", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        "invalid-base64", // Invalid base64
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		serviceURL := "https://adc-service.run.app"
		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to decode access key")
	})

	t.Run("InvalidSecretKey", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        "invalid-base64", // Invalid base64
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		serviceURL := "https://adc-service.run.app"
		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to decode secret key")
	})

	t.Run("HTTPRequestError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		// Use invalid URL to trigger HTTP error
		serviceURL := "https://invalid-url-that-will-fail.com"
		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// When HTTP request fails, the function continues and returns empty map with nil error
		// The workflow layer handles the empty map case and returns an error
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("FileNotFound", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/missing-file.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning 404
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// When file is not found (404), the function logs a warning and returns empty map with nil error
		// The workflow layer handles the empty map case and returns an error
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("TooManyRequests", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning 429
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// Should return empty map with no error when no files were successfully retrieved
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("InvalidJSONResponse", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning invalid JSON
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("invalid json"))
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// Should return empty map with no error when JSON parsing fails
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("MultipleRecords", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/directory"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning multiple records (directory)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := map[string]interface{}{
				"records": []map[string]interface{}{
					{"inode": 1, "size": 100, "filename": "file1"},
					{"inode": 2, "size": 200, "filename": "file2"},
				},
				"end-of-list": true,
				"num-records": 2,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// Should return empty map with no error when directory has multiple records
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("ZeroInode", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning zero inode
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := map[string]interface{}{
				"records": []map[string]interface{}{
					{
						"inode":    0, // Invalid inode
						"size":     1024,
						"filename": "file1.txt",
					},
				},
				"end-of-list": true,
				"num-records": 1,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// Should return empty map with no error when zero inode is invalid
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("PartialSuccess", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt", "/file2.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		callCount := 0
		// Create mock HTTP server - first file succeeds, second fails
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			if callCount == 1 {
				// First file succeeds
				response := map[string]interface{}{
					"records": []map[string]interface{}{
						{
							"inode":    12345,
							"size":     1024,
							"filename": "file1.txt",
						},
					},
					"end-of-list": true,
					"num-records": 1,
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			} else {
				// Second file not found
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// Should succeed with partial results
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "12345", result["/file1.txt"].Inode)
	})

	t.Run("TemporaryRedirect", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning 307 redirect
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			response := map[string]interface{}{
				"records": []map[string]interface{}{
					{
						"inode":    12345,
						"size":     1024,
						"filename": "file1.txt",
					},
				},
				"end-of-list": true,
				"num-records": 1,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTemporaryRedirect)
			_ = json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result))
	})

	t.Run("HTTPRequestCreationFailure", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Use invalid URL that will cause http.NewRequest to fail
		// Using a URL with invalid characters that will cause parsing to fail
		serviceURL := "http://[invalid-url"

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		assert.Error(t, err)
		assert.Nil(t, result)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Contains(t, customErr.OriginalErr.Error(), "failed to create HTTP request")
		}
	})

	t.Run("ResponseBodyReadError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server that returns a response with a body that fails on read
		// We'll use a custom handler that closes the connection prematurely
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Set headers and status before attempting to write
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// Try to hijack and close connection to cause read error
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				if conn != nil {
					err := conn.Close()
					if err != nil {
						return
					}
				}
			}
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// When response body read fails, the function continues and returns empty map with nil error
		// The workflow layer handles the empty map case and returns an error
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("OtherStatusCode", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create mock HTTP server returning 500 Internal Server Error
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Internal Server Error"))
		}))
		defer server.Close()

		serviceURL := server.URL

		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// When server returns non-OK status code, the function logs a warning and returns empty map with nil error
		// The workflow layer handles the empty map case and returns an error
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result))
	})

	t.Run("ResponseBodyCloseError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockSE := database.NewMockStorage(t)
		activity := activities.ADCActivity{SE: mockSE}

		adcParams := &commonparams.ADCParams{
			DestEndpointUUID: "endpoint-uuid",
			SnapshotUUID:     "snapshot-uuid",
			BucketName:       "test-bucket",
			AccessKey:        base64.StdEncoding.EncodeToString([]byte("test-access-key")),
			SecretKey:        base64.StdEncoding.EncodeToString([]byte("test-secret-key")),
			ProvideType:      "GoogleCloud",
			ServerURL:        "storage.googleapis.com",
			Port:             443,
		}

		filePaths := []string{"/file1.txt"}

		// Mock GetStandardAuthToken
		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context, audience string) (string, error) {
			return "test-identity-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		// Create a mock ReadCloser that returns an error on Close (line 608)
		closeErr := errors.New("close error")
		mockBody := &mockReadCloser{
			data:     []byte(`{"records":[{"inode":12345,"size":1024,"filename":"file1.txt"}],"end-of-list":true,"num-records":1}`),
			readErr:  nil,
			closeErr: closeErr,
		}

		// Override the HTTP client to return our custom body
		originalHTTPClient := activities.RestHTTPClient
		activities.RestHTTPClient = &mockHTTPClient{
			transport: &mockHTTPTransport{
				response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       mockBody,
					Header:     make(http.Header),
				},
			},
		}
		defer func() { activities.RestHTTPClient = originalHTTPClient }()

		serviceURL := "https://test-service.com"

		// The function should handle the close error gracefully (line 608)
		result, err := activity.GetFileInodeNumbers(ctx, adcParams, serviceURL, filePaths)
		// Should still succeed despite close error
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result))
	})
}

// mockReadCloser is a mock io.ReadCloser for testing
type mockReadCloser struct {
	data      []byte
	readErr   error
	closeErr  error
	readCount int
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	if len(m.data) == 0 {
		return 0, io.EOF
	}
	n = copy(p, m.data)
	m.data = m.data[n:]
	m.readCount++
	return n, nil
}

func (m *mockReadCloser) Close() error {
	return m.closeErr
}

// mockHTTPTransport is a helper to mock HTTP transport
type mockHTTPTransport struct {
	response *http.Response
}

func (m *mockHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.response, nil
}

// mockHTTPClient implements rest.HTTPClient interface
type mockHTTPClient struct {
	transport *mockHTTPTransport
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.transport.RoundTrip(req)
}
