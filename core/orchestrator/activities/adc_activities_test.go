package activities_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
				"description":                "ADC service for backup and restore operations",
				"run.googleapis.com/ingress": "internal",
			},
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"DISABLE_VERIFY_SSL": "0",
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
				"description":                "ADC service for backup and restore operations",
				"run.googleapis.com/ingress": "internal",
			},
			EnvVars: map[string]string{
				"RUN_REST":           "1",
				"REST_PORT":          "80",
				"PROVIDER":           "GoogleCloud",
				"LOG_LEVEL":          "2",
				"DISABLE_VERIFY_SSL": "0",
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
		ctx := context.Background()
		projectID := "test-project"
		region := "us-central1"
		serviceName := "adc-svc-test"

		expectedResponse := &hyperscalermodels.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteCloudRunService", ctx, projectID, region, serviceName).Return(expectedResponse, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		response, err := activity.CleanupADCCloudRunService(ctx, projectID, region, serviceName)
		assert.Nil(t, err)
		assert.Equal(t, expectedResponse, response)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		response, err := activity.CleanupADCCloudRunService(ctx, "project", "region", "service")
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})

	t.Run("OnDeleteServiceFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("DeleteCloudRunService", ctx, "project", "region", "service").Return(nil, fmt.Errorf("failed to delete service"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		response, err := activity.CleanupADCCloudRunService(ctx, "project", "region", "service")
		assert.NotNil(t, err)
		assert.Nil(t, response)
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
		ctx := context.Background()
		operationName := "operations/test-operation"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CheckOperationStatus", ctx, operationName).Return(true, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		isReady, err := activity.CheckOperationStatus(ctx, operationName)
		assert.Nil(t, err)
		assert.True(t, isReady)
	})

	t.Run("OnSuccess_OperationInProgress", func(t *testing.T) {
		ctx := context.Background()
		operationName := "operations/test-operation"

		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CheckOperationStatus", ctx, operationName).Return(false, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		isReady, err := activity.CheckOperationStatus(ctx, operationName)
		assert.Nil(t, err)
		assert.False(t, isReady)
	})

	t.Run("OnCloudServiceGetFailure", func(t *testing.T) {
		ctx := context.Background()
		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("failed to get cloud service"))
		}

		activity := activities.ADCActivity{}
		isReady, err := activity.CheckOperationStatus(ctx, "operation")
		assert.NotNil(t, err)
		assert.False(t, isReady)
	})

	t.Run("OnCheckStatusFailure", func(t *testing.T) {
		ctx := context.Background()
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("CheckOperationStatus", ctx, "operation").Return(false, fmt.Errorf("failed to check status"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}

		activity := activities.ADCActivity{}
		isReady, err := activity.CheckOperationStatus(ctx, "operation")
		assert.NotNil(t, err)
		assert.False(t, isReady)
	})
}

func TestCreateHmacKeys(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		params := &common.HmacKeyCreateParams{
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
		keys, err := activity.CreateHmacKeys(ctx, &common.HmacKeyCreateParams{})
		assert.NotNil(t, err)
		assert.Nil(t, keys)
	})

	t.Run("OnCreateHmacKeyFailure", func(t *testing.T) {
		ctx := context.Background()
		params := &common.HmacKeyCreateParams{
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
		params := &common.HmacKeyCreateParams{
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
		assert.Len(t, timestamp, 14)
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
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
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
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
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
		adcParams := &common.ADCParams{
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
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
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
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.InitialDeleteRequestWithCloudRun(ctx, adcParams, serviceURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})
}

func TestCheckDeleteStatusWithCloudRun(t *testing.T) {
	t.Run("OnSuccess_WithNewRedirect", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "/api/status/456")
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		defer server.Close()

		ctx := setupTestContext()
		params := createTestADCParams()
		serviceURL := server.URL
		redirectURL := "/api/status/123"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.CheckDeleteStatusWithCloudRun(ctx, params, serviceURL, redirectURL)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusTemporaryRedirect, response.StatusCode)
		assert.Equal(t, "/api/status/456", response.RedirectURL)
	})

	t.Run("OnSuccess_Completion", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := setupTestContext()
		params := createTestADCParams()
		serviceURL := server.URL
		redirectURL := "/api/status/123"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
			return "test-token", nil
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.CheckDeleteStatusWithCloudRun(ctx, params, serviceURL, redirectURL)
		assert.Nil(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, http.StatusOK, response.StatusCode)
	})

	t.Run("OnEmptyRedirectURL", func(t *testing.T) {
		ctx := setupTestContext()
		params := createTestADCParams()
		serviceURL := "http://test.com"
		redirectURL := ""

		activity := activities.ADCActivity{}
		response, err := activity.CheckDeleteStatusWithCloudRun(ctx, params, serviceURL, redirectURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
		assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "missing redirect URL")
	})

	t.Run("OnInvalidADCParams", func(t *testing.T) {
		ctx := setupTestContext()
		params := &common.ADCParams{
			AccessKey: "invalid-base64",
			SecretKey: "invalid-base64",
		}
		serviceURL := "http://test.com"
		redirectURL := "/api/status/123"

		activity := activities.ADCActivity{}
		response, err := activity.CheckDeleteStatusWithCloudRun(ctx, params, serviceURL, redirectURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})

	t.Run("OnIdentityTokenFailure", func(t *testing.T) {
		ctx := setupTestContext()
		params := createTestADCParams()
		serviceURL := "http://test.com"
		redirectURL := "/api/status/123"

		originalGetStandardAuthToken := activities.GetStandardAuthToken
		activities.GetStandardAuthToken = func(ctx context.Context) (string, error) {
			return "", fmt.Errorf("failed to get token")
		}
		defer func() { activities.GetStandardAuthToken = originalGetStandardAuthToken }()

		activity := activities.ADCActivity{}
		response, err := activity.CheckDeleteStatusWithCloudRun(ctx, params, serviceURL, redirectURL)
		assert.NotNil(t, err)
		assert.Nil(t, response)
	})
}

func TestGetStandardAuthToken(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := setupTestContext()
		expectedToken := "test-identity-token"

		originalGetCloudService := activities.GetCloudService
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetIdentityToken").Return(expectedToken, nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		defer func() { activities.GetCloudService = originalGetCloudService }()

		token, err := activities.GetStandardAuthToken(ctx)
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

		token, err := activities.GetStandardAuthToken(ctx)
		assert.NotNil(t, err)
		assert.Empty(t, token)
	})

	t.Run("OnGetIdentityTokenFailure", func(t *testing.T) {
		ctx := setupTestContext()

		originalGetCloudService := activities.GetCloudService
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetIdentityToken").Return("", fmt.Errorf("failed to get identity token"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler.Services, error) {
			return mockGCPService, nil
		}
		defer func() { activities.GetCloudService = originalGetCloudService }()

		token, err := activities.GetStandardAuthToken(ctx)
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

func createTestADCParams() *common.ADCParams {
	return &common.ADCParams{
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
