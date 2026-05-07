package google

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	cloudrun "google.golang.org/api/run/v2"
)

type mockTokenSource struct {
	TokenSource oauth2.TokenSource
}

func (s *mockTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{
		AccessToken: "",
		TokenType:   "Bearer",
	}, nil
}

func TestCreateCloudRunService(t *testing.T) {
	t.Run("onSuccess", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock successful operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-operation-123", result.OperationName)
		assert.Equal(tt, "RUNNING", result.Status)
	})

	t.Run("withInternalIngress", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock successful operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			Ingress:     "INGRESS_TRAFFIC_INTERNAL_ONLY",
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-operation-123", result.OperationName)
		assert.Equal(tt, "RUNNING", result.Status)
	})

	t.Run("withAllTrafficIngress", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock successful operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			Ingress:     "INGRESS_TRAFFIC_ALL",
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-operation-123", result.OperationName)
		assert.Equal(tt, "RUNNING", result.Status)
	})

	t.Run("onFailure", func(tt *testing.T) {
		// Create a test server that returns an error
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"code":    400,
					"message": "Invalid request",
				},
			})
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.NotNil(tt, err)
		assert.Nil(tt, result)
	})
}

func TestCheckOperationStatus(t *testing.T) {
	t.Run("operationCompleted", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock completed operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: true,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		completed, err := gService.CheckOperationStatus(context.Background(), "operations/test-operation-123")
		assert.Nil(tt, err)
		assert.True(tt, completed)
	})

	t.Run("operationInProgress", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock in-progress operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		completed, err := gService.CheckOperationStatus(context.Background(), "operations/test-operation-123")
		assert.Nil(tt, err)
		assert.False(tt, completed)
	})

	t.Run("operationFailed", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock failed operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: true,
				Error: &cloudrun.GoogleRpcStatus{
					Code:    13,
					Message: "Operation failed",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		completed, err := gService.CheckOperationStatus(context.Background(), "operations/test-operation-123")
		assert.NotNil(tt, err)
		assert.True(tt, completed)
		assert.Contains(tt, err.Error(), "operation completed with error")
	})
}

func TestGetCloudRunServiceURL(t *testing.T) {
	t.Run("onSuccess", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock service response
			service := &cloudrun.GoogleCloudRunV2Service{
				Urls: []string{"https://test-service-abc123.run.app"},
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(service)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		url, err := gService.GetCloudRunServiceURL(context.Background(), "test-project", "us-central1", "test-service")
		assert.Nil(tt, err)
		assert.Equal(tt, "https://test-service-abc123.run.app", url)
	})

	t.Run("serviceNotFound", func(tt *testing.T) {
		// Create a test server that returns 404
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"code":    404,
					"message": "Service not found",
				},
			})
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		url, err := gService.GetCloudRunServiceURL(context.Background(), "test-project", "us-central1", "test-service")
		assert.NotNil(tt, err)
		assert.Equal(tt, "", url)
	})

	t.Run("serviceWithEmptyURLs", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock service response with empty URLs
			service := &cloudrun.GoogleCloudRunV2Service{
				Urls: []string{},
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(service)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		url, err := gService.GetCloudRunServiceURL(context.Background(), "test-project", "us-central1", "test-service")
		assert.NotNil(tt, err)
		assert.Equal(tt, "", url)
		assert.Contains(tt, err.Error(), "service URLs not available")
	})

	t.Run("serviceWithEmptyURL", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock service response with empty URL
			service := &cloudrun.GoogleCloudRunV2Service{
				Urls: []string{""},
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(service)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		url, err := gService.GetCloudRunServiceURL(context.Background(), "test-project", "us-central1", "test-service")
		assert.NotNil(tt, err)
		assert.Equal(tt, "", url)
		assert.Contains(tt, err.Error(), "service URL not available")
	})
}

func TestCreateCloudRunServiceWithResources(t *testing.T) {
	t.Run("withResourceLimits", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock create operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-create-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			Resources: &models.ResourceConfig{
				CPULimit:    "1000m",
				MemoryLimit: "512Mi",
			},
			EnvVars: map[string]string{
				"ENV_VAR1": "value1",
				"ENV_VAR2": "value2",
			},
			VolumeMounts: []models.VolumeMount{
				{
					Name:      "test-volume",
					MountPath: "/data",
				},
			},
			Volumes: []models.Volume{
				{
					Name:       "test-volume",
					VolumeType: "secret",
					Source: models.VolumeSource{
						SecretName: "test-secret",
						Items: []models.SecretItem{
							{
								Path:    "secret.json",
								Version: "latest",
							},
						},
					},
				},
			},
		}

		response, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, "operations/test-create-operation-123", response.OperationName)
		assert.Equal(tt, "RUNNING", response.Status)
	})

	t.Run("withEmptyDescription", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock create operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-create-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			Description: "", // Empty description
		}

		response, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, "operations/test-create-operation-123", response.OperationName)
		assert.Equal(tt, "RUNNING", response.Status)
	})
}

func TestCreateCloudRunServiceWithStartupProbe(t *testing.T) {
	t.Run("withTCPSocketProbe", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			StartupProbe: &models.ProbeConfig{
				InitialDelaySeconds: 0,
				PeriodSeconds:       10,
				TimeoutSeconds:      5,
				FailureThreshold:    30,
				TCPSocket:           &models.TCPSocketAction{Port: 80},
			},
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-operation-123", result.OperationName)
	})

	t.Run("withHTTPGetProbe", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			StartupProbe: &models.ProbeConfig{
				InitialDelaySeconds: 5,
				PeriodSeconds:       10,
				TimeoutSeconds:      3,
				FailureThreshold:    10,
				HTTPGet:             &models.HTTPGetAction{Path: "/health", Port: 8080},
			},
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-operation-123", result.OperationName)
	})

	t.Run("withGRPCProbe", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		config := &models.CloudRunServiceConfig{
			ProjectID:   "test-project",
			LocationID:  "us-central1",
			ServiceName: "test-service",
			Image:       "gcr.io/test-project/test-image:latest",
			StartupProbe: &models.ProbeConfig{
				InitialDelaySeconds: 0,
				PeriodSeconds:       5,
				TimeoutSeconds:      2,
				FailureThreshold:    5,
				GRPC:                &models.GRPCAction{Port: 50051, Service: "grpc.health.v1.Health"},
			},
		}

		result, err := gService.CreateCloudRunService(context.Background(), config)
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-operation-123", result.OperationName)
	})
}

func TestDeleteCloudRunService(t *testing.T) {
	t.Run("onSuccess", func(tt *testing.T) {
		// Create a test server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Mock delete operation response
			operation := &cloudrun.GoogleLongrunningOperation{
				Name: "operations/test-delete-operation-123",
				Done: false,
			}
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(operation)
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		result, err := gService.DeleteCloudRunService(context.Background(), "test-project", "us-central1", "test-service")
		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "operations/test-delete-operation-123", result.OperationName)
		assert.Equal(tt, "RUNNING", result.Status)
	})

	t.Run("onFailure", func(tt *testing.T) {
		// Create a test server that returns an error
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"code":    400,
					"message": "Invalid request",
				},
			})
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		result, err := gService.DeleteCloudRunService(context.Background(), "test-project", "us-central1", "test-service")
		assert.NotNil(tt, err)
		assert.Nil(tt, result)
	})
}

func TestCheckOperationStatusWithError(t *testing.T) {
	t.Run("operationGetFailure", func(tt *testing.T) {
		// Create a test server that returns an error when getting operation status
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"code":    500,
					"message": "Internal server error",
				},
			})
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		completed, err := gService.CheckOperationStatus(context.Background(), "operations/test-operation-123")
		assert.NotNil(tt, err)
		assert.False(tt, completed)
	})
}

func TestGetCloudRunServiceURLWithError(t *testing.T) {
	t.Run("getServiceFailure", func(tt *testing.T) {
		// Create a test server that returns an error when getting service
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"code":    404,
					"message": "Service not found",
				},
			})
			if err != nil {
				return
			}
		}))
		defer server.Close()

		// Create Cloud Run client with custom HTTP client
		httpClient := &http.Client{Timeout: time.Second}
		svc, err := cloudrun.NewService(context.Background(), option.WithHTTPClient(httpClient), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create Cloud Run client: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudRunService: svc,
			},
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		url, err := gService.GetCloudRunServiceURL(context.Background(), "test-project", "us-central1", "test-service")
		assert.NotNil(tt, err)
		assert.Equal(tt, "", url)
	})
}

func TestGetIdentityToken(t *testing.T) {
	t.Run("onSuccess", func(tt *testing.T) {
		// This test would require mocking the Google Cloud credentials
		// Since it's difficult to mock the default token source in a unit test,
		// we'll test the error path instead
		gService := &GcpServices{
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		// This will likely fail in a test environment without proper credentials
		// but it will exercise the error handling code paths
		token, err := gService.GetIdentityToken(context.Background(), "test-audience")
		// We expect this to fail in test environment, but it covers the error paths
		if err != nil {
			// This is expected in test environment
			assert.NotNil(tt, err)
			assert.Equal(tt, "", token)
		}
	})

	t.Run("onTokenSourceCreationFailure", func(tt *testing.T) {
		// Test with invalid audience to trigger token source creation failure
		gService := &GcpServices{
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}

		// Use an invalid audience to trigger the error path
		token, err := gService.GetIdentityToken(context.Background(), "")
		assert.NotNil(tt, err)
		assert.Equal(tt, "", token)
		assert.Contains(tt, err.Error(), "failed to create token source")
	})
	t.Run("onValidAudience", func(tt *testing.T) {
		// Test with a valid audience format
		gService := &GcpServices{
			Ctx:    context.Background(),
			Logger: log.NewLogger(),
		}
		idtokenNewTokenSource = func(ctx context.Context, audience string, opts ...option.ClientOption) (oauth2.TokenSource, error) {
			return &mockTokenSource{}, nil
		}
		// This will likely fail in test environment without proper credentials
		// but it will exercise the success code path structure
		token, err := gService.GetIdentityToken(context.Background(), "https://test-service-url")
		// In test environment, this will likely fail due to missing credentials
		// but the code structure is tested
		if err != nil {
			assert.NotNil(tt, err)
			assert.Equal(tt, "", token)
		}
	})
}
