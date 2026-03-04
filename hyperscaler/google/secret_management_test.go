package google

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/option"
	"google.golang.org/api/secretmanager/v1"
)

func Test_GetSecretWithLatestVersion(t *testing.T) {
	projectId := "1079058383248"
	secretName := "secretName"
	t.Run("WhenGetSecretWithLatestVersionFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/a/%s", projectId, secretName)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&secretmanager.Secret{Name: secretName})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := gService.GetSecretWithLatestVersion(projectId, secretName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
	t.Run("WhenGetSecretWithLatestVersionFailsNotFound", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s/versions/latest:access", projectId, secretName)
		resp := &hyperscaler.CustomSecret{Name: secretName}

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusNotFound)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := gService.GetSecretWithLatestVersion(projectId, secretName)
		if err != nil && secret != nil {
			tt.Error("Expected an error but got nothing")
		}
	})
	t.Run("WhenGetSecretWithLatestVersionFailsWithEmptyValue", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()

		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		url2 := fmt.Sprintf("/v1/projects/%s/secrets/%s/versions/latest:access", projectId, secretName)
		resp := &hyperscaler.CustomSecret{Name: secretName}

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
			if req.URL.Path == url2 && req.Method == http.MethodGet {
				response, _ := json.Marshal(&secretmanager.Secret{
					Name: secretName,
				})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := gService.GetSecretWithLatestVersion(projectId, secretName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
	t.Run("WhenGetSecretWithLatestVersionSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		resp2 := &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretName}
		resp := &hyperscaler.CustomSecret{Name: secretName, SecretVersion: resp2}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		originalGetSecretVersion := GetSecretVersion
		defer func() { GetSecretVersion = originalGetSecretVersion }()
		GetSecretVersion = func(gService *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretName}, nil
		}
		secret, err := gService.GetSecretWithLatestVersion(projectId, secretName)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if secret == nil || secret.Name != secretName {
			tt.Errorf("Unexpected operation: %+v", secretName)
		}
	})

	t.Run("WhenGetSecretWithLatestVersionFailsForNotFoundVersion", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		resp2 := &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretName}
		resp := &hyperscaler.CustomSecret{Name: secretName, SecretVersion: resp2}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		originalGetSecretVersion := GetSecretVersion
		defer func() { GetSecretVersion = originalGetSecretVersion }()
		GetSecretVersion = func(gService *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return nil, nil
		}
		secret, err := gService.GetSecretWithLatestVersion(projectId, secretName)
		if err != nil && secret != nil {
			tt.Error("Expected nil for err and secret")
		}
	})
	t.Run("WhenGetSecretWithLatestVersionFailsForFailedConvertSecretToCustomSecret", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		resp2 := &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretName}
		resp := &hyperscaler.CustomSecret{Name: secretName, SecretVersion: resp2}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		originalGetSecretVersion := GetSecretVersion
		originalConvertSecretToCustomSecret := ConvertSecretToCustomSecret
		defer func() {
			GetSecretVersion = originalGetSecretVersion
			ConvertSecretToCustomSecret = originalConvertSecretToCustomSecret
		}()
		GetSecretVersion = func(gService *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretName}, nil
		}

		ConvertSecretToCustomSecret = func(secret *secretmanager.Secret, version *hyperscaler.CustomSecretVersion) (*hyperscaler.CustomSecret, error) {
			return nil, fmt.Errorf("error")
		}
		secret, err := gService.GetSecretWithLatestVersion(projectId, secretName)
		if err != nil && err.Error() == "error" {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
}

func Test_CreateSecret(t *testing.T) {
	region := "test-region"
	projectId := "1079058383248"
	secretName := "secretName"
	secretValue := "password"
	t.Run("WhenCreateSecretFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s", projectId)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&secretmanager.Secret{Name: secretName})
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
	t.Run("WhenCreateSecretFailsIfSecretVersionFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		resp := &hyperscaler.CustomSecret{Name: secretName, SecretVersion: &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		AddSecretVersion = func(gService *GcpServices, projectId, secretName, secretValue string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{}, fmt.Errorf("error")
		}

		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
	t.Run("WhenCreateSecretSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		resp := &hyperscaler.CustomSecret{Name: secretName, SecretVersion: &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		AddSecretVersion = func(gService *GcpServices, projectId, secretName, secretValue string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}, nil
		}

		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if secret == nil || secret.Name != secretName {
			tt.Errorf("Unexpected operation: %+v", secretName)
		}
	})
	t.Run("WhenCreateSecretAlreadyExists_AddsVersionIdempotent", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		createURL := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		getURL := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		secretMeta := &secretmanager.Secret{Name: fmt.Sprintf("projects/%s/secrets/%s", projectId, secretName)}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == createURL && req.Method == http.MethodPost {
				errBody := map[string]interface{}{"error": map[string]interface{}{"code": float64(409), "message": "already exists"}}
				body, _ := json.Marshal(errBody)
				rw.WriteHeader(http.StatusConflict)
				_, _ = rw.Write(body)
				return
			}
			if req.URL.Path == getURL && req.Method == http.MethodGet {
				response, _ := json.Marshal(secretMeta)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{secretManagerService: svc},
			Ctx: ctx, Logger: util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		// Secret exists but has no versions (e.g. CreateSecretVersion failed with 503); AddVersion path.
		GetSecretVersion = func(g *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return nil, fmt.Errorf("not found or has no versions")
		}
		AddSecretVersion = func(g *GcpServices, projectId, secretName, secretValue string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}, nil
		}
		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		assert.Contains(tt, secret.Name, secretName)
	})
	t.Run("WhenCreateSecretAlreadyExists_ExistingVersionReturnedIdempotent", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		createURL := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		getURL := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		secretMeta := &secretmanager.Secret{Name: fmt.Sprintf("projects/%s/secrets/%s", projectId, secretName)}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == createURL && req.Method == http.MethodPost {
				errBody := map[string]interface{}{"error": map[string]interface{}{"code": float64(409), "message": "already exists"}}
				body, _ := json.Marshal(errBody)
				rw.WriteHeader(http.StatusConflict)
				_, _ = rw.Write(body)
				return
			}
			if req.URL.Path == getURL && req.Method == http.MethodGet {
				response, _ := json.Marshal(secretMeta)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		assert.NoError(tt, err)
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{secretManagerService: svc},
			Ctx: ctx, Logger: util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		existingVersion := &hyperscaler.CustomSecretVersion{Name: "existing-version", Value: "existing-value"}
		GetSecretVersion = func(g *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return existingVersion, nil
		}
		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		assert.Equal(tt, existingVersion.Value, secret.SecretVersion.Value)
	})
	t.Run("WhenCreateSecretAlreadyExists_GetSecretFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		createURL := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		getURL := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == createURL && req.Method == http.MethodPost {
				errBody := map[string]interface{}{"error": map[string]interface{}{"code": float64(409), "message": "already exists"}}
				body, _ := json.Marshal(errBody)
				rw.WriteHeader(http.StatusConflict)
				_, _ = rw.Write(body)
				return
			}
			if req.URL.Path == getURL && req.Method == http.MethodGet {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		assert.NoError(tt, err)
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{secretManagerService: svc},
			Ctx: ctx, Logger: util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		assert.Error(tt, err)
		assert.Nil(tt, secret)
	})
	t.Run("WhenCreateSecretAlreadyExists_AddSecretVersionFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		createURL := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		getURL := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		secretMeta := &secretmanager.Secret{Name: fmt.Sprintf("projects/%s/secrets/%s", projectId, secretName)}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == createURL && req.Method == http.MethodPost {
				errBody := map[string]interface{}{"error": map[string]interface{}{"code": float64(409), "message": "already exists"}}
				body, _ := json.Marshal(errBody)
				rw.WriteHeader(http.StatusConflict)
				_, _ = rw.Write(body)
				return
			}
			if req.URL.Path == getURL && req.Method == http.MethodGet {
				response, _ := json.Marshal(secretMeta)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		assert.NoError(tt, err)
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{secretManagerService: svc},
			Ctx: ctx, Logger: util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		GetSecretVersion = func(g *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return nil, fmt.Errorf("no versions")
		}
		AddSecretVersion = func(g *GcpServices, projectId, secretName, secretValue string) (*hyperscaler.CustomSecretVersion, error) {
			return nil, fmt.Errorf("add version failed")
		}
		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		assert.Error(tt, err)
		assert.Nil(tt, secret)
	})
	t.Run("WhenCreateSecretAlreadyExists_ConvertSecretToCustomSecretFailsOnExistingVersion", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		createURL := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		getURL := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		secretMeta := &secretmanager.Secret{Name: fmt.Sprintf("projects/%s/secrets/%s", projectId, secretName)}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == createURL && req.Method == http.MethodPost {
				errBody := map[string]interface{}{"error": map[string]interface{}{"code": float64(409), "message": "already exists"}}
				body, _ := json.Marshal(errBody)
				rw.WriteHeader(http.StatusConflict)
				_, _ = rw.Write(body)
				return
			}
			if req.URL.Path == getURL && req.Method == http.MethodGet {
				response, _ := json.Marshal(secretMeta)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		assert.NoError(tt, err)
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{secretManagerService: svc},
			Ctx: ctx, Logger: util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		GetSecretVersion = func(g *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}, nil
		}
		originalConvert := ConvertSecretToCustomSecret
		defer func() { ConvertSecretToCustomSecret = originalConvert }()
		ConvertSecretToCustomSecret = func(secret *secretmanager.Secret, version *hyperscaler.CustomSecretVersion) (*hyperscaler.CustomSecret, error) {
			return nil, fmt.Errorf("convert error")
		}
		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		assert.Error(tt, err)
		assert.Nil(tt, secret)
	})
	t.Run("WhenCreateSecretAlreadyExists_ConvertSecretToCustomSecretFailsAfterAddVersion", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		createURL := fmt.Sprintf("/v1/projects/%s/secrets", projectId)
		getURL := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		secretMeta := &secretmanager.Secret{Name: fmt.Sprintf("projects/%s/secrets/%s", projectId, secretName)}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == createURL && req.Method == http.MethodPost {
				errBody := map[string]interface{}{"error": map[string]interface{}{"code": float64(409), "message": "already exists"}}
				body, _ := json.Marshal(errBody)
				rw.WriteHeader(http.StatusConflict)
				_, _ = rw.Write(body)
				return
			}
			if req.URL.Path == getURL && req.Method == http.MethodGet {
				response, _ := json.Marshal(secretMeta)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		assert.NoError(tt, err)
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{secretManagerService: svc},
			Ctx: ctx, Logger: util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		GetSecretVersion = func(g *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return nil, fmt.Errorf("no versions")
		}
		AddSecretVersion = func(g *GcpServices, projectId, secretName, secretValue string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}, nil
		}
		originalConvert := ConvertSecretToCustomSecret
		defer func() { ConvertSecretToCustomSecret = originalConvert }()
		ConvertSecretToCustomSecret = func(secret *secretmanager.Secret, version *hyperscaler.CustomSecretVersion) (*hyperscaler.CustomSecret, error) {
			return nil, fmt.Errorf("convert after add error")
		}
		secret, err := gService.CreateSecret(projectId, region, secretName, secretValue)
		assert.Error(tt, err)
		assert.Nil(tt, secret)
	})
}

func Test_getSecretVersion(t *testing.T) {
	projectId := "1079058383248"
	secretName := "secretName"
	t.Run("WhenGetSecretVersionFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/a/%s", projectId, secretName)

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&secretmanager.Secret{Name: secretName})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := GetSecretVersion(gService, projectId, secretName, LatestVersion)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
	t.Run("WhenGetSecretVersionSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s/versions/latest:access", projectId, secretName)
		resp := &secretmanager.AccessSecretVersionResponse{Name: secretName, Payload: &secretmanager.SecretPayload{Data: "c29tZSBkYXRhIHdpdGggACBhbmQg77u/"}}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		secret, err := _getSecretVersion(gService, projectId, secretName, LatestVersion)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if secret == nil || secret.Name != secretName {
			tt.Errorf("Unexpected operation: %+v", secretName)
		}
	})
	t.Run("WhenGetSecretNotFound", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s/versions/latest:access", projectId, secretName)
		resp := &hyperscaler.CustomSecret{Name: secretName}

		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusNotFound)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := _getSecretVersion(gService, projectId, secretName, LatestVersion)
		if err != nil && secret != nil {
			tt.Error("Expected an error but got nothing")
		}
	})
}

func Test_addSecretVersion(t *testing.T) {
	projectId := "1079058383248"
	secretName := "secretName"
	secretValue := "password"
	t.Run("WhenAddSecretVersionFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()

		url := fmt.Sprintf("/v1/projects/%s/secrets/%s:", projectId, secretName)
		resp := &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		secret, err := AddSecretVersion(gService, projectId, secretName, secretValue)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})
	t.Run("WhenAddSecretVersionSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s:addVersion", projectId, secretName)
		resp := &hyperscaler.CustomSecretVersion{Name: secretName, Value: secretValue}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		secret, err := AddSecretVersion(gService, projectId, secretName, secretValue)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if secret == nil || secret.Name != secretName {
			tt.Errorf("Unexpected operation: %+v", secretName)
		}
	})
	t.Run("WhenAddSecretVersionFailsWithEmptySecretValue", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		// Test with empty secret value - covers lines 143-144
		secret, err := AddSecretVersion(gService, projectId, secretName, "")
		if err == nil {
			tt.Error("Expected an error for empty secret value but got nothing")
		}
		if secret != nil {
			tt.Errorf("Expected nil secret but got: %+v", secret)
		}
		// The error is wrapped in VCPError, so just check that we got an error
		assert.Error(tt, err)
		// The actual error message is in the underlying error, VCPError wraps it
	})
	t.Run("WhenAddSecretVersionWithShortSecretValue", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s:addVersion", projectId, secretName)
		resp := &hyperscaler.CustomSecretVersion{Name: secretName, Value: "abc"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		// Test with secret value length <= 4 - covers line 151
		shortValue := "abc"
		secret, err := AddSecretVersion(gService, projectId, secretName, shortValue)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if secret == nil || secret.Name != secretName {
			tt.Errorf("Unexpected operation: %+v", secretName)
		}
	})
}
func Test_DeleteSecret(t *testing.T) {
	projectId := "1079058383248"
	secretName := "secretName"
	t.Run("WhenDeleteSecretFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodDelete {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		err = gService.DeleteSecret(projectId, secretName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		}
	})
	t.Run("WhenDeleteSecretSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodDelete {
				response, _ := json.Marshal(&hyperscaler.CustomSecret{})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		err = gService.DeleteSecret(projectId, secretName)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
}

func Test_GetSecretWithCustomVersion(t *testing.T) {
	projectId := "1079058383248"
	secretName := "secretName"
	versionId := "5"
	t.Run("WhenGetSecretWithCustomVersionFailsToGetSecret", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		secret, err := gService.GetSecretWithCustomVersion(projectId, secretName, versionId)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})

	t.Run("WhenGetSecretWithCustomVersionFailsToGetVersion", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		resp := &hyperscaler.CustomSecret{Name: secretName}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		originalGetSecretVersion := GetSecretVersion
		defer func() { GetSecretVersion = originalGetSecretVersion }()
		GetSecretVersion = func(gService *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return nil, fmt.Errorf("error")
		}
		secret, err := gService.GetSecretWithCustomVersion(projectId, secretName, versionId)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if secret != nil {
			tt.Errorf("Expected nil operation but got: %+v", secret)
		}
	})

	t.Run("WhenGetSecretWithCustomVersionSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/secrets/%s", projectId, secretName)
		resp2 := &hyperscaler.CustomSecretVersion{Name: secretName, Value: "val"}
		resp := &hyperscaler.CustomSecret{Name: secretName, SecretVersion: resp2}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := secretmanager.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				secretManagerService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		originalGetSecretVersion := GetSecretVersion
		defer func() { GetSecretVersion = originalGetSecretVersion }()
		GetSecretVersion = func(gService *GcpServices, projectId, secretName, versionId string) (*hyperscaler.CustomSecretVersion, error) {
			return &hyperscaler.CustomSecretVersion{Name: secretName, Value: "val"}, nil
		}
		secret, err := gService.GetSecretWithCustomVersion(projectId, secretName, versionId)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if secret == nil || secret.Name != secretName {
			tt.Errorf("Unexpected operation: %+v", secret)
		}
	})
}
