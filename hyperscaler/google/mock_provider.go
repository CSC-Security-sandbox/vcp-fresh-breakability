package google

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	projectsManagement "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
)

func _initializeMockManagementService(ctx context.Context) (*serviceconsumermanagement.APIService, error) {
	logger := util.GetLogger(ctx)
	if strings.TrimSpace(VSAMockPath) == "" {
		return nil, fmt.Errorf("VSAMockPath is not set")
	}
	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("#1 Using mock path for serviceconsumermanagement API: ", VSAMockPath)
	// default path -> https://serviceconsumermanagement.googleapis.com/
	svc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	logger.Info("#2 Using mock path for serviceconsumermanagement API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)
	return svc, nil
}

func _initializeMockNetworkingService(ctx context.Context) (*servicenetworking.APIService, error) {
	logger := util.GetLogger(ctx)
	if strings.TrimSpace(VSAMockPath) == "" {
		return nil, fmt.Errorf("VSAMockPath is not set")
	}
	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("#1 Using mock path for servicenetworking API: ", VSAMockPath)
	svc, err := servicenetworking.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	logger.Info("#2 Using mock path for servicenetworking API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)
	return svc, nil
}

func _initializeMockIamService(ctx context.Context) (*iam.Service, error) {
	logger := util.GetLogger(ctx)
	if strings.TrimSpace(VSAMockPath) == "" {
		return nil, fmt.Errorf("VSAMockPath is not set")
	}
	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("Using mock path for IAM API: ", VSAMockPath)
	opts := option.WithHTTPClient(client)
	svc, err := iam.NewService(ctx, opts)
	if err != nil {
		return nil, err
	}
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)

	logger.Info("Mock IAM service initialized successfully")
	return svc, nil
}

func _initializeMockCloudProjectsService(ctx context.Context) (*projectsManagement.Service, error) {
	slogger := util.GetLogger(ctx)
	if strings.TrimSpace(VSAMockPath) == "" {
		return nil, fmt.Errorf("VSAMockPath is not set")
	}
	slogger.Info("#1 Using mock path for cloudresourcemanager API: ", VSAMockPath)
	client := &http.Client{Timeout: time.Second * 3}
	opts := option.WithHTTPClient(client)
	svc, err := projectsManagement.NewService(ctx, opts)
	if err != nil {
		slogger.Error("error while creating new client for _initializeMockCloudProjectsService", err)
		return nil, err
	}
	slogger.Info("#2 Using mock path for cloudresourcemanager API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)

	slogger.Info("Mock Cloud Projects service initialized successfully")
	return svc, nil
}

func _initializeMockComputeService(ctx context.Context) (*compute.Service, error) {
	logger := util.GetLogger(ctx)
	if strings.TrimSpace(VSAMockPath) == "" {
		return nil, fmt.Errorf("VSAMockPath is not set")
	}
	client := &http.Client{Timeout: time.Second * 3}
	logger.Info("#1 Using mock path for compute API: ", VSAMockPath)
	// default path -> https://serviceconsumermanagement.googleapis.com/
	svc, err := compute.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	logger.Info("#2 Using mock path for compute API: ", VSAMockPath)
	svc.BasePath = fmt.Sprintf("http://%s/", VSAMockPath)
	return svc, nil
}
