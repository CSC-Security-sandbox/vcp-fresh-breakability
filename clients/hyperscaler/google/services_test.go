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

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/servicenetworking/v1"
)

func Test_getComputeRegionalOpStatus(t *testing.T) {
	url := "/projects/1079058383248/regions/us-central1/operations/op"
	projectNumber := "1079058383248"
	region := "us-central1"
	operationName := "op"

	t.Run("When_getComputeRegionalOpStatus", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()
		svc, err := compute.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		adminService := AdminGCPService{computeService: svc}

		gService := &GcpServices{
			serviceNetworkingEndpoint: "endpoint.goog",
			AdminGCPService:           &adminService,
			Logger:                    log.NewLogger(),
		}
		out, err := getComputeRegionalOpStatus(gService, projectNumber, region, operationName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if out != nil {
				tt.Errorf("Unexpected output: %+v\n", out)
			}
			if !strings.Contains(err.Error(), "response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})
	t.Run("WhenOK", func(tt *testing.T) {
		resp := &servicenetworking.Operation{Name: "op1"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := compute.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		adminService := AdminGCPService{computeService: svc}

		gService := &GcpServices{
			serviceNetworkingEndpoint: "endpoint.goog",
			AdminGCPService:           &adminService,
			Logger:                    log.NewLogger(),
		}
		out, err := getComputeRegionalOpStatus(gService, projectNumber, region, operationName)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != "op1" {
					tt.Errorf("Unexpected operation name %s", out.Name)
				}
			}
		}
	})
}

func TestGetComputeGlobalOpStatus(t *testing.T) {
	url := "/projects/1079058383248/global/operations/op"
	t.Run("WhenGetComputeGlobalOpStatus", func(tt *testing.T) {
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()
		svc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		adminService := AdminGCPService{computeService: svc}

		gService := &GcpServices{
			Ctx:             ctx,
			AdminGCPService: &adminService,
			Logger:          log.NewLogger(),
		}
		out, err := getComputeGlobalOpStatus(gService, "1079058383248", "op")
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if out != nil {
				tt.Errorf("Unexpected output: %+v\n", out)
			}
			if !strings.Contains(err.Error(), "response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})
	t.Run("WhenOK", func(tt *testing.T) {
		resp := &compute.Operation{Name: "op1"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := compute.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		adminService := AdminGCPService{computeService: svc}

		gService := &GcpServices{
			AdminGCPService: &adminService,
			Logger:          log.NewLogger(),
		}
		out, err := getComputeGlobalOpStatus(gService, "1079058383248", "op")
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != "op1" {
					tt.Errorf("Unexpected operation name %s", out.Name)
				}
			}
		}
	})
}

// Unit tests for _waitForComputeNetGlobalOpStatus
func Test_waitForComputeNetGlobalOpStatus(t *testing.T) {
	mockLogger := log.NewLogger()
	mockGcpService := &GcpServices{
		Logger: mockLogger,
	}

	origGetComputeGlobalOpStatus := getComputeGlobalOpStatus
	origTimeSleep := timeSleep
	origWaitTimeout := waitTimeoutMinutes
	origDefaultSleep := defaultSleepTime

	defer func() {
		getComputeGlobalOpStatus = origGetComputeGlobalOpStatus
		timeSleep = origTimeSleep
		waitTimeoutMinutes = origWaitTimeout
		defaultSleepTime = origDefaultSleep
	}()

	t.Run("OperationCompletesSuccessfully", func(t *testing.T) {
		calls := 0
		getComputeGlobalOpStatus = func(gcpService *GcpServices, tenantProject, operationName string) (*models.ComputeOperation, error) {
			calls++
			if calls < 2 {
				return &models.ComputeOperation{Status: "PENDING", Progress: 50}, nil
			}
			return &models.ComputeOperation{Status: "DONE", Progress: 100}, nil
		}
		timeSleep = func(d time.Duration) {}
		op, err := _waitForComputeNetGlobalOpStatus(mockGcpService, "project", "op1")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if op == nil || op.Status != "DONE" || op.Progress != 100 {
			t.Errorf("expected operation to be done, got %+v", op)
		}
	})

	t.Run("OperationReturnsError (not NotReady)", func(t *testing.T) {
		getComputeGlobalOpStatus = func(gcpService *GcpServices, tenantProject, operationName string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("some error")
		}
		timeSleep = func(d time.Duration) {}
		op, err := _waitForComputeNetGlobalOpStatus(mockGcpService, "project", "op2")
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if op != nil {
			t.Errorf("expected nil op, got %+v", op)
		}
	})

	t.Run("OperationReturnsNotReadyErrorThenSuccess", func(t *testing.T) {
		calls := 0
		getComputeGlobalOpStatus = func(gcpService *GcpServices, tenantProject, operationName string) (*models.ComputeOperation, error) {
			calls++
			if calls < 2 {
				return nil, errors.NewNotReadyErr("not ready")
			}
			return &models.ComputeOperation{Status: "DONE", Progress: 100}, nil
		}
		timeSleep = func(d time.Duration) {}
		op, err := _waitForComputeNetGlobalOpStatus(mockGcpService, "project", "op3")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if op == nil || op.Status != "DONE" || op.Progress != 100 {
			t.Errorf("expected operation to be done, got %+v", op)
		}
	})

	t.Run("TimeoutWaitingForOperation", func(t *testing.T) {
		waitTimeoutMinutes = time.Millisecond * 10
		defaultSleepTime = time.Millisecond * 2
		getComputeGlobalOpStatus = func(gcpService *GcpServices, tenantProject, operationName string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Status: "PENDING", Progress: 50}, nil
		}
		timeSleep = func(d time.Duration) { time.Sleep(time.Millisecond * 5) }
		op, err := _waitForComputeNetGlobalOpStatus(mockGcpService, "project", "op4")
		if err == nil || !strings.Contains(err.Error(), "Timeout while confirming compute network google components") {
			t.Errorf("expected timeout error, got %v", err)
		}
		if op != nil {
			t.Errorf("expected nil op, got %+v", op)
		}
	})
}

// Unit tests for _waitForComputeRegionalOperation
func Test_waitForComputeRegionalOperation(t *testing.T) {
	mockLogger := log.NewLogger()
	mockGcpService := &GcpServices{
		Logger: mockLogger,
	}

	origGetComputeRegionalOpStatus := getComputeRegionalOpStatus
	origTimeSleep := timeSleep
	origWaitTimeout := waitTimeoutMinutes
	origDefaultSleep := defaultSleepTime

	defer func() {
		getComputeRegionalOpStatus = origGetComputeRegionalOpStatus
		timeSleep = origTimeSleep
		waitTimeoutMinutes = origWaitTimeout
		defaultSleepTime = origDefaultSleep
	}()

	t.Run("OperationCompletesSuccessfully", func(t *testing.T) {
		calls := 0
		getComputeRegionalOpStatus = func(gcpService *GcpServices, projectNumber, region, operationName string) (*models.ComputeOperation, error) {
			calls++
			if calls < 2 {
				return &models.ComputeOperation{Status: "PENDING", Progress: 50}, nil
			}
			return &models.ComputeOperation{Status: "DONE", Progress: 100}, nil
		}
		timeSleep = func(d time.Duration) {}
		op, err := _waitForComputeRegionalOperation(mockGcpService, "project", "region", "op1")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if op == nil || op.Status != "DONE" || op.Progress != 100 {
			t.Errorf("expected operation to be done, got %+v", op)
		}
	})

	t.Run("OperationReturnsError (not NotReady)", func(t *testing.T) {
		getComputeRegionalOpStatus = func(gcpService *GcpServices, projectNumber, region, operationName string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("some error")
		}
		timeSleep = func(d time.Duration) {}
		op, err := _waitForComputeRegionalOperation(mockGcpService, "project", "region", "op2")
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if op != nil {
			t.Errorf("expected nil op, got %+v", op)
		}
	})

	t.Run("OperationReturnsNotReadyErrorThenSuccess", func(t *testing.T) {
		calls := 0
		getComputeRegionalOpStatus = func(gcpService *GcpServices, projectNumber, region, operationName string) (*models.ComputeOperation, error) {
			calls++
			if calls < 2 {
				return nil, errors.NewNotReadyErr("not ready")
			}
			return &models.ComputeOperation{Status: "DONE", Progress: 100}, nil
		}
		timeSleep = func(d time.Duration) {}
		op, err := _waitForComputeRegionalOperation(mockGcpService, "project", "region", "op3")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if op == nil || op.Status != "DONE" || op.Progress != 100 {
			t.Errorf("expected operation to be done, got %+v", op)
		}
	})

	t.Run("TimeoutWaitingForOperation", func(t *testing.T) {
		waitTimeoutMinutes = time.Millisecond * 10
		defaultSleepTime = time.Millisecond * 2
		getComputeRegionalOpStatus = func(gcpService *GcpServices, projectNumber, region, operationName string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Status: "PENDING", Progress: 50}, nil
		}
		timeSleep = func(d time.Duration) { time.Sleep(time.Millisecond * 5) }
		op, err := _waitForComputeRegionalOperation(mockGcpService, "project", "region", "op4")
		if err == nil || !strings.Contains(err.Error(), "Timeout while confirming compute network google components") {
			t.Errorf("expected timeout error, got %v", err)
		}
		if op != nil {
			t.Errorf("expected nil op, got %+v", op)
		}
	})
}
