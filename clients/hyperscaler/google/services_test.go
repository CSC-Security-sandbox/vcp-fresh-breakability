package google

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"

	"google.golang.org/api/option"
	"google.golang.org/api/servicenetworking/v1"
)

func TestGetSearchRangeOperationStatus(t *testing.T) {
	url := "/v1/op"
	t.Run("WhenGetSearchRangeOperationStatus", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()
		svc, err := servicenetworking.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		adminService := AdminGCPService{networkingService: svc}

		gService := &GcpServices{
			serviceNetworkingEndpoint: "endpoint.goog",
			AdminGCPService:           &adminService,
			Logger:                    log.NewLogger(),
		}
		out, err := getNetworkingOperationStatus(gService, "op")
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
		svc, err := servicenetworking.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		adminService := AdminGCPService{networkingService: svc}

		gService := &GcpServices{
			serviceNetworkingEndpoint: "endpoint.goog",
			AdminGCPService:           &adminService,
			Logger:                    log.NewLogger(),
		}
		out, err := getNetworkingOperationStatus(gService, "op")
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

func TestParseProjectId(t *testing.T) {
	t.Run("ValidNetwork", func(tt *testing.T) {
		project, network, err := parseProjectId("projects/12345/global/networks/my-network")
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		}
		if project != "12345" {
			tt.Errorf("Unexpected project ID: %s", project)
		}
		if network != "my-network" {
			tt.Errorf("Unexpected network name: %s", network)
		}
	})

	t.Run("InvalidNetwork", func(tt *testing.T) {
		_, _, err := parseProjectId("invalid/network/format")
		if err == nil {
			tt.Error("Expected an error but got none")
		} else if !strings.Contains(err.Error(), "VPC peering network for TenancyUnit") {
			tt.Errorf("Unexpected error message: %s", err.Error())
		}
	})
}

//func TestWaitForServiceNetworkOperationStatus(t *testing.T) {
//	t.Run("WhenGetSearchRangeOperationStatusThrowsError", func(tt *testing.T) {
//		expectedErr := errors.New("GetSearchRangeOperationStatus Error")
//		operation := &servicenetworking.Operation{
//			Name: "op",
//			Response: []error{
//				expectedErr,
//			},
//		}
//		//provider := &hyperscaler.MockGoogleServices{
//		//	SearchRangeOperation: operation,
//		//	Errs: []error{
//		//		expectedErr,
//		//	},
//		//}
//		getNetworkingOperationStatus = func(gcpService *GcpServices, operation string) (*servicenetworking.Operation, error) {
//			return nil, errors.New("initializeManagementService failed")
//		}
//		gcpService := &GcpServices{}
//		oldWaitTimeout := waitTimeoutMinutes
//		waitTimeoutMinutes = time.Second * 5
//		defer func() {
//			waitTimeoutMinutes = oldWaitTimeout
//		}()
//
//		op, err := waitForServiceNetworkOperationStatus(gcpService, operation.Name)
//		assert.Equal(tt, op, operation)
//		assert.Equal(tt, err, expectedErr)
//	})
//	t.Run("WhenGetSearchRangeOperationReturnedWithError", func(tt *testing.T) {
//		operation := &servicenetworking.Operation{
//			Name: "op",
//		}
//		errMsg := "GetSearchRangeOperationStatus Error"
//		//resp := &servicenetworking.Operation{
//		//	Name: operation.Name,
//		//	Done: true,
//		//	Error: &servicenetworking.Status{
//		//		Message: errMsg,
//		//	},
//		//}
//		//provider := &mock.MockGoogleServices{
//		//	SearchRangeOperation: resp,
//		//}
//
//		gcpService := &GcpServices{}
//		oldWaitTimeout := waitTimeoutMinutes
//		waitTimeoutMinutes = time.Second * 5
//		defer func() {
//			waitTimeoutMinutes = oldWaitTimeout
//		}()
//		op, err := waitForServiceNetworkOperationStatus(gcpService, operation.Name)
//		assert.Equal(tt, op.Name, operation.Name)
//		assert.True(tt, op.Done)
//		assert.Equal(tt, op.Error.Message, errMsg)
//		assert.Error(tt, err, errMsg)
//	})
//	t.Run("WhenGetSearchRangeOperationReturnedWithoutError", func(tt *testing.T) {
//		operation := &servicenetworking.Operation{
//			Name: "op",
//		}
//		//resp := &servicenetworking.Operation{
//		//	Name: operation.Name,
//		//	Done: true,
//		//}
//		//provider := &mock.MockGoogleServices{
//		//	SearchRangeOperation: resp,
//		//}
//
//		gcpService := &GcpServices{}
//		oldWaitTimeout := waitTimeoutMinutes
//		waitTimeoutMinutes = time.Second * 5
//		defer func() {
//			waitTimeoutMinutes = oldWaitTimeout
//		}()
//		op, err := waitForServiceNetworkOperationStatus(gcpService, operation.Name)
//		assert.Equal(tt, op.Name, operation.Name)
//		assert.True(tt, op.Done)
//		assert.NoError(tt, err)
//	})
//	t.Run("WhenGetSearchRangeOperationTimesOut", func(tt *testing.T) {
//		operation := &servicenetworking.Operation{
//			Name: "op",
//		}
//		//provider := &mock.MockGoogleServices{
//		//	SearchRangeOperation: operation,
//		//}
//
//		gcpService := &GcpServices{}
//		oldWaitTimeout := waitTimeoutMinutes
//		waitTimeoutMinutes = time.Second * 5
//		defer func() {
//			waitTimeoutMinutes = oldWaitTimeout
//		}()
//		op, err := waitForServiceNetworkOperationStatus(gcpService, operation.Name)
//		assert.Nil(tt, op)
//		assert.Error(tt, err, "Timeout while confirming service network google components")
//	})
//}

// test cases for _waitForServiceNetworkOperationStatus
//func Test_waitForServiceNetworkOperationStatus(t *testing.T) {
//	t.Run("WhenGetSearchRangeOperationStatusError", func(tt *testing.T) {
//		defer testReset(t)
//		mgs := hyperscaler.NewMockGoogleServices(tt)
//		defer mgs.CloseMockGoogleServices()
//		resp := &servicenetworking.Operation{
//			Done: false,
//			Name: "funcTest",
//		}
//		waitSleep = time.Millisecond * 5
//		go func() {
//			defer mgs.MockGoogleServicesDone()
//			_, err := _waitForServiceNetworkOperationStatus(mgs, resp)
//			if err == nil {
//				tt.Error("Expected an error")
//			} else if err.Error() != "GetSearchRangeOperationStatus failure" {
//				tt.Errorf("Unexpected error returned: %s", err.Error())
//			}
//		}()
//		mgs.AssertGetTrace(trace)
//		mgs.AssertGetReporter(pr)
//		mgs.AssertGetSearchRangeOperationStatus(resp.Name, nil, errors.New("GetSearchRangeOperationStatus failure"))
//		mgs.AssertMockGoogleServicesDone()
//	})
//	t.Run("WhenGetSearchRangeOperationStatusNoError", func(tt *testing.T) {
//		defer testReset(t)
//		mgs := services.NewMockGoogleServices(tt)
//		defer mgs.CloseMockGoogleServices()
//
//		resp := &servicenetworking.Operation{
//			Done: true,
//			Name: "funcTest",
//		}
//		waitSleep = time.Millisecond * 5
//		go func() {
//			defer mgs.MockGoogleServicesDone()
//			ops, err := _waitForServiceNetworkOperationStatus(mgs, resp)
//			if ops == nil {
//				tt.Error("Expected Response")
//			} else if err != nil {
//				tt.Error("Unexpected error")
//			} else if !reflect.DeepEqual(resp, ops) {
//				tt.Error("Not Equal")
//			}
//		}()
//		mgs.AssertGetTrace(trace)
//		mgs.AssertGetReporter(pr)
//		mgs.AssertGetSearchRangeOperationStatus(resp.Name, resp, nil)
//		mgs.AssertMockGoogleServicesDone()
//	})
//	/*	t.Run("WhenNoOperationError", func(tt *testing.T) {
//		defer testReset(t)
//		mgs := services.NewMockGoogleServices(tt)
//		defer mgs.CloseMockGoogleServices()
//		resp := &servicenetworking.Operation{
//			Done: true,
//			Name: "funcTest",
//			Error: &servicenetworking.Status{
//				Code:    9,
//				Message: "I dont like this anymore",
//			},
//		}
//
//		isNotReady := errors.NewNotReadyErr("not ready")
//
//		go func() {
//			defer mgs.MockGoogleServicesDone()
//			ops, err := _waitForServiceNetworkOperationStatus(mgs, resp)
//			if ops == nil {
//				tt.Error("Expected an error")
//			} else if err.Error() != "I dont like this anymore" {
//				tt.Errorf("Unexpected error returned: %s", err.Error())
//			} else if !reflect.DeepEqual(resp, ops) {
//				tt.Error("Not Equal")
//			}
//
//		}()
//		mgs.AssertGetTrace(trace)
//		mgs.AssertGetSearchRangeOperationStatus(resp.Name, nil, isNotReady)
//		mgs.AssertGetSearchRangeOperationStatus(resp.Name, resp, nil)
//		mgs.AssertMockGoogleServicesDone()
//	}) */
//	//t.Run("WhenTimeoutError", func(tt *testing.T) {
//	//	defer testReset(t)
//	//	mgs := services.NewMockGoogleServices(tt)
//	//	defer mgs.CloseMockGoogleServices()
//	//
//	//	resp := &servicenetworking.Operation{
//	//		Done: false,
//	//		Name: "funcTest",
//	//		Error: &servicenetworking.Status{
//	//			Code:    9,
//	//			Message: "I dont like this anymore",
//	//		},
//	//	}
//	//	waitTimeout = time.Millisecond
//	//	waitSleep = waitTimeout + waitTimeout
//	//
//	//	go func() {
//	//		defer mgs.MockGoogleServicesDone()
//	//		_, err := _waitForServiceNetworkOperationStatus(mgs, resp)
//	//		waitTimeout = time.Minute * 5
//	//		waitSleep = time.Second * 3
//	//		if err == nil {
//	//			tt.Error("Expected an error")
//	//		} else if err.Error() != "Timeout while confirming service network google components" {
//	//			tt.Errorf("Unexpected error returned: %s", err.Error())
//	//		}
//	//	}()
//	//	mgs.AssertGetTrace(trace)
//	//	mgs.AssertGetSearchRangeOperationStatus(resp.Name, resp, nil)
//	//	mgs.AssertMockGoogleServicesDone()
//	//})
//}
