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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
)

func testReset(t *testing.T) {
	waitTimeoutMinutes = time.Minute * time.Duration(env.GetInt("GCP_LRO_TIMEOUT_MINUTES", 20))
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "mock-consumer-endpoint.com")
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "mock-endpoint.com")
	newClient = _newClient

	createSubnetworkForTenantProject = _createSubnetworkForTenantProject
	createSubnetwork = _createSubnetwork
	createVPC = _createVPC
	insertFirewall = _insertFirewall
}

func Test_GetTenantProject(t *testing.T) {
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "autopush-netapp.sandbox.googleapis.com")
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "netapp-tst-autopush-endpoint.appspot.com")
	ctx := context.Background()
	customerProjectNumber := "1079058383248"
	consumerNetwork := "projects/1079058383248/global/networks/network-to-netapp2"
	region := "US-East-4"
	t.Run("WhenGetTenantProjectFails", func(tt *testing.T) {
		defer testReset(tt)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/services/"+serviceConsumerManagementEndpoint+"/projects/"+customerProjectNumber+"/tenancyUnits" {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
		}))
		defer server.Close()
		mgmtSvc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		_, err = gService.GetTenantProject(consumerNetwork, customerProjectNumber, region)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})
	t.Run("WhenNotGettingTenantProject_usingSDK", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/services/"+serviceConsumerManagementEndpoint+"/projects/"+customerProjectNumber+"/tenancyUnits" {
				response, _ := json.Marshal(&serviceconsumermanagement.ListTenancyUnitsResponse{TenancyUnits: []*serviceconsumermanagement.TenancyUnit{{TenantResources: []*serviceconsumermanagement.TenantResource{{Resource: "projects/175643", Tag: consumerNetwork + "-" + region}}}}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		mgmtSvc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		_, err = gService.GetTenantProject(consumerNetwork, customerProjectNumber, region)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		}
	})
	t.Run("WhenTenancyUnitFails", func(tt *testing.T) {
		defer testReset(tt)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/services/"+serviceConsumerManagementEndpoint+"/projects/"+customerProjectNumber+"/tenancyUnits" {
				response, err := json.Marshal(&serviceconsumermanagement.ListTenancyUnitsResponse{TenancyUnits: []*serviceconsumermanagement.TenancyUnit{{TenantResources: []*serviceconsumermanagement.TenantResource{{Resource: "projects/175643", Tag: "incorrectconsumerNetwork" + "-" + region}}}}})
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		mgmtSvc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		_, err = gService.GetTenantProject(consumerNetwork, customerProjectNumber, region)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), fmt.Sprintf("VPC peering network for TenancyUnit '%s' not found. Use the correct vpc name and ensure VPC network peering with tenant project has already been established.", consumerNetwork)) {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer testReset(tt)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/services/"+serviceConsumerManagementEndpoint+"/projects/"+customerProjectNumber+"/tenancyUnits" {
				response, err := json.Marshal(&serviceconsumermanagement.ListTenancyUnitsResponse{TenancyUnits: []*serviceconsumermanagement.TenancyUnit{{TenantResources: []*serviceconsumermanagement.TenantResource{{Resource: "projects/175643", Tag: consumerNetwork + "-" + region}}}}})
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		mgmtSvc, err := serviceconsumermanagement.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		_, err = gService.GetTenantProject(consumerNetwork, customerProjectNumber, region)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		}
	})
}

func Test_createSubnetworkForTenantProjectInternal(t *testing.T) {
	tenantProjectNumber := "1234"
	url := fmt.Sprintf("/v1/services/endpoint.goog/projects/%s:addSubnetwork", tenantProjectNumber)
	t.Run("When_createSubnetworkForTenantProjectFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()
		svc, err := servicenetworking.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := createSubnetworkForTenantProject(gService, &servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if out != nil {
				tt.Errorf("Unexpected output: %+v\n", out)
			}
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
		createSubnetworkForTenantProject = _createSubnetworkForTenantProject
	})
	t.Run("When_createSubnetworkForTenantProjectGoogleFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		errMsg := "Please create Service Networking connection with service 'netapp-sqa-autopush-endpoint.appspot.com' from consumer project '452619736732' network 'vpc-ap-tst' again.\\nHelp Token: AVzH8v1Y08A4HRQKRMzS6bVbeOO44HRY9Tg4k12uNjlwNXqwat_lZukbsxoB2TBH2FKBctDZDLYtD6CdaLke-XzSkgYvkeTwsnzwgRId7S25scxj"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(&servicenetworking.Operation{Name: "net-op-1", Error: &servicenetworking.Status{Message: errMsg}})
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		svc, err := servicenetworking.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		serviceConsumerManagementEndpoint = "endpoint.goog"
		serviceNetworkingEndpoint = "endpoint.goog"

		defer func() {
			serviceConsumerManagementEndpoint = "abc"
			serviceNetworkingEndpoint = "something"
		}()
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := createSubnetworkForTenantProject(gService, &servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if out != nil {
				tt.Errorf("Unexpected output: %+v\n", out)
			}
			if !strings.Contains(err.Error(), errMsg) {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
		createSubnetworkForTenantProject = _createSubnetworkForTenantProject
	})
	t.Run("WhenOKWithError", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		errMsg := "Please create Service Networking connection with service 'netapp-sqa-autopush-endpoint.appspot.com' from consumer project '452619736732' network 'vpc-ap-tst' again.\\nHelp Token: AVzH8v1Y08A4HRQKRMzS6bVbeOO44HRY9Tg4k12uNjlwNXqwat_lZukbsxoB2TBH2FKBctDZDLYtD6CdaLke-XzSkgYvkeTwsnzwgRId7S25scxj"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, _ := json.Marshal(&servicenetworking.Operation{Name: "net-op-1", Error: &servicenetworking.Status{Message: errMsg}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := servicenetworking.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := createSubnetworkForTenantProject(gService, &servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
		if err == nil {
			tt.Errorf("Error expected: %s", err.Error())
		} else {
			if out != nil {
				tt.Errorf("Expected nil")
			}
		}
		createSubnetworkForTenantProject = _createSubnetworkForTenantProject
	})
	t.Run("WhenOK", func(tt *testing.T) {
		defer testReset(tt)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, err := json.Marshal(&servicenetworking.Operation{Name: "net-op-1", Error: nil})
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		serviceConsumerManagementEndpoint = "endpoint.goog"
		serviceNetworkingEndpoint = "endpoint.goog"

		defer func() {
			serviceConsumerManagementEndpoint = "abc"
			serviceNetworkingEndpoint = "something"
		}()
		svc, err := servicenetworking.NewService(
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		consumerNetwork := "projects/1234/global/networks/network-to-netapp2"
		ctx := context.Background()
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: "endpoint.goog",
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		req := &servicenetworking.AddSubnetworkRequest{
			Consumer:        "projects/1234",
			Region:          "us-east-4",
			Description:     "vsa-network",
			IpPrefixLength:  28,
			ConsumerNetwork: consumerNetwork,
			Subnetwork:      "vsa-" + "us-east-4",
		}
		out, err := createSubnetworkForTenantProject(gService, req, tenantProjectNumber)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != "net-op-1" {
					tt.Errorf("Unexpected interconnect name %s", out.Name)
				}
			}
		}
		createSubnetworkForTenantProject = _createSubnetworkForTenantProject
	})
}

func Test_CreateVPC(t *testing.T) {
	projectName := "test-project"
	vpcNetwork := &models.VPCNetwork{
		Name:        "test-vpc-network",
		ProjectName: projectName,
	}
	url := "/projects/" + projectName + "/global/networks"
	t.Run("WhenCreateVPCFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
			}
		}))
		defer server.Close()
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		op, err := createVPC(gService, vpcNetwork)
		if err == nil {
			tt.Error("Expected an error but got none")
		}
		if op != nil {
			tt.Errorf("Expected nil operation but got: %+v", op)
		}
		createVPC = _createVPC
	})

	t.Run("WhenCreateVPCSucceeds", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		operationName := "test-operation"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				response, _ := json.Marshal(&compute.Operation{Name: operationName})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
			}
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gcpService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		op, err := createVPC(gcpService, vpcNetwork)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if op == nil || op.Name != operationName {
			tt.Errorf("Unexpected operation: %+v", op)
		}
		createVPC = _createVPC
	})
}

func Test_CreateSubnetwork(t *testing.T) {
	projectName := "test-project"
	region := "us-central1"
	subnetworkRequest := &models.Subnet{
		Name:        "test-subnetwork",
		Network:     "test-vpc-network",
		Region:      &region,
		ProjectName: projectName,
	}
	url := fmt.Sprintf("/projects/%s/regions/%s/subnetworks", projectName, region)
	t.Run("WhenCreateSubnetworkFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		op, err := createSubnetwork(gService, subnetworkRequest)
		if err == nil {
			tt.Error("Expected an error but got none")
		}
		if op != nil {
			tt.Errorf("Expected nil operation but got: %+v", op)
		}
		createSubnetwork = _createSubnetwork
	})

	t.Run("WhenCreateSubnetworkSucceeds", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		operationName := "test-operation"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodPost && req.URL.Path == url {
				response, _ := json.Marshal(&compute.Operation{Name: operationName})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		op, err := createSubnetwork(gService, subnetworkRequest)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if op == nil || op.Name != operationName {
			tt.Errorf("Unexpected operation: %+v", op)
		}
		createSubnetwork = _createSubnetwork
	})
}

func Test_GetSubnetwork(t *testing.T) {
	t.Run("WhenGetSubnetworkFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		subnetName := "vpc1-subnet"
		region := "US-East-4"
		url := "/projects/project/regions/region/subnetworks/r"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = gService.GetSubnetwork(projectName, region, subnetName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 400 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		subnetName := "vpc1-subnet"
		region := "US-East-4"
		url := fmt.Sprintf("/projects/%s/regions/%s/subnetworks/%s", projectName, region, subnetName)
		counter := 0
		resp := &compute.Subnetwork{Name: subnetName}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetSubnetwork(projectName, region, subnetName)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != subnetName {
					tt.Errorf("Unexpected subnetwork name %s", out.Name)
				}
			}
			if gService.Retry.GetRetryCount() != 0 {
				tt.Errorf("RetryStrategy was not reset %d", gService.Retry.GetRetryCount())
			}
		}
	})
}

func Test_GetVPCNetwork(t *testing.T) {
	projectName := "1079058383248"
	vpcName := "vpc1"
	url := "/projects/1079058383248/global/networks/vpc1"
	t.Run("WhenGetVPCNetworkFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = gService.GetVPCNetwork(projectName, vpcName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		counter := 0
		resp := &compute.Network{Name: vpcName}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1

				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetVPCNetwork(projectName, vpcName)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != vpcName {
					tt.Errorf("Unexpected subnetwork name %s", out.Name)
				}
			}
			if gService.Retry.GetRetryCount() != 0 {
				tt.Errorf("RetryStrategy was not reset %d", gService.Retry.GetRetryCount())
			}
		}
	})
}

func Test_GetFirewall(t *testing.T) {
	projectName := "1079058383248"
	vpcName := "vpc1"
	firewallRuleName := "ingress-" + vpcName
	url := "/projects/" + projectName + "/global/firewalls/" + firewallRuleName
	t.Run("WhenGetFirewallFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = gService.GetFirewall(projectName, firewallRuleName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})

	t.Run("WhenGetFirewallSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		counter := 0
		resp := &compute.Firewall{Name: firewallRuleName}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetFirewall(projectName, firewallRuleName)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != firewallRuleName {
					tt.Errorf("Unexpected firewall name %s", out.Name)
				}
			}
			if gService.Retry.GetRetryCount() != 0 {
				tt.Errorf("RetryStrategy was not reset %d", gService.Retry.GetRetryCount())
			}
		}
	})
}

func Test_InsertFirewall(t *testing.T) {
	projectName := "1079058383248"
	vpcName := "vpc1"
	url := "/projects/1079058383248/global/firewalls"
	allowedPortRules := []string{"tcp", "icmp", "udp"}
	sourceRanges := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	firewallRule := &models.Firewall{
		Name:             "ingress-" + vpcName,
		Description:      "Allow incoming traffic on specific ports",
		VPCNetworkName:   fmt.Sprintf("projects/%s/global/networks/%s", projectName, vpcName),
		AllowedPortRules: allowedPortRules,
		ProjectName:      projectName,
		SourceRanges:     sourceRanges,
		Direction:        "INGRESS",
		Priority:         1000,
	}
	t.Run("WhenInsertFirewallFails", func(tt *testing.T) {
		defer testReset(tt)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			context.Background(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		ctx := context.Background()
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
			Retry:                             NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = insertFirewall(gService, firewallRule)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
		insertFirewall = _insertFirewall
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		defer testReset(tt)
		counter := 0
		operationName := "random-operation-name"
		resp := &compute.Operation{Name: operationName}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1

				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		ctx := context.Background()
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := insertFirewall(gService, firewallRule)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != operationName {
					tt.Errorf("Unexpected subnetwork name %s", out.Name)
				}
			}
			if gService.Retry.GetRetryCount() != 0 {
				tt.Errorf("RetryStrategy was not reset %d", gService.Retry.GetRetryCount())
			}
		}
		insertFirewall = _insertFirewall
	})
}

// Unit tests for CreateSubnetworkForTenantProject
func Test_CreateSubnetworkForTenantProject(t *testing.T) {
	tenantProjectNumber := "123456789"
	consumerNetwork := "projects/123456789/global/networks/test-network"
	region := "us-central1"
	ctx := context.Background()
	t.Run("WhenParseProjectIdFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		consumerNetworkIncorrect := "projects/123456789/global/networks"

		_, err := gService.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetworkIncorrect, region)
		if err == nil || !strings.Contains(err.Error(), "parseProjectId failed for network : "+consumerNetworkIncorrect) {
			tt.Errorf("Expected parse error, got: %v", err)
		}
	})

	t.Run("WhenCreateSubnetworkForTenantProjectFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetworkForTenantProject
		createSubnetworkForTenantProject = func(*GcpServices, *servicenetworking.AddSubnetworkRequest, string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("create error")
		}
		defer func() { createSubnetworkForTenantProject = origCreate }()
		_, err := gService.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region)
		if err == nil || !strings.Contains(err.Error(), "create error") {
			tt.Errorf("Expected create error, got: %v", err)
		}
	})

	t.Run("WhenWaitForServiceNetworkOperationStatusFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetworkForTenantProject
		createSubnetworkForTenantProject = func(*GcpServices, *servicenetworking.AddSubnetworkRequest, string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { createSubnetworkForTenantProject = origCreate }()
		origWait := waitForServiceNetworkOperationStatus
		waitForServiceNetworkOperationStatus = func(*GcpServices, string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("wait error")
		}
		defer func() { waitForServiceNetworkOperationStatus = origWait }()
		_, err := gService.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region)
		if err == nil || !strings.Contains(err.Error(), "wait error") {
			tt.Errorf("Expected wait error, got: %v", err)
		}
	})
	t.Run("WhenGoogleTimeout", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetworkForTenantProject
		createSubnetworkForTenantProject = func(*GcpServices, *servicenetworking.AddSubnetworkRequest, string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { createSubnetworkForTenantProject = origCreate }()
		origWait := waitForServiceNetworkOperationStatus
		waitForServiceNetworkOperationStatus = func(*GcpServices, string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("Timeout while confirming service network google components")
		}
		defer func() { waitForServiceNetworkOperationStatus = origWait }()
		_, err := gService.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region)
		if err == nil || !strings.Contains(err.Error(), "Timeout while confirming service network google components") {
			tt.Errorf("Expected wait error, got: %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetworkForTenantProject
		createSubnetworkForTenantProject = func(*GcpServices, *servicenetworking.AddSubnetworkRequest, string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1", Response: []byte("success")}, nil
		}
		defer func() { createSubnetworkForTenantProject = origCreate }()
		origWait := waitForServiceNetworkOperationStatus
		waitForServiceNetworkOperationStatus = func(*GcpServices, string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Response: []byte("success")}, nil
		}
		defer func() { waitForServiceNetworkOperationStatus = origWait }()
		resp, err := gService.CreateSubnetworkForTenantProject(tenantProjectNumber, consumerNetwork, region)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if string(resp) != "success" {
			tt.Errorf("Expected response 'success', got: %s", string(resp))
		}
	})
}

func Test_InsertFirewallWithGetOperation(t *testing.T) {
	projectName := "test-project"
	firewallName := "test-firewall"
	firewallRule := &models.Firewall{
		Name:        firewallName,
		ProjectName: projectName,
	}
	ctx := context.Background()
	t.Run("WhenInsertFirewallFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origInsert := insertFirewall
		insertFirewall = func(_ *GcpServices, _ *models.Firewall) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("insert error")
		}
		defer func() { insertFirewall = origInsert }()
		err := gService.InsertFirewall(firewallRule)
		if err == nil || !strings.Contains(err.Error(), "insert error") {
			tt.Errorf("Expected insert error, got: %v", err)
		}
	})

	t.Run("WhenWaitForComputeNetGlobalOpStatusFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origInsert := insertFirewall
		insertFirewall = func(_ *GcpServices, _ *models.Firewall) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { insertFirewall = origInsert }()
		origWait := waitForComputeNetGlobalOpStatus
		waitForComputeNetGlobalOpStatus = func(_ *GcpServices, _ string, _ string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("wait error")
		}
		defer func() { waitForComputeNetGlobalOpStatus = origWait }()
		err := gService.InsertFirewall(firewallRule)
		if err == nil || !strings.Contains(err.Error(), "wait error") {
			tt.Errorf("Expected wait error, got: %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origInsert := insertFirewall
		insertFirewall = func(_ *GcpServices, _ *models.Firewall) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { insertFirewall = origInsert }()
		origWait := waitForComputeNetGlobalOpStatus
		waitForComputeNetGlobalOpStatus = func(_ *GcpServices, _ string, _ string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { waitForComputeNetGlobalOpStatus = origWait }()
		err := gService.InsertFirewall(firewallRule)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
}

// Unit tests for CreateSubnetwork
func Test_CreateSubnetworkWithOperation(t *testing.T) {
	projectName := "test-project"
	region := "us-central1"
	subnetworkRequest := &models.Subnet{
		Name:        "test-subnetwork",
		Network:     "test-vpc-network",
		Region:      &region,
		ProjectName: projectName,
	}
	ctx := context.Background()
	t.Run("WhenCreateSubnetworkFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetwork
		createSubnetwork = func(_ *GcpServices, _ *models.Subnet) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("create error")
		}
		defer func() { createSubnetwork = origCreate }()
		err := gService.CreateSubnetwork(subnetworkRequest)
		if err == nil || !strings.Contains(err.Error(), "create error") {
			tt.Errorf("Expected create error, got: %v", err)
		}
	})

	t.Run("WhenWaitForComputeRegionalOperationFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetwork
		createSubnetwork = func(_ *GcpServices, _ *models.Subnet) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { createSubnetwork = origCreate }()
		origWait := waitForComputeRegionalOperation
		waitForComputeRegionalOperation = func(_ *GcpServices, _, _, _ string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("wait error")
		}
		defer func() { waitForComputeRegionalOperation = origWait }()
		err := gService.CreateSubnetwork(subnetworkRequest)
		if err == nil || !strings.Contains(err.Error(), "wait error") {
			tt.Errorf("Expected wait error, got: %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := createSubnetwork
		createSubnetwork = func(_ *GcpServices, _ *models.Subnet) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { createSubnetwork = origCreate }()
		origWait := waitForComputeRegionalOperation
		waitForComputeRegionalOperation = func(_ *GcpServices, _, _, _ string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { waitForComputeRegionalOperation = origWait }()
		err := gService.CreateSubnetwork(subnetworkRequest)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
}

// Unit tests for CreateVPC
func Test_CreateVPCWithOperation(t *testing.T) {
	projectName := "test-project"
	vpcNetwork := &models.VPCNetwork{
		Name:        "test-vpc-network",
		ProjectName: projectName,
	}
	ctx := context.Background()
	t.Run("WhenCreateVPCFails", func(tt *testing.T) {
		gService := &GcpServices{
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}
		origCreate := createVPC
		createVPC = func(_ *GcpServices, _ *models.VPCNetwork) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("create error")
		}
		defer func() { createVPC = origCreate }()
		err := gService.CreateVPC(vpcNetwork)
		if err == nil || !strings.Contains(err.Error(), "create error") {
			tt.Errorf("Expected create error, got: %v", err)
		}
	})

	t.Run("WhenWaitForComputeNetGlobalOpStatusFails", func(tt *testing.T) {
		gService := &GcpServices{
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}
		origCreate := createVPC
		createVPC = func(_ *GcpServices, _ *models.VPCNetwork) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { createVPC = origCreate }()
		origWait := waitForComputeNetGlobalOpStatus
		waitForComputeNetGlobalOpStatus = func(_ *GcpServices, _, _ string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("wait error")
		}
		defer func() { waitForComputeNetGlobalOpStatus = origWait }()
		err := gService.CreateVPC(vpcNetwork)
		if err == nil || !strings.Contains(err.Error(), "wait error") {
			tt.Errorf("Expected wait error, got: %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		gService := &GcpServices{
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}
		origCreate := createVPC
		createVPC = func(_ *GcpServices, _ *models.VPCNetwork) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { createVPC = origCreate }()
		origWait := waitForComputeNetGlobalOpStatus
		waitForComputeNetGlobalOpStatus = func(_ *GcpServices, _, _ string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1"}, nil
		}
		defer func() { waitForComputeNetGlobalOpStatus = origWait }()
		err := gService.CreateVPC(vpcNetwork)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
}

func Test_insertFirewall(t *testing.T) {
	projectName := "1079058383248"
	vpcName := "vpc1"
	firewallRuleName := "ingress-" + vpcName
	url := "/projects/" + projectName + "/global/firewalls"
	firewallRequest := &models.Firewall{
		Name:             firewallRuleName,
		ProjectName:      projectName,
		VPCNetworkName:   vpcName,
		AllowedPortRules: []string{"tcp", "udp", "icmp"},
		SourceRanges:     []string{"10.0.0.0/8", "172.16.0.0/12"},
		Direction:        "INGRESS",
		Priority:         1000,
	}
	t.Run("WhenG_insertFirewallFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = _insertFirewall(gService, firewallRequest)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 500 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})

	t.Run("WhenGetFirewallSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		counter := 0
		resp := &compute.Firewall{Name: firewallRuleName}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := _insertFirewall(gService, firewallRequest)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == nil {
				tt.Errorf("Output unexpectedly nil")
			} else {
				if out.Name != firewallRuleName {
					tt.Errorf("Unexpected firewall name %s", out.Name)
				}
			}
			if gService.Retry.GetRetryCount() != 0 {
				tt.Errorf("RetryStrategy was not reset %d", gService.Retry.GetRetryCount())
			}
		}
	})
}

func TestReleaseSubnetwork(t *testing.T) {
	ctx := context.Background()
	region := "us-central1"
	snhost := "test-project"
	subnetwork := "test-subnet"
	deleteUrl := "/projects/test-project/regions/us-central1/subnetworks/test-subnet"

	operationName := "test-operation"
	t.Run("WhenDeleteSucceeds", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodDelete && req.URL.Path == deleteUrl {
				response, _ := json.Marshal(&compute.Operation{Name: operationName})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
			}
		}))
		defer server.Close()

		origDelete := waitForComputeOperation
		waitForComputeOperation = func(gService GcpServices, project, region, operation string) error {
			return nil
		}
		defer func() { waitForComputeOperation = origDelete }()

		defer func() {}()
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}

		err = gService.ReleaseSubnetwork(region, snhost, subnetwork)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
	t.Run("WhenDeleteReturnsOtherError", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			http.Error(rw, "internal error", http.StatusInternalServerError)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		origDelete := waitForComputeOperation
		waitForComputeOperation = func(gService GcpServices, project, region, operation string) error {
			return nil
		}
		defer func() { waitForComputeOperation = origDelete }()

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}

		err = gService.ReleaseSubnetwork(region, snhost, subnetwork)
		if err == nil || !strings.Contains(err.Error(), "internal error") {
			tt.Errorf("Expected internal error, got: %v", err)
		}
	})
	t.Run("WhenDeleteReturnsNotFoundError", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusNotFound)
			_, _ = rw.Write([]byte(`{"error": {"message": "notFound"}}`))
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		origDelete := waitForComputeOperation
		waitForComputeOperation = func(gService GcpServices, project, region, operation string) error {
			return nil
		}
		defer func() {
			waitForComputeOperation = origDelete
		}()

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}
		err = gService.ReleaseSubnetwork(region, snhost, subnetwork)
		if err == nil || (!strings.Contains(err.Error(), "notFound") && !strings.Contains(err.Error(), "not found")) {
			tt.Errorf("Expected notFound error, got: %v", err)
		}
	})
	t.Run("WhenWaitForComputeOperationFails", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodDelete && req.URL.Path == deleteUrl {
				response, _ := json.Marshal(&compute.Operation{Name: operationName})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
			}
		}))
		defer server.Close()
		computeSvc, err := compute.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}
		origDelete := waitForComputeOperation
		waitForComputeOperation = func(gService GcpServices, project, region, operation string) error {
			return fmt.Errorf("wait operation failed")
		}
		defer func() { waitForComputeOperation = origDelete }()
		gService := &GcpServices{AdminGCPService: &AdminGCPService{computeService: computeSvc}, Ctx: ctx, Logger: util.GetLogger(ctx)}
		err = gService.ReleaseSubnetwork(region, snhost, subnetwork)
		if err == nil || !strings.Contains(err.Error(), "wait operation failed") {
			tt.Errorf("Expected wait operation failed error, got: %v", err)
		}
	})
	t.Run("WhenAreNotSuccessfullyConnectedYetError", func(tt *testing.T) {
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusBadRequest)
			_, _ = rw.Write([]byte(`{"error": {"message": "are not successfully connected yet"}}`))
		}))
		defer server.Close()
		svc, err := servicenetworking.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{AdminGCPService: &AdminGCPService{networkingService: svc}, Ctx: ctx, Logger: util.GetLogger(ctx)}
		_, err = _createSubnetworkForTenantProject(gService, &servicenetworking.AddSubnetworkRequest{}, "test-project")
		if err == nil || !strings.Contains(err.Error(), "are not successfully connected yet") {
			tt.Errorf("Expected are not successfully connected yet error, got: %v", err)
		}
	})
}

// Unit tests for ListSubnetwork
func Test_ListSubnetwork(t *testing.T) {
	projectName := "test-project"
	region := "us-central1"
	url := fmt.Sprintf("/projects/%s/regions/%s/subnetworks", projectName, region)

	t.Run("WhenListSubnetworkFails", func(tt *testing.T) {
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		_, err = gService.ListSubnetwork(projectName, region)
		if err == nil {
			tt.Error("Expected an error but got none")
		}
	})

	t.Run("WhenListSubnetworkSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		resp := &compute.Subnetwork{Name: "test-subnet"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == url {
				response, _ := json.Marshal(&compute.SubnetworkList{Items: []*compute.Subnetwork{resp}})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			tt.Fatalf("Failed to create compute service: %v", err)
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}

		out, err := gService.ListSubnetwork(projectName, region)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if out == nil || len(*out) != 1 || (*out)[0].Name != "test-subnet" {
			tt.Errorf("Unexpected subnetwork list: %+v", out)
		}
	})
}

func Test_GetSnHost(t *testing.T) {
	t.Run("WhenGetSnHostFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		url := fmt.Sprintf("/projects/%s/getXpnHost", projectName)
		counter := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = gService.GetSnHost(projectName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "googleapi: got HTTP response code 400 with body") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})
	t.Run("WhenGetSnHostNotFound", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		url := fmt.Sprintf("/projects/%s/getXpnHost", projectName)
		counter := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusNotFound)
				_, _ = rw.Write([]byte(`{"error": {"message": "notFound"}}`))
				return
			}
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = gService.GetSnHost(projectName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "not found") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})

	t.Run("WhenSnHostEmpty", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		url := fmt.Sprintf("/projects/%s/getXpnHost", projectName)
		counter := 0
		resp := &compute.Project{Name: ""}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		_, err = gService.GetSnHost(projectName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(err.Error(), "SN Producer Host Project") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		url := fmt.Sprintf("/projects/%s/getXpnHost", projectName)
		counter := 0
		resp := &compute.Project{Name: "sn-host-project"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if counter == 0 {
				counter = 1
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			if req.URL.Path == url {
				response, err := json.Marshal(resp)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				computeService: computeSvc,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 3),
		}
		out, err := gService.GetSnHost(projectName)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == "" {
				tt.Errorf("Output unexpectedly nil")
			}
			if gService.Retry.GetRetryCount() != 0 {
				tt.Errorf("RetryStrategy was not reset %d", gService.Retry.GetRetryCount())
			}
		}
	})
}
