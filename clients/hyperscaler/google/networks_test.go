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

	"github.com/stretchr/testify/assert"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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

	CreateTPSubnetOp = _createTPSubnetOp
	AddSecretVersion = _addSecretVersion
	GetSecretVersion = _getSecretVersion
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

func Test_CreateTPSubnetOpInternal(t *testing.T) {
	tenantProjectNumber := "1234"
	url := fmt.Sprintf("/v1/services/endpoint.goog/projects/%s:addSubnetwork", tenantProjectNumber)
	t.Run("When_CreateTPSubnetOpFails", func(tt *testing.T) {
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
		out, err := CreateTPSubnetOp(gService, &servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
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
		CreateTPSubnetOp = _createTPSubnetOp
	})
	t.Run("When_CreateTPSubnetOpGoogleFails", func(tt *testing.T) {
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
		out, err := CreateTPSubnetOp(gService, &servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
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
		CreateTPSubnetOp = _createTPSubnetOp
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
		out, err := CreateTPSubnetOp(gService, &servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
		if err == nil {
			tt.Errorf("Error expected: %s", err.Error())
		} else {
			if out != nil {
				tt.Errorf("Expected nil")
			}
		}
		CreateTPSubnetOp = _createTPSubnetOp
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
		out, err := CreateTPSubnetOp(gService, req, tenantProjectNumber)
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
		CreateTPSubnetOp = _createTPSubnetOp
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
		}

		op, err := gService.CreateVPC(vpcNetwork)
		if err == nil {
			tt.Error("Expected an error but got none")
		}
		if op != "" {
			tt.Errorf("Expected nil operation but got: %+v", op)
		}
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
		}

		op, err := gcpService.CreateVPC(vpcNetwork)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if op != operationName {
			tt.Errorf("Unexpected operation: %+v", op)
		}
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
		}

		op, err := gService.CreateSubnetwork(subnetworkRequest)
		if err == nil {
			tt.Error("Expected an error but got none")
		}
		if op != "" {
			tt.Errorf("Expected nil operation but got: %+v", op)
		}
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
		}

		op, err := gService.CreateSubnetwork(subnetworkRequest)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if op != operationName {
			tt.Errorf("Unexpected operation: %+v", op)
		}
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
		resp := &compute.Subnetwork{Name: subnetName}
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
		resp := &compute.Network{Name: vpcName}
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
		resp := &compute.Firewall{Name: firewallRuleName}
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
		_, err = gService.InsertFirewall(firewallRule)
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
		operationName := "random-operation-name"
		resp := &compute.Operation{Name: operationName}
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
		}
		out, err := gService.InsertFirewall(firewallRule)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out != operationName {
				tt.Errorf("Unexpected operation name %s", out)
			}
		}
	})
}

// Unit tests for CreateTPSubnetOp
func Test_CreateTPSubnetOp(t *testing.T) {
	tenantProjectNumber := "123456789"
	consumerNetwork := "projects/123456789/global/networks/test-network"
	region := "us-central1"
	ctx := context.Background()
	subnetName := fmt.Sprintf("vsa-us-c1-%s", tenantProjectNumber)

	t.Run("WhenParseProjectIdFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		consumerNetworkIncorrect := "projects/123456789/global/networks"

		_, err := gService.CreateTPSubnetOp(tenantProjectNumber, consumerNetworkIncorrect, region, subnetName)
		if err == nil || !strings.Contains(err.Error(), "parseProjectId failed for network : "+consumerNetworkIncorrect) {
			tt.Errorf("Expected parse error, got: %v", err)
		}
	})

	t.Run("WhenCreateTPSubnetOpFails", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := CreateTPSubnetOp
		CreateTPSubnetOp = func(*GcpServices, *servicenetworking.AddSubnetworkRequest, string) (*models.ComputeOperation, error) {
			return nil, fmt.Errorf("create error")
		}
		defer func() { CreateTPSubnetOp = origCreate }()
		_, err := gService.CreateTPSubnetOp(tenantProjectNumber, consumerNetwork, region, subnetName)
		if err == nil || !strings.Contains(err.Error(), "create error") {
			tt.Errorf("Expected create error, got: %v", err)
		}
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		gService := &GcpServices{Ctx: ctx, Logger: util.GetLogger(ctx)}
		origCreate := CreateTPSubnetOp
		CreateTPSubnetOp = func(*GcpServices, *servicenetworking.AddSubnetworkRequest, string) (*models.ComputeOperation, error) {
			return &models.ComputeOperation{Name: "op-1", Response: []byte("success")}, nil
		}
		defer func() { CreateTPSubnetOp = origCreate }()
		resp, err := gService.CreateTPSubnetOp(tenantProjectNumber, consumerNetwork, region, subnetName)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if string(*resp) != "op-1" {
			tt.Errorf("Expected response 'success', got: %s", string(*resp))
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
		if err != nil {
			tt.Errorf("Subnet not found means, the subnet doesn't exist or is deleted. Unexpected error, got: %v", err)
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
		_, err = _createTPSubnetOp(gService, &servicenetworking.AddSubnetworkRequest{}, "test-project")
		if err == nil || !strings.Contains(err.Error(), "are not successfully connected yet") {
			tt.Errorf("Expected are not successfully connected yet error, got: %v", err)
		}
	})
}

// Unit tests for ListSubnetworks
func Test_ListSubnetworks(t *testing.T) {
	projectName := "test-project"
	region := "us-central1"
	url := fmt.Sprintf("/projects/%s/regions/%s/subnetworks", projectName, region)

	t.Run("WhenListSubnetworksFails", func(tt *testing.T) {
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == url {
				rw.WriteHeader(http.StatusInternalServerError)
				return
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
		}

		_, err = gService.ListSubnetworks(projectName, region)
		if err == nil {
			tt.Error("Expected an error but got none")
		}
	})

	t.Run("WhenListSubnetworksSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		resp := &compute.Subnetwork{Name: "test-subnet"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet && req.URL.Path == url {
				response, _ := json.Marshal(&compute.SubnetworkList{Items: []*compute.Subnetwork{resp}})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
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
		}

		out, err := gService.ListSubnetworks(projectName, region)
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
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
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
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
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
		}
		_, err = gService.GetSnHost(projectName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if !strings.Contains(strings.ToLower(err.Error()), "notfound") {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
		}
	})

	t.Run("WhenSnHostEmpty", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		projectName := "1079058383248"
		url := fmt.Sprintf("/projects/%s/getXpnHost", projectName)
		resp := &compute.Project{Name: ""}
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
		resp := &compute.Project{Name: "sn-host-project"}
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
		}
		out, err := gService.GetSnHost(projectName)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else {
			if out == "" {
				tt.Errorf("Output unexpectedly nil")
			}
		}
	})
}

func Test_newClient(t *testing.T) {
	origNewClientScopes := newClientScopes
	defer func() { newClientScopes = origNewClientScopes }()

	ctx := context.Background()

	t.Run("returns client and endpoint", func(t *testing.T) {
		expectedClient := &http.Client{}
		expectedEndpoint := "test-endpoint"
		newClientScopes = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return expectedClient, expectedEndpoint, nil
		}
		client, endpoint, err := _newClient(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if client != expectedClient {
			t.Errorf("expected client %v, got %v", expectedClient, client)
		}
		if endpoint != expectedEndpoint {
			t.Errorf("expected endpoint %v, got %v", expectedEndpoint, endpoint)
		}
	})

	t.Run("returns error", func(t *testing.T) {
		newClientScopes = func(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
			return nil, "", fmt.Errorf("Error getting service up")
		}
		client, endpoint, err := _newClient(ctx)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if client != nil {
			t.Errorf("expected nil client, got %v", client)
		}
		if endpoint != "" {
			t.Errorf("expected empty endpoint, got %v", endpoint)
		}
	})
}

// Unit tests for GetContext
func TestGcpServices_GetContext(t *testing.T) {
	t.Run("ReturnsExistingContext", func(t *testing.T) {
		ctx := context.Background()
		gcpService := &GcpServices{
			Ctx: ctx,
		}
		got := gcpService.GetContext()
		assert.Equal(t, ctx, got)
	})

	t.Run("InitializesContextIfNil", func(t *testing.T) {
		gcpService := &GcpServices{}
		got := gcpService.GetContext()
		assert.NotNil(t, got)
		assert.Equal(t, gcpService.Ctx, got)
	})
}

func Test_updateFirewall(t *testing.T) {
	projectName := "1079058383248"
	vpcName := "vpc1"
	firewallName := "ingress-" + vpcName
	url := "/projects/1079058383248/global/firewalls/" + firewallName
	allowedPortRules := []string{"tcp", "icmp", "udp"}
	sourceRanges := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	firewallRule := &models.Firewall{
		Name:             firewallName,
		Description:      "Allow incoming traffic on specific ports",
		VPCNetworkName:   fmt.Sprintf("projects/%s/global/networks/%s", projectName, vpcName),
		AllowedPortRules: allowedPortRules,
		ProjectName:      projectName,
		SourceRanges:     sourceRanges,
		Direction:        "INGRESS",
		Priority:         1000,
	}

	t.Run("WhenUpdateFails", func(tt *testing.T) {
		defer testReset(tt)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}
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
		}
		_, err = gService.UpdateFirewall(firewallRule)
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
		operationName := "random-operation-name"
		resp := &compute.Operation{Name: operationName}
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
		}
		out, err := gService.UpdateFirewall(firewallRule)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		} else if out != operationName {
			tt.Errorf("Unexpected subnetwork name %s", out)
		}
	})
}

func Test_GetServiceNetOpStatus(t *testing.T) {
	defer testReset(t)

	t.Run("Success", func(tt *testing.T) {
		operationName := "operations/operation-1234567890123456789"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.Path, "operations/operation-1234567890123456789") {
				response := &servicenetworking.Operation{
					Name: operationName,
					Done: true,
				}
				responseJson, _ := json.Marshal(response)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(responseJson)
				return
			}
			rw.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		serviceNetworkingEndpoint = server.URL
		ctx := context.Background()

		networkingService, err := servicenetworking.NewService(ctx, option.WithEndpoint(server.URL), option.WithoutAuthentication())
		if err != nil {
			tt.Fatalf("Failed to create networking service: %v", err)
		}

		gcpService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: networkingService,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}

		result, err := gcpService.GetServiceNetOpStatus(operationName)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, operationName, result.Name)
		assert.True(tt, result.Done)
	})

	t.Run("Error_OperationError", func(tt *testing.T) {
		operationName := "operations/operation-error"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.Path, "operations/operation-error") {
				response := &servicenetworking.Operation{
					Name: operationName,
					Done: true,
					Error: &servicenetworking.Status{
						Message: "Operation failed",
					},
				}
				responseJson, _ := json.Marshal(response)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(responseJson)
				return
			}
			rw.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		serviceNetworkingEndpoint = server.URL
		ctx := context.Background()

		networkingService, err := servicenetworking.NewService(ctx, option.WithEndpoint(server.URL), option.WithoutAuthentication())
		if err != nil {
			tt.Fatalf("Failed to create networking service: %v", err)
		}

		gcpService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: networkingService,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}

		result, err := gcpService.GetServiceNetOpStatus(operationName)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Operation failed")
	})

	t.Run("Error_HTTPError", func(tt *testing.T) {
		operationName := "operations/operation-notfound"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		serviceNetworkingEndpoint = server.URL
		ctx := context.Background()

		networkingService, err := servicenetworking.NewService(ctx, option.WithEndpoint(server.URL), option.WithoutAuthentication())
		if err != nil {
			tt.Fatalf("Failed to create networking service: %v", err)
		}

		gcpService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: networkingService,
			},
			Ctx:    ctx,
			Logger: util.GetLogger(ctx),
		}

		result, err := gcpService.GetServiceNetOpStatus(operationName)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func Test_GetZones(t *testing.T) {
	projectNumber := "123456789"
	projectID := "test-project"
	region := "us-central1"
	regionUrl := "https://www.googleapis.com/compute/v1/projects/" + projectID + "/regions/" + region
	zones := []string{"us-central1-a", "us-central1-b"}

	t.Run("WhenGetZonesSucceeds", func(tt *testing.T) {
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.Path, "/zones") {
				resp := map[string]interface{}{
					"items": []map[string]interface{}{
						{"name": zones[0], "region": regionUrl},
						{"name": zones[1], "region": regionUrl},
					},
				}
				_ = json.NewEncoder(rw).Encode(resp)
			}
		}))
		defer server.Close()

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
		}

		// Mock getProjectIDFromNumber
		getProjectIDFromNumber = func(_ *GcpServices, _ string) (string, error) {
			return projectID, nil
		}
		defer func() { getProjectIDFromNumber = _getProjectIDFromNumber }()

		out, err := gService.GetZones(projectNumber, region)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if len(out) != 2 || out[0] != zones[0] || out[1] != zones[1] {
			tt.Errorf("Unexpected zones: %+v", out)
		}
	})

	t.Run("WhenGetZonesFails", func(tt *testing.T) {
		ctx := context.Background()
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			http.Error(rw, "internal error", http.StatusInternalServerError)
		}))
		defer server.Close()

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
			Retry:  NewExponentialRetryStrategy(time.Millisecond, 1),
		}

		// Mock getProjectIDFromNumber
		getProjectIDFromNumber = func(_ *GcpServices, _ string) (string, error) {
			return projectID, nil
		}
		defer func() { getProjectIDFromNumber = _getProjectIDFromNumber }()

		_, err = gService.GetZones(projectNumber, region)
		if err == nil {
			tt.Error("Expected error but got none")
		}
	})
}

func Test_getProjectIDFromNumber(t *testing.T) {
	ctx := context.Background()
	projectNumber := "123456789"
	projectName := "test-project"

	t.Run("WhenGetProjectSucceeds", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.Path, "/projects/"+projectNumber) {
				resp := &compute.Project{Name: projectName}
				_ = json.NewEncoder(rw).Encode(resp)
			} else {
				http.Error(rw, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{computeService: computeSvc},
			Ctx:             ctx,
		}

		name, err := _getProjectIDFromNumber(gService, projectNumber)
		if err != nil {
			tt.Fatalf("unexpected error: %v", err)
		}
		if name != projectName {
			tt.Errorf("expected %s, got %s", projectName, name)
		}
	})

	t.Run("WhenGetProjectFails", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			http.Error(rw, "internal error", http.StatusInternalServerError)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{computeService: computeSvc},
			Ctx:             ctx,
		}

		_, err = _getProjectIDFromNumber(gService, projectNumber)
		if err == nil {
			tt.Error("expected error, got nil")
		}
	})
}

func Test_IsMachineTypeAvailable(t *testing.T) {
	ctx := context.Background()
	projectNumber := "123456789"
	projectName := "test-project"
	zone := "us-central1-a"
	machineType := "n2-standard-4"

	t.Run("WhenMachineTypeIsAvailable", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.Path, "/projects/"+projectName+"/zones/"+zone+"/machineTypes/"+machineType) {
				// Return a successful response indicating machine type exists
				resp := &compute.MachineType{Name: machineType, Zone: zone}
				_ = json.NewEncoder(rw).Encode(resp)
			} else if strings.Contains(req.URL.Path, "/projects/"+projectNumber) {
				// Mock project lookup
				resp := &compute.Project{Name: projectName}
				_ = json.NewEncoder(rw).Encode(resp)
			} else {
				http.Error(rw, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{computeService: computeSvc},
			Ctx:             ctx,
			Logger:          util.GetLogger(ctx),
			Retry:           NewExponentialRetryStrategy(time.Millisecond, 1),
		}

		available, err := gService.IsMachineTypeAvailable(projectNumber, zone, machineType)
		if err != nil {
			tt.Fatalf("unexpected error: %v", err)
		}
		if !available {
			tt.Error("expected machine type to be available")
		}
	})

	t.Run("WhenMachineTypeIsNotAvailable", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.Path, "/projects/"+projectName+"/zones/"+zone+"/machineTypes/"+machineType) {
				// Return 404 indicating machine type doesn't exist
				http.Error(rw, "not found", http.StatusNotFound)
			} else if strings.Contains(req.URL.Path, "/projects/"+projectNumber) {
				// Mock project lookup
				resp := &compute.Project{Name: projectName}
				_ = json.NewEncoder(rw).Encode(resp)
			} else {
				http.Error(rw, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{computeService: computeSvc},
			Ctx:             ctx,
			Logger:          util.GetLogger(ctx),
			Retry:           NewExponentialRetryStrategy(time.Millisecond, 1),
		}

		available, err := gService.IsMachineTypeAvailable(projectNumber, zone, machineType)
		if err != nil {
			tt.Fatalf("unexpected error: %v", err)
		}
		if available {
			tt.Error("expected machine type to not be available")
		}
	})

	t.Run("WhenGetProjectIDFromNumberFails", func(tt *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			// Return error for project lookup
			http.Error(rw, "internal error", http.StatusInternalServerError)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{computeService: computeSvc},
			Ctx:             ctx,
			Logger:          util.GetLogger(ctx),
			Retry:           NewExponentialRetryStrategy(time.Millisecond, 1),
		}

		available, err := gService.IsMachineTypeAvailable(projectNumber, zone, machineType)
		if err == nil {
			tt.Error("expected error but got none")
		}
		if available {
			tt.Error("expected false when error occurs")
		}
	})
}

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
		out, err := gService.GetComputeRegionalOpStatus(projectNumber, region, operationName)
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
	t.Run("WhenOperationErrored", func(tt *testing.T) {
		resp := &compute.Operation{Error: &compute.OperationError{Errors: []*compute.OperationErrorErrors{{Message: "operation not found"}}}}
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
		out, err := gService.GetComputeRegionalOpStatus(projectNumber, region, operationName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if out != nil {
				tt.Errorf("Unexpected output: %+v\n", out)
			}
			if !strings.Contains(err.Error(), "operation not found") {
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
			serviceNetworkingEndpoint: "endpoint.goog",
			AdminGCPService:           &adminService,
			Logger:                    log.NewLogger(),
		}
		out, err := gService.GetComputeRegionalOpStatus(projectNumber, region, operationName)
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
	projectNumber := "1079058383248"
	operationName := "op"
	url := fmt.Sprintf("/projects/%s/global/operations/%s", projectNumber, operationName)

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
		out, err := gService.GetComputeGlobalOpStatus(projectNumber, operationName)
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
	t.Run("WhenOperationErrored", func(tt *testing.T) {
		resp := &compute.Operation{Error: &compute.OperationError{Errors: []*compute.OperationErrorErrors{{Message: "operation not found"}}}}
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
		out, err := gService.GetComputeGlobalOpStatus(projectNumber, operationName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else {
			if out != nil {
				tt.Errorf("Unexpected output: %+v\n", out)
			}
			if !strings.Contains(err.Error(), "operation not found") {
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
		out, err := gService.GetComputeGlobalOpStatus(projectNumber, operationName)
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
