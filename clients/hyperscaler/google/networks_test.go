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

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/option"
	"google.golang.org/api/serviceconsumermanagement/v1"
	"google.golang.org/api/servicenetworking/v1"
)

func testReset(t *testing.T) {
	waitTimeoutMinutes = time.Minute * time.Duration(env.GetInt("GCP_LRO_TIMEOUT_MINUTES", 20))
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "mock-consumer-endpoint.com")
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "mock-endpoint.com")
	newClient = _newClient
}

func Test_GetTenantProject(t *testing.T) {
	serviceConsumerManagementEndpoint = env.GetString("GCP_CONSUMER_MGMT_ENDPOINT_URL", "autopush-netapp.sandbox.googleapis.com")
	serviceNetworkingEndpoint = env.GetString("GCP_SERVICE_NETWORKING_ENDPOINT_URL", "netapp-tst-autopush-endpoint.appspot.com")
	t.Run("WhenGetTenantProjectFails", func(tt *testing.T) {
		defer testReset(tt)
		customerProjectNumber := "1079058383248"
		consumerNetwork := "projects/1079058383248/global/networks/network-to-netapp2"
		region := "US-East-4"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/services/"+serviceConsumerManagementEndpoint+"/projects/"+customerProjectNumber+"/tenancyUnits" {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
		}))
		defer server.Close()
		mgmtSvc, err := serviceconsumermanagement.NewService(context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Logger:                            log.NewLogger(),
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
		customerProjectNumber := "1079058383248"
		consumerNetwork := "projects/1079058383248/global/networks/network-to-netapp2"
		region := "US-East-4"
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/services/"+serviceConsumerManagementEndpoint+"/projects/"+customerProjectNumber+"/tenancyUnits" {
				response, _ := json.Marshal(&serviceconsumermanagement.ListTenancyUnitsResponse{TenancyUnits: []*serviceconsumermanagement.TenancyUnit{{TenantResources: []*serviceconsumermanagement.TenantResource{{Resource: "projects/175643", Tag: consumerNetwork + "-" + region}}}}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		mgmtSvc, err := serviceconsumermanagement.NewService(context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Logger:                            log.NewLogger(),
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
		customerProjectNumber := "1079058383248"
		consumerNetwork := "projects/1079058383248/global/networks/network-to-netapp2"
		region := "US-East-4"
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
		mgmtSvc, err := serviceconsumermanagement.NewService(context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Logger:                            log.NewLogger(),
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
		customerProjectNumber := "1079058383248"
		consumerNetwork := "projects/1079058383248/global/networks/network-to-netapp2"
		region := "US-East-4"
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
		mgmtSvc, err := serviceconsumermanagement.NewService(context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				managementService: mgmtSvc,
			},
			Logger:                            log.NewLogger(),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		_, err = gService.GetTenantProject(consumerNetwork, customerProjectNumber, region)
		if err != nil {
			tt.Errorf("Unexpected error: %s", err.Error())
		}
	})

}

func Test_AddSubnetwork(t *testing.T) {
	url := "/v1/services/endpoint.goog/projects/1234:addSubnetwork"
	t.Run("WhenAddSubnetworkFails", func(tt *testing.T) {
		defer testReset(tt)
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
		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Logger:                            log.NewLogger(),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := gService.AddSubnetwork(&servicenetworking.AddSubnetworkRequest{}, "1234")
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
	})
	t.Run("WhenAddSubnetworkGoogleFails", func(tt *testing.T) {
		defer testReset(tt)
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
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}
		tenantProjectNumber := "1234"
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
			Logger:                            log.NewLogger(),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := gService.AddSubnetwork(&servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
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
	})
	t.Run("WhenOKWithError", func(tt *testing.T) {
		defer testReset(tt)
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
			context.TODO(), option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		tenantProjectNumber := "1234"

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Logger:                            log.NewLogger(),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := gService.AddSubnetwork(&servicenetworking.AddSubnetworkRequest{}, tenantProjectNumber)
		if err == nil {
			tt.Errorf("Error expected: %s", err.Error())
		} else {
			if out != nil {
				tt.Errorf("Expected nil")
			}
		}
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
		tenantProjectNumber := "1234"
		consumerNetwork := "projects/1234/global/networks/network-to-netapp2"

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				networkingService: svc,
			},
			Logger:                            log.NewLogger(),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
			serviceNetworkingEndpoint:         serviceNetworkingEndpoint,
		}
		out, err := gService.AddSubnetwork(&servicenetworking.AddSubnetworkRequest{
			Consumer:        "projects/1234",
			Region:          "us-east-4",
			Description:     "vsanetwork",
			IpPrefixLength:  28,
			ConsumerNetwork: consumerNetwork,
			Subnetwork:      "vsa-" + "us-east-4",
		}, tenantProjectNumber)
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
	})
}
