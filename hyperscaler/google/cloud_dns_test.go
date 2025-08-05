package google

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/option"
)

func TestCreateResourceRecordSet(t *testing.T) {
	projectId := "1079058383248"
	managedZone := "us-east4.vsa"
	ipAddress := "10.0.0.1"
	recordName := "dns1.cluster1.us-east4.vsa."
	t.Run("WhenCreateResourceRecordSetSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s/rrsets", projectId, managedZone)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&dns.ResourceRecordSet{Rrdatas: []string{recordName}})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		ogValidateAndConvertToCustomCloudDNSRecord := ValidateAndConvertToCustomCloudDNSRecord
		ValidateAndConvertToCustomCloudDNSRecord = func(resp *dns.ResourceRecordSet, managedZone string) (*models.CustomCloudDNSRecord, error) {
			return &models.CustomCloudDNSRecord{
				RecordName:  recordName,
				Type:        "A",
				TTL:         env.CloudDNSCacheTTL,
				ManagedZone: managedZone,
				Data:        ipAddress,
			}, nil
		}
		defer func() {
			ValidateAndConvertToCustomCloudDNSRecord = ogValidateAndConvertToCustomCloudDNSRecord
		}()

		_, err = gService.CreateResourceRecordSet(projectId, managedZone, ipAddress, recordName)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
	t.Run("WhenCreateResourceRecordSetFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s/rrsets-wrong", projectId, managedZone)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&dns.ResourceRecordSet{})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		_, err = gService.CreateResourceRecordSet(projectId, managedZone, ipAddress, recordName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		}
	})
}

func TestDeleteResourceRecordSet(t *testing.T) {
	projectId := "1079058383248"
	managedZone := "us-east4.vsa"
	recordName := "dns1.cluster1.us-east4.vsa."
	t.Run("WhenDeleteResourceRecordSetSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s/rrsets/%s/%s", projectId, managedZone, recordName, recordType)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodDelete {
				response, _ := json.Marshal(&dns.ResourceRecordSetsDeleteResponse{})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		err = gService.DeleteResourceRecordSet(projectId, managedZone, recordName)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
	})
	t.Run("WhenDeleteResourceRecordSetFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/managedZones/%s/rrsets/%s/wrong", projectId, managedZone, recordName)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodDelete {
				response, _ := json.Marshal(&dns.ResourceRecordSetsDeleteResponse{})
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		err = gService.DeleteResourceRecordSet(projectId, managedZone, recordName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		}
	})
}

func TestGetResourceRecordSet(t *testing.T) {
	projectId := "1079058383248"
	managedZone := "us-east4.vsa"
	recordName := "dns1.cluster1.us-east4.vsa."
	t.Run("WhenGetResourceRecordSetSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s/rrsets", projectId, managedZone)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&dns.ResourceRecordSetsListResponse{
					Rrsets: []*dns.ResourceRecordSet{
						{
							Name:    recordName,
							Type:    "A",
							Ttl:     300,
							Rrdatas: []string{"10.0.1.9"},
						},
					},
				})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		record, err := gService.GetResourceRecordSet(projectId, managedZone, recordName)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if record == nil || record.RecordName != recordName {
			tt.Errorf("Unexpected operation: %+v", recordName)
		}
	})
	t.Run("WhenGetResourceRecordSetEmptyCaseFailure", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s/rrsets", projectId, managedZone)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&models.CustomCloudDNSRecord{})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		record, err := gService.GetResourceRecordSet(projectId, managedZone, recordName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if record != nil {
			tt.Errorf("Expected nil operation but got: %+v", recordName)
		}
	})
	t.Run("WhenGetResourceRecordSetFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s/rrsets-wrong", projectId, managedZone)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&dns.ResourceRecordSet{})
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := dns.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				cloudDnsService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		record, err := gService.GetResourceRecordSet(projectId, managedZone, recordName)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if record != nil {
			tt.Errorf("Expected nil operation but got: %+v", recordName)
		}
	})
}
