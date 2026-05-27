package google

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	hyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/privateca/v1"
)

func Test_GetCertificate(t *testing.T) {
	projectId := "1079058383248"
	region := "us-east4"
	certID := "cert-id"
	pooID := "pool-id"
	t.Run("WhenGetCertificateFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		errString := fmt.Errorf("GetCertificate failed for certificate : %s", certID)
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s/get", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&privateca.Operation{Name: certID, Error: &privateca.Status{Message: errString.Error(), Code: 505}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		cert, err := gService.GetCertificate(projectId, region, pooID, certID)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if cert != nil {
			tt.Errorf("Expected nil operation but got: %+v", cert)
		}
	})
	t.Run("WhenGetCertificateFailsRevoked", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&privateca.Certificate{
					Name: certID,
					RevocationDetails: &privateca.RevocationDetails{
						RevocationState: PrivilegeWithdrawn,
					},
				})
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		cert, err := gService.GetCertificate(projectId, region, pooID, certID)
		if err != nil && cert != nil {
			tt.Error("Expected cert to be nil for revoked certificate")
		}
	})
	t.Run("WhenGetCertificateFailsNotFound", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		errString := fmt.Errorf("GetCertificate failed for certificate : %s", certID)
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s/get", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&privateca.Operation{Name: certID, Error: &privateca.Status{Message: errString.Error(), Code: 404}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		cert, err := gService.GetCertificate(projectId, region, pooID, certID)
		if err != nil && cert != nil {
			tt.Error("Expected an error but got nothing")
		}
	})
	t.Run("WhenGetCertificateSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		resp := &hyperscaler.CustomCertificate{Name: certID}
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodGet {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		cert, err := gService.GetCertificate(projectId, region, pooID, certID)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if cert == nil || cert.Name != certID {
			tt.Errorf("Unexpected operation: %+v", cert)
		}
	})
}

func Test_CreateCertificate(t *testing.T) {
	projectId := "1079058383248"
	region := "us-east4"
	certID := "cert-id"
	caName := "ca-name"
	pooID := "pool-id"
	t.Run("WhenCreateCertificateFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		errString := fmt.Errorf("CreateCertificate failed for certificate : %s", certID)
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s", projectId, region, pooID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&privateca.Operation{Name: certID, Error: &privateca.Status{Message: errString.Error(), Code: 505}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		certificate := &hyperscaler.CustomCertificate{
			CaName:           caName,
			CaGroupName:      pooID,
			Region:           region,
			CertOwningEntity: projectId,
			PemCsr:           "pem-csr",
		}
		cert, err := gService.CreateCertificate(certificate)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if cert != nil {
			tt.Errorf("Expected nil operation but got: %+v", cert)
		}
	})
	t.Run("WhenCreateCertificateSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates", projectId, region, pooID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&privateca.Operation{Name: certID, Error: nil})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		certificate := &hyperscaler.CustomCertificate{
			CaName:           caName,
			CaGroupName:      pooID,
			Region:           region,
			CertOwningEntity: projectId,
			CertificateID:    certID,
			PemCsr:           "pem-csr",
		}

		cert, err := gService.CreateCertificate(certificate)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if cert == nil || cert.Name != certID {
			tt.Errorf("Unexpected operation: %+v", cert)
		}
	})
}

func Test_RevokeCertificate(t *testing.T) {
	projectId := "1079058383248"
	region := "us-east4"
	certID := "cert-id"
	pooID := "pool-id"
	t.Run("WhenRevokeCertificateFails", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()
		errString := fmt.Errorf("GetCertificate failed for certificate : %s", certID)
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s:revok", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&privateca.Operation{Name: certID, Error: &privateca.Status{Message: errString.Error(), Code: 505}})
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}
		certificate := &hyperscaler.CustomCertificate{
			CaGroupName:      pooID,
			Region:           region,
			CertOwningEntity: projectId,
			CertificateID:    certID,
		}
		cert, err := gService.RevokeCertificate(certificate)
		if err == nil {
			tt.Error("Expected an error but got nothing")
		} else if cert != "" {
			tt.Errorf("Expected nil operation but got: %+v", cert)
		}
	})
	t.Run("WhenRevokeCertificateFailsWithCAQuotaLimit", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()

		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s:revoke", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				rw.Header().Set("Content-Type", "application/json")
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write([]byte(`{"error":{"code":400,"message":"Maximum number of unexpired revoked certificates per CA reached. Please try again after revoked certificates expire.","status":"FAILED_PRECONDITION"}}`))
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		certificate := &hyperscaler.CustomCertificate{
			CaGroupName:      pooID,
			Region:           region,
			CertOwningEntity: projectId,
			CertificateID:    certID,
		}
		resourceName, err := gService.RevokeCertificate(certificate)
		if err != nil {
			tt.Errorf("Expected nil error for CA revocation quota limit, got: %v", err)
		}
		expectedResourceName := fmt.Sprintf("projects/%s/locations/%s/caPools/%s/certificates/%s", projectId, region, pooID, certID)
		if resourceName != expectedResourceName {
			tt.Errorf("Expected resourceName %s but got: %s", expectedResourceName, resourceName)
		}
	})
	t.Run("WhenRevokeCertificateSuccess", func(tt *testing.T) {
		defer testReset(tt)
		ctx := context.Background()

		resp := &hyperscaler.CustomCertificate{Name: certID}
		url := fmt.Sprintf("/v1/projects/%s/locations/%s/caPools/%s/certificates/%s:revoke", projectId, region, pooID, certID)
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url && req.Method == http.MethodPost {
				response, _ := json.Marshal(&resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
		}))
		defer server.Close()
		svc, err := privateca.NewService(ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL))
		if err != nil {
			t.Errorf("Error getting service up: '%s'", err.Error())
		}

		gService := &GcpServices{
			AdminGCPService: &AdminGCPService{
				privateCaService: svc,
			},
			Ctx:                               ctx,
			Logger:                            util.GetLogger(ctx),
			serviceConsumerManagementEndpoint: serviceConsumerManagementEndpoint,
		}

		certificate := &hyperscaler.CustomCertificate{
			CaGroupName:      pooID,
			Region:           region,
			CertOwningEntity: projectId,
			CertificateID:    certID,
		}
		cert, err := gService.RevokeCertificate(certificate)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if cert != fmt.Sprintf("projects/%s/locations/%s/caPools/%s/certificates/%s", projectId, region, pooID, certID) {
			tt.Errorf("Unexpected operation: %+v", cert)
		}
	})
}
func Test_googleResourceNotFoundCheck(t *testing.T) {
	// Case: googleapi.Error with 404 code returns nil
	gErr := &googleapi.Error{Code: 404}
	err := googleResourceNotFoundCheck(gErr)
	if err != nil {
		t.Errorf("Expected nil for 404 error, got: %v", err)
	}

	// Case: googleapi.Error with non-404 code returns the error
	gErr = &googleapi.Error{Code: 500}
	err = googleResourceNotFoundCheck(gErr)
	if err == nil {
		t.Errorf("Expected non-nil for non-404 error")
	}

	// Case: non-googleapi.Error returns the error
	otherErr := fmt.Errorf("some error")
	err = googleResourceNotFoundCheck(otherErr)
	if err == nil {
		t.Errorf("Expected non-nil for generic error")
	}
}
