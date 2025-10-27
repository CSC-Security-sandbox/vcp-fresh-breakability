package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type AuthTransport struct{}

func NewAuthTransport() *AuthTransport {
	return &AuthTransport{}
}

func (at *AuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cacheKey := cache.GetAuthDataKeyFromContext(req.Context())
	if cacheKey == "" {
		return nil, fmt.Errorf("no cache key found in request context")
	}

	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return nil, fmt.Errorf("no authentication data found in cache for key: %s", cacheKey)
	}

	transport, err := buildTransportForAuthType(authData)
	if err != nil {
		return nil, fmt.Errorf("failed to build transport: %w", err)
	}

	return transport.RoundTrip(req)
}

func configureRequestAuthentication(req *http.Request, authData *models.AuthData) error {
	if authData.AuthType == models.USER_CERTIFICATE {
		return nil
	}
	if authData.Username == "" || authData.Password == "" {
		return fmt.Errorf("missing username or password for basic authentication")
	}
	req.SetBasicAuth(authData.Username, authData.Password)
	return nil
}

func buildTransportForAuthType(authData *models.AuthData) (*http.Transport, error) {
	switch authData.AuthType {
	case models.USER_CERTIFICATE:
		return buildCertificateTransport(authData)
	case models.USERNAME_PWD, models.USERNAME_PWD_SEC_MGR:
		return buildBasicAuthTransport()
	default:
		return buildBasicAuthTransport()
	}
}

func buildCertificateTransport(authData *models.AuthData) (*http.Transport, error) {
	if authData.Certificate == nil {
		return nil, fmt.Errorf("certificate not found for certificate authentication")
	}

	cert := &models.Certificate{
		SignedCertificate:        authData.Certificate.SignedCertificate,
		PrivateKey:               authData.Certificate.PrivateKey,
		InterMediateCertificates: authData.Certificate.InterMediateCertificates,
		CommonName:               authData.Certificate.CommonName,
		RootCaCertificate:        authData.Certificate.RootCaCertificate,
	}

	rootCA, clientCert, err := _getAPICallCertificate(cert)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare certificate: %w", err)
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: false,
			RootCAs:            rootCA,
			Certificates:       []tls.Certificate{clientCert},
		},
	}, nil
}

func buildBasicAuthTransport() (*http.Transport, error) {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		},
	}, nil
}

func setCommonHeaders(req *http.Request) {
	req.Header.Set("X-Forwarded-For", req.RemoteAddr)
	req.Header.Set("X-Proxy-By", "ontap-proxy")
	req.Header.Set("Accept", "application/json")
}

func BuildOntapRESTProxy() *httputil.ReverseProxy {
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			logger := util.GetLogger(req.Context())

			cacheKey := cache.GetAuthDataKeyFromContext(req.Context())
			if cacheKey == "" {
				logger.ErrorContext(req.Context(), "No cache key found in context")
				return
			}

			authData, exists := cache.GetFromAuthDataCache(cacheKey)
			if !exists || authData == nil {
				logger.ErrorContext(req.Context(), "No authentication data found in cache", "cacheKey", cacheKey)
				return
			}

			ontapPath := extractOntapPath(req.URL.Path)
			if ontapPath == "" {
				logger.ErrorContext(req.Context(), "Could not extract ONTAP path", "path", req.URL.Path)
				return
			}

			var ontapAddress string
			if len(authData.OntapEndpoints) > 0 {
				ontapAddress = authData.OntapEndpoints[0].DNS
			} else {
				logger.ErrorContext(req.Context(), "No ONTAP endpoints found in authData", "cacheKey", cacheKey)
				return
			}

			targetURL := buildTargetURL(ontapAddress, ontapPath, req.URL.RawQuery)
			target, err := url.Parse(targetURL)
			if err != nil {
				logger.ErrorContext(req.Context(), "Error parsing target URL", "error", err, "targetURL", targetURL)
				return
			}

			req.URL = target
			req.Host = target.Host

			err = configureRequestAuthentication(req, authData)
			if err != nil {
				logger.ErrorContext(req.Context(), "Failed to configure request authentication", "error", err, "authType", authData.AuthType)
				return
			}

			setCommonHeaders(req)

			logger.InfoContext(req.Context(), "Forwarding request",
				"targetURL", targetURL,
				"poolID", authData.PoolID,
				"authType", authData.AuthType,
				"method", req.Method,
				"path", ontapPath)
			logCurlCommand(req, targetURL)
		},
		Transport:      NewAuthTransport(),
		ModifyResponse: actions.ProcessResponseModification,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger := util.GetLogger(r.Context())
			logger.ErrorContext(r.Context(), "Error handling request", "error", err, "path", r.URL.Path)

			if strings.Contains(err.Error(), "context canceled") {
				http.Error(w, "Request timeout - ONTAP cluster not responding", http.StatusGatewayTimeout)
			} else if strings.Contains(err.Error(), "connection refused") {
				http.Error(w, "Cannot connect to ONTAP cluster", http.StatusBadGateway)
			} else if strings.Contains(err.Error(), "no such host") {
				http.Error(w, "ONTAP cluster host not found", http.StatusBadGateway)
			} else if strings.Contains(err.Error(), "Missing ONTAP credentials") {
				http.Error(w, "ONTAP credentials not configured", http.StatusInternalServerError)
			} else {
				http.Error(w, "Proxy error: "+err.Error(), http.StatusBadGateway)
			}
		},
	}

	return proxy
}

func extractOntapPath(fullPath string) string {
	parts := strings.Split(fullPath, "/")

	ontapApiIndex := -1
	for i, part := range parts {
		if part == "ontap-api" {
			ontapApiIndex = i
			break
		}
	}

	if ontapApiIndex == -1 {
		return ""
	}

	ontapPath := "/" + strings.Join(parts[ontapApiIndex+1:], "/")
	return ontapPath
}

func buildTargetURL(ontapAddress, ontapPath, rawQuery string) string {
	if !strings.HasPrefix(ontapAddress, "https://") && !strings.HasPrefix(ontapAddress, "http://") {
		ontapAddress = "https://" + ontapAddress
	}

	targetURL := ontapAddress + ontapPath

	if rawQuery != "" {
		targetURL += "?" + rawQuery
	}

	return targetURL
}

func logCurlCommand(req *http.Request, targetURL string) {
	logger := util.GetLogger(req.Context())

	curlCmd := fmt.Sprintf("curl -X %s", req.Method)

	for key, values := range req.Header {
		if key != "Authorization" {
			for _, value := range values {
				curlCmd += fmt.Sprintf(" -H \"%s: %s\"", key, value)
			}
		}
	}

	if req.Header.Get("Authorization") != "" {
		curlCmd += " -u \"username:password\""
	}

	curlCmd += fmt.Sprintf(" \"%s\"", targetURL)

	logger.DebugContext(req.Context(), "Equivalent curl command", "command", curlCmd)
}

func _getAPICallCertificate(cert *models.Certificate) (*x509.CertPool, tls.Certificate, error) {
	if len(cert.InterMediateCertificates) > 0 && cert.SignedCertificate != "" && cert.PrivateKey != "" {
		rootCA, err := utils.ParsePEMCertificate(cert.InterMediateCertificates, "CERTIFICATE")
		if err != nil {
			return nil, tls.Certificate{}, fmt.Errorf("error parsing root CA certificate: %v", err)
		}

		signedCertPem := []byte(cert.SignedCertificate)
		privateKeyPem := []byte(cert.PrivateKey)

		clientCert, err := tls.X509KeyPair(signedCertPem, privateKeyPem)
		if err != nil {
			return nil, tls.Certificate{}, fmt.Errorf("error loading client certificate and key: %v", err)
		}

		return rootCA, clientCert, nil
	}
	return nil, tls.Certificate{}, fmt.Errorf("invalid certificate parameters: ensure SignedCertificate, PrivateKey, and InterMediateCertificates are set correctly")
}
