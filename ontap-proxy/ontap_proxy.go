package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	ontapProxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	extractOntapPath = utils.ExtractOntapPath
)

type AuthTransport struct{}

// ConnectionPool manages HTTP clients for different ONTAP clusters and auth types
type ConnectionPool struct {
	clients          map[string]*http.Client
	clientTimestamps map[string]time.Time // Track last used time
	mutex            sync.RWMutex

	// Configuration
	maxIdleConns        int
	maxIdleConnsPerHost int
	idleConnTimeout     time.Duration
	tlsHandshakeTimeout time.Duration
	cleanupThreshold    time.Duration

	// Cleanup
	cleanupTicker *time.Ticker
	stopCleanup   chan bool
}

// Global connection pool instance
var globalConnectionPool *ConnectionPool

// Initialize the global connection pool
func init() {
	globalConnectionPool = NewConnectionPool()
}

// NewConnectionPool creates a new connection pool with optimized settings
func NewConnectionPool() *ConnectionPool {
	pool := &ConnectionPool{
		clients:          make(map[string]*http.Client),
		clientTimestamps: make(map[string]time.Time),

		// Configuration from environment variables with defaults
		maxIdleConns:        env.GetInt("ONTAP_MAX_IDLE_CONNS", 200),
		maxIdleConnsPerHost: env.GetInt("ONTAP_MAX_IDLE_CONNS_PER_HOST", 50),
		idleConnTimeout:     time.Duration(env.GetInt("ONTAP_IDLE_CONN_TIMEOUT_SECONDS", 120)) * time.Second,
		tlsHandshakeTimeout: time.Duration(env.GetInt("ONTAP_TLS_HANDSHAKE_TIMEOUT_SECONDS", 15)) * time.Second,
		cleanupThreshold:    time.Duration(env.GetInt("ONTAP_CLEANUP_THRESHOLD_SECONDS", 300)) * time.Second,

		stopCleanup: make(chan bool),
	}

	// Start cleanup routine
	pool.startCleanupRoutine()

	return pool
}

// GetClient returns a pooled HTTP client for the given ONTAP address and auth type
func (p *ConnectionPool) GetClient(ontapAddress string, authData *models.AuthData) (*http.Client, error) {
	key := p.generatePoolKey(ontapAddress, authData)

	// Try to get existing client
	p.mutex.RLock()
	if client, exists := p.clients[key]; exists {
		p.mutex.RUnlock()
		// Update last used timestamp
		p.mutex.Lock()
		p.clientTimestamps[key] = time.Now()
		p.mutex.Unlock()
		return client, nil
	}
	p.mutex.RUnlock()

	// Create new client
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Double-check after acquiring write lock
	if client, exists := p.clients[key]; exists {
		p.clientTimestamps[key] = time.Now()
		return client, nil
	}

	// Create new client
	client, err := p.createClient(ontapAddress, authData)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// connectivity test

	p.clients[key] = client
	p.clientTimestamps[key] = time.Now()
	return client, nil
}

// generatePoolKey creates a unique key for the connection pool
func (p *ConnectionPool) generatePoolKey(ontapAddress string, authData *models.AuthData) string {
	return fmt.Sprintf("%s:%d:%s", ontapAddress, authData.AuthType, authData.PoolID)
}

// createClient creates a new HTTP client with optimized transport
func (p *ConnectionPool) createClient(ontapAddress string, authData *models.AuthData) (*http.Client, error) {
	transport, err := p.buildOptimizedTransport(authData)
	if err != nil {
		return nil, fmt.Errorf("failed to build transport: %w", err)
	}

	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(env.GetInt("ONTAP_CLIENT_TIMEOUT_SECONDS", 60)) * time.Second,
	}, nil
}

// buildOptimizedTransport creates an optimized HTTP transport based on auth type
func (p *ConnectionPool) buildOptimizedTransport(authData *models.AuthData) (*http.Transport, error) {
	// Add TLS configuration based on auth type
	switch authData.AuthType {
	case models.USER_CERTIFICATE:
		return p.buildCertificateTransport(authData)
	case models.USERNAME_PWD, models.USERNAME_PWD_SEC_MGR:
		return p.buildBasicAuthTransport()
	default:
		return p.buildBasicAuthTransport()
	}
}

// createBaseTransport creates a transport with base settings and the provided TLS config
func (p *ConnectionPool) createBaseTransport(tlsConfig *tls.Config) *http.Transport {
	return &http.Transport{
		// Connection Pooling Settings
		MaxIdleConns:        p.maxIdleConns,
		MaxIdleConnsPerHost: p.maxIdleConnsPerHost,
		MaxConnsPerHost:     env.GetInt("ONTAP_MAX_CONNS_PER_HOST", 100),
		IdleConnTimeout:     p.idleConnTimeout,

		// Performance Settings
		DisableKeepAlives:  false, // Enable keep-alive
		DisableCompression: false, // Enable compression
		ForceAttemptHTTP2:  true,  // Enable HTTP/2

		// Timeout Settings
		TLSHandshakeTimeout:   p.tlsHandshakeTimeout,
		ResponseHeaderTimeout: time.Duration(env.GetInt("ONTAP_RESPONSE_HEADER_TIMEOUT_SECONDS", 30)) * time.Second,
		ExpectContinueTimeout: time.Duration(env.GetInt("ONTAP_EXPECT_CONTINUE_TIMEOUT_SECONDS", 1)) * time.Second,

		// Proxy and Environment
		Proxy: http.ProxyFromEnvironment,

		// TLS Configuration
		TLSClientConfig: tlsConfig,
	}
}

// buildCertificateTransport creates transport with certificate authentication
func (p *ConnectionPool) buildCertificateTransport(authData *models.AuthData) (*http.Transport, error) {
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

	// Create transport with base settings and certificate TLS config
	return p.createBaseTransport(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: false,
		RootCAs:            rootCA,
		Certificates:       []tls.Certificate{clientCert},
	}), nil
}

// buildBasicAuthTransport creates transport with basic authentication
func (p *ConnectionPool) buildBasicAuthTransport() (*http.Transport, error) {
	// Create transport with base settings and basic auth TLS config
	return p.createBaseTransport(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}), nil
}

// startCleanupRoutine starts the background cleanup routine
func (p *ConnectionPool) startCleanupRoutine() {
	p.cleanupTicker = time.NewTicker(time.Duration(env.GetInt("ONTAP_CLEANUP_INTERVAL_SECONDS", 60)) * time.Second)

	go func() {
		for {
			select {
			case <-p.cleanupTicker.C:
				p.cleanup()
			case <-p.stopCleanup:
				p.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// cleanup removes unused connections based on age and count
func (p *ConnectionPool) cleanup() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	maxConnections := env.GetInt("ONTAP_MAX_TOTAL_CONNECTIONS", 1000)
	now := time.Now()
	threshold := now.Add(-p.cleanupThreshold)

	// First, remove connections older than the threshold
	for key, lastUsed := range p.clientTimestamps {
		if lastUsed.Before(threshold) {
			if client, exists := p.clients[key]; exists {
				if transport, ok := client.Transport.(*http.Transport); ok {
					transport.CloseIdleConnections()
				}
				delete(p.clients, key)
			}
			delete(p.clientTimestamps, key)
		}
	}

	// If still over limit after time-based cleanup, remove oldest connections
	if len(p.clients) > maxConnections {
		// Find the oldest connection
		oldestKey := ""
		oldestTime := now
		for key, lastUsed := range p.clientTimestamps {
			if lastUsed.Before(oldestTime) {
				oldestTime = lastUsed
				oldestKey = key
			}
		}

		// Remove the oldest connection
		if oldestKey != "" {
			if client, exists := p.clients[oldestKey]; exists {
				if transport, ok := client.Transport.(*http.Transport); ok {
					transport.CloseIdleConnections()
				}
				delete(p.clients, oldestKey)
			}
			delete(p.clientTimestamps, oldestKey)
		}
	}
}

// GetStats returns connection pool statistics
func (p *ConnectionPool) GetStats() map[string]interface{} {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return map[string]interface{}{
		"total_connections": len(p.clients),
		"max_idle_conns":    p.maxIdleConns,
		"max_idle_per_host": p.maxIdleConnsPerHost,
		"idle_timeout":      p.idleConnTimeout.String(),
	}
}

// Close closes the connection pool and cleans up resources
func (p *ConnectionPool) Close() {
	p.stopCleanup <- true

	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Close all clients
	for _, client := range p.clients {
		if transport, ok := client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	p.clients = make(map[string]*http.Client)
	p.clientTimestamps = make(map[string]time.Time)
}

// Enhanced AuthTransport with connection pooling
type PooledAuthTransport struct {
	pool *ConnectionPool
}

// NewPooledAuthTransport creates a new pooled auth transport
func NewPooledAuthTransport() *PooledAuthTransport {
	return &PooledAuthTransport{
		pool: globalConnectionPool,
	}
}

// RoundTrip executes the request using a pooled connection
func (pat *PooledAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	logger := util.GetLogger(req.Context())

	cacheKey := cache.GetAuthDataKeyFromContext(req.Context())
	if cacheKey == "" {
		return nil, fmt.Errorf("no cache key found in request context")
	}

	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return nil, fmt.Errorf("no authentication data found in cache for key: %s", cacheKey)
	}

	// Use ONTAP address from req.URL.Host (already set by Director)
	// This avoids duplicating the extraction logic
	ontapAddress := req.URL.Host
	if ontapAddress == "" {
		return nil, fmt.Errorf("could not extract ONTAP address from request URL")
	}

	// Get pooled client
	client, err := pat.pool.GetClient(ontapAddress, authData)
	if err != nil {
		logger.ErrorContext(req.Context(), "Failed to get pooled client", "error", err, "ontapAddress", ontapAddress)
		return nil, fmt.Errorf("failed to get pooled client: %w", err)
	}

	// Create a new request from the existing one to avoid RequestURI issues
	// This is necessary because httputil.ReverseProxy may set RequestURI which
	// is not allowed for client requests
	newReq := req.Clone(req.Context())

	// Clear RequestURI if it was set (not allowed for client requests)
	newReq.RequestURI = ""

	// Configure authentication
	err = configureRequestAuthentication(newReq, authData)
	if err != nil {
		logger.ErrorContext(req.Context(), "Failed to configure authentication", "error", err)
		return nil, fmt.Errorf("failed to configure authentication: %w", err)
	}

	// Set common headers
	setCommonHeaders(newReq)

	logger.DebugContext(req.Context(), "Using pooled connection",
		"ontapAddress", ontapAddress,
		"authType", authData.AuthType,
		"poolID", authData.PoolID)

	// Execute request using pooled connection
	return client.Do(newReq)
}

// configureRequestAuthentication configures authentication for the request
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

// setCommonHeaders sets common headers for the request
func setCommonHeaders(req *http.Request) {
	req.Header.Set("X-Forwarded-For", req.RemoteAddr)
	req.Header.Set("X-Proxy-By", "ontap-proxy")
	req.Header.Set("Accept", "application/json")
	if req.Context().Value(models.RuleContextKey) != nil {
		req.Header.Set("Accept-Encoding", "")
	}
}

// BuildOntapRESTProxy creates the reverse proxy with connection pooling
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

			logger.InfoContext(req.Context(), "Forwarding request",
				"targetURL", targetURL,
				"poolID", authData.PoolID,
				"authType", authData.AuthType,
				"method", req.Method,
				"path", ontapPath)
			logCurlCommand(req, targetURL)
		},
		Transport:      NewPooledAuthTransport(), // Use pooled transport
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

// logCurlCommand logs the equivalent curl command for debugging
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

// _getAPICallCertificate prepares certificates for API calls
func _getAPICallCertificate(cert *models.Certificate) (*x509.CertPool, tls.Certificate, error) {
	if len(cert.InterMediateCertificates) > 0 && cert.SignedCertificate != "" && cert.PrivateKey != "" {
		rootCA, err := ontapProxyutils.ParsePEMCertificate(cert.InterMediateCertificates, "CERTIFICATE")
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
