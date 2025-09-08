package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func BuildOntapRESTProxy() *httputil.ReverseProxy {
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			logger := log.NewLogger()

			ontapPath := extractOntapPath(req.URL.Path)
			if ontapPath == "" {
				logger.Error("Could not extract ONTAP path")
				return
			}

			ontapAddress := getOntapAddress(req)
			if ontapAddress == "" {
				logger.Error("No ONTAP API address configured")
				return
			}

			username := env.GetString("ONTAP_API_USERNAME", "")
			password := env.GetString("ONTAP_API_PASSWORD", "")

			if username == "" || password == "" {
				logger.Error("Missing ONTAP credentials")
				return
			}

			targetURL := buildTargetURL(ontapAddress, ontapPath, req.URL.RawQuery)

			target, err := url.Parse(targetURL)
			if err != nil {
				logger.Error("Error parsing target URL", "error", err)
				return
			}

			req.URL = target
			req.Host = target.Host
			req.SetBasicAuth(username, password)

			req.Header.Set("X-Forwarded-For", req.RemoteAddr)
			req.Header.Set("X-Proxy-By", "ONTAP Expert Mode")
			req.Header.Set("Accept", "application/json")

			logger.Info("Forwarding request", "targetURL", targetURL)
			logCurlCommand(req, targetURL)
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		ModifyResponse: func(resp *http.Response) error {
			logger := log.NewLogger()

			if ctx := resp.Request.Context().Value("ruleContext"); ctx != nil {
				if action, ok := ctx.(actions.IAction); ok {
					if err := action.ProcessResponse(resp); err != nil {
						logger.Error("Error applying modifications", "error", err)
						return err
					}
				}
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger := log.NewLogger()
			logger.Error("Error handling request", "error", err)

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

func getOntapAddress(req *http.Request) string {
	return env.GetString("ONTAP_API_ADDRESS", "")
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
	if !strings.HasPrefix(ontapAddress, "http://") && !strings.HasPrefix(ontapAddress, "https://") {
		ontapAddress = "https://" + ontapAddress
	}

	targetURL := ontapAddress + ontapPath

	if rawQuery != "" {
		targetURL += "?" + rawQuery
	}

	return targetURL
}

func logCurlCommand(req *http.Request, targetURL string) {
	logger := log.NewLogger()

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

	logger.Info("Equivalent curl command", "command", curlCmd)
}
