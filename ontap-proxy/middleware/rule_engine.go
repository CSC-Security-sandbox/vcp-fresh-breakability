package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/rules"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func RuleEngineMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := log.NewLogger()

			path := extractOntapPath(r.URL.Path)
			if path == "" {
				logger.Error("Could not extract ONTAP path", "path", r.URL.Path)
				http.Error(w, "Invalid path", http.StatusBadRequest)
				return
			}

			proxyRules := rules.GetProxyRules()

			var matchedRule actions.Rule
			var matchedPath string
			for rulePath, rule := range proxyRules {
				if path == rulePath {
					matchedRule = rule
					matchedPath = rulePath
					break
				}
			}

			if matchedPath == "" {
				logger.Error("No rule found for path", "path", path)
				http.Error(w, "No rule configured for this endpoint", http.StatusNotFound)
				return
			}

			logger.Info("Found rule for path", "path", matchedPath)

			action := matchedRule.GetAction(r)
			if action == nil {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}

			if !action.ShouldAllow(r) {
				logger.Info("Request denied by action")
				if err := action.ProcessRequest(r, w); err != nil {
					logger.Error("Error in request handler", "error", err)
				}
				return
			}

			if err := action.ProcessRequest(r, w); err != nil {
				logger.Error("Error processing request", "error", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}

			ctx := context.WithValue(r.Context(), "ruleContext", action)
			r = r.WithContext(ctx)

			next.ServeHTTP(w, r)
		})
	}
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
