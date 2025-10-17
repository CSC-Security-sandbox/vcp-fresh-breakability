package middleware

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/rules"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// uuidPattern is a compiled regex pattern for matching UUIDs in URL paths
var uuidPattern = regexp.MustCompile(`/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}`)

func RuleEngineMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := log.NewLogger()

			matchedRule, matchedPath, found := findMatchingRule(r.URL.Path, logger)
			if !found {
				next.ServeHTTP(w, r)
				return
			}

			logger.Info("Found rule for path", "path", matchedPath)

			action := matchedRule.GetAction(r)
			if action == nil {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}

			allowed, err := action.ShouldAllow(r)
			if err != nil {
				logger.Error("Validation error", "error", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !allowed {
				logger.Info("Request denied by action")
				http.Error(w, "Request denied", http.StatusForbidden)
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

// findMatchingRule extracts the ONTAP path and finds the matching rule
// Returns: (rule, matchedPath, found)
func findMatchingRule(requestPath string, logger log.Logger) (actions.Rule, string, bool) {
	path := extractOntapPath(requestPath)
	if path == "" {
		logger.Error("Could not extract ONTAP path", "path", requestPath)
		return actions.Rule{}, "", false
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
		logger.Info("No rule found for path, passing to next middleware", "path", path)
		return actions.Rule{}, "", false
	}

	return matchedRule, matchedPath, true
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

	normalizedPath := normalizeUUIDs(ontapPath)

	return normalizedPath
}

// normalizeUUIDs replaces UUID-like patterns in the path with {uuid} placeholders
func normalizeUUIDs(path string) string {
	path = uuidPattern.ReplaceAllString(path, "/{uuid}")

	return path
}
