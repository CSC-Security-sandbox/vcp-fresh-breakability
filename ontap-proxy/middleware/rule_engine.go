package middleware

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/rules"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	extractOntapPathUtil = utils.ExtractOntapPath
)

// uuidPattern is a compiled regex pattern for matching UUIDs in URL paths
var uuidPattern = regexp.MustCompile(`/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}`)

func RuleEngineMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := util.GetLogger(r.Context())

			matchedRule, matchedPath, found := findMatchingRule(r.URL.Path, logger)
			if !found {
				logger.InfoContext(r.Context(), "No rule found for path, passing to next middleware", "path", r.URL.Path)
				next.ServeHTTP(w, r)
				return
			}

			logger.InfoContext(r.Context(), "Found rule for path", "path", matchedPath)

			action := matchedRule.GetAction(r)
			if action == nil {
				logger.WarnContext(r.Context(), "Method not allowed for path", "path", matchedPath, "method", r.Method)
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}

			allowed, err := action.ShouldAllow(r)
			if err != nil {
				logger.ErrorContext(r.Context(), "Validation error", "error", err, "path", matchedPath)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !allowed {
				logger.InfoContext(r.Context(), "Request denied by action", "path", matchedPath, "method", r.Method)
				http.Error(w, "Request denied", http.StatusForbidden)
				return
			}

			if err := action.ProcessRequest(r, w); err != nil {
				logger.ErrorContext(r.Context(), "Error processing request", "error", err, "path", matchedPath)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}

			ctx := context.WithValue(r.Context(), models.RuleContextKey, action)
			r = r.WithContext(ctx)

			logger.DebugContext(r.Context(), "Request processed successfully, forwarding to next middleware", "path", matchedPath)
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

	if rule, ok := proxyRules[path]; ok {
		return rule, path, true
	}

	for rulePath, rule := range proxyRules {
		if strings.HasSuffix(rulePath, "/*") {
			prefix := strings.TrimSuffix(rulePath, "/*")
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				return rule, rulePath, true
			}
		}
	}

	logger.Debug("No rule found for path", "path", path)
	return actions.Rule{}, "", false
}

func extractOntapPath(fullPath string) string {
	ontapPath := extractOntapPathUtil(fullPath)
	if ontapPath == "" {
		return ""
	}

	normalizedPath := normalizeUUIDs(ontapPath)
	return normalizedPath
}

// normalizeUUIDs replaces UUID-like patterns in the path with {uuid} placeholders
func normalizeUUIDs(path string) string {
	path = uuidPattern.ReplaceAllString(path, "/{uuid}")

	return path
}
