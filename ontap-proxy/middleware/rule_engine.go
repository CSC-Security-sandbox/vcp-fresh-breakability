package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	rules "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/rest"
	ontapproxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	extractOntapPath = ontapproxyutils.ExtractOntapPath
)

// RuleEngineMiddleware creates a middleware that applies DSL-based rules to requests.
// It matches the request path to rules, validates access, and attaches the action to context
// for response processing.
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
				ontapproxyutils.WriteErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
				return
			}

			// Parse request body once for POST/PATCH/PUT methods
			// This caches the parsed body in context for all validation conditions
			r = dsl.ParseRequestBody(r)

			// Resolve action once - evaluates conditions and returns the leaf action
			// This avoids re-evaluating conditions in ProcessRequest/ProcessResponse
			resolvedAction, allowed, reason := dsl.ResolveAction(action, r)
			if !allowed {
				logger.InfoContext(r.Context(), "Request denied by action", "path", matchedPath, "method", r.Method, "reason", reason)
				ontapproxyutils.WriteErrorResponse(w, http.StatusBadRequest, reason)
				return
			}

			// Process request modifications using the resolved action
			actionName, err := resolvedAction.ProcessRequest(r, w)
			if err != nil {
				logger.ErrorContext(r.Context(), "Error processing request", "error", err, "action", actionName, "path", matchedPath)
				ontapproxyutils.WriteErrorResponse(w, http.StatusInternalServerError, "Internal server error")
				return
			}

			// Attach resolved action to context for response processing
			// This stores the leaf action (e.g., Allow), not the When wrapper
			ctx := context.WithValue(r.Context(), models.RuleContextKey, resolvedAction)
			r = r.WithContext(ctx)

			logger.DebugContext(r.Context(), "Request processed successfully, forwarding to next middleware", "action", actionName, "path", matchedPath)
			next.ServeHTTP(w, r)
		})
	}
}

// findMatchingRule extracts the ONTAP path (with UUIDs normalized) and finds the matching rule
// Returns: (rule, matchedPath, found)
func findMatchingRule(requestPath string, logger log.Logger) (dsl.Rule, string, bool) {
	path := extractOntapPath(requestPath)
	if path == "" {
		logger.Error("Could not extract ONTAP path", "path", requestPath)
		return dsl.Rule{}, "", false
	}

	proxyRules := rules.GetProxyRules()

	// Normalize trailing slash so "api/storage/flexcache/flexcaches/" matches the rule for "api/storage/flexcache/flexcaches"
	path = strings.TrimSuffix(path, "/")

	// Try exact match first
	if rule, ok := proxyRules[path]; ok {
		return rule, path, true
	}

	// Try wildcard match
	for rulePath, rule := range proxyRules {
		if strings.HasSuffix(rulePath, "/*") {
			prefix := strings.TrimSuffix(rulePath, "/*")
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				return rule, rulePath, true
			}
		}
	}

	logger.Debug("No rule found for path", "path", path)
	return dsl.Rule{}, "", false
}

