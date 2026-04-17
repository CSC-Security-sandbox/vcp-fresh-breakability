package middleware

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	ontapproxyutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type PathParamRule struct {
	RegexPattern        string
	MinLength           int
	MaxLength           int
	InvalidCharsPattern *regexp.Regexp
	Required            bool
}

var (
	localRegion = env.GetString("LOCAL_REGION", "")

	regexMap = map[string]*regexp.Regexp{
		`^[1-9][0-9]{0,18}$`: regexp.MustCompile(`^[1-9][0-9]{0,18}$`),
		`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`: regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`),
	}

	pathParamRules = map[string]PathParamRule{
		"projectId": {
			RegexPattern: `^[1-9][0-9]{0,18}$`,
			MinLength:    0,
			MaxLength:    19,
			Required:     true,
		},
		"locationId": {
			RegexPattern:        "",
			MinLength:           0,
			MaxLength:           255,
			InvalidCharsPattern: regexp.MustCompile(`[^a-zA-Z0-9\-_]`),
			Required:            true,
		},
		"poolId": {
			RegexPattern: `^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`,
			MinLength:    36,
			MaxLength:    36,
			Required:     true,
		},
	}

	pathParamsExtractPattern = regexp.MustCompile(`/v1beta/projects/([^/]+)/locations/([^/]+)/pools/([^/]+)`)

	pathTraversalCombinedPattern = regexp.MustCompile(`(?i)(\.\./|\.\.\\|\.\.%2f|\.\.%5c|%2e%2e%2f|%2e%2e%5c)`)

	// Allowlist regexes (OWASP: define allowed characters; reject everything else)
	// ontapPathValidStructure: path segments separated by /; each segment is either exactly *;
	// a single trailing / is allowed (ONTAP and clients often use directory-style URLs).
	ontapPathValidStructure    = regexp.MustCompile(`^/?(\*|[a-zA-Z0-9\-_.]+)(/(\*|[a-zA-Z0-9\-_.]+))*/?$`)
	queryParamNameAllowedChars = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)
	// Backslash allowed for ONTAP APIs that use Windows-style domain\group in query params (e.g. CIFS local-groups).
	// Also allow decoded quote and ampersand for common filter/query patterns.
	queryParamValueAllowedChars = regexp.MustCompile(`^[a-zA-Z0-9\-_.,;:/*><=!@+% |\\"&]+$`)

	// blockedQueryParams contains query parameter names that are not allowed
	blockedQueryParams = map[string]string{
		"privilege_level": "privilege_level query parameter is not allowed",
	}
)

func URLValidationMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := util.GetLogger(r.Context())

			if err := validatePathParams(r); err != nil {
				logger.WarnContext(r.Context(), "Security validation failed for path params",
					"error", err,
					"path", r.URL.Path,
					"method", r.Method)
				ontapproxyutils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
				return
			}

			if err := validateQueryParams(r); err != nil {
				logger.WarnContext(r.Context(), "Security validation failed for query params",
					"error", err,
					"path", r.URL.Path,
					"method", r.Method,
					"query", r.URL.RawQuery)
				ontapproxyutils.WriteErrorResponse(w, http.StatusBadRequest, err.Error())
				return
			}

			logger.DebugContext(r.Context(), "Security validation passed",
				"path", r.URL.Path,
				"method", r.Method)
			next.ServeHTTP(w, r)
		})
	}
}

func validatePathParams(r *http.Request) error {
	pathParams := extractPathParams(r)

	for paramName, rule := range pathParamRules {
		value := pathParams[paramName]
		if err := validatePathParamWithRule(paramName, value, rule); err != nil {
			return err
		}
	}

	locationId := pathParams["locationId"]
	if !isLocationIdValid(locationId) {
		return &URLValidationError{
			Type:    "INVALID_LOCATION",
			Context: "locationId",
			Pattern: fmt.Sprintf("locationId '%s' is invalid", locationId),
		}
	}

	ontapPath := chi.URLParam(r, "*")
	if ontapPath == "" {
		path := r.URL.Path
		ontapPrefix := "/ontap/"
		if idx := strings.Index(path, ontapPrefix); idx != -1 {
			ontapPath = path[idx+len(ontapPrefix):]
		}
	}
	if ontapPath != "" {
		if err := validateOntapPath(ontapPath); err != nil {
			return err
		}
	}

	return nil
}

func extractPathParams(r *http.Request) map[string]string {
	params := make(map[string]string)
	extracted := extractPathParamsFromURL(r.URL.Path)

	if extracted.projectId != "" {
		params["projectId"] = extracted.projectId
	}
	if extracted.locationId != "" {
		params["locationId"] = extracted.locationId
	}
	if extracted.poolId != "" {
		params["poolId"] = extracted.poolId
	}

	return params
}

type pathParams struct {
	projectId  string
	locationId string
	poolId     string
}

func extractPathParamsFromURL(path string) pathParams {
	params := pathParams{}
	matches := pathParamsExtractPattern.FindStringSubmatch(path)
	if len(matches) == 4 {
		params.projectId = matches[1]
		params.locationId = matches[2]
		params.poolId = matches[3]
	}
	return params
}

func validatePathParamWithRule(paramName, value string, rule PathParamRule) error {
	if rule.Required && value == "" {
		return &URLValidationError{
			Type:    "INVALID_FORMAT",
			Context: paramName,
			Pattern: paramName + " is required",
		}
	}

	if value == "" {
		return nil
	}

	if strings.Contains(value, "\x00") {
		return &URLValidationError{
			Type:    "NULL_BYTE",
			Context: paramName,
			Pattern: "null byte",
		}
	}

	// Check for path traversal using combined pattern (faster than iterating)
	if pathTraversalCombinedPattern.MatchString(value) {
		return &URLValidationError{
			Type:    "PATH_TRAVERSAL",
			Context: paramName,
			Pattern: "path traversal",
		}
	}

	if rule.MinLength > 0 && len(value) < rule.MinLength {
		return &URLValidationError{
			Type:    "INVALID_FORMAT",
			Context: paramName,
			Pattern: paramName + " must be at least " + fmt.Sprintf("%d", rule.MinLength) + " characters",
		}
	}
	if rule.MaxLength > 0 && len(value) > rule.MaxLength {
		return &URLValidationError{
			Type:    "INVALID_FORMAT",
			Context: paramName,
			Pattern: paramName + " must be " + fmt.Sprintf("%d", rule.MaxLength) + " characters or less",
		}
	}

	if rule.RegexPattern != "" {
		pattern, ok := regexMap[rule.RegexPattern]
		if !ok {
			pattern = regexp.MustCompile(rule.RegexPattern)
			regexMap[rule.RegexPattern] = pattern
		}
		if !pattern.MatchString(value) {
			return &URLValidationError{
				Type:    "INVALID_FORMAT",
				Context: paramName,
				Pattern: paramName + " format is invalid",
			}
		}
	}

	if rule.InvalidCharsPattern != nil {
		if rule.InvalidCharsPattern.MatchString(value) {
			return &URLValidationError{
				Type:    "INVALID_FORMAT",
				Context: paramName,
				Pattern: paramName + " contains invalid characters",
			}
		}
	}

	return nil
}

func validateOntapPath(value string) error {
	if value == "" {
		return nil
	}

	if !utf8.ValidString(value) {
		return &URLValidationError{
			Type:    "INVALID_ENCODING",
			Context: "ONTAP path",
			Pattern: "invalid UTF-8",
		}
	}

	if strings.Contains(value, "\x00") {
		return &URLValidationError{
			Type:    "NULL_BYTE",
			Context: "ONTAP path",
			Pattern: "null byte",
		}
	}

	if pathTraversalCombinedPattern.MatchString(value) {
		return &URLValidationError{
			Type:    "PATH_TRAVERSAL",
			Context: "ONTAP path",
			Pattern: "path traversal",
		}
	}

	if !ontapPathValidStructure.MatchString(value) {
		return &URLValidationError{
			Type:    "INVALID_CHARS",
			Context: "ONTAP path",
			Pattern: "contains disallowed characters or * must be a full path segment",
		}
	}

	return nil
}

func validateQueryParams(r *http.Request) error {
	for key, values := range r.URL.Query() {
		if reason, blocked := blockedQueryParams[key]; blocked {
			return &URLValidationError{
				Type:    "BLOCKED_PARAM",
				Context: "query parameter",
				Pattern: reason,
			}
		}

		if err := validateQueryParamName(key); err != nil {
			return err
		}

		for _, value := range values {
			if err := validateQueryParamValue(value); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateQueryParamName(name string) error {
	if name == "" {
		return nil
	}

	if !utf8.ValidString(name) {
		return &URLValidationError{
			Type:    "INVALID_ENCODING",
			Context: "query parameter name",
			Pattern: "invalid UTF-8",
		}
	}

	if strings.Contains(name, "\x00") {
		return &URLValidationError{
			Type:    "NULL_BYTE",
			Context: "query parameter name",
			Pattern: "null byte",
		}
	}

	if !queryParamNameAllowedChars.MatchString(name) {
		return &URLValidationError{
			Type:    "INVALID_CHARS",
			Context: "query parameter name",
			Pattern: "contains disallowed characters",
		}
	}

	return nil
}

func validateQueryParamValue(value string) error {
	if value == "" {
		return nil
	}

	if !utf8.ValidString(value) {
		return &URLValidationError{
			Type:    "INVALID_ENCODING",
			Context: "query parameter",
			Pattern: "invalid UTF-8",
		}
	}

	if strings.Contains(value, "\x00") {
		return &URLValidationError{
			Type:    "NULL_BYTE",
			Context: "query parameter",
			Pattern: "null byte",
		}
	}

	if pathTraversalCombinedPattern.MatchString(value) {
		return &URLValidationError{
			Type:    "PATH_TRAVERSAL",
			Context: "query parameter",
			Pattern: "path traversal",
		}
	}

	if !queryParamValueAllowedChars.MatchString(value) {
		return &URLValidationError{
			Type:    "INVALID_CHARS",
			Context: "query parameter",
			Pattern: "contains disallowed characters",
		}
	}

	return nil
}

type URLValidationError struct {
	Type    string
	Context string
	Pattern string
}

func (e *URLValidationError) Error() string {
	if e.Type == "BLOCKED_PARAM" {
		return e.Pattern
	}
	return "security validation failed: " + e.Type + " detected in " + e.Context
}

func isLocationIdValid(locationId string) bool {
	if localRegion == "" {
		return false
	}

	if locationId == localRegion {
		return true
	}

	if strings.HasPrefix(locationId, localRegion+"-") {
		return true
	}

	return false
}
