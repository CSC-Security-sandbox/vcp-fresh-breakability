package middleware

import (
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// queryParamRenames maps old query parameter names to new names.
// These transformations are applied to all incoming requests before forwarding to ONTAP.
var queryParamRenames = map[string]string{
	"ontap_fields": "fields",
}

// QueryTransformMiddleware creates a middleware that transforms query parameters
// before forwarding requests to ONTAP. This allows the proxy to accept alternative
// parameter names and translate them to ONTAP's expected format.
func QueryTransformMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger := util.GetLogger(r.Context())

			transformed := renameQueryParams(r)
			if transformed {
				logger.DebugContext(r.Context(), "Query parameters transformed",
					"path", r.URL.Path,
					"new_query", r.URL.RawQuery)
			}

			next.ServeHTTP(w, r)
		})
	}
}

// renameQueryParams renames query parameters based on the queryParamRenames map.
// If both old and new parameter names exist, the old parameter values override the new ones.
// Returns true if any parameters were renamed.
func renameQueryParams(r *http.Request) bool {
	query := r.URL.Query()
	transformed := false

	for oldName, newName := range queryParamRenames {
		if values, exists := query[oldName]; exists {
			// Delete existing new param values (old param overrides)
			query.Del(newName)
			// Move all values from old param to new param
			for _, v := range values {
				query.Add(newName, v)
			}
			// Remove the old param
			query.Del(oldName)
			transformed = true
		}
	}

	if transformed {
		r.URL.RawQuery = query.Encode()
	}
	return transformed
}
