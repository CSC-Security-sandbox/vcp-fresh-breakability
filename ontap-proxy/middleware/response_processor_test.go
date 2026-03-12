package middleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

func TestProcessResponseModification(t *testing.T) {
	t.Run("WhenResponseIsNil_ShouldReturnNil", func(t *testing.T) {
		err := ProcessResponseModification(nil)
		assert.NoError(t, err)
	})

	t.Run("WhenRequestIsNil_ShouldReturnNil", func(t *testing.T) {
		resp := &http.Response{}
		err := ProcessResponseModification(resp)
		assert.NoError(t, err)
	})

	t.Run("WhenNoRuleContextInRequest_ShouldReturnNil", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(bytes.NewBufferString(`{"name": "test"}`)),
		}

		err := ProcessResponseModification(resp)

		assert.NoError(t, err)
	})

	t.Run("WhenContextHasInvalidType_ShouldReturnNil", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		ctx := context.WithValue(context.Background(), models.RuleContextKey, "invalid")
		req = req.WithContext(ctx)
		resp := &http.Response{
			Request: req,
			Body:    io.NopCloser(bytes.NewBufferString(`{"name": "test"}`)),
		}

		err := ProcessResponseModification(resp)

		assert.NoError(t, err)
	})

	t.Run("WhenContextHasValidAction_ShouldProcessResponse", func(t *testing.T) {
		action := dsl.Allow{
			Name: "Test Action",
			ModifyResponse: dsl.RemoveFields{
				Fields: []string{"$.sensitive"},
			},
		}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		ctx := context.WithValue(context.Background(), models.RuleContextKey, action)
		req = req.WithContext(ctx)

		resp := &http.Response{
			StatusCode: 200,
			Request:    req,
			Body:       io.NopCloser(bytes.NewBufferString(`{"name": "test", "sensitive": "secret"}`)),
			Header:     make(http.Header),
		}

		err := ProcessResponseModification(resp)

		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		assert.NotContains(t, string(body), "sensitive")
	})

	t.Run("WhenActionProcessResponseSucceeds_ShouldReturnNil", func(t *testing.T) {
		action := dsl.Allow{
			Name: "Test Action",
		}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		ctx := context.WithValue(context.Background(), models.RuleContextKey, action)
		req = req.WithContext(ctx)

		resp := &http.Response{
			StatusCode: 200,
			Request:    req,
			Body:       io.NopCloser(bytes.NewBufferString(`{"name": "test"}`)),
			Header:     make(http.Header),
		}

		err := ProcessResponseModification(resp)

		assert.NoError(t, err)
	})
}

func TestProcessResponseAndRecordBackendMetrics(t *testing.T) {
	t.Run("WhenResponseIsNil_ShouldReturnNil", func(t *testing.T) {
		err := ProcessResponseAndRecordBackendMetrics(nil)
		assert.NoError(t, err)
	})

	t.Run("WhenRequestIsNil_ShouldReturnNil", func(t *testing.T) {
		resp := &http.Response{
			StatusCode: 200,
			Request:    nil,
			Body:       io.NopCloser(bytes.NewBufferString("")),
		}
		err := ProcessResponseAndRecordBackendMetrics(resp)
		assert.NoError(t, err)
	})
}
