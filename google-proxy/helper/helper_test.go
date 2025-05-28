package helper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestAddLabelerAttributesWithMockedLabeler(t *testing.T) {
	type labelerTestContextKey struct{}

	labelerFromContextTest := func(ctx context.Context) (*gcpserver.Labeler, bool) {
		if l, ok := ctx.Value(labelerTestContextKey{}).(*gcpserver.Labeler); ok {
			return l, true
		}
		return &gcpserver.Labeler{}, false
	}

	originalLabelerFromContext := gcpgenserverLabelerFromContext
	gcpgenserverLabelerFromContext = labelerFromContextTest
	defer func() { gcpgenserverLabelerFromContext = originalLabelerFromContext }()

	t.Run("Valid context with loggerFields and traceURL", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, log.Fields{
			"traceURL": "https://netapp.com/trace",
		})
		labeler := &gcpserver.Labeler{}
		ctx = context.WithValue(ctx, labelerTestContextKey{}, labeler)

		AddLabelerAttributes(ctx, "12345", "us-central1")
		labeler, _ = labelerFromContextTest(ctx)
		attrSet := labeler.AttributeSet()

		httpRouteValue, _ := attrSet.Value("http.route")
		locationvalue, _ := attrSet.Value("locationID")
		projectNumber, _ := attrSet.Value("projectNumber")
		assert.EqualValues(t, "https://netapp.com/trace", httpRouteValue.AsString())
		assert.EqualValues(t, "us-central1", locationvalue.AsString())
		assert.EqualValues(t, "12345", projectNumber.AsString())
	})

	t.Run("Valid context without loggerFields", func(t *testing.T) {
		ctx := context.Background()
		labeler := &gcpserver.Labeler{}
		ctx = context.WithValue(ctx, labelerTestContextKey{}, labeler)

		AddLabelerAttributes(ctx, "67890", "europe-west1")
		labeler, _ = labelerFromContextTest(ctx)
		attrSet := labeler.AttributeSet()

		httpRouteValue, _ := attrSet.Value("http.route")
		locationvalue, _ := attrSet.Value("locationID")
		projectNumber, _ := attrSet.Value("projectNumber")
		assert.EqualValues(t, "", httpRouteValue.AsString())
		assert.EqualValues(t, "europe-west1", locationvalue.AsString())
		assert.EqualValues(t, "67890", projectNumber.AsString())
	})
}
