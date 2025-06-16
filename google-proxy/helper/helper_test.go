package helper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
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

	t.Run("With job (async)", func(t *testing.T) {
		ctx := context.Background()
		labeler := &gcpserver.Labeler{}
		ctx = context.WithValue(ctx, labelerTestContextKey{}, labeler)
		job := &models.Job{
			Type:       "CREATE_POOL",
			State:      "DONE",
			TrackingID: 42,
		}

		AddLabelerAttributes(ctx, "12345", "us-central1", job)
		attrSet := labeler.AttributeSet()

		jobType, _ := attrSet.Value("Job_Type")
		jobState, _ := attrSet.Value("Job_State")
		jobTrackingID, _ := attrSet.Value("Job_TrackingID")
		assert.EqualValues(t, "CREATE_POOL", jobType.AsString())
		assert.EqualValues(t, "DONE", jobState.AsString())
		assert.EqualValues(t, 42, jobTrackingID.AsInt64())
	})

	t.Run("Without job (sync)", func(t *testing.T) {
		ctx := context.Background()
		labeler := &gcpserver.Labeler{}
		ctx = context.WithValue(ctx, labelerTestContextKey{}, labeler)

		AddLabelerAttributes(ctx, "67890", "europe-west1", nil)
		attrSet := labeler.AttributeSet()

		jobType, _ := attrSet.Value("Job_Type")
		jobState, _ := attrSet.Value("Job_State")
		jobTrackingID, _ := attrSet.Value("Job_TrackingID")
		assert.EqualValues(t, "", jobType.AsString())
		assert.EqualValues(t, "", jobState.AsString())
		assert.EqualValues(t, 0, jobTrackingID.AsInt64())
	})
}
