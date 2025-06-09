package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func TestV1betaDescribeOperation_BadRequest(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "invalid-location",
		OperationId:   "op-123",
	}

	// Simulate invalid location to trigger BadRequest
	result, err := handler.V1betaDescribeOperation(context.Background(), params)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	badRequest, ok := result.(*gcpgenserver.V1betaDescribeOperationBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
}

func TestV1betaDescribeOperation_LabelerMissing(t *testing.T) {
	handler := Handler{}
	params := gcpgenserver.V1betaDescribeOperationParams{
		ProjectNumber: "test-project",
		LocationId:    "valid-location",
		OperationId:   "op-123",
	}
	// No labeler in context
	_, err := handler.V1betaDescribeOperation(context.Background(), params)
	assert.NoError(t, err)
	// Further assertions as needed
}
