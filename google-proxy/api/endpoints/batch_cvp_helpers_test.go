package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	cvpBatch "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/batch"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestBatchListFieldStrings(t *testing.T) {
	t.Run("WhenBatchListPoolFieldsSliceIsNilReturnsNil", func(tt *testing.T) {
		var poolFields []gcpgenserver.V1betaBatchListPoolsFieldsItem
		assert.Nil(tt, batchListFieldStrings(poolFields))
	})

	t.Run("WhenBatchListPoolFieldsSliceIsEmptyReturnsNil", func(tt *testing.T) {
		assert.Nil(tt, batchListFieldStrings([]gcpgenserver.V1betaBatchListPoolsFieldsItem{}))
	})

	t.Run("WhenBatchListPoolFieldsConvertsToStrings", func(tt *testing.T) {
		in := []gcpgenserver.V1betaBatchListPoolsFieldsItem{
			gcpgenserver.V1betaBatchListPoolsFieldsItemResourceId,
			gcpgenserver.V1betaBatchListPoolsFieldsItemSizeInBytes,
		}
		got := batchListFieldStrings(in)
		assert.Equal(tt, []string{"resourceId", "sizeInBytes"}, got)
	})

	t.Run("WhenBatchListSnapshotFieldsConvertsToStrings", func(tt *testing.T) {
		in := []gcpgenserver.V1betaBatchListSnapshotsFieldsItem{
			gcpgenserver.V1betaBatchListSnapshotsFieldsItemResourceId,
			gcpgenserver.V1betaBatchListSnapshotsFieldsItemCreated,
		}
		got := batchListFieldStrings(in)
		assert.Equal(tt, []string{"resourceId", "created"}, got)
	})

	t.Run("WhenBatchListReplicationFieldsConvertsToStrings", func(tt *testing.T) {
		in := []gcpgenserver.V1betaBatchListReplicationsFieldsItem{
			gcpgenserver.V1betaBatchListReplicationsFieldsItemResourceId,
			gcpgenserver.V1betaBatchListReplicationsFieldsItemCreated,
		}
		got := batchListFieldStrings(in)
		assert.Equal(tt, []string{"resourceId", "created"}, got)
	})
}

func TestApplyBatchCvpListCommonParams(t *testing.T) {
	t.Run("WhenApplySnapshotsParamsWithCorrelationSetsLocationFieldsAndCorrelationID", func(tt *testing.T) {
		p := cvpBatch.NewV1betaBatchListSnapshotsParamsWithContext(context.Background())
		corr := "corr-xyz"
		applyBatchCvpListCommonParams(
			p,
			"us-east4",
			[]string{"resourceId", "created"},
			gcpgenserver.NewOptString(corr),
		)
		assert.Equal(tt, "us-east4", p.LocationID)
		assert.Equal(tt, []string{"resourceId", "created"}, p.Fields)
		require.NotNil(tt, p.XCorrelationID)
		assert.Equal(tt, corr, *p.XCorrelationID)
	})

	t.Run("WhenApplyReplicationsParamsWithCorrelationSetsLocationFieldsAndCorrelationID", func(tt *testing.T) {
		p := cvpBatch.NewV1betaBatchListReplicationsParamsWithContext(context.Background())
		corr := "corr-repl"
		applyBatchCvpListCommonParams(
			p,
			"us-east4",
			[]string{"resourceId", "created"},
			gcpgenserver.NewOptString(corr),
		)
		assert.Equal(tt, "us-east4", p.LocationID)
		assert.Equal(tt, []string{"resourceId", "created"}, p.Fields)
		require.NotNil(tt, p.XCorrelationID)
		assert.Equal(tt, corr, *p.XCorrelationID)
	})

	t.Run("WhenApplyPoolsParamsWithCorrelationSetsLocationFieldsAndCorrelationID", func(tt *testing.T) {
		p := cvpBatch.NewV1betaBatchListPoolsParamsWithContext(context.Background())
		corr := "corr-pool"
		applyBatchCvpListCommonParams(
			p,
			"us-west2",
			[]string{"resourceId"},
			gcpgenserver.NewOptString(corr),
		)
		assert.Equal(tt, "us-west2", p.LocationID)
		assert.Equal(tt, []string{"resourceId"}, p.Fields)
		require.NotNil(tt, p.XCorrelationID)
		assert.Equal(tt, corr, *p.XCorrelationID)
	})

	t.Run("WhenApplySnapshotsParamsWithEmptyFieldListLeavesFieldsAndCorrelationNil", func(tt *testing.T) {
		p := cvpBatch.NewV1betaBatchListSnapshotsParamsWithContext(context.Background())
		applyBatchCvpListCommonParams(p, "loc-1", nil, gcpgenserver.OptString{})
		assert.Equal(tt, "loc-1", p.LocationID)
		assert.Nil(tt, p.Fields)
		assert.Nil(tt, p.XCorrelationID)
	})

	t.Run("WhenApplyReplicationsParamsWithEmptyFieldListLeavesFieldsAndCorrelationNil", func(tt *testing.T) {
		p := cvpBatch.NewV1betaBatchListReplicationsParamsWithContext(context.Background())
		applyBatchCvpListCommonParams(p, "loc-1", nil, gcpgenserver.OptString{})
		assert.Equal(tt, "loc-1", p.LocationID)
		assert.Nil(tt, p.Fields)
		assert.Nil(tt, p.XCorrelationID)
	})

	t.Run("WhenApplyPoolsParamsWithUnsetCorrelationLeavesXCorrelationIDNil", func(tt *testing.T) {
		p := cvpBatch.NewV1betaBatchListPoolsParamsWithContext(context.Background())
		applyBatchCvpListCommonParams(
			p,
			"eu-west1",
			[]string{"storagePoolState"},
			gcpgenserver.OptString{},
		)
		assert.Equal(tt, "eu-west1", p.LocationID)
		assert.Equal(tt, []string{"storagePoolState"}, p.Fields)
		assert.Nil(tt, p.XCorrelationID)
	})
}

func TestCvpClientFromContext(t *testing.T) {
	var gotLogger log.Logger
	var gotJWT string
	orig := createClient
	createClient = func(logger log.Logger, jwt string) cvpapi.Cvp {
		gotLogger = logger
		gotJWT = jwt
		return cvpapi.Cvp{}
	}
	defer func() { createClient = orig }()

	logger := log.NewLogger()
	ctx := context.WithValue(context.Background(), utilsmiddleware.ContextSLoggerKey, logger)
	ctx = context.WithValue(ctx, utilsmiddleware.HeaderContextKey, http.Header{
		"Authorization": []string{"Bearer unit-test-jwt"},
	})

	client := cvpClientFromContext(ctx)
	_ = client

	assert.Same(t, logger, gotLogger)
	assert.Equal(t, "Bearer unit-test-jwt", gotJWT)
}
