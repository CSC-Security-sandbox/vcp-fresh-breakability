package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	cvpBatch "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/batch"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func stubParseRegionAndZoneForBatchReplications() func() {
	orig := parseAndValidateRegionAndZone
	parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
		if locationID == "invalid location!" {
			return "", "", &gcpgenserver.Error{Code: 400, Message: "Invalid location"}
		}
		return locationID, "", nil
	}
	return func() { parseAndValidateRegionAndZone = orig }
}

func authContextForBatchReplication() context.Context {
	return context.WithValue(context.Background(), utilsmiddleware.HeaderContextKey, http.Header{})
}

func batchReplAuthContextWithLogger() context.Context {
	ctx := authContextForBatchReplication()
	return context.WithValue(ctx, utilsmiddleware.ContextSLoggerKey, log.NewLogger())
}

func validBatchReplicationURI() string {
	return "projects/p1/locations/us-east4/volumes/v1/replications/r1"
}

func replicationURI(project, location, volume, replication string) string {
	return fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s", project, location, volume, replication)
}

func stubFetchBatchReplicationsFromCVP(repls []gcpgenserver.BatchReplicationV1beta, err error) func() {
	orig := fetchBatchReplicationsFromCVPFn
	fetchBatchReplicationsFromCVPFn = func(_ context.Context, _ gcpgenserver.V1betaBatchListReplicationsParams, _ []string, _ map[string]bool) ([]gcpgenserver.BatchReplicationV1beta, error) {
		return repls, err
	}
	return func() { fetchBatchReplicationsFromCVPFn = orig }
}

func TestV1betaBatchListReplications_Validation(t *testing.T) {
	restoreRegion := stubParseRegionAndZoneForBatchReplications()
	defer restoreRegion()

	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := &Handler{Orchestrator: mockOrch}
	params := gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "us-east4"}

	t.Run("WhenReplicationURIListIsEmptyReturnsBadRequest", func(tt *testing.T) {
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{}, params)
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Equal(tt, "replicationUris is required and must have at least 1 item", badReq.Message)
	})

	t.Run("WhenRequestBodyIsNilReturnsBadRequest", func(tt *testing.T) {
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), nil, params)
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Equal(tt, "replicationUris is required and must have at least 1 item", badReq.Message)
	})

	t.Run("WhenDuplicateReplicationURIsEachCountTowardMaxBatchSize", func(tt *testing.T) {
		uri := replicationURI("p1", "us-east4", "v0", "r0")
		uris := make([]string, env.MaxBatchReplicationURIs+1)
		for i := range uris {
			uris[i] = uri
		}
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: uris,
		}, params)
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Contains(tt, badReq.Message, "at most")
	})

	t.Run("WhenLocationIdInvalidReturnsBadRequest", func(tt *testing.T) {
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{validBatchReplicationURI()},
		}, gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "invalid location!"})
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Equal(tt, float64(http.StatusBadRequest), badReq.Code)
	})

	t.Run("WhenReplicationURIListExceedsMaxReturnsBadRequest", func(tt *testing.T) {
		uris := make([]string, env.MaxBatchReplicationURIs+1)
		for i := range uris {
			uris[i] = replicationURI("p1", "us-east4", fmt.Sprintf("v%d", i), fmt.Sprintf("r%d", i))
		}
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: uris,
		}, params)
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Contains(tt, badReq.Message, "at most")
	})

	t.Run("WhenReplicationURIIsNotCCFEFormatReturnsBadRequest", func(tt *testing.T) {
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"not-a-valid-uri"},
		}, params)
		require.NoError(tt, err)
		_, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		assert.True(tt, ok)
	})

	t.Run("WhenReplicationURILocationDoesNotMatchParamReturnsBadRequest", func(tt *testing.T) {
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/p1/locations/other-region/volumes/v1/replications/r1"},
		}, params)
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Contains(tt, badReq.Message, "locationId")
	})

	t.Run("WhenReplicationURIPathMissingReplicationSegmentReturnsBadRequest", func(tt *testing.T) {
		res, err := handler.V1betaBatchListReplications(authContextForBatchReplication(), &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/p1/locations/us-east4/volumes/only"},
		}, params)
		require.NoError(tt, err)
		badReq, ok := res.(*gcpgenserver.V1betaBatchListReplicationsBadRequest)
		require.True(tt, ok)
		assert.Contains(tt, badReq.Message, "replicationURIs should match")
	})
}

func TestV1betaBatchListReplications_VCPOnlyIncludesAdditionalFields(t *testing.T) {
	restoreRegion := stubParseRegionAndZoneForBatchReplications()
	defer restoreRegion()
	origHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = origHost }()

	sourceName := "projects/p1/locations/us-east4/volumes/source-vol"
	sourceID := "11111111-1111-1111-1111-111111111111"
	destName := "projects/p1/locations/us-east4/volumes/dest-vol"
	destID := "22222222-2222-2222-2222-222222222222"
	replID := "33333333-3333-3333-3333-333333333333"
	resourceID := "replication-1"
	state := "READY"
	stateDetails := "Healthy"
	stateDetailsCode := int32(100001)
	role := "SOURCE"
	sched := "HOURLY"
	mirror := "MIRRORED"
	healthy := true
	cluster := "us-east4"
	hybridType := "MIGRATION"
	description := "desc"
	now := time.Now()
	totalBytes := 123.0
	commands := []string{"cluster peer create"}
	ctx := authContextForBatchReplication()

	mockOrch := factory.NewMockOrchestratorFactory(t)
	mockOrch.EXPECT().GetBatchReplications(
		ctx,
		commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{"projects/p1/locations/us-east4/volumes/v1/replications/r1"},
			LocationId:      "us-east4",
		},
	).Return([]commonparams.ReplicationV1beta{
		{
			ReplicationId:       &replID,
			ResourceId:          &resourceID,
			State:               &state,
			StateDetails:        &stateDetails,
			StateDetailsCode:    &stateDetailsCode,
			Role:                &role,
			ReplicationSchedule: &sched,
			MirrorState:         &mirror,
			Healthy:             &healthy,
			Created:             &now,
			Description:         &description,
			Labels:              map[string]string{"k": "v"},
			Source: &commonparams.ReplicationVolumeInformationV1beta{
				VolumeName: &sourceName,
				VolumeId:   &sourceID,
			},
			Destination: &commonparams.ReplicationVolumeInformationV1beta{
				VolumeName: &destName,
				VolumeId:   &destID,
			},
			TransferStats: &commonparams.TransferStatsV1beta{
				TotalTransferBytes: &totalBytes,
			},
			ClusterLocation:       &cluster,
			HybridReplicationType: &hybridType,
			HybridReplicationUserCommands: &commonparams.HybridReplicationUserCommandsV1beta{
				Commands: commands,
			},
		},
	}, nil)

	handler := &Handler{Orchestrator: mockOrch}
	res, err := handler.V1betaBatchListReplications(ctx, &gcpgenserver.ReplicationURIListV1beta{
		ReplicationUris: []string{"projects/p1/locations/us-east4/volumes/v1/replications/r1"},
	}, gcpgenserver.V1betaBatchListReplicationsParams{
		LocationId: "us-east4",
		Fields: []gcpgenserver.V1betaBatchListReplicationsFieldsItem{
			"created", "resourceId", "state", "stateDetails", "stateDetailsCode", "role",
			"replicationSchedule", "mirrorState", "description", "labels", "source", "destination",
			"healthy", "transferStats", "clusterLocation", "hybridReplicationType", "hybridReplicationUserCommands",
		},
	})
	require.NoError(t, err)
	ok, castOK := res.(*gcpgenserver.V1betaBatchListReplicationsOK)
	require.True(t, castOK)
	require.Len(t, ok.Replications, 1)
	assert.Equal(t, replID, ok.Replications[0].ReplicationId.Value)
	assert.Equal(t, stateDetailsCode, ok.Replications[0].StateDetailsCode.Value)
	require.True(t, ok.Replications[0].Source.Set)
	assert.Equal(t, sourceName, ok.Replications[0].Source.Value.VolumeName.Value)
	assert.Equal(t, sourceID, ok.Replications[0].Source.Value.VolumeId.Value)
	require.True(t, ok.Replications[0].Destination.Set)
	assert.Equal(t, destName, ok.Replications[0].Destination.Value.VolumeName.Value)
	assert.Equal(t, destID, ok.Replications[0].Destination.Value.VolumeId.Value)
	assert.True(t, ok.Replications[0].Healthy.Value)
	assert.Equal(t, totalBytes, ok.Replications[0].TransferStats.Value.TotalTransferBytes.Value)
	assert.Equal(t, commands, ok.Replications[0].HybridReplicationUserCommands.Value.Commands)
}

func TestBuildReplicationFieldSet_UsesRequestedFieldNames(t *testing.T) {
	fieldSet := buildReplicationFieldSet([]gcpgenserver.V1betaBatchListReplicationsFieldsItem{
		"stateDetailsCode",
		"transferStats",
		"clusterLocation",
		"hybridReplicationType",
		"hybridPeeringDetails",
		"hybridReplicationUserCommands",
	})

	assert.True(t, fieldSet["stateDetailsCode"])
	assert.True(t, fieldSet["transferStats"])
	assert.True(t, fieldSet["clusterLocation"])
	assert.True(t, fieldSet["hybridReplicationType"])
	assert.True(t, fieldSet["hybridPeeringDetails"])
	assert.True(t, fieldSet["hybridReplicationUserCommands"])
}

func TestBuildReplicationFieldSet_EmptyReturnsNoProjectedFields(t *testing.T) {
	fieldSet := buildReplicationFieldSet(nil)
	assert.Nil(t, fieldSet)

	fieldSet = buildReplicationFieldSet([]gcpgenserver.V1betaBatchListReplicationsFieldsItem{})
	assert.Nil(t, fieldSet)
}

func TestConvertCommonToBatchReplication_RequestedFieldsAlwaysSet(t *testing.T) {
	fieldSet := map[string]bool{
		"created":                       true,
		"resourceId":                    true,
		"state":                         true,
		"stateDetails":                  true,
		"stateDetailsCode":              true,
		"role":                          true,
		"replicationSchedule":           true,
		"source":                        true,
		"destination":                   true,
		"mirrorState":                   true,
		"description":                   true,
		"labels":                        true,
		"healthy":                       true,
		"transferStats":                 true,
		"clusterLocation":               true,
		"hybridReplicationType":         true,
		"hybridPeeringDetails":          true,
		"hybridReplicationUserCommands": true,
	}

	out := convertCommonToBatchReplication(commonparams.ReplicationV1beta{}, fieldSet)

	assert.True(t, out.Created.Set)
	assert.False(t, out.Created.IsNull())
	assert.True(t, out.ResourceId.Set)
	assert.False(t, out.ResourceId.IsNull())
	assert.Equal(t, "", out.ResourceId.Value)
	assert.True(t, out.State.Set)
	assert.Equal(t, gcpgenserver.BatchReplicationV1betaStateSTATEUNSPECIFIED, out.State.Value)
	assert.True(t, out.StateDetails.Set)
	assert.False(t, out.StateDetails.IsNull())
	assert.True(t, out.StateDetailsCode.Set)
	assert.False(t, out.StateDetailsCode.IsNull())
	assert.Equal(t, int32(0), out.StateDetailsCode.Value)
	assert.True(t, out.Role.Set)
	assert.Equal(t, gcpgenserver.BatchReplicationV1betaRoleSOURCE, out.Role.Value)
	assert.True(t, out.ReplicationSchedule.Set)
	assert.Equal(t, gcpgenserver.BatchReplicationV1betaReplicationScheduleEVERY10MINUTES, out.ReplicationSchedule.Value)
	assert.True(t, out.Source.Set)
	assert.False(t, out.Source.IsNull())
	assert.True(t, out.Destination.Set)
	assert.False(t, out.Destination.IsNull())
	assert.True(t, out.MirrorState.Set)
	assert.Equal(t, gcpgenserver.BatchReplicationV1betaMirrorStateUNINITIALIZED, out.MirrorState.Value)
	assert.True(t, out.Description.Set)
	assert.False(t, out.Description.IsNull())
	assert.True(t, out.Labels.Set)
	assert.False(t, out.Labels.IsNull())
	assert.True(t, out.Healthy.Set)
	assert.False(t, out.Healthy.IsNull())
	assert.False(t, out.Healthy.Value)
	assert.True(t, out.TransferStats.Set)
	assert.False(t, out.TransferStats.IsNull())
	assert.True(t, out.ClusterLocation.Set)
	assert.False(t, out.ClusterLocation.IsNull())
	assert.Equal(t, "", out.ClusterLocation.Value)
	assert.True(t, out.HybridReplicationType.Set)
	assert.Equal(t, gcpgenserver.BatchReplicationV1betaHybridReplicationTypeHYBRIDREPLICATIONTYPEUNSPECIFIED, out.HybridReplicationType.Value)
	assert.True(t, out.HybridPeeringDetails.Set)
	assert.False(t, out.HybridPeeringDetails.IsNull())
	assert.True(t, out.HybridReplicationUserCommands.Set)
	assert.False(t, out.HybridReplicationUserCommands.IsNull())
}

func TestV1betaBatchListReplications_Parallel(t *testing.T) {
	uri := validBatchReplicationURI()
	params := gcpgenserver.V1betaBatchListReplicationsParams{
		LocationId: "us-east4",
		Fields:     []gcpgenserver.V1betaBatchListReplicationsFieldsItem{"resourceId"},
	}

	t.Run("WhenVCPAndCVPBatchFetchSucceedCombinesResults", func(tt *testing.T) {
		restoreRegion := stubParseRegionAndZoneForBatchReplications()
		defer restoreRegion()
		origHost := cvp.CVP_HOST
		cvp.CVP_HOST = "http://cvp"
		defer func() { cvp.CVP_HOST = origHost }()
		restoreFetch := stubFetchBatchReplicationsFromCVP([]gcpgenserver.BatchReplicationV1beta{
			{ReplicationId: gcpgenserver.NewOptNilString("cvp-repl-1"), ResourceId: gcpgenserver.NewOptNilString("cvp-res")},
		}, nil)
		defer restoreFetch()

		replID := "orch-repl"
		resID := "orch-res"
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		mockOrch.EXPECT().GetBatchReplications(mock.Anything, commonparams.GetMultipleReplicationsParams{
			ReplicationURIs: []string{uri},
			LocationId:      "us-east4",
		}).Return([]commonparams.ReplicationV1beta{
			{ReplicationId: &replID, ResourceId: &resID},
		}, nil)

		handler := &Handler{Orchestrator: mockOrch}
		ctx := batchReplAuthContextWithLogger()

		res, err := handler.V1betaBatchListReplications(ctx, &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{uri},
		}, params)
		require.NoError(tt, err)
		okRes, ok := res.(*gcpgenserver.V1betaBatchListReplicationsOK)
		require.True(tt, ok)
		require.Len(tt, okRes.Replications, 2)
		ids := map[string]struct{}{}
		for _, r := range okRes.Replications {
			if r.ResourceId.Set {
				ids[r.ResourceId.Value] = struct{}{}
			}
		}
		assert.Contains(tt, ids, "orch-res")
		assert.Contains(tt, ids, "cvp-res")
	})

	t.Run("WhenVCPBatchFetchFailsAndCVPSucceedsReturnsCVPResults", func(tt *testing.T) {
		restoreRegion := stubParseRegionAndZoneForBatchReplications()
		defer restoreRegion()
		origHost := cvp.CVP_HOST
		cvp.CVP_HOST = "http://cvp"
		defer func() { cvp.CVP_HOST = origHost }()
		restoreFetch := stubFetchBatchReplicationsFromCVP([]gcpgenserver.BatchReplicationV1beta{
			{ReplicationId: gcpgenserver.NewOptNilString("cvp-only"), ResourceId: gcpgenserver.NewOptNilString("cvp-res")},
		}, nil)
		defer restoreFetch()

		mockOrch := factory.NewMockOrchestratorFactory(tt)
		mockOrch.EXPECT().GetBatchReplications(mock.Anything, mock.Anything).
			Return(nil, errors.New("orchestrator error"))

		handler := &Handler{Orchestrator: mockOrch}
		ctx := batchReplAuthContextWithLogger()

		res, err := handler.V1betaBatchListReplications(ctx, &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{uri},
		}, params)
		require.NoError(tt, err)
		okRes, ok := res.(*gcpgenserver.V1betaBatchListReplicationsOK)
		require.True(tt, ok)
		require.Len(tt, okRes.Replications, 1)
		assert.Equal(tt, "cvp-only", okRes.Replications[0].ReplicationId.Value)
	})

	t.Run("WhenVCPBatchFetchSucceedsAndCVPFailsReturnsVCPResults", func(tt *testing.T) {
		restoreRegion := stubParseRegionAndZoneForBatchReplications()
		defer restoreRegion()
		origHost := cvp.CVP_HOST
		cvp.CVP_HOST = "http://cvp"
		defer func() { cvp.CVP_HOST = origHost }()
		restoreFetch := stubFetchBatchReplicationsFromCVP(nil, errors.New("cvp down"))
		defer restoreFetch()

		replID := "orch-repl"
		resID := "orch-res"
		mockOrch := factory.NewMockOrchestratorFactory(tt)
		mockOrch.EXPECT().GetBatchReplications(mock.Anything, mock.Anything).Return([]commonparams.ReplicationV1beta{
			{ReplicationId: &replID, ResourceId: &resID},
		}, nil)

		handler := &Handler{Orchestrator: mockOrch}
		ctx := batchReplAuthContextWithLogger()

		res, err := handler.V1betaBatchListReplications(ctx, &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{uri},
		}, params)
		require.NoError(tt, err)
		okRes, ok := res.(*gcpgenserver.V1betaBatchListReplicationsOK)
		require.True(tt, ok)
		require.Len(tt, okRes.Replications, 1)
		assert.Equal(tt, "orch-res", okRes.Replications[0].ResourceId.Value)
	})

	t.Run("WhenVCPAndCVPBatchFetchBothFailReturns500", func(tt *testing.T) {
		restoreRegion := stubParseRegionAndZoneForBatchReplications()
		defer restoreRegion()
		origHost := cvp.CVP_HOST
		cvp.CVP_HOST = "http://cvp"
		defer func() { cvp.CVP_HOST = origHost }()
		restoreFetch := stubFetchBatchReplicationsFromCVP(nil, errors.New("cvp down"))
		defer restoreFetch()

		mockOrch := factory.NewMockOrchestratorFactory(tt)
		mockOrch.EXPECT().GetBatchReplications(mock.Anything, mock.Anything).
			Return(nil, errors.New("orchestrator down"))

		handler := &Handler{Orchestrator: mockOrch}
		ctx := batchReplAuthContextWithLogger()

		res, err := handler.V1betaBatchListReplications(ctx, &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{uri},
		}, params)
		require.NoError(tt, err)
		_, ok := res.(*gcpgenserver.V1betaBatchListReplicationsInternalServerError)
		assert.True(tt, ok)
	})
}

func TestV1betaBatchListReplications_VCPOnly_InternalError(t *testing.T) {
	restoreRegion := stubParseRegionAndZoneForBatchReplications()
	defer restoreRegion()
	origHost := cvp.CVP_HOST
	cvp.CVP_HOST = ""
	defer func() { cvp.CVP_HOST = origHost }()

	mockOrch := factory.NewMockOrchestratorFactory(t)
	mockOrch.EXPECT().GetBatchReplications(mock.Anything, mock.Anything).
		Return(nil, errors.New("boom"))

	handler := &Handler{Orchestrator: mockOrch}
	ctx := authContextForBatchReplication()
	res, err := handler.V1betaBatchListReplications(ctx, &gcpgenserver.ReplicationURIListV1beta{
		ReplicationUris: []string{validBatchReplicationURI()},
	}, gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "us-east4"})
	require.NoError(t, err)
	_, ok := res.(*gcpgenserver.V1betaBatchListReplicationsInternalServerError)
	require.True(t, ok)
}

func TestGetBatchReplicationsFromVCP_OrchestratorError(t *testing.T) {
	restoreRegion := stubParseRegionAndZoneForBatchReplications()
	defer restoreRegion()

	mockOrch := factory.NewMockOrchestratorFactory(t)
	mockOrch.EXPECT().GetBatchReplications(mock.Anything, mock.Anything).
		Return(nil, errors.New("lookup failed"))

	handler := &Handler{Orchestrator: mockOrch}
	ctx := context.Background()
	_, err := handler.getBatchReplicationsFromVCP(ctx, gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "us-east4"},
		[]string{validBatchReplicationURI()}, nil)
	require.Error(t, err)
}

func TestGetBatchReplicationsFromVCP_PassesXCorrelationID(t *testing.T) {
	restoreRegion := stubParseRegionAndZoneForBatchReplications()
	defer restoreRegion()

	replID := "r1"
	mockOrch := factory.NewMockOrchestratorFactory(t)
	corr := "corr-batch-repl"
	mockOrch.EXPECT().GetBatchReplications(mock.Anything, commonparams.GetMultipleReplicationsParams{
		ReplicationURIs: []string{validBatchReplicationURI()},
		LocationId:      "us-east4",
		XCorrelationID:  corr,
	}).Return([]commonparams.ReplicationV1beta{{ReplicationId: &replID}}, nil)

	handler := &Handler{Orchestrator: mockOrch}
	ctx := context.Background()
	out, err := handler.getBatchReplicationsFromVCP(ctx, gcpgenserver.V1betaBatchListReplicationsParams{
		LocationId:     "us-east4",
		XCorrelationID: gcpgenserver.NewOptString(corr),
	}, []string{validBatchReplicationURI()}, nil)
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestFetchBatchReplicationsFromCVP(t *testing.T) {
	t.Run("WhenCVPClientReturnsErrorPropagatesError", func(tt *testing.T) {
		mockBatch := cvpBatch.NewMockClientService(tt)
		mockBatch.EXPECT().V1betaBatchListReplications(mock.Anything).Return(nil, errors.New("boom"))

		origClient := createClient
		defer func() { createClient = origClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Batch: mockBatch}
		}

		ctx := batchReplAuthContextWithLogger()
		_, err := fetchBatchReplicationsFromCVP(ctx, gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "us-east4"},
			[]string{validBatchReplicationURI()}, map[string]bool{"resourceId": true})
		require.Error(tt, err)
		assert.Contains(tt, err.Error(), "CVP batch list replications failed")
	})

	t.Run("WhenCVPReturnsNilResponseReturnsEmptyList", func(tt *testing.T) {
		mockBatch := cvpBatch.NewMockClientService(tt)
		mockBatch.EXPECT().V1betaBatchListReplications(mock.Anything).Return(nil, nil)

		origClient := createClient
		defer func() { createClient = origClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Batch: mockBatch}
		}

		ctx := batchReplAuthContextWithLogger()
		out, err := fetchBatchReplicationsFromCVP(ctx, gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "us-east4"},
			[]string{validBatchReplicationURI()}, nil)
		require.NoError(tt, err)
		assert.Empty(tt, out)
	})

	t.Run("WhenCVPPayloadHasNilReplicationEntriesSkipsThem", func(tt *testing.T) {
		resID := "rid"
		mockBatch := cvpBatch.NewMockClientService(tt)
		mockBatch.EXPECT().V1betaBatchListReplications(mock.Anything).Return(&cvpBatch.V1betaBatchListReplicationsOK{
			Payload: &cvpBatch.V1betaBatchListReplicationsOKBody{
				Replications: []*cvpmodels.BatchReplicationV1beta{
					nil,
					{ReplicationID: "rid-1", ResourceID: &resID},
				},
			},
		}, nil)

		origClient := createClient
		defer func() { createClient = origClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Batch: mockBatch}
		}

		ctx := batchReplAuthContextWithLogger()
		out, err := fetchBatchReplicationsFromCVP(ctx, gcpgenserver.V1betaBatchListReplicationsParams{LocationId: "us-east4"},
			[]string{validBatchReplicationURI()}, map[string]bool{"resourceId": true})
		require.NoError(tt, err)
		require.Len(tt, out, 1)
		assert.Equal(tt, "rid-1", out[0].ReplicationId.Value)
		assert.Equal(tt, "rid", out[0].ResourceId.Value)
	})
}

func TestConvertCVPBatchReplicationToGCPBatchReplication(t *testing.T) {
	dt := strfmt.DateTime(time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC))
	rid := "res"
	role := "SOURCE"
	sd := "details"
	srcVol := "projects/p1/locations/us-east4/volumes/s1"
	dstVol := "projects/p1/locations/us-east4/volumes/d1"
	p := &cvpmodels.BatchReplicationV1beta{
		ReplicationID:       "repl-uuid-1111-1111-1111-111111111111",
		Created:             &dt,
		ResourceID:          &rid,
		State:               "READY",
		StateDetails:        &sd,
		Role:                &role,
		ReplicationSchedule: "HOURLY",
		MirrorState:         "MIRRORED",
		Description:         &sd,
		Labels:              map[string]string{"k": "v"},
		Source:              &cvpmodels.BatchReplicationVolumeDetailsV1beta{VolumeName: srcVol, VolumeID: "vid-s"},
		Destination:         &cvpmodels.BatchReplicationVolumeDetailsV1beta{VolumeName: dstVol, VolumeID: "vid-d"},
	}
	fs := map[string]bool{
		"created": true, "resourceId": true, "state": true, "stateDetails": true, "role": true,
		"replicationSchedule": true, "mirrorState": true, "description": true, "labels": true,
		"source": true, "destination": true,
	}
	out := convertCVPBatchReplicationToGCPBatchReplication(p, fs)
	assert.True(t, out.Created.Set)
	assert.Equal(t, "res", out.ResourceId.Value)
	require.True(t, out.Source.Set)
	assert.Equal(t, srcVol, out.Source.Value.VolumeName.Value)
	assert.Equal(t, "vid-s", out.Source.Value.VolumeId.Value)

	outNil := convertCVPBatchReplicationToGCPBatchReplication(p, nil)
	assert.False(t, outNil.Created.Set)
	assert.True(t, outNil.ReplicationId.Set)
}

func TestConvertCommonToBatchReplication_DestinationVolumeParametersAndHybrid(t *testing.T) {
	tierAction := "SNAPSHOT_ONLY"
	cool := int32(7)
	hot := true
	tput := 128.0
	iops := int64(4000)
	vpg := "vpg-1"
	spool := "projects/p1/locations/us-east4/storagePools/pool-1"
	desc := "d"
	now := time.Now()
	subnet := "10.0.0.1"
	cmd := "peer create"
	pass := "secret"
	pv := "pv"
	pc := "pc"
	svm := "svm"
	hybrid := &commonparams.HybridPeeringV1beta{
		SubnetIp:          &subnet,
		Command:           &cmd,
		Passphrase:        &pass,
		CommandExpiryTime: &now,
		PeerVolumeName:    &pv,
		PeerClusterName:   &pc,
		PeerSvmName:       &svm,
	}
	repl := commonparams.ReplicationV1beta{
		ReplicationId: strPtr("r1"),
		DestinationVolumeParameters: &commonparams.DestinationVolumeParametersV1beta{
			StoragePool: spool,
			VolumeId:    strPtr("v1"),
			ShareName:   strPtr("share"),
			Description: &desc,
			TieringPolicy: &commonparams.TieringPolicyV1beta{
				TierAction:               &tierAction,
				CoolingThresholdDays:     &cool,
				HotTierBypassModeEnabled: &hot,
			},
			ThroughputMibps:          &tput,
			Iops:                     &iops,
			VolumePerformanceGroupId: &vpg,
		},
		HybridPeeringDetails: hybrid,
	}
	fs := map[string]bool{
		"destinationVolumeParameters": true,
		"hybridPeeringDetails":        true,
	}
	out := convertCommonToBatchReplication(repl, fs)
	assert.True(t, out.DestinationVolumeParameters.Set)
	assert.Equal(t, spool, out.DestinationVolumeParameters.Value.StoragePool)
	assert.True(t, out.DestinationVolumeParameters.Value.TieringPolicy.Set)
	assert.True(t, out.HybridPeeringDetails.Set)
	assert.Equal(t, "10.0.0.1", out.HybridPeeringDetails.Value.SubnetIp.Value)
}

func TestConvertCommonToBatchReplication_DerivedDestinationVolumeParameters(t *testing.T) {
	destVolURI := "projects/p1/locations/us-east4/volumes/vol-x"
	desc := "from-repl"
	repl := commonparams.ReplicationV1beta{
		ReplicationId: strPtr("r1"),
		Description:   &desc,
		Destination: &commonparams.ReplicationVolumeInformationV1beta{
			VolumeName: &destVolURI,
		},
	}
	fs := map[string]bool{"destinationVolumeParameters": true}
	out := convertCommonToBatchReplication(repl, fs)
	require.True(t, out.DestinationVolumeParameters.Set)
	assert.Equal(t, "projects/p1/locations/us-east4/storagePools/-", out.DestinationVolumeParameters.Value.StoragePool)
	assert.Equal(t, "vol-x", out.DestinationVolumeParameters.Value.VolumeId.Value)
	assert.Equal(t, "from-repl", out.DestinationVolumeParameters.Value.Description.Value)
}

func TestParseVolumeURIAndDeriveStoragePool(t *testing.T) {
	p, loc, vol := parseVolumeURI("projects/p1/locations/us-east4/volumes/vol99")
	assert.Equal(t, "p1", p)
	assert.Equal(t, "us-east4", loc)
	assert.Equal(t, "vol99", vol)

	p2, _, _ := parseVolumeURI("short")
	assert.Empty(t, p2)

	dest := "projects/p1/locations/us-east4/volumes/vol99"
	repl := commonparams.ReplicationV1beta{
		Destination: &commonparams.ReplicationVolumeInformationV1beta{VolumeName: &dest},
	}
	assert.Equal(t, "projects/p1/locations/us-east4/storagePools/-", deriveStoragePoolURIFromReplication(repl))
	assert.Empty(t, deriveStoragePoolURIFromReplication(commonparams.ReplicationV1beta{}))
}

func TestParseProjectNumberFromReplicationURI(t *testing.T) {
	pn, err := utils.ParseProjectNumberFromURI("projects/45110233509/locations/us-east4/volumes/v/replications/r")
	require.NoError(t, err)
	assert.Equal(t, "45110233509", pn)

	_, err = utils.ParseProjectNumberFromURI("invalid")
	require.Error(t, err)
}

func TestValidateBatchReplicationURIList(t *testing.T) {
	err := validateBatchReplicationURIList([]string{validBatchReplicationURI()}, "us-east4")
	require.NoError(t, err)

	err = validateBatchReplicationURIList([]string{
		"projects/p1/locations/wrong/volumes/v1/replications/r1",
	}, "us-east4")
	require.Error(t, err)
}
