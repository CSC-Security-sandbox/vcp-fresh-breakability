package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// TestTrialMode_SecondCreateWithoutTrial_HandlerPaths verifies handler-level persist runs only
// when trialMode is set; a second create without trialMode must not call Persist again.
func TestTrialMode_SecondCreateWithoutTrial_HandlerPaths(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2025, 5, 14, 11, 0, 0, 0, time.UTC)
	end := time.Date(2025, 6, 13, 11, 0, 0, 0, time.UTC)
	trialOpt := gcpgenserver.NewOptTrialModeV1beta(gcpgenserver.TrialModeV1beta{StartTime: start, EndTime: end})

	t.Run("backup_policy", func(t *testing.T) {
		oldBackupEnabled := backupEnabled
		oldUseVCPRegion := cvp.CVP_HOST
		oldParse := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			cvp.CVP_HOST = oldUseVCPRegion
			parseAndValidateRegionAndZone = oldParse
		}()
		backupEnabled = true
		cvp.CVP_HOST = ""
		stubValidRegionParse()

		params := gcpgenserver.V1betaCreateBackupPolicyParams{
			LocationId:    "us-central1",
			ProjectNumber: "1234567890",
		}
		policy := &coremodels.BackupPolicy{BackupPolicyUUID: "uuid-1", ResourceID: "policy-1"}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			PersistAccountTrialMetadataIfSet(mock.Anything, params.ProjectNumber, mock.MatchedBy(func(trial *common.TrialModeParams) bool {
				return trial != nil && trial.Start.Equal(start) && trial.End.Equal(end)
			})).
			Return(nil).
			Once()
		mockOrchestrator.EXPECT().
			PersistAccountTrialMetadataIfSet(mock.Anything, params.ProjectNumber, mock.MatchedBy(func(trial *common.TrialModeParams) bool {
				return trial == nil
			})).
			Return(nil).
			Once()
		volumeCounts := map[string]int64{"uuid-1": 0, "uuid-2": 0}
		policiesByUUID := map[string]*coremodels.BackupPolicy{}
		mockOrchestrator.EXPECT().GetBackupPolicyByNameAndOwnerID(ctx, "policy-1", params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockOrchestrator.EXPECT().GetBackupPolicyByNameAndOwnerID(ctx, "policy-2", params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup policy", nil))
		mockOrchestrator.EXPECT().CreateBackupPolicy(ctx, mock.MatchedBy(func(p *common.CreateBackupPolicyParams) bool {
			return p.Name == "policy-1"
		})).Return(policy, nil)
		mockOrchestrator.EXPECT().CreateBackupPolicy(ctx, mock.MatchedBy(func(p *common.CreateBackupPolicyParams) bool {
			return p.Name == "policy-2" && p.TrialMode == nil
		})).Return(&coremodels.BackupPolicy{BackupPolicyUUID: "uuid-2", ResourceID: "policy-2"}, nil)
		mockOrchestrator.EXPECT().ListBackupPoliciesAndVolumeCount(ctx, params.ProjectNumber, mock.Anything).
			Return(volumeCounts, policiesByUUID, nil).Twice()

		_, err := Handler{Orchestrator: mockOrchestrator}.V1betaCreateBackupPolicy(ctx, &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: "policy-1",
			TrialMode:  trialOpt,
		}, params)
		require.NoError(t, err)

		_, err = Handler{Orchestrator: mockOrchestrator}.V1betaCreateBackupPolicy(ctx, &gcpgenserver.BackupPolicyCreateV1beta{
			ResourceId: "policy-2",
		}, params)
		require.NoError(t, err)
	})

	t.Run("backup_vault", func(t *testing.T) {
		oldBackupEnabled := backupEnabled
		oldUseVCPRegion := cvp.CVP_HOST
		oldParse := parseAndValidateRegionAndZone
		defer func() {
			backupEnabled = oldBackupEnabled
			cvp.CVP_HOST = oldUseVCPRegion
			parseAndValidateRegionAndZone = oldParse
		}()
		backupEnabled = true
		cvp.CVP_HOST = ""
		stubValidRegionParseEast4()

		params := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:    "us-east4",
			ProjectNumber: "12345",
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			PersistAccountTrialMetadataIfSet(mock.Anything, params.ProjectNumber, mock.MatchedBy(func(trial *common.TrialModeParams) bool {
				return trial != nil && trial.Start.Equal(start) && trial.End.Equal(end)
			})).
			Return(nil).
			Once()
		mockOrchestrator.EXPECT().
			PersistAccountTrialMetadataIfSet(mock.Anything, params.ProjectNumber, mock.MatchedBy(func(trial *common.TrialModeParams) bool {
				return trial == nil
			})).
			Return(nil).
			Once()
		mockOrchestrator.EXPECT().GetBackupVaultByNameAndOwnerID(ctx, "bv-1", params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup vault", nil))
		mockOrchestrator.EXPECT().CreateBackupVault(ctx, mock.Anything).Return(&coremodels.BackupVaultV1beta{Name: "bv-1"}, nil)

		_, err := Handler{Orchestrator: mockOrchestrator}.V1betaCreateBackupVault(ctx, &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("bv-1"),
			TrialMode:  trialOpt,
		}, params)
		require.NoError(t, err)

		mockOrchestrator.EXPECT().GetBackupVaultByNameAndOwnerID(ctx, "bv-2", params.ProjectNumber).
			Return(nil, utilerrors.NewNotFoundErr("backup vault", nil))
		mockOrchestrator.EXPECT().CreateBackupVault(ctx, mock.Anything).Return(&coremodels.BackupVaultV1beta{Name: "bv-2"}, nil)

		_, err = Handler{Orchestrator: mockOrchestrator}.V1betaCreateBackupVault(ctx, &gcpgenserver.BackupVaultCreateV1beta{
			ResourceId: gcpgenserver.NewOptString("bv-2"),
		}, params)
		require.NoError(t, err)
	})

	t.Run("active_directory_sync", func(t *testing.T) {
		defer withSDESyncADCreatePath(t)()

		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			ProjectNumber: "pn",
			LocationId:    "us-central1",
		}
		baseReq := func(resourceID string, trial gcpgenserver.OptTrialModeV1beta) *gcpgenserver.ActiveDirectoryV1beta {
			return &gcpgenserver.ActiveDirectoryV1beta{
				Username:   "user",
				ResourceId: resourceID,
				Password:   "pass",
				Domain:     "domain",
				DNS:        "dns",
				NetBIOS:    "netbios",
				TrialMode:  trial,
			}
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().
			PersistAccountTrialMetadataIfSet(mock.Anything, params.ProjectNumber, mock.MatchedBy(func(trial *common.TrialModeParams) bool {
				return trial != nil && trial.Start.Equal(start) && trial.End.Equal(end)
			})).
			Return(nil).
			Once()
		mockOrchestrator.EXPECT().
			PersistAccountTrialMetadataIfSet(mock.Anything, params.ProjectNumber, mock.MatchedBy(func(trial *common.TrialModeParams) bool {
				return trial == nil
			})).
			Return(nil).
			Once()

		done := true
		mockClient := active_directories.NewMockClientService(t)
		mockClient.EXPECT().V1betaCreateActiveDirectory(mock.Anything).Return(&active_directories.V1betaCreateActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{Name: "operations/op-1", Done: &done},
		}, nil).Once()
		origCreateClient := createClient
		defer func() { createClient = origCreateClient }()
		createClient = func(_ log.Logger, _ string) cvpapi.Cvp {
			return cvpapi.Cvp{ActiveDirectories: mockClient}
		}

		handler := Handler{Orchestrator: mockOrchestrator}
		_, err := handler.V1betaCreateActiveDirectory(context.Background(), baseReq("ad-1", trialOpt), params)
		require.NoError(t, err)

		mockClient.EXPECT().V1betaCreateActiveDirectory(mock.Anything).Return(&active_directories.V1betaCreateActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{Name: "operations/op-2", Done: &done},
		}, nil).Once()
		_, err = handler.V1betaCreateActiveDirectory(context.Background(), baseReq("ad-2", gcpgenserver.OptTrialModeV1beta{}), params)
		require.NoError(t, err)
	})

	t.Run("active_directory_async", func(t *testing.T) {
		defer withSDEAsyncADCreatePath(t)()

		params := gcpgenserver.V1betaCreateActiveDirectoryParams{
			ProjectNumber: "pn",
			LocationId:    "us-central1",
		}
		baseReq := func(resourceID string, trial gcpgenserver.OptTrialModeV1beta) *gcpgenserver.ActiveDirectoryV1beta {
			return &gcpgenserver.ActiveDirectoryV1beta{
				Username:   "user",
				ResourceId: resourceID,
				Password:   "pass",
				Domain:     "domain",
				DNS:        "dns",
				NetBIOS:    "netbios",
				TrialMode:  trial,
			}
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().CreateActiveDirectory(mock.Anything, mock.MatchedBy(func(p *common.CreateActiveDirectoryParams) bool {
			return p.TrialMode != nil
		})).Return(minimalActiveDirectoryForHandlerResponse("ad-1"), "job-1", nil).Once()
		mockOrchestrator.EXPECT().CreateActiveDirectory(mock.Anything, mock.MatchedBy(func(p *common.CreateActiveDirectoryParams) bool {
			return p.TrialMode == nil
		})).Return(minimalActiveDirectoryForHandlerResponse("ad-2"), "job-2", nil).Once()

		handler := Handler{Orchestrator: mockOrchestrator}
		_, err := handler.V1betaCreateActiveDirectory(context.Background(), baseReq("ad-1", trialOpt), params)
		require.NoError(t, err)
		_, err = handler.V1betaCreateActiveDirectory(context.Background(), baseReq("ad-2", gcpgenserver.OptTrialModeV1beta{}), params)
		require.NoError(t, err)
	})

	t.Run("pool_orchestrator_second_create_omits_trial_on_params", func(t *testing.T) {
		oldParse := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = oldParse }()
		stubPoolZonalRegionParse()

		params := poolCreateParams()
		mockOrchestrator := factory.NewMockOrchestratorFactory(t)
		mockOrchestrator.EXPECT().GetPoolByVendorID(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, utilerrors.NewNotFoundErr("pool", nil)).Twice()
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.MatchedBy(func(p *common.CreatePoolParams) bool {
			return p.TrialMode != nil
		})).Return(&coremodels.Pool{
			BaseModel:      coremodels.BaseModel{UUID: "pool-uuid-1"},
			PoolAttributes: &coremodels.PoolAttributes{},
		}, "op-1", nil).Once()
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.MatchedBy(func(p *common.CreatePoolParams) bool {
			return p.TrialMode == nil
		})).Return(&coremodels.Pool{
			BaseModel:      coremodels.BaseModel{UUID: "pool-uuid-2"},
			PoolAttributes: &coremodels.PoolAttributes{},
		}, "op-2", nil).Once()

		handler := Handler{Orchestrator: mockOrchestrator}
		_, err := handler.V1betaCreatePool(context.Background(), minimalPoolCreateRequest(trialOpt), params)
		require.NoError(t, err)

		req2 := minimalPoolCreateRequest(gcpgenserver.OptTrialModeV1beta{})
		req2.ResourceId = "test-pool-2"
		_, err = handler.V1betaCreatePool(context.Background(), req2, params)
		require.NoError(t, err)
	})
}
