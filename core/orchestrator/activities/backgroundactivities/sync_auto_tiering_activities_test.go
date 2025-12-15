package backgroundactivities

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/testsuite"
)

func TestAutoTierSyncActivity_UpdateAggregateInOntap(t *testing.T) {
	t.Run("UpdateAggregateInOntapSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdateAggregatesInOntap)

		node := &models.Node{
			EndpointAddress: "test-endpoint",
		}
		tieringFullnessThreshold := int64(80)

		// Mock provider and aggregate
		mockProvider := new(vsa.MockProvider)
		aggregate1 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid1",
			Name: "aggr1",
		}
		aggregate2 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid2",
			Name: "aggr2",
		}
		aggregate3 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid3",
			Name: "aggr3",
		}

		// Patch hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetAggregateByName", "aggr1").Return(aggregate1, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate1.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(nil)
		mockProvider.On("GetAggregateByName", "aggr2").Return(aggregate2, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate2.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(nil)
		mockProvider.On("GetAggregateByName", "aggr3").Return(aggregate3, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate3.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(nil)

		_, err := env.ExecuteActivity(activity.UpdateAggregatesInOntap, node, tieringFullnessThreshold, []string{"aggr1", "aggr2", "aggr3"})
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("UpdateAggregateInOntap_GetProviderFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdateAggregatesInOntap)

		node := &models.Node{
			EndpointAddress: "test-endpoint",
		}
		tieringFullnessThreshold := int64(80)

		// Patch hyperscaler.GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		_, err := env.ExecuteActivity(activity.UpdateAggregatesInOntap, node, tieringFullnessThreshold, []string{"aggr1", "aggr2", "aggr3"})
		assert.Error(tt, err)
	})

	t.Run("UpdateAggregateInOntap_GetAggregateFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdateAggregatesInOntap)

		node := &models.Node{
			EndpointAddress: "test-endpoint",
		}
		tieringFullnessThreshold := int64(80)

		mockProvider := new(vsa.MockProvider)

		// Patch hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetAggregateByName", activities.AggregateName).Return(nil, errors.New("failed to get aggregate"))

		_, err := env.ExecuteActivity(activity.UpdateAggregatesInOntap, node, tieringFullnessThreshold, []string{"aggr1", "aggr2", "aggr3"})
		assert.Error(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("UpdateAggregateInOntap_UpdateAggregateFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdateAggregatesInOntap)

		node := &models.Node{
			EndpointAddress: "test-endpoint",
		}
		tieringFullnessThreshold := int64(80)

		mockProvider := new(vsa.MockProvider)
		aggregate := &vsa.Aggregate{
			UUID: "test-aggregate-uuid",
			Name: activities.AggregateName,
		}

		// Patch hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("GetAggregateByName", activities.AggregateName).Return(aggregate, nil)
		mockProvider.On("UpdateAggregate", mock.AnythingOfType("vsa.UpdateAggregateParams")).Return(errors.New("failed to update aggregate"))

		_, err := env.ExecuteActivity(activity.UpdateAggregatesInOntap, node, tieringFullnessThreshold, []string{"aggr1", "aggr2", "aggr3"})
		assert.Error(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("UpdateAggregateInOntap_FirstAggregateSucceedsSecondFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdateAggregatesInOntap)

		node := &models.Node{
			EndpointAddress: "test-endpoint",
		}
		tieringFullnessThreshold := int64(80)
		aggrNames := []string{"aggr1", "aggr2", "aggr3"}

		mockProvider := new(vsa.MockProvider)
		aggregate1 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid-1",
			Name: "aggr1",
		}
		aggregate2 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid-2",
			Name: "aggr2",
		}

		// Patch hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// First aggregate succeeds
		mockProvider.On("GetAggregateByName", "aggr1").Return(aggregate1, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate1.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(nil)

		// Second aggregate fails
		mockProvider.On("GetAggregateByName", "aggr2").Return(aggregate2, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate2.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(errors.New("failed to update aggregate aggr2"))

		// Third aggregate should still be processed (function continues after errors)
		aggregate3 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid-3",
			Name: "aggr3",
		}
		mockProvider.On("GetAggregateByName", "aggr3").Return(aggregate3, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate3.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(nil)

		_, err := env.ExecuteActivity(activity.UpdateAggregatesInOntap, node, tieringFullnessThreshold, aggrNames)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to update aggregate aggr2")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("UpdateAggregateInOntap_MultipleAggregatesFail_ReturnsConcatenatedErrors", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdateAggregatesInOntap)

		node := &models.Node{
			EndpointAddress: "test-endpoint",
		}
		tieringFullnessThreshold := int64(80)
		aggrNames := []string{"aggr1", "aggr2", "aggr3"}

		mockProvider := new(vsa.MockProvider)
		aggregate1 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid-1",
			Name: "aggr1",
		}
		aggregate2 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid-2",
			Name: "aggr2",
		}
		aggregate3 := &vsa.Aggregate{
			UUID: "test-aggregate-uuid-3",
			Name: "aggr3",
		}

		// Patch hyperscaler.GetProviderByNode
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// First aggregate fails
		mockProvider.On("GetAggregateByName", "aggr1").Return(aggregate1, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate1.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(errors.New("failed to update aggregate aggr1"))

		// Second aggregate fails
		mockProvider.On("GetAggregateByName", "aggr2").Return(aggregate2, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate2.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(errors.New("failed to update aggregate aggr2"))

		// Third aggregate succeeds
		mockProvider.On("GetAggregateByName", "aggr3").Return(aggregate3, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.UUID == aggregate3.UUID && params.TieringFullnessThreshold == tieringFullnessThreshold
		})).Return(nil)

		_, err := env.ExecuteActivity(activity.UpdateAggregatesInOntap, node, tieringFullnessThreshold, aggrNames)
		assert.Error(tt, err)
		// Verify both errors are in the concatenated error message
		assert.Contains(tt, err.Error(), "failed to update aggregate aggr1")
		assert.Contains(tt, err.Error(), "failed to update aggregate aggr2")
		mockProvider.AssertExpectations(tt)
	})
}

func TestAutoTierSyncActivity_SegregatePools(t *testing.T) {
	t.Run("SegregatePoolsSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "pool-to-pause-uuid",
				AccountID: 123,
				Name:      "pool-to-pause",
			},
			{
				UUID:      "pool-to-resume-uuid",
				AccountID: 124,
				Name:      "pool-to-resume",
			},
			{
				UUID:      "pool-to-autoresize-uuid",
				AccountID: 125,
				Name:      "pool-to-autoresize",
			},
			{
				UUID:      "pool-not-ready-uuid",
				AccountID: 126,
				Name:      "pool-not-ready",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"pool-to-pause-uuid": {
				PoolConsumptionHotTier:  500000000000, // 500GB
				PoolConsumptionColdTier: 600000000000, // 600GB - will exceed pool size
			},
			"pool-to-resume-uuid": {
				PoolConsumptionHotTier:  200000000000, // 200GB
				PoolConsumptionColdTier: 100000000000, // 100GB - under pool size
			},
			"pool-to-autoresize-uuid": {
				PoolConsumptionHotTier:  500000000000, // 100% of 500GB hot tier
				PoolConsumptionColdTier: 50000000000,  // 50GB
			},
		}

		// Mock pool responses
		pausePool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "pool-to-pause-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				SizeInBytes:      1000000000000, // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 500000000000, // 500GB
					TieringStatus:      datamodel.TieringStatusResumed,
				},
			},
		}

		resumePool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 2, UUID: "pool-to-resume-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				SizeInBytes:      1000000000000, // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 500000000000, // 500GB
					TieringStatus:      datamodel.TieringStatusPaused,
				},
			},
		}

		autoResizePool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 3, UUID: "pool-to-autoresize-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				SizeInBytes:      1000000000000, // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes:      500000000000, // 500GB
					TieringStatus:           datamodel.TieringStatusResumed,
					EnableHotTierAutoResize: true,
				},
			},
		}

		notReadyPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 4, UUID: "pool-not-ready-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateCreating,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-to-pause-uuid", int64(123)).Return(pausePool, nil)
		mockStorage.On("GetPool", mock.Anything, "pool-to-resume-uuid", int64(124)).Return(resumePool, nil)
		mockStorage.On("GetPool", mock.Anything, "pool-to-autoresize-uuid", int64(125)).Return(autoResizePool, nil)
		mockStorage.On("GetPool", mock.Anything, "pool-not-ready-uuid", int64(126)).Return(notReadyPool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(3)).Return([]*datamodel.Volume{}, nil)

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 1)
		assert.Equal(tt, "pool-to-pause-uuid", result[PoolsToPauseKey][0].UUID)
		assert.Len(tt, result[PoolsToResumeKey], 1)
		assert.Equal(tt, "pool-to-resume-uuid", result[PoolsToResumeKey][0].UUID)
		assert.Len(tt, result[PoolsToAutoResizeKey], 1)
		assert.Equal(tt, "pool-to-autoresize-uuid", result[PoolsToAutoResizeKey][0].UUID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsWithEmptyPools", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{}
		poolConsumptionsMap := map[string]map[string]float64{}

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0)
	})

	t.Run("SegregatePoolsGetPoolFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "failed-pool-uuid",
				AccountID: 123,
				Name:      "failed-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"failed-pool-uuid": {
				PoolConsumptionHotTier:  500000000000,
				PoolConsumptionColdTier: 600000000000,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "failed-pool-uuid", int64(123)).Return(nil, errors.New("failed to get pool"))

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsAutoTieringDisabled", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "disabled-pool-uuid",
				AccountID: 123,
				Name:      "disabled-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"disabled-pool-uuid": {
				PoolConsumptionHotTier:  500000000000,
				PoolConsumptionColdTier: 600000000000,
			},
		}

		disabledPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "disabled-pool-uuid"},
				AllowAutoTiering: false, // Auto-tiering disabled
				State:            models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "disabled-pool-uuid", int64(123)).Return(disabledPool, nil)

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsPoolNotReady", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "not-ready-pool-uuid",
				AccountID: 123,
				Name:      "not-ready-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"not-ready-pool-uuid": {
				PoolConsumptionHotTier:  500000000000,
				PoolConsumptionColdTier: 600000000000,
			},
		}

		notReadyPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "not-ready-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateCreating, // Not ready
				SizeInBytes:      1000000000000,                 // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes: 500000000000, // 500GB
					TieringStatus:      datamodel.TieringStatusResumed,
				},
			},
		}

		mockStorage.On("GetPool", mock.Anything, "not-ready-pool-uuid", int64(123)).Return(notReadyPool, nil)

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		// Pool should still be segregated to pause even if not ready, since logic doesn't check state for pause/resume
		assert.Len(tt, result[PoolsToPauseKey], 1)
		assert.Equal(tt, "not-ready-pool-uuid", result[PoolsToPauseKey][0].UUID)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsNoConsumptionData", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "no-consumption-pool-uuid",
				AccountID: 123,
				Name:      "no-consumption-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{} // No consumption data

		noConsumptionPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "no-consumption-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "no-consumption-pool-uuid", int64(123)).Return(noConsumptionPool, nil)

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsCheckBypassModeFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "bypass-check-failed-pool-uuid",
				AccountID: 123,
				Name:      "bypass-check-failed-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"bypass-check-failed-pool-uuid": {
				PoolConsumptionHotTier:  450000000000, // 90% of 500GB hot tier
				PoolConsumptionColdTier: 50000000000,  // 50GB
			},
		}

		bypassCheckFailedPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "bypass-check-failed-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				SizeInBytes:      1000000000000, // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes:      500000000000, // 500GB
					TieringStatus:           datamodel.TieringStatusResumed,
					EnableHotTierAutoResize: true,
				},
			},
		}

		mockStorage.On("GetPool", mock.Anything, "bypass-check-failed-pool-uuid", int64(123)).Return(bypassCheckFailedPool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(nil, errors.New("failed to get volumes"))

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsWithBypassModeDisabled", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "bypass-disabled-pool-uuid",
				AccountID: 123,
				Name:      "bypass-disabled-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"bypass-disabled-pool-uuid": {
				PoolConsumptionHotTier:  500000000000, // 100% of 500GB hot tier
				PoolConsumptionColdTier: 50000000000,  // 50GB
			},
		}

		bypassDisabledPool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "bypass-disabled-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				SizeInBytes:      1000000000000, // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes:      500000000000, // 500GB
					TieringStatus:           datamodel.TieringStatusResumed,
					EnableHotTierAutoResize: true,
				},
			},
		}

		mockStorage.On("GetPool", mock.Anything, "bypass-disabled-pool-uuid", int64(123)).Return(bypassDisabledPool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return([]*datamodel.Volume{}, nil)

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 1) // Should auto-resize when bypass mode is disabled (false)
		assert.Equal(tt, "bypass-disabled-pool-uuid", result[PoolsToAutoResizeKey][0].UUID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SegregatePoolsAutoResizeNotMeetingThreshold", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.SegregatePools)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "low-usage-pool-uuid",
				AccountID: 123,
				Name:      "low-usage-pool",
			},
		}

		poolConsumptionsMap := map[string]map[string]float64{
			"low-usage-pool-uuid": {
				PoolConsumptionHotTier:  300000000000, // 300GB - 60% of 500GB hot tier (below 80% threshold)
				PoolConsumptionColdTier: 50000000000,  // 50GB
			},
		}

		lowUsagePool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "low-usage-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				SizeInBytes:      1000000000000, // 1TB
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					HotTierSizeInBytes:      500000000000, // 500GB
					TieringStatus:           datamodel.TieringStatusResumed,
					EnableHotTierAutoResize: true,
				},
			},
		}

		mockStorage.On("GetPool", mock.Anything, "low-usage-pool-uuid", int64(123)).Return(lowUsagePool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return([]*datamodel.Volume{}, nil)

		encodedValue, err := env.ExecuteActivity(activity.SegregatePools, pools, poolConsumptionsMap)
		assert.NoError(tt, err)
		var result map[string][]*database.PoolIdentifier
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result[PoolsToPauseKey], 0)
		assert.Len(tt, result[PoolsToResumeKey], 0)
		assert.Len(tt, result[PoolsToAutoResizeKey], 0) // Should not auto-resize due to low usage
		mockStorage.AssertExpectations(tt)
	})
}

func TestCheckPoolVolumesWithBypassModeEnabled(t *testing.T) {
	ctx := context.TODO()

	t.Run("CheckPoolVolumesWithBypassModeEnabledSuccessWithoutBypassVolume", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			},
		}

		volumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{ID: 1}},
			{BaseModel: datamodel.BaseModel{ID: 2}},
		}

		mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return(volumes, nil)

		result, err := checkPoolVolumesWithBypassModeEnabled(ctx, mockStorage, pool)
		assert.NoError(tt, err)
		assert.False(tt, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CheckPoolVolumesWithBypassModeEnabledSuccessWithBypassVolume", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			},
		}

		volumes := []*datamodel.Volume{
			{BaseModel: datamodel.BaseModel{ID: 1}, AutoTieringEnabled: true, AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				HotTierBypassModeEnabled: true,
			}},
			{BaseModel: datamodel.BaseModel{ID: 2}},
		}

		mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return(volumes, nil)

		result, err := checkPoolVolumesWithBypassModeEnabled(ctx, mockStorage, pool)
		assert.NoError(tt, err)
		assert.True(tt, result)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CheckPoolVolumesWithBypassModeEnabled_GetVolumesFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			},
		}

		mockStorage.On("GetVolumesByPoolID", ctx, pool.ID).Return(nil, errors.New("failed to get volumes"))

		result, err := checkPoolVolumesWithBypassModeEnabled(ctx, mockStorage, pool)
		assert.Error(tt, err)
		assert.False(tt, result)
		mockStorage.AssertExpectations(tt)
	})
}

func TestAutoTierSyncActivity_FetchAndSavePoolsTieringInfo(t *testing.T) {
	t.Run("FetchAndSavePoolsTieringInfoSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)), // 100GB
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),  // 50GB
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)), // 150GB logical
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		assert.Equal(tt, float64(100000000000), result["test-pool-uuid"][PoolConsumptionColdTier])
		assert.Equal(tt, float64(50000000000), result["test-pool-uuid"][PoolConsumptionHotTier])
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_GetPoolFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(nil, errors.New("failed to get pool"))

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_PoolNotAutoTiering", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: false, // Not auto-tiering enabled
				State:            models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotContains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_PoolNotReady", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateCreating, // Not ready
			},
		}

		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)

		// Mock GetOntapRestProviderForPool to return error for non-ready pool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return nil, errors.New("pool not ready")
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotContains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_GetOntapProviderFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)

		// Mock GetOntapRestProviderForPool to return error
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotContains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_GetVolumesFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockProvider.On("GetVolumes").Return(nil, errors.New("failed to get volumes"))

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotContains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_GetVolumesByPoolIDFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(nil, errors.New("failed to get volumes"))
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotContains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_CalculateConsumptionFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid-1",
				},
			},
			{
				BaseModel: datamodel.BaseModel{ID: 2, UUID: "db-vol-uuid-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid-2",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid-1"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil) // Returns 2 volumes but ONTAP has 1
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		// With the updated logic, calculateAndUpdateHotColdTierConsumption no longer returns
		// an error for volume count mismatch, so the pool will be included in the result
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	// Tests for TieringFullnessThreshold migration logic
	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_ThresholdIs50AndNotPaused", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		// Create VLMConfig with DataAggr for threshold migration test
		vlmConfig := &vlm.VLMConfig{
			DataAggr: []vlm.DataAggrConfig{
				{
					Name:     "aggr1",
					Aggruuid: "aggr-uuid",
					Size:     1000,
					HomeNode: "node1",
				},
			},
		}
		vlmConfigJSON, _ := json.Marshal(vlmConfig)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				Name:             "test-pool",
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				VLMConfig:        string(vlmConfigJSON),
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations - should be called to update threshold to 0
		mockAggregate := &vsa.Aggregate{
			UUID: "aggr-uuid",
			Name: "aggr1",
		}
		mockProvider.On("GetAggregateByName", "aggr1").Return(mockAggregate, nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			// Verify the threshold is being set to 0
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid"
		})).Return(nil)

		// Mock UpdatePoolTieringConfig - should be called with threshold 0
		thresholdZero := int64(0)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "test-pool-uuid", (*int64)(nil), (*int64)(nil), &thresholdZero, (*datamodel.TieringStatus)(nil)).Return(nil)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_ThresholdIs50ButPaused", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusPaused, // Paused - should not update
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations - should NOT be called because pool is paused
		// (Not adding mock expectations means test will fail if they're called)

		// UpdatePoolTieringConfig should NOT be called
		// (Not adding mock expectation means test will fail if it's called)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_ThresholdIsNot50", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 75, // Not 50 - should not update
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations - should NOT be called because threshold is not 50
		// (Not adding mock expectations means test will fail if they're called)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_AutoTieringConfigIsNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:         datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				AllowAutoTiering:  true,
				State:             models.LifeCycleStateREADY,
				AutoTieringConfig: nil, // Nil - should not crash
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations - should NOT be called because AutoTieringConfig is nil
		// (Not adding mock expectations means test will fail if they're called)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_AggregateUpdateFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		// Create VLMConfig with DataAggr for threshold migration test
		vlmConfig := &vlm.VLMConfig{
			DataAggr: []vlm.DataAggrConfig{
				{
					Name:     "aggr1",
					Aggruuid: "aggr-uuid",
					Size:     1000,
					HomeNode: "node1",
				},
			},
		}
		vlmConfigJSON, _ := json.Marshal(vlmConfig)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				Name:             "test-pool",
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				VLMConfig:        string(vlmConfigJSON),
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations - should fail
		mockAggregate := &vsa.Aggregate{
			UUID: "aggr-uuid",
			Name: "aggr1",
		}
		mockProvider.On("GetAggregateByName", "aggr1").Return(mockAggregate, nil)
		mockProvider.On("UpdateAggregate", mock.Anything).Return(errors.New("failed to update aggregate"))

		// UpdatePoolTieringConfig should NOT be called because aggregate update failed
		// (Not adding mock expectation means test will fail if it's called)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err) // Should not error, just log warning
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_DBUpdateFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		// Create VLMConfig with DataAggr for threshold migration test
		vlmConfig := &vlm.VLMConfig{
			DataAggr: []vlm.DataAggrConfig{
				{
					Name:     "aggr1",
					Aggruuid: "aggr-uuid",
					Size:     1000,
					HomeNode: "node1",
				},
			},
		}
		vlmConfigJSON, _ := json.Marshal(vlmConfig)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				Name:             "test-pool",
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				VLMConfig:        string(vlmConfigJSON),
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations - should succeed
		mockAggregate := &vsa.Aggregate{
			UUID: "aggr-uuid",
			Name: "aggr1",
		}
		mockProvider.On("GetAggregateByName", "aggr1").Return(mockAggregate, nil)
		mockProvider.On("UpdateAggregate", mock.Anything).Return(nil)

		// Mock UpdatePoolTieringConfig - should fail
		thresholdZero := int64(0)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "test-pool-uuid", (*int64)(nil), (*int64)(nil), &thresholdZero, (*datamodel.TieringStatus)(nil)).Return(errors.New("database error"))

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err) // Should not error, just log warning
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_MultipleAggregates", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		// Create VLMConfig with multiple DataAggr for threshold migration test
		vlmConfig := &vlm.VLMConfig{
			DataAggr: []vlm.DataAggrConfig{
				{
					Name:     "aggr1",
					Aggruuid: "aggr-uuid-1",
					Size:     1000,
					HomeNode: "node1",
				},
				{
					Name:     "aggr2",
					Aggruuid: "aggr-uuid-2",
					Size:     2000,
					HomeNode: "node2",
				},
				{
					Name:     "aggr3",
					Aggruuid: "aggr-uuid-3",
					Size:     3000,
					HomeNode: "node3",
				},
			},
		}
		vlmConfigJSON, _ := json.Marshal(vlmConfig)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				Name:             "test-pool",
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				VLMConfig:        string(vlmConfigJSON),
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations for multiple aggregates - all should be updated
		mockAggregate1 := &vsa.Aggregate{
			UUID: "aggr-uuid-1",
			Name: "aggr1",
		}
		mockAggregate2 := &vsa.Aggregate{
			UUID: "aggr-uuid-2",
			Name: "aggr2",
		}
		mockAggregate3 := &vsa.Aggregate{
			UUID: "aggr-uuid-3",
			Name: "aggr3",
		}

		// Mock GetAggregateByName for each aggregate
		mockProvider.On("GetAggregateByName", "aggr1").Return(mockAggregate1, nil)
		mockProvider.On("GetAggregateByName", "aggr2").Return(mockAggregate2, nil)
		mockProvider.On("GetAggregateByName", "aggr3").Return(mockAggregate3, nil)

		// Mock UpdateAggregate for each aggregate - all should be called with threshold 0
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid-1"
		})).Return(nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid-2"
		})).Return(nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid-3"
		})).Return(nil)

		// Mock UpdatePoolTieringConfig - should be called with threshold 0 after all aggregates are updated
		thresholdZero := int64(0)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "test-pool-uuid", (*int64)(nil), (*int64)(nil), &thresholdZero, (*datamodel.TieringStatus)(nil)).Return(nil)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err)
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_MultipleAggregates_OneFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		// Create VLMConfig with multiple DataAggr for threshold migration test
		vlmConfig := &vlm.VLMConfig{
			DataAggr: []vlm.DataAggrConfig{
				{
					Name:     "aggr1",
					Aggruuid: "aggr-uuid-1",
					Size:     1000,
					HomeNode: "node1",
				},
				{
					Name:     "aggr2",
					Aggruuid: "aggr-uuid-2",
					Size:     2000,
					HomeNode: "node2",
				},
				{
					Name:     "aggr3",
					Aggruuid: "aggr-uuid-3",
					Size:     3000,
					HomeNode: "node3",
				},
			},
		}
		vlmConfigJSON, _ := json.Marshal(vlmConfig)

		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				Name:             "test-pool",
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				VLMConfig:        string(vlmConfigJSON),
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		dbVolumes := []*datamodel.Volume{
			{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "db-vol-uuid"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-uuid",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-vol-uuid"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(dbVolumes, nil)
		mockStorage.On("BatchUpdateVolumeTieringFields", mock.Anything, mock.Anything).Return(nil)
		mockProvider.On("GetVolumes").Return(volumes, nil)

		// Mock aggregate operations for multiple aggregates
		mockAggregate1 := &vsa.Aggregate{
			UUID: "aggr-uuid-1",
			Name: "aggr1",
		}
		mockAggregate2 := &vsa.Aggregate{
			UUID: "aggr-uuid-2",
			Name: "aggr2",
		}
		mockAggregate3 := &vsa.Aggregate{
			UUID: "aggr-uuid-3",
			Name: "aggr3",
		}

		// Mock GetAggregateByName for each aggregate
		mockProvider.On("GetAggregateByName", "aggr1").Return(mockAggregate1, nil)
		mockProvider.On("GetAggregateByName", "aggr2").Return(mockAggregate2, nil)
		mockProvider.On("GetAggregateByName", "aggr3").Return(mockAggregate3, nil)

		// Mock UpdateAggregate - aggr1 and aggr3 succeed, aggr2 fails
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid-1"
		})).Return(nil)
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid-2"
		})).Return(errors.New("failed to update aggregate aggr2"))
		mockProvider.On("UpdateAggregate", mock.MatchedBy(func(params vsa.UpdateAggregateParams) bool {
			return params.TieringFullnessThreshold == 0 && params.UUID == "aggr-uuid-3"
		})).Return(nil)

		// UpdatePoolTieringConfig should NOT be called because one aggregate update failed
		// (The code only updates DB if allAggregatesUpdated is true - see line 268 in sync_auto_tiering_activities.go)
		// (Not adding mock expectation means test will fail if it's called)

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err) // Should not error, just log warnings for failed aggregates
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.Contains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("FetchAndSavePoolsTieringInfo_TieringThresholdMigration_InvalidVLMConfig", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FetchAndSavePoolsTieringInfo)

		pools := []*database.PoolIdentifier{
			{
				UUID:      "test-pool-uuid",
				AccountID: 123,
				Name:      "test-pool",
			},
		}

		// Create pool with invalid VLMConfig JSON to trigger parseVlmConfig error
		pool := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel:        datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
				Name:             "test-pool",
				AllowAutoTiering: true,
				State:            models.LifeCycleStateREADY,
				VLMConfig:        "invalid json {", // Invalid JSON that will cause parseVlmConfig to fail
				AutoTieringConfig: &datamodel.AutoTieringConfig{
					TieringFullnessThreshold: 50,
					TieringStatus:            datamodel.TieringStatusResumed,
				},
			},
		}

		mockProvider := new(vsa.MockProvider)
		mockStorage.On("GetPool", mock.Anything, "test-pool-uuid", int64(123)).Return(pool, nil)

		// The following should NOT be called because parseVlmConfig failure causes early return
		// (Not adding mock expectations means test will fail if they're called):
		// - GetVolumesByPoolID
		// - BatchUpdateVolumeTieringFields
		// - GetVolumes
		// - GetAggregateByName
		// - UpdateAggregate
		// - UpdatePoolTieringConfig

		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.FetchAndSavePoolsTieringInfo, pools)
		assert.NoError(tt, err) // Should not error, just log error and skip pool
		var result map[string]map[string]float64
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		// Pool should NOT be in result because parseVlmConfig failure causes early return
		// from the goroutine, skipping volume processing
		assert.NotContains(tt, result, "test-pool-uuid")
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})
}

func TestCalculateAndUpdateHotColdTierConsumption(t *testing.T) {
	ctx := context.TODO()

	t.Run("calculateAndUpdateHotColdTierConsumptionSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		dbVolumeMap := map[string]*datamodel.Volume{
			"external-uuid-1": {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-uuid-1",
				},
			},
			"external-uuid-2": {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-uuid-2",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-1"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)), // 100GB
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),  // 50GB
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)), // 150GB logical
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-2"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(200000000000)), // 200GB
						PerformanceTierFootprint: nillable.ToPointer(int64(100000000000)), // 100GB
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(300000000000)), // 300GB logical
						},
					},
				},
			},
		}
		expectedVolCount := int64(2)

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, volumes, expectedVolCount, mockStorage, dbVolumeMap)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(300000000000), coldTier) // 300GB total
		assert.Equal(tt, int64(150000000000), hotTier)  // 150GB total
		mockStorage.AssertExpectations(tt)
	})

	t.Run("calculateAndUpdateHotColdTierConsumption_SkipSvmRoot", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			"external-uuid-1": {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-uuid-1",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-root"),
					IsSvmRoot: nillable.ToPointer(true), // Should be skipped
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-1"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(200000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(100000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(300000000000)),
						},
					},
				},
			},
		}
		expectedVolCount := int64(1) // Only counting non-SVM root volumes

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, volumes, expectedVolCount, mockStorage, dbVolumeMap)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(200000000000), coldTier)
		assert.Equal(tt, int64(100000000000), hotTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("calculateAndUpdateHotColdTierConsumption_SkipVolumeWithoutSpace", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			"external-uuid-1": {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-uuid-1",
				},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-no-space"),
					IsSvmRoot: nillable.ToPointer(false),
					Space:     nil, // Should be skipped
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-1"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(200000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(100000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(300000000000)),
						},
					},
				},
			},
		}
		expectedVolCount := int64(1) // Only counting volumes with space data

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, volumes, expectedVolCount, mockStorage, dbVolumeMap)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(200000000000), coldTier)
		assert.Equal(tt, int64(100000000000), hotTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("calculateAndUpdateHotColdTierConsumption_VolumeCountMismatch", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			"external-uuid-1": {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-uuid-1"},
			},
		}

		volumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      nillable.ToPointer("external-uuid-1"),
					IsSvmRoot: nillable.ToPointer(false),
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nillable.ToPointer(int64(100000000000)),
						PerformanceTierFootprint: nillable.ToPointer(int64(50000000000)),
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: nillable.ToPointer(int64(150000000000)),
						},
					},
				},
			},
		}
		expectedVolCount := int64(2) // Mismatch: expecting 2 but got 1

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, volumes, expectedVolCount, mockStorage, dbVolumeMap)
		// The function no longer returns an error for volume count mismatch
		assert.NoError(tt, err)
		// The function still computes based on what it got
		assert.NotEqual(tt, int64(0), hotTier)
		assert.NotEqual(tt, int64(0), coldTier)
	})

	t.Run("calculateAndUpdateHotColdTierConsumption_EmptyVolumes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{}
		volumes := []*vsa.Volume{}
		expectedVolCount := int64(0)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, volumes, expectedVolCount, mockStorage, dbVolumeMap)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), hotTier)
		assert.Equal(tt, int64(0), coldTier)
	})
}

func TestAutoTierSyncActivity_ToggleHotTierBypassModeForPoolVolumes(t *testing.T) {
	t.Run("ToggleHotTierBypassModeForPoolVolumes_Success_PauseMode", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ToggleHotTierBypassModeForPoolVolumes)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusPaused, // Paused - should set to "none"
			},
		}

		volumes := []*datamodel.Volume{
			{
				BaseModel:          datamodel.BaseModel{ID: 1, UUID: "vol-1-uuid"},
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					HotTierBypassModeEnabled: true,
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-1-uuid",
				},
			},
			{
				BaseModel:          datamodel.BaseModel{ID: 2, UUID: "vol-2-uuid"},
				AutoTieringEnabled: false, // Should be skipped
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-2-uuid",
				},
			},
		}

		mockProvider := vsa.NewMockProvider(tt)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(volumes, nil)
		mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
			return params.UUID == "external-vol-1-uuid" &&
				params.TieringPolicy.CoolAccessTieringPolicy == ontaprestmodel.VolumeInlineTieringPolicyNone
		})).Return(nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		_, err := env.ExecuteActivity(activity.ToggleHotTierBypassModeForPoolVolumes, pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ToggleHotTierBypassModeForPoolVolumes_Success_ResumeMode", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ToggleHotTierBypassModeForPoolVolumes)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusResumed, // Not paused - should set to "all"
			},
		}

		volumes := []*datamodel.Volume{
			{
				BaseModel:          datamodel.BaseModel{ID: 1, UUID: "vol-1-uuid"},
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					HotTierBypassModeEnabled: true,
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-1-uuid",
				},
			},
		}

		mockProvider := vsa.NewMockProvider(tt)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(volumes, nil)
		mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
			return params.UUID == "external-vol-1-uuid" &&
				params.TieringPolicy.CoolAccessTieringPolicy == ontaprestmodel.VolumeInlineTieringPolicyAll
		})).Return(nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		_, err := env.ExecuteActivity(activity.ToggleHotTierBypassModeForPoolVolumes, pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("ToggleHotTierBypassModeForPoolVolumes_NoVolumesWithBypassMode", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ToggleHotTierBypassModeForPoolVolumes)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusResumed,
			},
		}

		volumes := []*datamodel.Volume{
			{
				BaseModel:          datamodel.BaseModel{ID: 1, UUID: "vol-1-uuid"},
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					HotTierBypassModeEnabled: false, // No bypass mode
				},
			},
		}

		mockProvider := vsa.NewMockProvider(tt)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(volumes, nil)

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		_, err := env.ExecuteActivity(activity.ToggleHotTierBypassModeForPoolVolumes, pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		// Provider should not be called since no volumes need updating
	})

	t.Run("ToggleHotTierBypassModeForPoolVolumes_GetProviderFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ToggleHotTierBypassModeForPoolVolumes)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusResumed,
			},
		}

		// Mock GetOntapRestProviderForPool to return error
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		_, err := env.ExecuteActivity(activity.ToggleHotTierBypassModeForPoolVolumes, pool)
		assert.Error(tt, err)
	})

	t.Run("ToggleHotTierBypassModeForPoolVolumes_GetVolumesFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ToggleHotTierBypassModeForPoolVolumes)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusResumed,
			},
		}

		mockProvider := vsa.NewMockProvider(tt)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(nil, errors.New("failed to get volumes"))

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		_, err := env.ExecuteActivity(activity.ToggleHotTierBypassModeForPoolVolumes, pool)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ToggleHotTierBypassModeForPoolVolumes_UpdateVolumeFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.ToggleHotTierBypassModeForPoolVolumes)

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				TieringStatus: datamodel.TieringStatusResumed,
			},
		}

		volumes := []*datamodel.Volume{
			{
				BaseModel:          datamodel.BaseModel{ID: 1, UUID: "vol-1-uuid"},
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					HotTierBypassModeEnabled: true,
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "external-vol-1-uuid",
				},
			},
		}

		mockProvider := vsa.NewMockProvider(tt)
		mockStorage.On("GetVolumesByPoolID", mock.Anything, int64(1)).Return(volumes, nil)
		mockProvider.On("UpdateVolume", mock.Anything).Return(errors.New("failed to update volume"))

		// Mock GetOntapRestProviderForPool
		originalGetOntapRestProviderForPool := GetOntapRestProviderForPool
		defer func() { GetOntapRestProviderForPool = originalGetOntapRestProviderForPool }()
		GetOntapRestProviderForPool = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) (vsa.Provider, error) {
			return mockProvider, nil
		}

		_, err := env.ExecuteActivity(activity.ToggleHotTierBypassModeForPoolVolumes, pool)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
		mockProvider.AssertExpectations(tt)
	})
}

func TestAutoTierSyncActivity_UpdatePoolTieringConsumptionInDB(t *testing.T) {
	t.Run("UpdatePoolTieringConsumptionInDB_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatePoolTieringConsumptionInDB)

		poolsConsumptionsMap := map[string]map[string]float64{
			"pool-1-uuid": {
				PoolConsumptionHotTier:  500000000000, // 500GB
				PoolConsumptionColdTier: 600000000000, // 600GB
			},
			"pool-2-uuid": {
				PoolConsumptionHotTier:  300000000000, // 300GB
				PoolConsumptionColdTier: 400000000000, // 400GB
			},
		}

		hot1 := int64(500000000000)
		cold1 := int64(600000000000)
		hot2 := int64(300000000000)
		cold2 := int64(400000000000)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "pool-1-uuid", &hot1, &cold1, (*int64)(nil), (*datamodel.TieringStatus)(nil)).Return(nil)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "pool-2-uuid", &hot2, &cold2, (*int64)(nil), (*datamodel.TieringStatus)(nil)).Return(nil)

		_, err := env.ExecuteActivity(activity.UpdatePoolTieringConsumptionInDB, poolsConsumptionsMap)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdatePoolTieringConsumptionInDB_EmptyMap", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatePoolTieringConsumptionInDB)

		poolsConsumptionsMap := map[string]map[string]float64{}

		_, err := env.ExecuteActivity(activity.UpdatePoolTieringConsumptionInDB, poolsConsumptionsMap)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdatePoolTieringConsumptionInDB_UpdateFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatePoolTieringConsumptionInDB)

		poolsConsumptionsMap := map[string]map[string]float64{
			"pool-1-uuid": {
				PoolConsumptionHotTier:  500000000000,
				PoolConsumptionColdTier: 600000000000,
			},
		}

		hot := int64(500000000000)
		cold := int64(600000000000)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "pool-1-uuid", &hot, &cold, (*int64)(nil), (*datamodel.TieringStatus)(nil)).Return(errors.New("database error"))

		_, err := env.ExecuteActivity(activity.UpdatePoolTieringConsumptionInDB, poolsConsumptionsMap)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to update pool tiering consumption in DB")
		assert.Contains(tt, err.Error(), "pool-1-uuid")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdatePoolTieringConsumptionInDB_SinglePool", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatePoolTieringConsumptionInDB)

		poolsConsumptionsMap := map[string]map[string]float64{
			"single-pool-uuid": {
				PoolConsumptionHotTier:  100000000000, // 100GB
				PoolConsumptionColdTier: 200000000000, // 200GB
			},
		}

		hot := int64(100000000000)
		cold := int64(200000000000)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "single-pool-uuid", &hot, &cold, (*int64)(nil), (*datamodel.TieringStatus)(nil)).Return(nil)

		_, err := env.ExecuteActivity(activity.UpdatePoolTieringConsumptionInDB, poolsConsumptionsMap)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdatePoolTieringConsumptionInDB_ZeroConsumption", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatePoolTieringConsumptionInDB)

		poolsConsumptionsMap := map[string]map[string]float64{
			"zero-pool-uuid": {
				PoolConsumptionHotTier:  0,
				PoolConsumptionColdTier: 0,
			},
		}

		zero := int64(0)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "zero-pool-uuid", &zero, &zero, (*int64)(nil), (*datamodel.TieringStatus)(nil)).Return(nil)

		_, err := env.ExecuteActivity(activity.UpdatePoolTieringConsumptionInDB, poolsConsumptionsMap)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdatePoolTieringConsumptionInDB_MultiplePoolsWithOneFailure", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := AutoTierSyncActivity{SE: mockStorage}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatePoolTieringConsumptionInDB)

		poolsConsumptionsMap := map[string]map[string]float64{
			"failing-pool-uuid": {
				PoolConsumptionHotTier:  500000000000,
				PoolConsumptionColdTier: 600000000000,
			},
		}

		// Mock to return error for this specific pool
		hot := int64(500000000000)
		cold := int64(600000000000)
		mockStorage.On("UpdatePoolTieringConfig", mock.Anything, "failing-pool-uuid", &hot, &cold, (*int64)(nil), (*datamodel.TieringStatus)(nil)).Return(errors.New("database error"))

		_, err := env.ExecuteActivity(activity.UpdatePoolTieringConsumptionInDB, poolsConsumptionsMap)
		// Should fail when encountering the error
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failing-pool-uuid")
		mockStorage.AssertExpectations(tt)
	})
}

func Test_calculateAndUpdateHotColdTierConsumption(t *testing.T) {
	ctx := context.TODO()

	// Helper variables for pointers
	boolFalse := false
	boolTrue := true
	int64Val0 := int64(0)
	int64Val50 := int64(50000000)
	int64Val100 := int64(100000000)
	int64Val150 := int64(150000000)
	int64Val200 := int64(200000000)
	int64Val300 := int64(300000000)
	int64Val350 := int64(350000000)
	int64Val500 := int64(500000000)
	int64Val800 := int64(800000000)
	int64Val1000 := int64(1000000000)

	// Helper UUID strings
	uuidStr1 := "uuid-1"
	uuidStr2 := "uuid-2"

	t.Run("SuccessfulCalculationWithValidVolumes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr1: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr1,
				},
			},
			uuidStr2: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr2,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val100, // 100MB cold tier
						PerformanceTierFootprint: &int64Val200, // 200MB hot tier
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300, // 300MB logical space used
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr2,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val200, // 200MB cold tier
						PerformanceTierFootprint: &int64Val800, // 800MB hot tier
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val1000, // 1000MB logical space used
						},
					},
				},
			},
		}
		expectedVolCount := int64(2)

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		// Hot tier should be sum of performance tier footprints: 200000000 + 800000000 = 1000000000
		assert.Equal(tt, int64(1000000000), hotTier)
		// Cold tier calculation involves ratio correction
		// Volume 1: ratio = 100/(100+200) = 0.333, corrected = 300*0.333 = 100000000
		// Volume 2: ratio = 200/(200+800) = 0.2, corrected = 1000*0.2 = 200000000
		// Total: 100000000 + 200000000 = 300000000
		assert.Equal(tt, int64(300000000), coldTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SkipsVolumeWhenDenominatorIsZero", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr2: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr2,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val0, // 0MB cold tier
						PerformanceTierFootprint: &int64Val0, // 0MB hot tier - denominator will be 0
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val100,
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr2,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val100,
						PerformanceTierFootprint: &int64Val200,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300,
						},
					},
				},
			},
		}
		expectedVolCount := int64(2)

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		// First volume should be skipped (denominator = 0)
		// Only second volume should be counted
		assert.Equal(tt, int64(200000000), hotTier)
		assert.Equal(tt, int64(100000000), coldTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SkipsSvmRootVolumes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr2: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr2,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolTrue, // SVM root volume - should be skipped
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val100,
						PerformanceTierFootprint: &int64Val200,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300,
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr2,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val50,
						PerformanceTierFootprint: &int64Val150,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val200,
						},
					},
				},
			},
		}
		expectedVolCount := int64(1) // Only counting non-root volumes

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		// Only second volume should be counted
		assert.Equal(tt, int64(150000000), hotTier)
		assert.Equal(tt, int64(50000000), coldTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SkipsVolumesWithNilSpace", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr2: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr2,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolFalse,
					Space:     nil, // Nil space - should be skipped
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr2,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val100,
						PerformanceTierFootprint: &int64Val200,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300,
						},
					},
				},
			},
		}
		expectedVolCount := int64(1)

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(200000000), hotTier)
		assert.Equal(tt, int64(100000000), coldTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("SkipsVolumesWithNilSpaceFields", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr2: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-2"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr2,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    nil, // Nil field - should be skipped
						PerformanceTierFootprint: &int64Val200,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300,
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr2,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val100,
						PerformanceTierFootprint: &int64Val200,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300,
						},
					},
				},
			},
		}
		expectedVolCount := int64(1)

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(200000000), hotTier)
		assert.Equal(tt, int64(100000000), coldTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenVolumeCountMismatch", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr1: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-1"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr1,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val100,
						PerformanceTierFootprint: &int64Val200,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val300,
						},
					},
				},
			},
		}
		expectedVolCount := int64(2) // Expecting 2 but only got 1

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		// The function no longer returns an error for volume count mismatch
		assert.NoError(tt, err)
		// The function still computes based on what it got
		assert.NotEqual(tt, int64(0), hotTier)
		assert.NotEqual(tt, int64(0), coldTier)
	})

	t.Run("HandlesMultipleVolumesWithDenominatorZero", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		uuidStr3 := "uuid-3"
		dbVolumeMap := map[string]*datamodel.Volume{
			uuidStr3: {
				BaseModel: datamodel.BaseModel{UUID: "db-vol-3"},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: uuidStr3,
				},
			},
		}

		ontapVolumes := []*vsa.Volume{
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr1,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val0,
						PerformanceTierFootprint: &int64Val0, // Denominator = 0
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val100,
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr2,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val0,
						PerformanceTierFootprint: &int64Val0, // Denominator = 0
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val200,
						},
					},
				},
			},
			{
				Volume: ontaprestmodel.Volume{
					UUID:      &uuidStr3,
					IsSvmRoot: &boolFalse,
					Space: &ontaprestmodel.VolumeInlineSpace{
						CapacityTierFootprint:    &int64Val150,
						PerformanceTierFootprint: &int64Val350,
						LogicalSpace: &ontaprestmodel.VolumeInlineSpaceInlineLogicalSpace{
							Used: &int64Val500,
						},
					},
				},
			},
		}
		expectedVolCount := int64(3)

		mockStorage.On("BatchUpdateVolumeTieringFields", ctx, mock.Anything).Return(nil)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		// First two volumes skipped (denominator = 0), only third volume counted
		assert.Equal(tt, int64(350000000), hotTier)
		assert.Equal(tt, int64(150000000), coldTier)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("HandlesEmptyVolumesList", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		dbVolumeMap := map[string]*datamodel.Volume{}
		ontapVolumes := []*vsa.Volume{}
		expectedVolCount := int64(0)

		hotTier, coldTier, err := calculateAndUpdateHotColdTierConsumption(ctx, ontapVolumes, expectedVolCount, mockStorage, dbVolumeMap)

		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), hotTier)
		assert.Equal(tt, int64(0), coldTier)
	})
}
