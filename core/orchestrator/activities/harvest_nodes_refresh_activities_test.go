package activities

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestGetNodeGroupMaps(t *testing.T) {
	t.Run("EmptyResult", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{}

		// Mock empty result
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: 0,
		}).Return([]*datamodel.NodeNodeGroupMap{}, nil)

		result, err := activity.GetNodeGroupMaps(ctx, params)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("SinglePage", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{}

		expectedMaps := []*datamodel.NodeNodeGroupMap{
			{BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 100},
			{BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 200},
		}

		// Mock single page result
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: 0,
		}).Return(expectedMaps, nil)

		result, err := activity.GetNodeGroupMaps(ctx, params)
		assert.NoError(t, err)
		assert.Equal(t, expectedMaps, result)
	})

	t.Run("MultiplePagesExactLimit", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{}

		// Create exactly paginationLimit items for first page
		firstPageMaps := make([]*datamodel.NodeNodeGroupMap, paginationLimit)
		for i := 0; i < paginationLimit; i++ {
			firstPageMaps[i] = &datamodel.NodeNodeGroupMap{
				BaseModel: datamodel.BaseModel{ID: int64(i + 1)},
				NodeID:    int64(100 + i),
			}
		}

		secondPageMaps := []*datamodel.NodeNodeGroupMap{
			{BaseModel: datamodel.BaseModel{ID: int64(paginationLimit + 1)}, NodeID: int64(100 + paginationLimit)},
			{BaseModel: datamodel.BaseModel{ID: int64(paginationLimit + 2)}, NodeID: int64(101 + paginationLimit)},
		}

		// Mock first page
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: 0,
		}).Return(firstPageMaps, nil)

		// Mock second page
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: paginationLimit,
		}).Return(secondPageMaps, nil)

		result, err := activity.GetNodeGroupMaps(ctx, params)
		assert.NoError(t, err)
		assert.Len(t, result, paginationLimit+2)
		assert.Equal(t, firstPageMaps[0], result[0])
		assert.Equal(t, secondPageMaps[1], result[paginationLimit+1])
	})

	t.Run("ErrorOnFirstPage", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{}

		expectedErr := assert.AnError

		// Mock error on first page
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: 0,
		}).Return(nil, expectedErr)

		result, err := activity.GetNodeGroupMaps(ctx, params)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})

	t.Run("ErrorOnSecondPage", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{}

		// Create exactly paginationLimit items for first page
		firstPageMaps := make([]*datamodel.NodeNodeGroupMap, paginationLimit)
		for i := 0; i < paginationLimit; i++ {
			firstPageMaps[i] = &datamodel.NodeNodeGroupMap{
				BaseModel: datamodel.BaseModel{ID: int64(i + 1)},
				NodeID:    int64(100 + i),
			}
		}

		expectedErr := assert.AnError

		// Mock successful first page
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: 0,
		}).Return(firstPageMaps, nil)

		// Mock error on second page
		mockStorage.EXPECT().ListNodeNodeGroupMap(ctx, false, &dbutils.Pagination{
			Limit:  paginationLimit,
			Offset: paginationLimit,
		}).Return(nil, expectedErr)

		result, err := activity.GetNodeGroupMaps(ctx, params)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		// Should return partial results collected so far
		assert.Equal(t, firstPageMaps, result)
	})
}

func TestRefreshHarvestNodes(t *testing.T) {
	t.Run("EmptyNodeGroupMaps", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{
			NodeGroupMaps: []*datamodel.NodeNodeGroupMap{},
			RefreshURL:    "",
		}

		err := activity.RefreshHarvestNodes(ctx, params)
		assert.NoError(t, err)
	})
	t.Run("AllInvalidNodeGroupMaps", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{
			NodeGroupMaps: []*datamodel.NodeNodeGroupMap{
				nil,
				{BaseModel: datamodel.BaseModel{ID: 1}, NodeID: 0},
				{BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 100, NodeGroup: nil},
			},
			RefreshURL: "",
		}

		err := activity.RefreshHarvestNodes(ctx, params)
		assert.NoError(t, err)
	})
	t.Run("MixedValidAndInvalidMaps", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		// Mock successful upload response
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Success"))
		}))
		defer mockServer.Close()

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{
			NodeGroupMaps: []*datamodel.NodeNodeGroupMap{
				nil, // Invalid
				{
					BaseModel:     datamodel.BaseModel{ID: 1},
					NodeID:        100,
					NodeGroup:     &datamodel.NodeGroup{LeaseName: "lease1"},
					HarvestConfig: &datamodel.HarvestConfig{}}, // Valid
				{
					BaseModel: datamodel.BaseModel{ID: 2}, NodeID: 0}, // Invalid
				{
					BaseModel:     datamodel.BaseModel{ID: 3},
					NodeID:        200,
					NodeGroup:     &datamodel.NodeGroup{LeaseName: "lease2"},
					HarvestConfig: &datamodel.HarvestConfig{}}, // Valid
			},
			RefreshURL: mockServer.URL,
		}

		err := activity.RefreshHarvestNodes(ctx, params)
		assert.NoError(t, err)
	})
	t.Run("ProcessingWithHttpError", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		// Mock server that returns error
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Internal Server Error"))
		}))
		defer mockServer.Close()

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{
			NodeGroupMaps: []*datamodel.NodeNodeGroupMap{
				{
					BaseModel: datamodel.BaseModel{ID: 1},
					NodeID:    100,
					NodeGroup: &datamodel.NodeGroup{LeaseName: "lease1"},
					HarvestConfig: &datamodel.HarvestConfig{
						SECRET_ID: "test-secretID",
					}},
			},
			RefreshURL: mockServer.URL,
		}

		// Should not return error as individual failures are logged but don't fail the entire operation
		err := activity.RefreshHarvestNodes(ctx, params)
		assert.NoError(t, err)
	})
	t.Run("ConcurrentProcessing", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)

		var requestCount int32
		var mu sync.Mutex

		// Mock server that tracks concurrent requests
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			atomic.AddInt32(&requestCount, 1)
			mu.Unlock()

			// Simulate processing time
			time.Sleep(10 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()
		// Create more maps than the concurrency limit
		var nodeGroupMaps []*datamodel.NodeNodeGroupMap
		for i := 0; i < harvestNodeRefreshMaxConcurrency+5; i++ {
			nodeGroupMaps = append(nodeGroupMaps, &datamodel.NodeNodeGroupMap{
				BaseModel: datamodel.BaseModel{ID: int64(i + 1)},
				NodeID:    int64(100 + i),
				NodeGroup: &datamodel.NodeGroup{LeaseName: fmt.Sprintf("lease%d", i)},
				HarvestConfig: &datamodel.HarvestConfig{
					SECRET_ID: "test-secretID",
				},
			})
		}

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{
			NodeGroupMaps: nodeGroupMaps,
			RefreshURL:    mockServer.URL,
		}
		err := activity.RefreshHarvestNodes(ctx, params)
		assert.NoError(t, err)
		assert.Equal(t, int32(len(nodeGroupMaps)), atomic.LoadInt32(&requestCount))
	})
	t.Run("TemplateRenderError", func(t *testing.T) {
		ctx := context.Background()
		mockStorage := database.NewMockStorage(t)
		oldRenderTemplate := renderTemplateForharvest
		// Mock template rendering to always return error
		renderTemplateForharvest = func(harvestConfig *datamodel.HarvestConfig) (string, error) {
			return "", fmt.Errorf("template render error")
		}
		defer func() {
			renderTemplateForharvest = oldRenderTemplate
		}()

		activity := HarvestNodesRefreshActivity{SE: mockStorage}
		params := &HarvestNodesRefreshActivityParams{
			NodeGroupMaps: []*datamodel.NodeNodeGroupMap{
				{
					BaseModel:     datamodel.BaseModel{ID: 1},
					NodeID:        100,
					NodeGroup:     &datamodel.NodeGroup{LeaseName: "lease1"},
					HarvestConfig: &datamodel.HarvestConfig{},
				},
			},
			RefreshURL: "",
		}

		// Should not return error as individual failures are logged but don't fail the entire operation
		err := activity.RefreshHarvestNodes(ctx, params)
		assert.NoError(t, err)
	})
}
