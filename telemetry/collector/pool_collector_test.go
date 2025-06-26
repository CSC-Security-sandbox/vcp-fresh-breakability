package collector

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	orch "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"testing"
)

func Test_ReturnsEmptyMetricsWhenNoPoolsFound(t *testing.T) {
	mockOrchestrator := orch.NewMockOrchestratorFactory(t)
	mockOrchestrator.On("ListAllPools", mock.Anything).Return(nil, nil)

	metrics, err := GetPoolMetrics(mockOrchestrator)

	assert.Empty(t, metrics)
	assert.EqualError(t, err, "no pools found from DB")
}

func Test_ReturnsMetricsForPoolsWithValidData(t *testing.T) {
	mockOrchestrator := orch.NewMockOrchestratorFactory(t)
	mockOrchestrator.On("ListAllPools", mock.Anything).Return([]*models.Pool{
		{BaseModel: models.BaseModel{UUID: "pool1"}, Name: "Pool1", SizeInBytes: 1024, Region: "us-east-1", AccountName: "Account1"},
		{BaseModel: models.BaseModel{UUID: "pool2"}, Name: "Pool2", SizeInBytes: 2048, Region: "us-west-1", AccountName: "Account2"},
	}, nil)

	metrics, err := GetPoolMetrics(mockOrchestrator)

	assert.NoError(t, err)
	assert.Len(t, metrics, 2)
	assert.Equal(t, "Pool1", *metrics[0].Metadata.ResourceName)
	assert.Equal(t, float64(1024), metrics[0].Value)
	assert.Equal(t, "Pool2", *metrics[1].Metadata.ResourceName)
	assert.Equal(t, float64(2048), metrics[1].Value)
}

func Test_ReturnsErrorWhenDatabaseFailsToListPools(t *testing.T) {
	mockOrchestrator := orch.NewMockOrchestratorFactory(t)
	mockOrchestrator.On("ListAllPools", mock.Anything).Return(nil, errors.New("database error"))

	metrics, err := GetPoolMetrics(mockOrchestrator)

	assert.Empty(t, metrics)
	assert.EqualError(t, err, "database error")
}
