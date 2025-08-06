package database

import (
	"fmt"

	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// getVcpModels returns the list of Models to be migrated.
func getMetricModels() []interface{} {
	return []interface{}{
		&datamodel.HydratedMetrics{},
		&datamodel.AggregatedUsage{},
		&datamodel.BillingGcpUsage{},
		&datamodel.Job{},
	}
}

type Factory func(config dbutils.DbConfig, logger log.Logger) (Storage, error)

var registry = make(map[string]Factory)

func Register(dbType string, factory Factory) {
	registry[dbType] = factory
}

func New(config dbutils.DbConfig, logger log.Logger) (Storage, error) {
	factory, ok := registry[config.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
	return factory(config, logger)
}
