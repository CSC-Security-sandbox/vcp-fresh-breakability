package database

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type Factory func(config DbConfig, logger log.Logger) (Storage, error)

var registry = make(map[string]Factory)

func Register(dbType string, factory Factory) {
	registry[dbType] = factory
}

func New(config DbConfig, logger log.Logger) (Storage, error) {
	factory, ok := registry[config.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
	return factory(config, logger)
}
