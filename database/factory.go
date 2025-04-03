package database

import "fmt"

type Factory func(config DbConfig) (Storage, error)

var registry = make(map[string]Factory)

func Register(dbType string, factory Factory) {
	registry[dbType] = factory
}

func New(config DbConfig) (Storage, error) {
	factory, ok := registry[config.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
	return factory(config)
}
