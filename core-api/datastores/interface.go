package datastores

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/datamodel"

// Datastore is an interface that abstracts the data storage (Database) layer.
type Datastore interface {
	GetPool(uuid string) (*datamodel.Pool, error)
	CreatePool(pool datamodel.Pool) error
	UpdatePool(pool datamodel.Pool) error
	DeletePool(uuid string) error
}
