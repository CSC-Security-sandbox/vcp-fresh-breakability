package datastores

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"

// Datastore is an interface that abstracts the data storage (Database) layer.
type Datastore interface {
	GetPool(uuid string) (*api.Pool, error)
	CreatePool(pool api.Pool) error
	UpdatePool(pool *api.Pool) error
	DeletePool(uuid string) error

	GetSVM(uuid string) (*api.Svm, error)
	CreateSVM(svm *api.Svm) error
	UpdateSVM(svm *api.Svm) error
	DeleteSVM(uuid string) error

	GetVolume(uuid string) (*api.Volume, error)
	CreateVolume(volume *api.Volume) error
	UpdateVolume(volume *api.Volume) error
	DeleteVolume(uuid string) error
}
