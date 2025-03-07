package datastore

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"

type Datastore interface {
	GetPool(uuid string) (*api.Pool, error)
	CreatePool(pool *api.Pool) error
	UpdatePool(pool *api.Pool) error
	DeletePool(uuid string) error

	GetSVM(uuid string) (*api.SVM, error)
	CreateSVM(svm *api.SVM) error
	UpdateSVM(svm *api.SVM) error
	DeleteSVM(uuid string) error

	GetVolume(uuid string) (*api.Volume, error)
	CreateVolume(volume *api.Volume) error
	UpdateVolume(volume *api.Volume) error
	DeleteVolume(uuid string) error
}
