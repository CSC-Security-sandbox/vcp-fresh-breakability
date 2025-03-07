package datastore

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"

// ensure that we've conformed to the `ServerInterface` with a compile-time check
var _ Datastore = (*FireStoreDatastore)(nil)

type FireStoreDatastore struct{}

func (d *FireStoreDatastore) GetSVM(uuid string) (*api.SVM, error) {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) CreateSVM(svm *api.SVM) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) UpdateSVM(svm *api.SVM) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) DeleteSVM(uuid string) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) GetVolume(uuid string) (*api.Volume, error) {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) CreateVolume(volume *api.Volume) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) UpdateVolume(volume *api.Volume) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) DeleteVolume(uuid string) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) CreatePool(pool *api.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) UpdatePool(pool *api.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) DeletePool(uuid string) error {
	//TODO implement me
	panic("implement me")
}

func (d *FireStoreDatastore) GetPool(uuid string) (*api.Pool, error) {
	// Implementation logic here
	return nil, nil
}
