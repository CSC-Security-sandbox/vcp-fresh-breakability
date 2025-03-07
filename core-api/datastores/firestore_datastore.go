package datastores

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/labstack/gommon/log"
	"github.com/mitchellh/mapstructure"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"
)

// ensure that we've conformed to the `ServerInterface` with a compile-time check
var _ Datastore = (*FireStoreDatastore)(nil)

type FireStoreDatastore struct {
	projectID  string
	databaseID string
	firestore  *firestore.Client
}

func NewFireStoreDatastore(projectID, databaseID string) *FireStoreDatastore {
	ctx := context.Background()
	client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID)
	if err != nil {
		log.Fatalf("Failed to create Firestore client: %v", err)
	}

	return &FireStoreDatastore{
		projectID:  projectID,
		databaseID: databaseID,
		firestore:  client,
	}
}

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

func (d *FireStoreDatastore) CreatePool(pool api.Pool) error {
	result, err := d.firestore.Collection("pools").Doc(pool.Id).Set(context.Background(), pool)
	if err != nil {
		return err
	}
	log.Infof("Created Firestore Pool At %s", result.UpdateTime)

	return nil
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
	result, err := d.firestore.Collection("pools").Doc(uuid).Get(context.Background())
	if err != nil {
		log.Errorf("Error getting Firestore Pool at uuid %s: %v", uuid, err)
		return nil, err
	}

	pool := &api.Pool{}
	err = mapstructure.Decode(result.Data(), &pool)
	if err != nil {
		log.Errorf("Error decoding Firestore Pool at uuid %s: %v", uuid, err)
		return nil, err
	}
	return pool, nil
}
