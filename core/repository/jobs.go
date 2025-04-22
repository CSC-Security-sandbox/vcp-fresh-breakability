package repository

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

func (d *DataStoreRepository) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) GetJob(ctx context.Context, id string) (*datamodel.Job, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) UpdateJobStatus(ctx context.Context, id string, status string) error {
	//TODO implement me
	panic("implement me")
}
