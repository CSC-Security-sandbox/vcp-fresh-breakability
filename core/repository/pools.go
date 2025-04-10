package repository

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

type DataStoreRepository struct {
	db *gorm.Wrapper
}

func NewDataStoreRepository(db *gorm.Wrapper) *DataStoreRepository {
	return &DataStoreRepository{db: db}
}

func (d *DataStoreRepository) CreatePool(ctx context.Context, pool *datamodel.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) GetPool(ctx context.Context, id string) (*datamodel.Pool, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) UpdatePool(ctx context.Context, pool *datamodel.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) DeletePool(ctx context.Context, id string) error {
	//TODO implement me
	panic("implement me")
}

func (d *DataStoreRepository) ListPools(ctx context.Context) ([]*datamodel.Pool, error) {
	//TODO implement me
	panic("implement me")
}
