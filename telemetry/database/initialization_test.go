package database

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/db"
)

func Test_InitializesDatabaseSuccessfully(t *testing.T) {
	origInitializeDatabase := db.InitializeDatabase
	// Mock the InitializeDatabase function to return a mock database connection
	db.InitializeDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (database.Storage, error) {
		mockDB := &database.MockStorage{} // Assuming you have a mock implementation
		return mockDB, nil
	}
	defer func() {
		db.InitializeDatabase = origInitializeDatabase
	}()
	orch := InitializeDatabase()

	assert.NotNil(t, orch)
}

func Test_ReturnsNilWhenDatabaseInitializationFails(t *testing.T) {
	origInitializeDatabase := db.InitializeDatabase
	db.InitializeDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (database.Storage, error) {
		return nil, errors.New("mocked database initialization failure")
	}
	defer func() {
		db.InitializeDatabase = origInitializeDatabase
	}()

	orch := InitializeDatabase()

	assert.Nil(t, orch)
}
