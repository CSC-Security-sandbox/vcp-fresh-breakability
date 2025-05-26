package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewTestStorage(t *testing.T) {
	logger := &log.MockLogger{}
	store, err := NewTestStorage(logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, store.db)
	assert.NotNil(t, store.dataStore)
}

func TestClearInMemoryDB(t *testing.T) {
	logger := &log.MockLogger{}
	store, err := NewTestStorage(logger)
	assert.NoError(t, err)

	err = ClearInMemoryDB(store.db.GORM())
	assert.NoError(t, err)
}
