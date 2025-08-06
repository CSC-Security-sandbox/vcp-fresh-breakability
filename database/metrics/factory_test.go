package database

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func mockFactory(config dbutils.DbConfig, logger log.Logger) (Storage, error) {
	return &MockStorage{}, nil
}

func mockErrorFactory(config dbutils.DbConfig, logger log.Logger) (Storage, error) {
	return nil, errors.New("factory error")
}

func TestRegisterAndNew_Success(t *testing.T) {
	registry = make(map[string]Factory) // Reset registry for test isolation
	Register("mockdb", mockFactory)

	config := dbutils.DbConfig{Type: "mockdb"}
	storage, err := New(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, storage)
}

func TestNew_UnsupportedType(t *testing.T) {
	registry = make(map[string]Factory) // Reset registry for test isolation
	config := dbutils.DbConfig{Type: "unknown"}
	storage, err := New(config, nil)
	assert.Error(t, err)
	assert.Nil(t, storage)
}

func TestNew_FactoryError(t *testing.T) {
	registry = make(map[string]Factory) // Reset registry for test isolation
	Register("errdb", mockErrorFactory)

	config := dbutils.DbConfig{Type: "errdb"}
	storage, err := New(config, nil)
	assert.Error(t, err)
	assert.Nil(t, storage)
}
