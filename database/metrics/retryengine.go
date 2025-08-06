package database

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type retryEngine struct {
	dataStore *DataStoreRepository
}

func (re *retryEngine) logError(fun string, err error) {
	logger := log.NewLogger()
	logger.Error("Wrapped function returned error.", "function", fun, "err", err)
}
