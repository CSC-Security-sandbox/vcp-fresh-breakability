package database

import (
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"strings"
)

type retryEngine struct {
	dataStore *repository.DataStoreRepository
}

func (re *retryEngine) logError(fun string, err error) {
	logger := log.NewLogger()
	logger.Error("Wrapped function returned error.", "function", fun, "err", err)
}

var transientErrorCodes = map[string]struct{}{
	"08000": {}, "08003": {}, "08006": {}, "08001": {}, "08004": {}, "08007": {}, "08P01": {}, // Class 08 — Connection Exception
	"25000": {}, "25005": {}, "25P02": {}, "25P03": {}, // Class 25 — Invalid Transaction State
	"40000": {}, "40002": {}, "40001": {}, "40003": {}, "40P01": {}, // Class 40 — Transaction Rollback
	"53000": {}, "53100": {}, "53200": {}, "53300": {}, "53400": {}, // Class 53 — Insufficient Resources
	"55000": {}, "55006": {}, "55P03": {}, // Class 55 — Object Not In Prerequisite State
	"57000": {}, "57014": {}, "57P03": {}, "57P05": {}, // Class 57 — Operator Intervention
}

func getRootError(err error) error {
	unwrapped := errors.Unwrap(err)
	if unwrapped != nil {
		return getRootError(unwrapped)
	}
	return err
}

func isTransientErr(err error) bool {
	if err == nil {
		return false
	}

	// Handle Postgres specific errors
	// https://www.postgresql.org/docs/16/errcodes-appendix.html
	// This list may change with newer versions of postgres
	e := getRootError(err)
	if pgerr, ok := e.(*pgconn.PgError); ok {
		_, isTransient := transientErrorCodes[pgerr.Code]
		return isTransient
	}

	// Handle generic errors
	if strings.Contains(err.Error(), "dial error") {
		return true
	}
	if strings.Contains(err.Error(), "invalid connection") {
		return true
	}
	if strings.Contains(strings.ToLower(err.Error()), "unexpected eof") {
		return true
	}
	for errorCode := range transientErrorCodes {
		if strings.Contains(err.Error(), errorCode) {
			return true
		}
	}

	return false
}
