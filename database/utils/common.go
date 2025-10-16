package utils

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	PgInvalidCatalogName = "3D000" // Database doesn't exist
	PgDuplicateDatabase  = "42P04" // Database already exists
	Postgres             = "postgres"
	SQLite               = "sqlite3"
)

var transientErrorCodes = map[string]struct{}{
	"08000": {}, "08003": {}, "08006": {}, "08001": {}, "08004": {}, "08007": {}, "08P01": {}, // Class 08 — Connection Exception
	"25000": {}, "25005": {}, "25P02": {}, "25P03": {}, // Class 25 — Invalid Transaction State
	"40000": {}, "40002": {}, "40001": {}, "40003": {}, "40P01": {}, // Class 40 — Transaction Rollback
	"53000": {}, "53100": {}, "53200": {}, "53300": {}, "53400": {}, // Class 53 — Insufficient Resources
	"55000": {}, "55006": {}, "55P03": {}, // Class 55 — Object Not In Prerequisite State
	"57000": {}, "57014": {}, "57P03": {}, "57P05": {}, // Class 57 — Operator Intervention
}

func GetRootError(err error) error {
	unwrapped := errors.Unwrap(err)
	if unwrapped != nil {
		return GetRootError(unwrapped)
	}
	return err
}

func IsTransientErr(err error) bool {
	if err == nil {
		return false
	}

	// Handle Postgres specific errors
	// https://www.postgresql.org/docs/16/errcodes-appendix.html
	// This list may change with newer versions of postgres
	e := GetRootError(err)
	if pgerr, ok := e.(*pgconn.PgError); ok {
		_, isTransient := transientErrorCodes[pgerr.Code]
		return isTransient
	}

	// Get error message once for efficiency
	errMsgLower := strings.ToLower(e.Error())

	// Case-insensitive checks using pre-lowercased string
	if strings.Contains(errMsgLower, "dial error") {
		return true
	}
	if strings.Contains(errMsgLower, "invalid connection") {
		return true
	}

	if strings.Contains(errMsgLower, "unexpected eof") {
		return true
	}
	if strings.Contains(errMsgLower, "connection reset by peer") {
		return true
	}
	if strings.Contains(errMsgLower, "context canceled") {
		return true
	}

	// Check for Postgres error codes in error message
	for errorCode := range transientErrorCodes {
		if strings.Contains(err.Error(), errorCode) {
			return true
		} // Not safe to use lowercased string here
	}

	return false
}
