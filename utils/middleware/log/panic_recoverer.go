package log

import (
	"encoding/json"
	"errors"
	"net/http"
	"runtime/debug"
	"strings"
	"syscall"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
)

func RecoverMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		slogger := req.Context().Value(utilsmiddleware.ContextSLoggerKey).(Logger)
		defer RecoverAndExecHTTPServer(slogger, func(r interface{}) {
			message := "Internal server error - please contact support"
			code := float64(http.StatusInternalServerError)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			err := json.NewEncoder(w).Encode(&cvpmodels.Error{Code: code, Message: message})
			if err != nil {
				slogger.Errorf("Error encoding JSON response: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
		})
		h.ServeHTTP(w, req)
	})
}

type RecoveryFunc func(r interface{})

// RecoverAndExecHTTPServer recovers from a panic and logs out the panic and stack trace
// as well as, executing the specified recovery function (but only after a panic)
// If the error is due to the client hanging up, we just log a warning and do nothing else.
func RecoverAndExecHTTPServer(traceLog Logger, f RecoveryFunc) {
	if r := recover(); r != nil {
		e, ok := r.(error)
		if ok && (errors.Is(e, syscall.EPIPE) || errors.Is(e, syscall.ECONNRESET)) {
			traceLog.With(Fields{
				"error": r,
				"stack": strings.TrimRight(strings.ReplaceAll(strings.ReplaceAll(string(debug.Stack()), "\n", "|"), "\t", " "), "|"),
			}).Warn("Caller hung up")
		} else {
			logRecovery(traceLog, r, debug.Stack())
			if f != nil {
				defer Recover(traceLog) // XXX: in case the recovery function panics :P
				f(r)
			}
		}
	}
}

func logRecovery(slogger Logger, r interface{}, stack []byte) {
	slogger.With(Fields{
		"panic": r,
		"stack": strings.TrimRight(strings.ReplaceAll(strings.ReplaceAll(string(stack), "\n", "|"), "\t", " "), "|"),
	}).Error("Recovered from panic")
}

// Recover recovers from a panic and logs out the panic and stack trace
func Recover(traceLog Logger) {
	if r := recover(); r != nil {
		logRecovery(traceLog, r, debug.Stack())
	}
}
