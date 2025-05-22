package transport

import (
	"regexp"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type ontapRESTError interface {
	GetPayload() *models.ErrorResponse
}

type ontapJobRESTError interface {
	GetPayload() *models.Job
}

type ontapRESTPrivError interface {
	GetPayload() *privmodels.ErrorResponse
}

var (
	whiteSpaceRegExp = regexp.MustCompile(`\r?\n|\t|\*\*`)
	spacesRegExp     = regexp.MustCompile("  +")
	// ConvertFromRESTError attempts to convert an ontap rest error (or ontap-rest job failure) to a handled error
	ConvertFromRESTError = _convertFromRESTError
)

func _convertFromRESTError(logger log.Logger, err error) error {
	switch t := err.(type) {
	case ontapRESTError:
		if t.GetPayload() != nil &&
			t.GetPayload().Error != nil &&
			t.GetPayload().Error.Code != nil &&
			t.GetPayload().Error.Message != nil {
			return handleError(logger, *t.GetPayload().Error.Code, *t.GetPayload().Error.Message)
		}
	case ontapJobRESTError:
		if t.GetPayload() != nil &&
			t.GetPayload().Error != nil &&
			t.GetPayload().Error.Code != nil &&
			t.GetPayload().Error.Message != nil {
			return handleError(logger, *t.GetPayload().Error.Code, *t.GetPayload().Error.Message)
		}
	case ontapRESTPrivError:
		if t.GetPayload() != nil &&
			t.GetPayload().Error != nil &&
			t.GetPayload().Error.Code != nil &&
			t.GetPayload().Error.Message != nil {
			return handleError(logger, *t.GetPayload().Error.Code, *t.GetPayload().Error.Message)
		}
	}

	return err
}

func handleError(logger log.Logger, code, message string) error {
	if strings.Contains(message, "entry doesn't exist") || strings.Contains(message, "entry not found") {
		return errors.NewNotFoundErr("entry", nil)
	}

	if code == "404" {
		return errors.NewNotFoundErr(strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(message), " not found", ""), "could not find ", ""), nil)
	}

	return convertToUserFacingError(logger, message)
}

var convertToUserFacingError = _convertToUserFacingError

func _convertToUserFacingError(traceLog log.Logger, reason string) error {
	// MD: NotFound and Conflict errors are used by the idempotency layer to make ontap-REST calls resilient against connection disruption
	// Make sure you understand that before adding them here
	reason = convertONTAPErrToRegexCompatibleErr(reason)
	return errors.New(reason)
}

func convertONTAPErrToRegexCompatibleErr(error string) string {
	error = whiteSpaceRegExp.ReplaceAllString(error, " ")
	error = spacesRegExp.ReplaceAllString(error, " ")
	return error
}
