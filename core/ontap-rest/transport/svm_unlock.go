package transport

import (
	"reflect"

	goopenapiRuntime "github.com/go-openapi/runtime"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// SvmUnlockTransport is a wrapper for ClientTransport that attempts to unlock a svm and retrying the original operation
// as an added bonus the SvmUnlockTransport polls jobs automatically to completion
type SvmUnlockTransport struct {
	transport     goopenapiRuntime.ClientTransport
	svmUnlockFunc func() error
	pollingFunc   func(jobUUID string) error
}

// NewSvmUnlockTransport returns a new instance of SvmUnlockTransport
func NewSvmUnlockTransport(svmUnlockFunc func() error, pollingFunc func(jobUUID string) error, transport goopenapiRuntime.ClientTransport) *SvmUnlockTransport {
	return &SvmUnlockTransport{
		transport:     transport,
		svmUnlockFunc: svmUnlockFunc,
		pollingFunc:   pollingFunc,
	}
}

// Submit submits the transport operation
func (t *SvmUnlockTransport) Submit(operation *goopenapiRuntime.ClientOperation) (interface{}, error) {
	result, err := t.transport.Submit(operation)
	if err != nil {
		// MD: Never retry a svm unlock error that originates from a svm unlock operation
		if !errors.IsSvmLockedError(err) || operation.ID == "svm_unlock_create" {
			return nil, err
		}

		if err = t.svmUnlockFunc(); err != nil {
			return nil, err
		}

		return t.transport.Submit(operation)
	}

	noPoll := false
	// MD: volume clone split can take a long time, so we cannot auto poll here
	if vmp, ok := operation.Params.(*storage.VolumeModifyParams); ok {
		noPoll = vmp.Info != nil && vmp.Info.Clone != nil && vmp.Info.Clone.SplitInitiated != nil && *vmp.Info.Clone.SplitInitiated
	}

	if !noPoll {
		if jobUUID := getJobUUID(result); jobUUID != "" {
			if err = t.pollingFunc(jobUUID); err != nil {
				if !errors.IsSvmLockedError(err) {
					return nil, err
				}

				if err = t.svmUnlockFunc(); err != nil {
					return nil, err
				}

				result, err = t.transport.Submit(operation)
				if err != nil {
					return nil, err
				}

				if jobUUID = getJobUUID(result); jobUUID != "" {
					if err = t.pollingFunc(jobUUID); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return result, nil
}

type hasCode interface {
	Code() int
}

var getJobUUID = _getJobUUID

func _getJobUUID(i any) string {
	if accepted, ok := i.(hasCode); ok && accepted.Code() == 202 {
		payload := reflect.ValueOf(accepted).Elem().FieldByName("Payload")
		if payload.IsValid() && !payload.IsNil() && payload.Kind() == reflect.Pointer {
			jobValue := payload.Elem().FieldByName("Job")
			if jobValue.IsValid() && !jobValue.IsNil() && jobValue.Kind() == reflect.Pointer {
				if jt, ok := jobValue.Interface().(*models.JobLink); ok {
					return string(*jt.UUID)
				}
			}
		}
	}

	return ""
}
