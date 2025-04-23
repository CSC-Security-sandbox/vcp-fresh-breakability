package ontap_rest

import (
	"context"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/svm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

var resolveRESTClientRouterConflict = _resolveRESTClientRouterConflict

func _resolveRESTClientRouterConflict(logger log.Logger, rc RESTClient, operation *runtime.ClientOperation) (interface{}, error) {
	switch params := operation.Params.(type) {
	case *svm.SvmCreateParams:
		return resolveSvmCreateConflict(logger, rc.SVM(), params)
	default:
		return nil, errors.NewNotImplementedYetErr()
	}
}

var (
	timeSleep = time.Sleep
)

var resolveSvmCreateConflict = _resolveSvmCreateConflict

func _resolveSvmCreateConflict(logger log.Logger, svms SVMClient, params *svm.SvmCreateParams) (*svm.SvmCreateCreated, error) {
	t2 := time.Now().Add(timeout)
	for time.Now().Before(t2) {
		osvm, err := svms.SvmGet(&SvmGetParams{
			BaseParams: BaseParams{Fields: []string{"name", "ipspace.name", "state"}},
			SvmName:    *params.Info.Name,
		})
		if err != nil {
			if errors.IsNotFoundErr(err) {
				return nil, errors.New("storage server unexpectedly missing while creating")
			}
			return nil, err
		}

		if *osvm.Ipspace.Name != *params.Info.Ipspace.Name {
			return nil, errors.NewConflictErr("conflict in storage server - please contact support")
		}

		state := nillable.FromPointerWithFallback(osvm.State, "?")
		switch state {
		case models.SvmStateRunning:
			return &svm.SvmCreateCreated{Payload: &models.SvmJobLinkResponse{Records: []*models.Svm{&osvm.Svm}}}, nil
		case models.SvmStateStarting, models.SvmStateInitializing:
			logger.WithFields(log.Fields{
				"state":   state,
				"ipspace": *osvm.Ipspace.Name,
				"name":    *params.Info.Name,
			}).Warn(context.TODO(), "svm is in creating state")
			timeSleep(wait)
			continue
		default:
			logger.WithFields(log.Fields{
				"state":   state,
				"ipspace": *osvm.Ipspace.Name,
				"name":    *params.Info.Name,
			}).Error(context.TODO(), "svm is in an unexpected state while creating")
			return nil, errors.New("storage server in an unexpected state while creating")
		}
	}

	return nil, errors.New("timed out while attempting to create storage server")
}
