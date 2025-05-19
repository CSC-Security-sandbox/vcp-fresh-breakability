package ontap_rest

import (
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/svm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestResolveRESTClientRouterConflict(t *testing.T) {
	t.Run("WhenOperationIsNotImplemented_ThenReturnError", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		rc := NewMockRESTClient(tt)
		operation := &runtime.ClientOperation{Params: nil}

		_, err := resolveRESTClientRouterConflict(*logger, rc, operation)
		assert.EqualError(tt, err, "Not implemented yet")
	})

	t.Run("WhenOperationIsSvmCreate_ThenCallResolveSvmCreateConflict", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		rc := NewMockRESTClient(tt)
		params := &svm.SvmCreateParams{}
		operation := &runtime.ClientOperation{Params: params}

		rc.Mock.On("SVM").Return(NewMockSVMClient(tt))

		mockResolve := func(log.Slogger, SVMClient, *svm.SvmCreateParams) (*svm.SvmCreateCreated, error) {
			return &svm.SvmCreateCreated{}, nil
		}
		resolveSvmCreateConflict = mockResolve
		defer func() { resolveSvmCreateConflict = _resolveSvmCreateConflict }()

		result, err := resolveRESTClientRouterConflict(*logger, rc, operation)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})
}

func TestResolveSvmCreateConflict(t *testing.T) {
	t.Run("WhenSvmNotFound_ThenReturnError", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		svms := NewMockSVMClient(tt)
		params := &svm.SvmCreateParams{Info: &models.Svm{Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")}, Name: nillable.ToPointer("svm1")}}

		svms.Mock.On("SvmGet", mock.Anything).Return(nil, errors.NewNotFoundErr("svm", nil))

		_, err := resolveSvmCreateConflict(*logger, svms, params)
		assert.EqualError(tt, err, "storage server unexpectedly missing while creating")
	})

	t.Run("WhenIpspaceConflict_ThenReturnError", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		svms := NewMockSVMClient(tt)
		params := &svm.SvmCreateParams{Info: &models.Svm{Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")}, Name: nillable.ToPointer("svm1")}}

		svms.Mock.On("SvmGet", mock.Anything).Return(&Svm{Svm: models.Svm{Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace2")}}}, nil)

		_, err := resolveSvmCreateConflict(*logger, svms, params)
		assert.EqualError(tt, err, "conflict in storage server - please contact support")
	})

	t.Run("WhenSvmStateIsRunning_ThenReturnSvm", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		svms := NewMockSVMClient(tt)
		params := &svm.SvmCreateParams{Info: &models.Svm{Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")}, Name: nillable.ToPointer("svm1")}}

		svmResponse := &Svm{
			Svm: models.Svm{
				Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")},
				State:   nillable.ToPointer(models.SvmStateRunning),
			},
		}
		svms.Mock.On("SvmGet", mock.Anything).Return(svmResponse, nil)

		result, err := resolveSvmCreateConflict(*logger, svms, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, &svmResponse.Svm, result.Payload.Records[0])
	})

	t.Run("WhenSvmStateIsCreating_ThenRetry", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		svms := NewMockSVMClient(tt)
		params := &svm.SvmCreateParams{Info: &models.Svm{Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")}, Name: nillable.ToPointer("svm1")}}

		svmResponse := &Svm{
			Svm: models.Svm{
				Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")},
				State:   nillable.ToPointer(models.SvmStateStarting),
			},
		}
		svms.Mock.On("SvmGet", mock.Anything).Return(svmResponse, nil).Once()
		svms.Mock.On("SvmGet", mock.Anything).Return(&Svm{
			Svm: models.Svm{
				Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")},
				State:   nillable.ToPointer(models.SvmStateRunning),
			},
		}, nil)

		mockSleep := func(d time.Duration) {}
		timeSleep = mockSleep
		defer func() { timeSleep = time.Sleep }()

		result, err := resolveSvmCreateConflict(*logger, svms, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenSvmStateIsUnexpected_ThenReturnError", func(tt *testing.T) {
		logger := log.NewLogger().(*log.Slogger)
		svms := NewMockSVMClient(tt)
		params := &svm.SvmCreateParams{Info: &models.Svm{Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")}, Name: nillable.ToPointer("svm1")}}

		svmResponse := &Svm{
			Svm: models.Svm{
				Ipspace: &models.SvmInlineIpspace{Name: nillable.ToPointer("ipspace1")},
				State:   nillable.ToPointer("unexpected"),
			},
		}
		svms.Mock.On("SvmGet", mock.Anything).Return(svmResponse, nil)

		_, err := resolveSvmCreateConflict(*logger, svms, params)
		assert.EqualError(tt, err, "storage server in an unexpected state while creating")
	})
}
