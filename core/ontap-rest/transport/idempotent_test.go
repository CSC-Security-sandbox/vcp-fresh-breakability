package transport

import (
	"bytes"
	"io"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/svm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestNewIdempotentTransport(t *testing.T) {
	mct := NewMockClientTransport(t)
	x := 1

	operation := &runtime.ClientOperation{
		Method: "OOPS",
	}
	eErr := errors.New("123")
	it := NewIdempotentTransport(mct, func(_operation *runtime.ClientOperation) (interface{}, error) {
		assert.Equal(t, operation, _operation)
		return x, eErr
	}).(*IdempotentTransport)
	assert.IsType(t, &IdempotentTransport{}, it)
	assert.Equal(t, it.transport, mct)

	mdl, err := it.conflictResolver(operation)

	assert.Equal(t, 1, mdl.(int))
	assert.Equal(t, eErr, err)
}

func TestIdempotentTransportSubmit(t *testing.T) {
	t.Run("WhenSubmitFails", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)

		tr := &IdempotentTransport{
			transport: mct,
		}
		operation := &runtime.ClientOperation{}
		eErr := errors.New("123")
		mct.On("Submit", operation).Return(nil, eErr).Once()

		result, err := tr.Submit(operation)

		assert.Nil(tt, result)
		assert.Equal(tt, eErr, err)
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
	})

	t.Run("WhenSubmitSucceedsPOSTDuplicateMethodNotImplemented", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)
		mcr := NewMockClientResponseReader(tt)
		operation := &runtime.ClientOperation{
			Method: "POST",
			Reader: mcr,
		}
		conflictFunc := func(operation *runtime.ClientOperation) (interface{}, error) {
			return nil, errors.NewNotImplementedYetErr()
		}
		tr := &IdempotentTransport{
			transport:        mct,
			conflictResolver: conflictFunc,
		}
		eErr := errors.NewConflictErr("error")
		mct.On("Submit", operation).Return(nil, eErr).Once()

		result, err := tr.Submit(operation)

		assert.Nil(tt, result)
		assert.EqualError(tt, eErr, err.Error())
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
	})

	t.Run("WhenSubmitSucceedsPOSTDuplicateMethodConflictNotResolved", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)
		mcrr := NewMockClientResponseReader(tt)

		operation := &runtime.ClientOperation{
			Method: "POST",
			Reader: mcrr,
		}
		eErr := errors.NewConflictErr("new conflict error")
		conflictFunc := func(_operation *runtime.ClientOperation) (interface{}, error) {
			assert.Equal(tt, operation, _operation)
			return nil, eErr
		}
		tr := &IdempotentTransport{
			transport:        mct,
			conflictResolver: conflictFunc,
		}
		mct.On("Submit", operation).Return(nil, errors.NewConflictErr("error")).Once()

		result, err := tr.Submit(operation)

		assert.Nil(tt, result)
		assert.Equal(tt, eErr, err)
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
	})

	t.Run("WhenSubmitSucceedsDeleteNotFound", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)
		mcrr := NewMockClientResponseReader(tt)
		tr := &IdempotentTransport{
			transport: mct,
		}
		operation := &runtime.ClientOperation{
			Method: "DELETE",
			Reader: mcrr,
		}
		mct.On("Submit", operation).Return(nil, errors.NewNotFoundErr("error", nil)).Once()
		mcrr.On("ReadResponse", &successEmptyResponse{code: 200}, &successEmptyConsumer{}).Return([]interface{}{}, nil).Once()

		result, err := tr.Submit(operation)

		assert.NotNil(tt, result)
		assert.Nil(tt, err)
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
		mcrr.AssertCalled(tt, "ReadResponse", &successEmptyResponse{code: 200}, &successEmptyConsumer{})
		mcrr.AssertExpectations(tt)
	})

	t.Run("WhenSucceedsConflictResolvedNoReturnRecords", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)
		mcrr := NewMockClientResponseReader(tt)

		operation := &runtime.ClientOperation{
			Method: "POST",
			Reader: mcrr,
		}

		conflictFuncCalled := false
		conflictFunc := func(_operation *runtime.ClientOperation) (interface{}, error) {
			assert.Equal(tt, operation, _operation)
			assert.False(tt, conflictFuncCalled)
			conflictFuncCalled = true
			return nil, nil
		}

		tr := &IdempotentTransport{
			transport:        mct,
			conflictResolver: conflictFunc,
		}
		mct.On("Submit", operation).Return(nil, errors.NewConflictErr("error")).Once()
		mcrr.On("ReadResponse", &successEmptyResponse{code: 201}, &successEmptyConsumer{}).Return([]interface{}{1}, nil).Once()

		result, err := tr.Submit(operation)

		assert.Nil(tt, err)
		assert.Equal(tt, []interface{}{1}, result)
		assert.True(tt, conflictFuncCalled)
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
		mcrr.AssertCalled(tt, "ReadResponse", &successEmptyResponse{code: 201}, &successEmptyConsumer{})
		mcrr.AssertExpectations(tt)
	})

	t.Run("WhenSucceedsWithRecord", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)
		mcrr := NewMockClientResponseReader(tt)
		operation := &runtime.ClientOperation{
			Method: "POST",
			Reader: mcrr,
		}
		pp := &svm.SvmCreateCreated{
			Payload: &models.SvmJobLinkResponse{Records: []*models.Svm{
				{Name: nillable.ToPointer("name")},
			}},
		}
		conflictFuncCalled := false
		conflictFunc := func(_operation *runtime.ClientOperation) (interface{}, error) {
			assert.Equal(tt, operation, _operation)
			assert.False(tt, conflictFuncCalled)
			conflictFuncCalled = true
			return pp, nil
		}
		tr := &IdempotentTransport{
			transport:        mct,
			conflictResolver: conflictFunc,
		}

		mct.On("Submit", operation).Return(nil, errors.NewConflictErr("error")).Once()

		result, err := tr.Submit(operation)
		assert.Nil(tt, err)
		assert.Equal(tt, pp, result)
		assert.True(tt, conflictFuncCalled)
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
	})

	t.Run("WhenSucceeds", func(tt *testing.T) {
		mct := NewMockClientTransport(tt)
		tr := &IdempotentTransport{
			transport: mct,
		}
		operation := &runtime.ClientOperation{}
		mct.On("Submit", operation).Return([]interface{}{}, nil).Once()

		result, err := tr.Submit(operation)
		assert.NotNil(tt, result)
		assert.Nil(tt, err)
		mct.AssertCalled(tt, "Submit", operation)
		mct.AssertExpectations(tt)
	})
}

func Test_successEmptyResponse(t *testing.T) {
	dummy := successEmptyResponse{code: 200}
	assert.Equal(t, 200, dummy.Code())
	assert.Equal(t, "", dummy.Message())
	assert.Equal(t, "", dummy.GetHeader("abc"))
	assert.Equal(t, []string{}, dummy.GetHeaders("cde"))
	assert.Equal(t, io.NopCloser(nopCloser{bytes.NewBufferString("")}), dummy.Body())
	assert.Nil(t, nopCloser{}.Close())
	assert.Nil(t, nillable.ToPointer(successEmptyConsumer{}).Consume(nil, nil))
}
