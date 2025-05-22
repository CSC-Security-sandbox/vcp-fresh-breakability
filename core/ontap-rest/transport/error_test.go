package transport

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	utilsErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Mocks for error interfaces
type mockOntapRESTError struct {
	payload *models.ErrorResponse
}

func (m *mockOntapRESTError) Error() string {
	// Dummy implementation for the error interface
	return fmt.Sprintf("code %d: %+v", m.payload.Error.Code, m.payload.Error.Message)
}

func (m *mockOntapRESTError) GetPayload() *models.ErrorResponse { return m.payload }

type mockOntapJobRESTError struct {
	payload *models.Job
}

func (m *mockOntapJobRESTError) Error() string {
	// Dummy implementation for the error interface
	return fmt.Sprintf("code %d: %+v", m.payload.Error.Code, m.payload.Error.Message)
}

func (m *mockOntapJobRESTError) GetPayload() *models.Job { return m.payload }

type mockOntapRESTPrivError struct {
	payload *privmodels.ErrorResponse
}

func (m *mockOntapRESTPrivError) Error() string {
	// Dummy implementation for the error interface
	return fmt.Sprintf("code %d: %+v", m.payload.Error.Code, m.payload.Error.Message)
}

func (m *mockOntapRESTPrivError) GetPayload() *privmodels.ErrorResponse { return m.payload }

func TestConvertFromRESTError_OntapRESTError(t *testing.T) {
	logger := log.NewMockLogger(t)
	code := "404"
	msg := "entry not found"
	err := &mockOntapRESTError{
		payload: &models.ErrorResponse{
			Error: &models.ReturnedError{
				Code:    &code,
				Message: &msg,
			},
		},
	}
	got := ConvertFromRESTError(logger, err)
	assert.Error(t, got)
	assert.True(t, utilsErrors.IsNotFoundErr(got))
}

func TestConvertFromRESTError_OntapJobRESTError(t *testing.T) {
	logger := log.NewMockLogger(t)
	code := "404"
	msg := "entry doesn't exist"
	err := &mockOntapJobRESTError{
		payload: &models.Job{
			Error: &models.JobInlineError{
				Code:    &code,
				Message: &msg,
			},
		},
	}
	got := ConvertFromRESTError(logger, err)
	assert.Error(t, got)
	assert.True(t, utilsErrors.IsNotFoundErr(got))
}

func TestConvertFromRESTError_OntapRESTPrivError(t *testing.T) {
	logger := log.NewMockLogger(t)
	code := "404"
	msg := "could not find something"
	err := &mockOntapRESTPrivError{
		payload: &privmodels.ErrorResponse{
			Error: &privmodels.ReturnedError{
				Code:    &code,
				Message: &msg,
			},
		},
	}
	got := ConvertFromRESTError(logger, err)
	assert.Error(t, got)
	assert.True(t, utilsErrors.IsNotFoundErr(got))
}

func TestConvertFromRESTError_UnknownError(t *testing.T) {
	logger := log.NewMockLogger(t)

	origErr := errors.New("some other error")
	got := ConvertFromRESTError(logger, origErr)
	assert.Equal(t, origErr, got)
}

func TestHandleError_UserFacing(t *testing.T) {
	logger := log.NewMockLogger(t)

	code := "500"
	msg := "random error"
	got := handleError(logger, code, msg)
	assert.Error(t, got)
	assert.Contains(t, got.Error(), "random error")
}

func TestConvertToUserFacingError_Whitespace(t *testing.T) {
	logger := log.NewMockLogger(t)
	msg := "error with\nnewlines\tand  extra  spaces"
	got := convertToUserFacingError(logger, msg)
	assert.EqualError(t, got, "error with newlines and extra spaces")
}
