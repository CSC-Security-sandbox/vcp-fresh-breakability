package endpoints

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
)

var (
	eventRetentionTestPoolUUID = uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
)

func listEventRetentionParams() oasgenserver.V1ListEventRetentionPoliciesParams {
	return oasgenserver.V1ListEventRetentionPoliciesParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
	}
}

func createEventRetentionParams() oasgenserver.V1CreateEventRetentionPolicyParams {
	return oasgenserver.V1CreateEventRetentionPolicyParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
	}
}

func getEventRetentionParams() oasgenserver.V1GetEventRetentionPolicyParams {
	return oasgenserver.V1GetEventRetentionPolicyParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
		PolicyName:    "test-policy",
	}
}

func deleteEventRetentionPolicyParams() oasgenserver.V1DeleteEventRetentionPolicyParams {
	return oasgenserver.V1DeleteEventRetentionPolicyParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
		PolicyName:    "test-policy",
	}
}

func updateEventRetentionPolicyParams() oasgenserver.V1UpdateEventRetentionPolicyParams {
	return oasgenserver.V1UpdateEventRetentionPolicyParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
		PolicyName:    "test-policy",
	}
}

const sampleListCLIOutput = `
Vserver           Name                Retention Period
----------------- ------------------- --------------------
svm1              policy-a            7 years
svm1              policy-b            30 months
2 entries were displayed.
`

const sampleGetCLIOutput = `
Policy Name: test-policy
Event Retention Period: 7 years
`

func TestListEventRetentionPolicies(t *testing.T) {
	listParams := listEventRetentionParams()
	handler := Handler{}
	ctx := enableSnaplockForEventRetentionTests(t)

	t.Run("WhenOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		badReq, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Event retention policy operation is disabled", badReq.Message)
	})

	t.Run("WhenIAMRoleMissing_ReturnsForbidden", func(t *testing.T) {
		res, err := handler.V1ListEventRetentionPolicies(context.Background(), listParams)
		require.NoError(t, err)
		forbidden, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesForbidden)
		require.True(t, ok, "expected Forbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
		assert.Contains(t, unauth.Message, "authentication error")
	})

	t.Run("WhenEnsureCertificateOrPasswordFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenNewOntapClientFails_ReturnsInternalServerError", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) {
			return nil, errors.New("no client")
		}

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "failed to connect to ONTAP")
	})

	t.Run("WhenNewOntapClientFailsWithProxyHTTPError_ReturnsMappedResponse", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) {
			return nil, &middleware.ProxyHTTPError{Status: http.StatusBadRequest, Message: "Pool is in deleting state"}
		}

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, http.StatusBadRequest, badReq.Code)
		assert.Equal(t, "Pool is in deleting state", badReq.Message)
	})

	t.Run("WhenCLISuccess_ReturnsRecords", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: sampleListCLIOutput}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		body, ok := res.(*oasgenserver.EBRPolicyResponse)
		require.True(t, ok, "expected EBRPolicyResponse, got %T", res)
		require.Len(t, body.Records, 2)
		assert.Equal(t, "policy-a", body.Records[0].Name)
		assert.Equal(t, "P7Y", body.Records[0].RetentionPeriod)
		assert.Equal(t, "policy-b", body.Records[1].Name)
		assert.Equal(t, "P30M", body.Records[1].RetentionPeriod)
		assert.True(t, body.NumRecords.Set)
		assert.Equal(t, 2, body.NumRecords.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: something failed"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIFails_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("cli request failed")).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "ONTAP operation failed")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenParseFails_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "garbage without table"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1ListEventRetentionPolicies(ctx, listParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionPoliciesInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		assert.Contains(t, internal.Message, "parse")
		mockClient.AssertExpectations(t)
	})
}

func TestGetEventRetentionPolicy(t *testing.T) {
	getParams := getEventRetentionParams()
	handler := Handler{}
	ctx := enableSnaplockForEventRetentionTests(t)

	t.Run("WhenOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		badReq, ok := res.(*oasgenserver.V1GetEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Event retention policy operation is disabled", badReq.Message)
	})

	t.Run("WhenIAMRoleMissing_ReturnsForbidden", func(t *testing.T) {
		res, err := handler.V1GetEventRetentionPolicy(context.Background(), getParams)
		require.NoError(t, err)
		forbidden, ok := res.(*oasgenserver.V1GetEventRetentionPolicyForbidden)
		require.True(t, ok, "expected Forbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1GetEventRetentionPolicyUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenExecuteCLIReturnsNotFound_Returns404", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{StatusCode: http.StatusBadRequest, Code: "4", Message: "entry doesn't exist"}).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1GetEventRetentionPolicyNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLISuccess_ReturnsPolicy", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: sampleGetCLIOutput}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		policy, ok := res.(*oasgenserver.EBRPolicy)
		require.True(t, ok, "expected EBRPolicy, got %T", res)
		assert.Equal(t, "test-policy", policy.Name)
		assert.Equal(t, "P7Y", policy.RetentionPeriod)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsGenericError_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("internal error")).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1GetEventRetentionPolicyInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenIsCLISuccessFalseWithNotFound_Returns404", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: policy not found"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1GetEventRetentionPolicyNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenParseFails_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "garbage no table"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1GetEventRetentionPolicyInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Contains(t, internal.Message, "parse")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenZeroRows_ReturnsNotFound", func(t *testing.T) {
		output := `
Vserver           Name                Retention Period
----------------- ------------------- --------------------
0 entries were displayed.
`
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: output}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1GetEventRetentionPolicy(ctx, getParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1GetEventRetentionPolicyNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		assert.Contains(t, notFound.Message, "not found")
		mockClient.AssertExpectations(t)
	})
}

func TestCreateEventRetentionPolicy(t *testing.T) {
	createParams := createEventRetentionParams()
	handler := Handler{}
	ctx := enableSnaplockForEventRetentionTests(t)
	req := &oasgenserver.EBRPolicy{Name: "new-policy", RetentionPeriod: "P7Y"}

	t.Run("WhenOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		res, err := handler.V1CreateEventRetentionPolicy(ctx, req, createParams)
		require.NoError(t, err)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Event retention policy operation is disabled", badReq.Message)
	})

	t.Run("WhenIAMRoleMissing_ReturnsForbidden", func(t *testing.T) {
		res, err := handler.V1CreateEventRetentionPolicy(context.Background(), req, createParams)
		require.NoError(t, err)
		forbidden, ok := res.(*oasgenserver.V1CreateEventRetentionPolicyForbidden)
		require.True(t, ok, "expected Forbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}

		res, err := handler.V1CreateEventRetentionPolicy(ctx, req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1CreateEventRetentionPolicyUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenCLISuccess_ReturnsPolicy", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Policy created."}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1CreateEventRetentionPolicy(ctx, req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		policy, ok := res.(*oasgenserver.EBRPolicy)
		require.True(t, ok, "expected EBRPolicy, got %T", res)
		assert.Equal(t, "new-policy", policy.Name)
		assert.Equal(t, "P7Y", policy.RetentionPeriod)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsError_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{StatusCode: http.StatusBadRequest, Code: "13114", Message: "policy already exists"}).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1CreateEventRetentionPolicy(ctx, req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Contains(t, badReq.Message, "already exists")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturns500_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{StatusCode: http.StatusInternalServerError, Message: "internal error"}).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1CreateEventRetentionPolicy(ctx, req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest (OntapCLIError from 500), got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenIsCLISuccessFalse_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: invalid retention period"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1CreateEventRetentionPolicy(ctx, req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})
}

func TestUpdateEventRetentionPolicy(t *testing.T) {
	updateParams := updateEventRetentionPolicyParams()
	handler := Handler{}
	ctx := enableSnaplockForEventRetentionTests(t)

	t.Run("WhenOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()
		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{RetentionPeriod: oasgenserver.NewOptString("P30M")}

		res, err := handler.V1UpdateEventRetentionPolicy(ctx, req, updateParams)
		require.NoError(t, err)
		badReq, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Event retention policy operation is disabled", badReq.Message)
	})

	t.Run("WhenIAMRoleMissing_ReturnsForbidden", func(t *testing.T) {
		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{RetentionPeriod: oasgenserver.NewOptString("P30M")}
		res, err := handler.V1UpdateEventRetentionPolicy(context.Background(), req, updateParams)
		require.NoError(t, err)
		forbidden, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyForbidden)
		require.True(t, ok, "expected Forbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenRetentionPeriodNotSet_ReturnsBadRequest", func(t *testing.T) {
		// Handler validates before calling ONTAP; no ExecuteCLI call.
		mockClient := &handlers.MockOntapClient{}
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{} // RetentionPeriod not set
		res, err := handler.V1UpdateEventRetentionPolicy(ctx, req, updateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "retention_period is required")
		mockClient.AssertNotCalled(t, "ExecuteCLI")
	})

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}
		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{
			RetentionPeriod: oasgenserver.NewOptString("P30M"),
		}

		res, err := handler.V1UpdateEventRetentionPolicy(ctx, req, updateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenCLISuccess_ReturnsOK", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Policy modified."}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{
			RetentionPeriod: oasgenserver.NewOptString("P30M"),
		}
		res, err := handler.V1UpdateEventRetentionPolicy(ctx, req, updateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyOK)
		require.True(t, ok, "expected V1UpdateEventRetentionPolicyOK, got %T", res)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIFails_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{StatusCode: http.StatusBadRequest, Code: "13115", Message: "policy not found"}).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{
			RetentionPeriod: oasgenserver.NewOptString("P30M"),
		}
		res, err := handler.V1UpdateEventRetentionPolicy(ctx, req, updateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 13115, badReq.Code) // ONTAP code is passed through
		assert.Contains(t, badReq.Message, "policy not found")
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenIsCLISuccessFalse_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: cannot modify policy"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		req := &oasgenserver.V1UpdateEventRetentionPolicyReq{
			RetentionPeriod: oasgenserver.NewOptString("P30M"),
		}
		res, err := handler.V1UpdateEventRetentionPolicy(ctx, req, updateParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1UpdateEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})
}

func TestDeleteEventRetentionPolicy(t *testing.T) {
	deleteParams := deleteEventRetentionPolicyParams()
	handler := Handler{}
	ctx := enableSnaplockForEventRetentionTests(t)

	t.Run("WhenOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		original := snapLockOperationEnabled
		snapLockOperationEnabled = false
		defer func() { snapLockOperationEnabled = original }()

		res, err := handler.V1DeleteEventRetentionPolicy(ctx, deleteParams)
		require.NoError(t, err)
		badReq, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Event retention policy operation is disabled", badReq.Message)
	})

	t.Run("WhenIAMRoleMissing_ReturnsForbidden", func(t *testing.T) {
		res, err := handler.V1DeleteEventRetentionPolicy(context.Background(), deleteParams)
		require.NoError(t, err)
		forbidden, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyForbidden)
		require.True(t, ok, "expected Forbidden, got %T", res)
		assert.Equal(t, 403, forbidden.Code)
		assert.Equal(t, snaplockIAMRoleRequiredMessage, forbidden.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}

		res, err := handler.V1DeleteEventRetentionPolicy(ctx, deleteParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenCLISuccess_ReturnsOK", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Policy deleted."}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1DeleteEventRetentionPolicy(ctx, deleteParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyOK)
		require.True(t, ok, "expected DeleteEventRetentionPolicyOK, got %T", res)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIFails_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{StatusCode: http.StatusInternalServerError, Message: "internal error"}).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1DeleteEventRetentionPolicy(ctx, deleteParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest (OntapCLIError from 500 body), got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenIsCLISuccessFalseWithDoesNotExist_Returns404", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: policy does not exist"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1DeleteEventRetentionPolicy(ctx, deleteParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenIsCLISuccessFalse_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: cannot delete policy in use"}, nil).Once()
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return nil }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return mockClient, nil }

		res, err := handler.V1DeleteEventRetentionPolicy(ctx, deleteParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1DeleteEventRetentionPolicyBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})
}
