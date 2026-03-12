package endpoints

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
)

func listEventRetentionOperationsParams() oasgenserver.V1ListEventRetentionOperationsParams {
	return oasgenserver.V1ListEventRetentionOperationsParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
	}
}

func createEventRetentionOperationParams() oasgenserver.V1CreateEventRetentionOperationParams {
	return oasgenserver.V1CreateEventRetentionOperationParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
	}
}

func getEventRetentionOperationParams(id int64) oasgenserver.V1GetEventRetentionOperationParams {
	return oasgenserver.V1GetEventRetentionOperationParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
		ID:            id,
	}
}

func abortEventRetentionOperationParams(id int64) oasgenserver.V1AbortEventRetentionOperationParams {
	return oasgenserver.V1AbortEventRetentionOperationParams{
		ProjectNumber: "123456",
		LocationId:    "us-central1",
		PoolId:        eventRetentionTestPoolUUID,
		ID:            id,
	}
}

// Tabular output: Vserver, Operation Id, State, Path, Policy, Volume (separator line then data rows).
const sampleOperationsListCLIOutput = `
Vserver    Operation Id    State        Path    Policy    Volume
---------- --------------- ----------- ------- --------  -------
svm1       16842754        in_progress  /       p1day     vol1
svm1       16842755        completed    /dir   p7y       vol2
2 entries were displayed.
`

// Real ONTAP default table: Operation ID, Vserver, Volume, Operation Status (Vserver and Volume may be one token when single space between them).
const sampleOperationsListCLIOutputOntapFormat = `
Operation ID   Vserver         Volume          Operation Status
-------------- --------------- --------------- ----------------
16842753       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
16842754       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
16842755       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
3 entries were displayed.
`

const sampleOperationGetCLIOutput = `
Vserver: svm1
Operation Id: 16842754
State: in_progress
Path: /
Policy Name: p1day
Volume Name: vol1
`

func TestListEventRetentionOperations(t *testing.T) {
	params := listEventRetentionOperationsParams()
	handler := Handler{}

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1ListEventRetentionOperationsUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenEnsureCertificateFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1ListEventRetentionOperationsUnauthorized)
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
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, errors.New("no client") }
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionOperationsInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
	})

	t.Run("WhenCLISuccess_ReturnsRecords", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: sampleOperationsListCLIOutput}, nil).Once()
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
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		body, ok := res.(*oasgenserver.EBROperationResponse)
		require.True(t, ok, "expected EBROperationResponse, got %T", res)
		require.Len(t, body.Records, 2)
		assert.True(t, body.Records[0].ID.IsSet())
		assert.Equal(t, int64(16842754), body.Records[0].ID.Value)
		assert.True(t, body.Records[0].State.IsSet())
		assert.Equal(t, "in_progress", body.Records[0].State.Value)
		assert.Equal(t, "/", body.Records[0].Path.Value)
		assert.Equal(t, "p1day", body.Records[0].Policy.Value.Name.Value)
		assert.Equal(t, "vol1", body.Records[0].Volume.Value.Name.Value)
		assert.True(t, body.NumRecords.IsSet())
		assert.Equal(t, 2, body.NumRecords.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLISuccess_OntapDefaultTableFormat_ReturnsRecords", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: sampleOperationsListCLIOutputOntapFormat}, nil).Once()
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
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		body, ok := res.(*oasgenserver.EBROperationResponse)
		require.True(t, ok, "expected EBROperationResponse, got %T", res)
		require.Len(t, body.Records, 3)
		assert.Equal(t, int64(16842753), body.Records[0].ID.Value)
		assert.True(t, body.Records[0].Svm.IsSet())
		assert.Equal(t, "gcnv-dfcb696927b4c65-svm-01", body.Records[0].Svm.Value.Name.Value)
		assert.Equal(t, "snaplock_vol1", body.Records[0].Volume.Value.Name.Value)
		assert.Equal(t, "completed", body.Records[0].State.Value)
		assert.Equal(t, 3, body.NumRecords.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIFails_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("cli failed")).Once()
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
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionOperationsInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: permission denied"}, nil).Once()
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
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionOperationsInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenParseFails_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "garbage output with no table"}, nil).Once()
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
		res, err := handler.V1ListEventRetentionOperations(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1ListEventRetentionOperationsInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})
}

func TestCreateEventRetentionOperation(t *testing.T) {
	createParams := createEventRetentionOperationParams()
	handler := Handler{}

	t.Run("WhenReqNil_ReturnsBadRequest", func(t *testing.T) {
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), nil, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "path")
	})

	t.Run("WhenPathEmpty_ReturnsBadRequest", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "path")
	})

	t.Run("WhenPolicyNameEmpty_ReturnsBadRequest", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: ""},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "policy")
	})

	t.Run("WhenVolumeMissing_ReturnsBadRequest", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1"},
		}
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "volume")
	})

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1CreateEventRetentionOperationUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenEnsureCertificateFails_ReturnsUnauthorized", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1CreateEventRetentionOperationUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenNewOntapClientFails_ReturnsInternalServerError", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
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
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, errors.New("no client") }
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1CreateEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
	})

	t.Run("WhenVolumeNameProvidedAndCLISuccess_ReturnsEBROperation", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Operation started."}, nil).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		body, ok := res.(*oasgenserver.EBROperation)
		require.True(t, ok, "expected EBROperation, got %T", res)
		assert.True(t, body.State.IsSet())
		assert.Equal(t, "in_progress", body.State.Value)
		assert.Equal(t, "/", body.Path.Value)
		assert.Equal(t, "p1day", body.Policy.Value.Name.Value)
		assert.Equal(t, "vol1", body.Volume.Value.Name.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeUUIDProvided_CallsGetVolumeThenApply", func(t *testing.T) {
		volUUID := uuid.MustParse("b96f976e-404b-11e9-bff2-0050568e4dbe")
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				UUID: oasgenserver.NewOptUUID(volUUID),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		volInfo := &handlers.VolumeInfo{Name: "resolved-vol", UUID: volUUID.String()}
		volInfo.SVM.Name = "svm1"
		mockClient.On("GetVolume", mock.Anything, volUUID.String()).
			Return(volInfo, nil).Once()
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Operation started."}, nil).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		body, ok := res.(*oasgenserver.EBROperation)
		require.True(t, ok, "expected EBROperation, got %T", res)
		assert.Equal(t, "in_progress", body.State.Value)
		assert.Equal(t, "resolved-vol", body.Volume.Value.Name.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeUUIDProvidedAndGetVolumeFails_ReturnsNotFound", func(t *testing.T) {
		volUUID := uuid.MustParse("b96f976e-404b-11e9-bff2-0050568e4dbe")
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				UUID: oasgenserver.NewOptUUID(volUUID),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, volUUID.String()).
			Return(nil, errors.New("volume not found")).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1CreateEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenVolumeUUIDProvidedAndGetVolumeReturnsEmptyName_ReturnsInternalServerError", func(t *testing.T) {
		volUUID := uuid.MustParse("b96f976e-404b-11e9-bff2-0050568e4dbe")
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				UUID: oasgenserver.NewOptUUID(volUUID),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("GetVolume", mock.Anything, volUUID.String()).
			Return(&handlers.VolumeInfo{Name: ""}, nil).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1CreateEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsError_ReturnsInternalServerError", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("connection refused")).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1CreateEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIError_ReturnsBadRequest", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{Code: "400", Message: "invalid policy"}).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_NotFound_ReturnsNotFound", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1CreateEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_Other_ReturnsBadRequest", func(t *testing.T) {
		req := &oasgenserver.EBROperationCreate{
			Path:   "/",
			Policy: oasgenserver.EBROperationCreatePolicy{Name: "p1day"},
			Volume: oasgenserver.NewOptEBROperationCreateVolume(oasgenserver.EBROperationCreateVolume{
				Name: oasgenserver.NewOptString("vol1"),
			}),
		}
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: permission denied"}, nil).Once()
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
		res, err := handler.V1CreateEventRetentionOperation(context.Background(), req, createParams)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1CreateEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})
}

func TestGetEventRetentionOperation(t *testing.T) {
	params := getEventRetentionOperationParams(16842754)
	handler := Handler{}

	t.Run("WhenEnsureCertificateFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, nil }
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1GetEventRetentionOperationUnauthorized)
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
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, errors.New("no client") }
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1GetEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIErrorNotFound_ReturnsNotFound", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{Code: "4", Message: "operation does not exist"}).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1GetEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsNonOntapCLError_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("network error")).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1GetEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_NotFound_ReturnsNotFound", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: operation not found"}, nil).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1GetEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenParseFails_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "garbage with no key-value or table"}, nil).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1GetEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenParseReturnsZeroRows_ReturnsNotFound", func(t *testing.T) {
		// Table with separator but no data rows (0 entries) so parser returns empty slice.
		zeroRowsOutput := "Operation ID   Vserver   Volume   Operation Status\n-------------- --------- --------------- ----------------\n0 entries were displayed.\n"
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: zeroRowsOutput}, nil).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1GetEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLISuccess_ReturnsEBROperation", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: sampleOperationGetCLIOutput}, nil).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		body, ok := res.(*oasgenserver.EBROperation)
		require.True(t, ok, "expected EBROperation, got %T", res)
		assert.True(t, body.ID.IsSet())
		assert.Equal(t, int64(16842754), body.ID.Value)
		assert.Equal(t, "in_progress", body.State.Value)
		assert.Equal(t, "/", body.Path.Value)
		assert.Equal(t, "p1day", body.Policy.Value.Name.Value)
		assert.Equal(t, "vol1", body.Volume.Value.Name.Value)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		// Use an error that does not trigger 404 (not "not found" / "does not exist") so handler returns 500.
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: permission denied"}, nil).Once()
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
		res, err := handler.V1GetEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1GetEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})
}

func TestAbortEventRetentionOperation(t *testing.T) {
	params := abortEventRetentionOperationParams(16842754)
	handler := Handler{}

	t.Run("WhenSetupCredentialsFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = oldSetup }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no credentials")
		}
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1AbortEventRetentionOperationUnauthorized)
		require.True(t, ok, "expected Unauthorized, got %T", res)
		assert.Equal(t, 401, unauth.Code)
	})

	t.Run("WhenEnsureCertificateFails_ReturnsUnauthorized", func(t *testing.T) {
		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		oldClient := newOntapClientFromContext
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
			newOntapClientFromContext = oldClient
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, nil }
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		unauth, ok := res.(*oasgenserver.V1AbortEventRetentionOperationUnauthorized)
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
		newOntapClientFromContext = func(context.Context) (handlers.OntapClient, error) { return nil, errors.New("no client") }
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1AbortEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIErrorNotFound_ReturnsNotFound", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{Code: "4", Message: "operation does not exist"}).Once()
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
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1AbortEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsOntapCLIErrorOther_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, &handlers.OntapCLIError{Code: "400", Message: "invalid state"}).Once()
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
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1AbortEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenExecuteCLIReturnsNonOntapCLError_ReturnsInternalServerError", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("network error")).Once()
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
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		internal, ok := res.(*oasgenserver.V1AbortEventRetentionOperationInternalServerError)
		require.True(t, ok, "expected InternalServerError, got %T", res)
		assert.Equal(t, 500, internal.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_ReturnsBadRequest", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: permission denied"}, nil).Once()
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
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		badReq, ok := res.(*oasgenserver.V1AbortEventRetentionOperationBadRequest)
		require.True(t, ok, "expected BadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLISuccess_ReturnsOK", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Operation aborted."}, nil).Once()
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
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1AbortEventRetentionOperationOK)
		require.True(t, ok, "expected V1AbortEventRetentionOperationOK, got %T", res)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenCLINotSuccess_ReturnsBadRequestOrNotFound", func(t *testing.T) {
		mockClient := &handlers.MockOntapClient{}
		mockClient.On("ExecuteCLI", mock.Anything, mock.Anything, mock.Anything).
			Return(&handlers.CLIResponse{Output: "Error: operation not found"}, nil).Once()
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
		res, err := handler.V1AbortEventRetentionOperation(context.Background(), params)
		require.NoError(t, err)
		require.NotNil(t, res)
		notFound, ok := res.(*oasgenserver.V1AbortEventRetentionOperationNotFound)
		require.True(t, ok, "expected NotFound, got %T", res)
		assert.Equal(t, 404, notFound.Code)
		mockClient.AssertExpectations(t)
	})
}
