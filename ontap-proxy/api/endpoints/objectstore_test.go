package endpoints

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

var (
	objectStoreTestPoolUUID              = uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	objectStoreTestObjectStoreUUID       = uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")
	objectStoreTestDestinationEndpointUUID = uuid.MustParse("770e8400-e29b-41d4-a716-446655440002")
	objectStoreTestSnapshotUUID          = uuid.MustParse("880e8400-e29b-41d4-a716-446655440003")
)

func baseObjectStoreParams() (oasgenserver.V1GetDestinationEndpointInfoParams, oasgenserver.V1DeleteDestinationEndpointParams, oasgenserver.V1GetSnapshotsParams, oasgenserver.V1DeleteSnapshotParams) {
	common := struct {
		ProjectNumber        string
		LocationId           string
		PoolId               uuid.UUID
		ObjectStoreId        uuid.UUID
		DestinationEndpointId uuid.UUID
	}{
		ProjectNumber:        "123456",
		LocationId:           "us-central1",
		PoolId:               objectStoreTestPoolUUID,
		ObjectStoreId:        objectStoreTestObjectStoreUUID,
		DestinationEndpointId: objectStoreTestDestinationEndpointUUID,
	}
	getInfo := oasgenserver.V1GetDestinationEndpointInfoParams{
		ProjectNumber:         common.ProjectNumber,
		LocationId:            common.LocationId,
		PoolId:                common.PoolId,
		ObjectStoreId:         common.ObjectStoreId,
		DestinationEndpointId: common.DestinationEndpointId,
	}
	deleteEndpoint := oasgenserver.V1DeleteDestinationEndpointParams{
		ProjectNumber:         common.ProjectNumber,
		LocationId:            common.LocationId,
		PoolId:                common.PoolId,
		ObjectStoreId:         common.ObjectStoreId,
		DestinationEndpointId: common.DestinationEndpointId,
	}
	getSnapshots := oasgenserver.V1GetSnapshotsParams{
		ProjectNumber:         common.ProjectNumber,
		LocationId:            common.LocationId,
		PoolId:                common.PoolId,
		ObjectStoreId:         common.ObjectStoreId,
		DestinationEndpointId: common.DestinationEndpointId,
	}
	deleteSnapshot := oasgenserver.V1DeleteSnapshotParams{
		ProjectNumber:         common.ProjectNumber,
		LocationId:            common.LocationId,
		PoolId:                common.PoolId,
		ObjectStoreId:         common.ObjectStoreId,
		DestinationEndpointId: common.DestinationEndpointId,
		SnapshotId:            objectStoreTestSnapshotUUID,
	}
	return getInfo, deleteEndpoint, getSnapshots, deleteSnapshot
}

func TestV1GetDestinationEndpointInfo(t *testing.T) {
	getInfo, _, _, _ := baseObjectStoreParams()
	handler := Handler{}

	t.Run("WhenAdminCredentialOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = false
		defer func() { smcOperationEnabled = old }()

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetDestinationEndpointInfoBadRequest)
		require.True(t, ok, "expected GetDestinationEndpointInfoBadRequest, got %T", res)
		assert.Equal(t, 400, e.Code)
		assert.Equal(t, "Operation is disabled", e.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		old := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = old }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no creds")
		}

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetDestinationEndpointInfoInternalServerError)
		require.True(t, ok, "expected GetDestinationEndpointInfoInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "credentials")
	})

	t.Run("WhenEnsureCertificateOrPasswordFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetDestinationEndpointInfoInternalServerError)
		require.True(t, ok, "expected GetDestinationEndpointInfoInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "certificate")
	})

	t.Run("WhenNewOntapClientFromContextFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

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

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetDestinationEndpointInfoInternalServerError)
		require.True(t, ok, "expected GetDestinationEndpointInfoInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "connect to ONTAP")
	})

	t.Run("WhenOntapReturns200ValidJSON_ReturnsPassthrough", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Connection pool may call GET /api/svm/svms first; then our GET object-store path. Accept any, return 200 + JSON.
			// ObjectStoreEndpointInfo uses OptUUID for uuid, so value must be a valid UUID.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"uuid":"21c3abec-ee22-11ea-8048-00505682f04b"}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-getinfo-200-valid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.NoError(t, err)
		require.NotNil(t, res)
		info, ok := res.(*oasgenserver.ObjectStoreEndpointInfo)
		require.True(t, ok, "expected ObjectStoreEndpointInfo, got %T", res)
		require.NotNil(t, info)
	})

	t.Run("WhenOntapReturns200InvalidJSON_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Reachability check and our request both get 200 + invalid body; handler fails on parse.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-getinfo-200-invalid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetDestinationEndpointInfoInternalServerError)
		require.True(t, ok, "expected GetDestinationEndpointInfoInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "invalid ONTAP response")
	})

	t.Run("WhenOntapReturnsNon200_ReturnsErrorStatusCode", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Return 404 for all requests (reachability still succeeds; our request gets error response).
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"endpoint not found","code":"404"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-getinfo-non200"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1GetDestinationEndpointInfo(context.Background(), getInfo)
		require.Error(t, err)
		require.Nil(t, res)
		sc, ok := err.(*oasgenserver.ErrorStatusCode)
		require.True(t, ok, "expected ErrorStatusCode, got %T", err)
		assert.Equal(t, 404, sc.StatusCode)
		assert.Equal(t, 404, sc.Response.Code)
		assert.Contains(t, sc.Response.Message, "endpoint not found")
	})
}

func TestV1DeleteDestinationEndpoint(t *testing.T) {
	_, deleteEndpoint, _, _ := baseObjectStoreParams()
	handler := Handler{}

	t.Run("WhenAdminCredentialOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = false
		defer func() { smcOperationEnabled = old }()

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteDestinationEndpointBadRequest)
		require.True(t, ok, "expected DeleteDestinationEndpointBadRequest, got %T", res)
		assert.Equal(t, 400, e.Code)
		assert.Equal(t, "Operation is disabled", e.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		old := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = old }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no creds")
		}

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteDestinationEndpointInternalServerError)
		require.True(t, ok, "expected DeleteDestinationEndpointInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "credentials")
	})

	t.Run("WhenEnsureCertificateOrPasswordFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteDestinationEndpointInternalServerError)
		require.True(t, ok, "expected DeleteDestinationEndpointInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "certificate")
	})

	t.Run("WhenNewOntapClientFromContextFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

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

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteDestinationEndpointInternalServerError)
		require.True(t, ok, "expected DeleteDestinationEndpointInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "connect to ONTAP")
	})

	t.Run("WhenOntapReturns200ValidJSON_ReturnsOK", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"job":{"uuid":"job-1"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-ep-200"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1DeleteDestinationEndpointOK)
		require.True(t, ok, "expected DeleteDestinationEndpointOK, got %T", res)
	})

	t.Run("WhenOntapReturns202ValidJSON_ReturnsAccepted", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"job":{"uuid":"job-2"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-ep-202"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1DeleteDestinationEndpointAccepted)
		require.True(t, ok, "expected DeleteDestinationEndpointAccepted, got %T", res)
	})

	t.Run("WhenOntapReturns200InvalidJSON_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-ep-invalid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteDestinationEndpointInternalServerError)
		require.True(t, ok, "expected DeleteDestinationEndpointInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "invalid ONTAP response")
	})

	t.Run("WhenOntapReturnsNon200_ReturnsErrorStatusCode", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"message":"endpoint in use","code":"409"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-ep-409"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteDestinationEndpoint(context.Background(), deleteEndpoint)
		require.Error(t, err)
		require.Nil(t, res)
		sc, ok := err.(*oasgenserver.ErrorStatusCode)
		require.True(t, ok, "expected ErrorStatusCode, got %T", err)
		assert.Equal(t, 409, sc.StatusCode)
		assert.Contains(t, sc.Response.Message, "endpoint in use")
	})
}

func TestV1GetSnapshots(t *testing.T) {
	_, _, getSnapshots, _ := baseObjectStoreParams()
	handler := Handler{}

	t.Run("WhenAdminCredentialOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = false
		defer func() { smcOperationEnabled = old }()

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetSnapshotsBadRequest)
		require.True(t, ok, "expected GetSnapshotsBadRequest, got %T", res)
		assert.Equal(t, 400, e.Code)
		assert.Equal(t, "Operation is disabled", e.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		old := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = old }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no creds")
		}

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetSnapshotsInternalServerError)
		require.True(t, ok, "expected GetSnapshotsInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "credentials")
	})

	t.Run("WhenEnsureCertificateOrPasswordFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetSnapshotsInternalServerError)
		require.True(t, ok, "expected GetSnapshotsInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "certificate")
	})

	t.Run("WhenNewOntapClientFromContextFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

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

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetSnapshotsInternalServerError)
		require.True(t, ok, "expected GetSnapshotsInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "connect to ONTAP")
	})

	t.Run("WhenOntapReturns200ValidJSON_ReturnsPassthrough", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// SnapmirrorObjectStoreEndpointSnapshotResponse; snapshot uuid must be valid UUID.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"num_records":1,"records":[{"uuid":"04fb1ddb-2947-4eb0-af09-3eb6dc538926"}]}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-getsnap-200"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.SnapmirrorObjectStoreEndpointSnapshotResponse)
		require.True(t, ok, "expected SnapmirrorObjectStoreEndpointSnapshotResponse, got %T", res)
	})

	t.Run("WhenOntapReturns200InvalidJSON_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-getsnap-invalid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1GetSnapshotsInternalServerError)
		require.True(t, ok, "expected GetSnapshotsInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "invalid ONTAP response")
	})

	t.Run("WhenOntapReturnsNon200_ReturnsErrorStatusCode", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"message":"access denied","code":"403"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-getsnap-403"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1GetSnapshots(context.Background(), getSnapshots)
		require.Error(t, err)
		require.Nil(t, res)
		sc, ok := err.(*oasgenserver.ErrorStatusCode)
		require.True(t, ok, "expected ErrorStatusCode, got %T", err)
		assert.Equal(t, 403, sc.StatusCode)
		assert.Contains(t, sc.Response.Message, "access denied")
	})
}

func TestV1DeleteSnapshot(t *testing.T) {
	_, _, _, deleteSnapshot := baseObjectStoreParams()
	handler := Handler{}

	t.Run("WhenAdminCredentialOperationDisabled_ReturnsBadRequest", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = false
		defer func() { smcOperationEnabled = old }()

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteSnapshotBadRequest)
		require.True(t, ok, "expected DeleteSnapshotBadRequest, got %T", res)
		assert.Equal(t, 400, e.Code)
		assert.Equal(t, "Operation is disabled", e.Message)
	})

	t.Run("WhenSetupCredentialsFails_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		old := setupCredentialsForHandler
		defer func() { setupCredentialsForHandler = old }()
		setupCredentialsForHandler = func(context.Context, string, string, string) (context.Context, error) {
			return nil, errors.New("no creds")
		}

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteSnapshotInternalServerError)
		require.True(t, ok, "expected DeleteSnapshotInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "credentials")
	})

	t.Run("WhenEnsureCertificateOrPasswordFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) { return ctx, nil }
		ensureCertificateOrPassword = func(context.Context) error { return errors.New("no cert") }

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteSnapshotInternalServerError)
		require.True(t, ok, "expected DeleteSnapshotInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "certificate")
	})

	t.Run("WhenNewOntapClientFromContextFails_ReturnsInternalServerError", func(t *testing.T) {
		old := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = old }()

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

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteSnapshotInternalServerError)
		require.True(t, ok, "expected DeleteSnapshotInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "connect to ONTAP")
	})

	t.Run("WhenOntapReturns200ValidJSON_ReturnsOK", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"job":{"uuid":"job-snap-1"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-snap-200"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1DeleteSnapshotOK)
		require.True(t, ok, "expected DeleteSnapshotOK, got %T", res)
	})

	t.Run("WhenOntapReturns202ValidJSON_ReturnsAccepted", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"job":{"uuid":"job-snap-2"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-snap-202"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		_, ok := res.(*oasgenserver.V1DeleteSnapshotAccepted)
		require.True(t, ok, "expected DeleteSnapshotAccepted, got %T", res)
	})

	t.Run("WhenOntapReturns200InvalidJSON_ReturnsInternalServerError", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-snap-invalid"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.NoError(t, err)
		require.NotNil(t, res)
		e, ok := res.(*oasgenserver.V1DeleteSnapshotInternalServerError)
		require.True(t, ok, "expected DeleteSnapshotInternalServerError, got %T", res)
		assert.Equal(t, 500, e.Code)
		assert.Contains(t, e.Message, "invalid ONTAP response")
	})

	t.Run("WhenOntapReturnsNon200_ReturnsErrorStatusCode", func(t *testing.T) {
		oldAdmin := smcOperationEnabled
		smcOperationEnabled = true
		defer func() { smcOperationEnabled = oldAdmin }()

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"message":"snapshot not found","code":"404"}}`))
		}))
		defer server.Close()

		endpoint := server.Listener.Addr().String()
		cacheKey := "auth:objstore-delete-snap-404"
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "u",
			Password:    "p",
			PoolID:      cacheKey,
			AccountName: "test",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint},
			},
		}
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		oldSetup := setupCredentialsForHandler
		oldEnsure := ensureCertificateOrPassword
		defer func() {
			setupCredentialsForHandler = oldSetup
			ensureCertificateOrPassword = oldEnsure
		}()
		setupCredentialsForHandler = func(ctx context.Context, _, _ string, _ string) (context.Context, error) {
			return context.WithValue(ctx, models.AuthDataKey, cacheKey), nil
		}
		ensureCertificateOrPassword = func(context.Context) error { return nil }

		res, err := handler.V1DeleteSnapshot(context.Background(), deleteSnapshot)
		require.Error(t, err)
		require.Nil(t, res)
		sc, ok := err.(*oasgenserver.ErrorStatusCode)
		require.True(t, ok, "expected ErrorStatusCode, got %T", err)
		assert.Equal(t, 404, sc.StatusCode)
		assert.Contains(t, sc.Response.Message, "snapshot not found")
	})
}
