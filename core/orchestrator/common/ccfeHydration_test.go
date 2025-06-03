package common

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestHydrateReplicationCreate(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	replication := models.ReplicationHydrateObject{
		ResourceId:       "replication-name",
		ReplicationState: "CREATING",
	}
	region := "mocked-region"
	projectId := "mocked-project"
	volumeResourceID := "mocked-volume-id"
	token := "mocked-token"

	// Save and mock hydrateToCffe
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationCreate(ctx, mockLogger, replication, region, projectId, volumeResourceID, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationCreate(ctx, mockLogger, replication, region, projectId, volumeResourceID, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateVolumeCreate(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	volume := models.VolumeHydrateObject{
		ResourceId: "vol-1",
		VolumeId:   "uuid-1",
		PoolId:     "pool-1",
		Protocols:  []string{"NFS"},
		State:      "READY",
		QuotaInGib: 10,
	}
	location := "mocked-location"
	projectId := "mocked-project"
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateVolumeCreate(ctx, mockLogger, volume, location, projectId, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateVolumeCreate(ctx, mockLogger, volume, location, projectId, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateVolumeDelete(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	volumeResourceID := "vol-1"
	region := "mocked-region"
	projectId := "mocked-project"
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateVolumeDelete(ctx, mockLogger, volumeResourceID, region, projectId, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateVolumeDelete(ctx, mockLogger, volumeResourceID, region, projectId, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateReplicationDelete(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	replicationResourceId := "replication-1"
	volumeResourceID := "volume-1"
	region := "mocked-region"
	projectId := "mocked-project"
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationDelete(ctx, mockLogger, replicationResourceId, volumeResourceID, region, projectId, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationDelete(ctx, mockLogger, replicationResourceId, volumeResourceID, region, projectId, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateReplicationStateFunc(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	region := "mocked-region"
	projectId := "mocked-project"
	volumeResourceID := "mocked-volume-id"
	replicationId := "mocked-replication-id"
	state := models.VolumeReplicationHydrateState("READY")
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationState(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationState(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, token)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateReplicationStateAndTypeFunc(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	region := "mocked-region"
	projectId := "mocked-project"
	volumeResourceID := "mocked-volume-id"
	replicationId := "mocked-replication-id"
	state := models.VolumeReplicationHydrateState("READY")
	hybridReplicationType := models.HybridReplicationHydrateType("cres")
	token := "mocked-token"
	originalHydrateToCffe := hydrateToCffe
	defer func() { hydrateToCffe = originalHydrateToCffe }()
	t.Run("WhenHydrateToCffeReturnError", func(tt *testing.T) {
		expectedErr := &errs.CustomError{
			OriginalErr: errors.New("some error"),
		}
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return expectedErr
		}
		err := _hydrateReplicationStateAndType(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, hybridReplicationType, token)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, expectedErr, err)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		hydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateReplicationStateAndType(ctx, mockLogger, region, projectId, volumeResourceID, replicationId, state, hybridReplicationType, token)
		assert.NoError(tt, err, nil)
	})
}

func Test_doHydrateToCffe(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	testToken := "test-token"
	testURL := "http://example.com"
	testMethod := "POST"
	testBody := map[string]string{"foo": "bar"}

	// Save and mock dependencies
	originalJsonMarshal := jsonMarshal
	originalHttpNewRequest := httpNewRequest
	originalHttpClientDo := httpClientDo
	originalIoReadAll := ioReadAll
	originalJsonUnmarshal := jsonUnmarshal
	defer func() {
		jsonMarshal = originalJsonMarshal
		httpNewRequest = originalHttpNewRequest
		httpClientDo = originalHttpClientDo
		ioReadAll = originalIoReadAll
		jsonUnmarshal = originalJsonUnmarshal
	}()
	t.Run("WhenJsonMarshalFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return nil, errors.New("marshal error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("marshal error"))
	})

	t.Run("WhenHttpNewRequestFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			return nil, errors.New("request error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("request error"))
	})

	t.Run("WhenHttpClientDoFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("client do error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("client do error"))
	})

	t.Run("WhenResponseBodyReadFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(bytes.NewReader([]byte("body"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return nil, errors.New("read error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("read error"))
	})

	t.Run("WhenJsonUnmarshalFails", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Body: io.NopCloser(bytes.NewReader([]byte("body"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte("body"), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return errors.New("unmarshal error")
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
		assert.Equal(tt, err.(*errs.CustomError).Unwrap(), errors.New("unmarshal error"))
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		jsonMarshal = func(v any) ([]byte, error) {
			return []byte("{}"), nil
		}
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			req, _ := http.NewRequest(method, url, body)
			return req, nil
		}
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
				StatusCode: 200,
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte("{}"), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return nil
		}
		err := _doHydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.NoError(tt, err, nil)
	})
}

func TestHydrateToCffe(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	testToken := "test-token"
	testURL := "http://example.com"
	testMethod := "POST"
	testBody := map[string]string{"foo": "bar"}

	// Save and mock dependencies
	originalDoHydrateToCffe := doHydrateToCffe
	defer func() { doHydrateToCffe = originalDoHydrateToCffe }()
	t.Run("RetriesOn429WithQuotaLimit", func(tt *testing.T) {
		retryCount := 0
		doHydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			retryCount++
			httpCode := 429
			return &errs.CustomError{
				OriginalErr: errors.New("Quota limit exceeded"),
				HttpCode:    &httpCode,
				Message:     "Quota limit exceeded",
			}
		}
		_ = _hydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.GreaterOrEqual(tt, retryCount, 1)
	})

	t.Run("WhenDoHydrateToCffeReturnsNil", func(tt *testing.T) {
		doHydrateToCffe = func(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error {
			return nil
		}
		err := _hydrateToCffe(ctx, mockLogger, testBody, testURL, testMethod, testToken)
		assert.NoError(tt, err)
	})
}

func Test_getQuotaLimit(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	region := "mock-region"
	projectId := "mock-project"
	token := "mock-token"
	resourceType := ResourceTypeVolume
	originalHydrateToCffe := getQuotaLimitsForResource
	defer func() { getQuotaLimitsForResource = originalHydrateToCffe }()

	t.Run("WhenHydrateToCffeReturnsError", func(tt *testing.T) {
		expectedErr := errors.New("some error")

		getQuotaLimitsForResource = func(ctx context.Context, projectId string, region string, quotaType QuotaType, token string, logger log.Logger) (int, error) {
			return 0, errors.New("some error")
		}
		_, err := _getQuotaLimit(ctx, mockLogger, region, projectId, token, resourceType)
		assert.Equal(tt, expectedErr, err)
	})

	t.Run("WhenHydrateToCffeReturnsNil", func(tt *testing.T) {
		getQuotaLimitsForResource = func(ctx context.Context, projectId string, region string, quotaType QuotaType, token string, logger log.Logger) (int, error) {
			return 0, nil
		}
		_, err := _getQuotaLimit(ctx, mockLogger, region, projectId, token, resourceType)

		assert.Equal(tt, err, nil)
	})
}

func TestGetQuotaLimitsForResource(t *testing.T) {
	mockLogger := log.NewLogger()
	ctx := context.Background()
	projectId := "mock-project"
	region := "mock-region"
	quotaType := FlexVolumesPerRegion
	token := "mock-token"

	originalHttpNewRequest := httpNewRequest
	originalHttpClientDo := httpClientDo
	originalIoReadAll := ioReadAll
	originalJsonUnmarshal := jsonUnmarshal
	originalStringConvAtoi := stringConvAtoi
	defer func() {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = originalHttpClientDo
		ioReadAll = originalIoReadAll
		jsonUnmarshal = originalJsonUnmarshal
		stringConvAtoi = originalStringConvAtoi
	}()

	t.Run("WhenHttpNewRequestFails", func(tt *testing.T) {
		expectedErr := errors.New("request error")
		httpNewRequest = func(method, url string, body io.Reader) (*http.Request, error) {
			return nil, errors.New("request error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenHttpClientDoFails", func(tt *testing.T) {
		expectedErr := errors.New("client do error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return nil, expectedErr
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenIoReadAllFails", func(tt *testing.T) {
		expectedErr := errors.New("client do error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return nil, expectedErr
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenJsonUnmarshalFailsOnSuccess", func(tt *testing.T) {
		expectedErr := errors.New("unmarshal error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte("{}"), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return errors.New("unmarshal error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenStringConvAtoiFails", func(tt *testing.T) {
		expectedErr := errors.New("atoi error")
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"type":"quota","value":"notanint"}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{"type":"quota","value":"notanint"}`), nil
		}
		jsonUnmarshal = originalJsonUnmarshal
		stringConvAtoi = func(s string) (int, error) {
			return 0, errors.New("atoi error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, expectedErr, err.(*errs.CustomError).OriginalErr)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"type":"quota","value":"42"}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{"type":"quota","value":"42"}`), nil
		}
		jsonUnmarshal = originalJsonUnmarshal
		stringConvAtoi = strconv.Atoi
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 42, quota)
		assert.NoError(tt, err, nil)
	})

	t.Run("WhenStatusCodeNot200AndJsonUnmarshalFails", func(tt *testing.T) {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{}`), nil
		}
		jsonUnmarshal = func(data []byte, v any) error {
			return errors.New("unmarshal error")
		}
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Error(tt, err.(*errs.CustomError).Unwrap())
	})

	t.Run("WhenStatusCodeNot200AndSuccess", func(tt *testing.T) {
		httpNewRequest = originalHttpNewRequest
		httpClientDo = func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 400,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"code":400,"message":"quota error","status":"FAILED"}`))),
			}, nil
		}
		ioReadAll = func(r io.Reader) ([]byte, error) {
			return []byte(`{"code":400,"message":"quota error","status":"FAILED"}`), nil
		}
		jsonUnmarshal = originalJsonUnmarshal
		quota, err := _getQuotaLimitsForResource(ctx, projectId, region, quotaType, token, mockLogger)
		assert.Equal(tt, 0, quota)
		assert.Equal(tt, "quota error", err.(*errs.CustomError).GetMessage())
	})
}
