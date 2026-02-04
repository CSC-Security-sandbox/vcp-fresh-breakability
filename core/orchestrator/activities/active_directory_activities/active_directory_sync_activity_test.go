package active_directory_activities

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/internal_active_directories"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type fakeInternalADClient struct {
	resp   *internal_active_directories.V1betaPushActiveDirectoryPasswordCreated
	err    error
	onCall func(params *internal_active_directories.V1betaPushActiveDirectoryPasswordParams)
}

func (f fakeInternalADClient) V1betaPushActiveDirectoryPassword(params *internal_active_directories.V1betaPushActiveDirectoryPasswordParams) (*internal_active_directories.V1betaPushActiveDirectoryPasswordCreated, error) {
	if f.onCall != nil {
		f.onCall(params)
	}
	return f.resp, f.err
}

func (f fakeInternalADClient) SetTransport(runtime.ClientTransport) {}

type fakeAsyncClient struct {
	resp *cvpModels.OperationV1beta
	err  error
}

func (f fakeAsyncClient) V1betaDescribeOperation(params *async.V1betaDescribeOperationParams) (*async.V1betaDescribeOperationOK, error) {
	return &async.V1betaDescribeOperationOK{Payload: f.resp}, f.err
}

func (f fakeAsyncClient) SetTransport(runtime.ClientTransport) {}

func buildAuthContext() context.Context {
	header := http.Header{}
	header.Set("Authorization", "Bearer test-token")
	return context.WithValue(context.Background(), middleware.HeaderContextKey, header)
}

func TestPushActiveDirectoryPasswordActivity(t *testing.T) {
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()

	t.Run("ReturnsErrorWhenADIsNil", func(tt *testing.T) {
		activity := ActiveDirectorySyncActivity{}
		_, err := activity.PushActiveDirectoryPasswordActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   nil,
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorOnCvpFailure", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{InternalActiveDirectories: fakeInternalADClient{err: assert.AnError}}
		}

		activity := ActiveDirectorySyncActivity{}
		params := &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}
		_, err := activity.PushActiveDirectoryPasswordActivity(buildAuthContext(), params)
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorOnEmptyResponse", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{InternalActiveDirectories: fakeInternalADClient{resp: nil}}
		}

		activity := ActiveDirectorySyncActivity{}
		params := &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}
		_, err := activity.PushActiveDirectoryPasswordActivity(buildAuthContext(), params)
		assert.Error(tt, err)
	})

	t.Run("ReturnsSuccess", func(tt *testing.T) {
		originalSecretProject := env.SecretManagerProjectID
		env.SecretManagerProjectID = "secret-proj"
		defer func() { env.SecretManagerProjectID = originalSecretProject }()

		expectedOp := &cvpModels.OperationV1beta{Name: "operation/123"}
		var capturedParams *internal_active_directories.V1betaPushActiveDirectoryPasswordParams
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{
				InternalActiveDirectories: fakeInternalADClient{
					resp: &internal_active_directories.V1betaPushActiveDirectoryPasswordCreated{
						Payload: expectedOp,
					},
					onCall: func(p *internal_active_directories.V1betaPushActiveDirectoryPasswordParams) {
						capturedParams = p
					},
				},
			}
		}

		activity := ActiveDirectorySyncActivity{}
		params := &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}

		result, err := activity.PushActiveDirectoryPasswordActivity(buildAuthContext(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedOp, result)
		assert.NotNil(tt, capturedParams)
		assert.NotNil(tt, capturedParams.Body)
		assert.Equal(tt, "secret-proj", capturedParams.Body.SdeProjectID)
	})
}

func TestPollPushPasswordOperationActivity(t *testing.T) {
	originalCvpClient := CvpClient
	defer func() { CvpClient = originalCvpClient }()

	t.Run("ReturnsWhenOperationIsNil", func(tt *testing.T) {
		activity := ActiveDirectorySyncActivity{}
		err := activity.PollPushPasswordOperationActivity(buildAuthContext(), &SyncActiveDirectoryParams{}, nil)
		assert.NoError(tt, err)
	})

	t.Run("ReturnsErrorWhenOperationDoneWithError", func(tt *testing.T) {
		activity := ActiveDirectorySyncActivity{}
		done := true
		errObj := cvpModels.StatusV1Beta{Message: "failed"}
		err := activity.PollPushPasswordOperationActivity(buildAuthContext(), &SyncActiveDirectoryParams{}, &cvpModels.OperationV1beta{Done: &done, Error: &errObj})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenOperationNameMissing", func(tt *testing.T) {
		activity := ActiveDirectorySyncActivity{}
		done := false
		err := activity.PollPushPasswordOperationActivity(buildAuthContext(), &SyncActiveDirectoryParams{}, &cvpModels.OperationV1beta{Done: &done})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenPollReturnsError", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Async: fakeAsyncClient{err: assert.AnError}}
		}
		activity := ActiveDirectorySyncActivity{}
		done := false
		err := activity.PollPushPasswordOperationActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			AccountName:    "acct",
			LocationID:     "loc",
			XCorrelationID: "corr",
		}, &cvpModels.OperationV1beta{Done: &done, Name: "operations/test-op"})
		assert.Error(tt, err)
	})

	t.Run("ReturnsTemporalErrorWhenJobNotFinished", func(tt *testing.T) {
		done := false
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Async: fakeAsyncClient{resp: &cvpModels.OperationV1beta{Done: &done, Name: "operations/test-op"}}}
		}
		activity := ActiveDirectorySyncActivity{}
		err := activity.PollPushPasswordOperationActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			AccountName:    "acct",
			LocationID:     "loc",
			XCorrelationID: "corr",
		}, &cvpModels.OperationV1beta{Name: "operations/test-op"})
		assert.Error(tt, err)
	})

	t.Run("ReturnsSuccessWhenPollCompletes", func(tt *testing.T) {
		done := true
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Async: fakeAsyncClient{resp: &cvpModels.OperationV1beta{Done: &done, Name: "operations/test-op"}}}
		}
		activity := ActiveDirectorySyncActivity{}
		err := activity.PollPushPasswordOperationActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			AccountName:    "acct",
			LocationID:     "loc",
			XCorrelationID: "corr",
		}, &cvpModels.OperationV1beta{Name: "operations/test-op"})
		assert.NoError(tt, err)
	})
}

func TestCreateActiveDirectoryInVCPActivity(t *testing.T) {
	activity := ActiveDirectorySyncActivity{}

	t.Run("ReturnsErrorWhenADIsNil", func(tt *testing.T) {
		_, err := activity.CreateActiveDirectoryInVCPActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenGetAccountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(nil, assert.AnError)

		activity.SE = mockStorage
		_, err := activity.CreateActiveDirectoryInVCPActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenCreateADFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil)
		mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		activity.SE = mockStorage
		_, err := activity.CreateActiveDirectoryInVCPActivity(buildAuthContext(), &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsCreatedADWithAttributes", func(tt *testing.T) {
		originalSecretProject := env.SecretManagerProjectID
		env.SecretManagerProjectID = "secret-proj"
		defer func() { env.SecretManagerProjectID = originalSecretProject }()

		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil)
		mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{ID: 10},
			AdName:    "ad-name",
		}, nil)
		mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{}, nil)

		activity.SE = mockStorage
		params := &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			ActiveDirectory: &models.ActiveDirectory{
				AdName:   "ad-name",
				Username: "user",
				Domain:   "domain",
				DNS:      "1.1.1.1",
				NetBIOS:  "NB",
				ActiveDirectoryAttributes: &models.ActiveDirectoryAttributes{
					OrganizationalUnit:         "ou",
					Site:                       "site",
					BackupOperators:            []string{"b1"},
					Administrators:             []string{"a1"},
					SecurityOperators:          []string{"s1"},
					KdcIP:                      "2.2.2.2",
					KdcHostname:                "host",
					AesEncryption:              true,
					EncryptDCConnections:       true,
					LdapSigning:                true,
					AllowLocalNFSUsersWithLdap: true,
					Description:                "desc",
				},
			},
		}

		created, err := activity.CreateActiveDirectoryInVCPActivity(buildAuthContext(), params)
		assert.NoError(tt, err)
		assert.NotNil(tt, created)
		assert.Equal(tt, int64(10), created.ID)
		assert.NotEmpty(tt, created.CredentialPath)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdatePoolActiveDirectoryIDActivity(t *testing.T) {
	activity := ActiveDirectorySyncActivity{}
	t.Run("ReturnsErrorOnUpdateFailure", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-uuid", mock.Anything).Return(assert.AnError)
		activity.SE = mockStorage

		err := activity.UpdatePoolActiveDirectoryIDActivity(buildAuthContext(), &SyncActiveDirectoryParams{PoolUUID: "pool-uuid"}, 11)
		assert.Error(tt, err)
	})

	t.Run("ReturnsNilOnSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-uuid", mock.Anything).Return(nil)
		activity.SE = mockStorage

		err := activity.UpdatePoolActiveDirectoryIDActivity(buildAuthContext(), &SyncActiveDirectoryParams{PoolUUID: "pool-uuid"}, 11)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}
