package active_directory_activities

import (
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/internal_active_directories"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	adHelper "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/helper"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/testsuite"
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

func newTestActivityEnv() *testsuite.TestActivityEnvironment {
	ts := &testsuite.WorkflowTestSuite{}
	return ts.NewTestActivityEnvironment()
}

func TestPushActiveDirectoryPasswordActivity(t *testing.T) {
	originalCvpClient := CvpClient
	originalGetSignedJwtToken := getSignedJwtToken
	defer func() {
		CvpClient = originalCvpClient
		getSignedJwtToken = originalGetSignedJwtToken
	}()
	getSignedJwtToken = func(string) (string, error) { return "test-token", nil }

	t.Run("ReturnsErrorWhenADIsNil", func(tt *testing.T) {
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PushActiveDirectoryPasswordActivity)

		_, err := env.ExecuteActivity(activityObj.PushActiveDirectoryPasswordActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   nil,
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenTokenFetchFails", func(tt *testing.T) {
		orig := getSignedJwtToken
		getSignedJwtToken = func(string) (string, error) { return "", assert.AnError }
		defer func() { getSignedJwtToken = orig }()

		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PushActiveDirectoryPasswordActivity)

		_, err := env.ExecuteActivity(activityObj.PushActiveDirectoryPasswordActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorOnCvpFailure", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{InternalActiveDirectories: fakeInternalADClient{err: assert.AnError}}
		}

		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PushActiveDirectoryPasswordActivity)

		_, err := env.ExecuteActivity(activityObj.PushActiveDirectoryPasswordActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		})
		assert.Error(tt, err)
	})

	t.Run("ReturnsRetryableErrorOnConflict", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{InternalActiveDirectories: fakeInternalADClient{
				err: &internal_active_directories.V1betaPushActiveDirectoryPasswordConflict{},
			}}
		}
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PushActiveDirectoryPasswordActivity)

		_, err := env.ExecuteActivity(activityObj.PushActiveDirectoryPasswordActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		})
		assert.Error(tt, err)
		customErr := vsaerrors.ExtractCustomError(err)
		assert.True(tt, customErr.IsError(vsaerrors.ErrADSyncADOperationInProgress))
	})

	t.Run("ReturnsErrorOnEmptyResponse", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{InternalActiveDirectories: fakeInternalADClient{resp: nil}}
		}

		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PushActiveDirectoryPasswordActivity)

		_, err := env.ExecuteActivity(activityObj.PushActiveDirectoryPasswordActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		})
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

		activityObj := ActiveDirectorySyncActivity{}
		testEnv := newTestActivityEnv()
		testEnv.RegisterActivity(activityObj.PushActiveDirectoryPasswordActivity)

		params := &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			XCorrelationID:    "corr",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}

		expectedSecret := adHelper.GeneratePasswordSecretId("secret-proj", "acct", "ad-name", "loc")
		val, err := testEnv.ExecuteActivity(activityObj.PushActiveDirectoryPasswordActivity, params)
		assert.NoError(tt, err)

		var result PushActiveDirectoryPasswordResult
		assert.NoError(tt, val.Get(&result))
		assert.Equal(tt, expectedOp.Name, result.Operation.Name)
		assert.Equal(tt, expectedSecret, result.SecretName)

		assert.NotNil(tt, capturedParams)
		assert.NotNil(tt, capturedParams.Body)
		assert.Equal(tt, "ad-id", capturedParams.Body.ActiveDirectoryID)
		assert.Equal(tt, "secret-proj", capturedParams.Body.SdeProjectID)
		expectedSecret = adHelper.GeneratePasswordSecretId("secret-proj", "acct", "ad-name", "loc")
		assert.Equal(tt, expectedSecret, capturedParams.Body.SecretName)
		if assert.NotNil(tt, capturedParams.XCorrelationID) {
			assert.Equal(tt, "corr", *capturedParams.XCorrelationID)
		}
		assert.Equal(tt, "acct", capturedParams.ProjectNumber)
		assert.Equal(tt, "loc", capturedParams.LocationID)
	})
}

func TestPollPushPasswordOperationActivity(t *testing.T) {
	originalCvpClient := CvpClient
	originalGetSignedJwtToken := getSignedJwtToken
	defer func() {
		CvpClient = originalCvpClient
		getSignedJwtToken = originalGetSignedJwtToken
	}()
	getSignedJwtToken = func(string) (string, error) { return "test-token", nil }

	t.Run("ReturnsWhenOperationIsNil", func(tt *testing.T) {
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{}, (*cvpModels.OperationV1beta)(nil))
		assert.NoError(tt, err)
	})

	t.Run("ReturnsErrorWhenOperationDoneWithError", func(tt *testing.T) {
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		done := true
		errObj := cvpModels.StatusV1Beta{Message: "failed"}
		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{}, &cvpModels.OperationV1beta{Done: &done, Error: &errObj})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenOperationNameMissing", func(tt *testing.T) {
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		done := false
		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{}, &cvpModels.OperationV1beta{Done: &done})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenTokenFetchFails", func(tt *testing.T) {
		orig := getSignedJwtToken
		getSignedJwtToken = func(string) (string, error) { return "", assert.AnError }
		defer func() { getSignedJwtToken = orig }()

		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		notDone := false
		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{
			AccountName:    "acct",
			LocationID:     "loc",
			XCorrelationID: "corr",
		}, &cvpModels.OperationV1beta{Done: &notDone, Name: "operations/test-op"})
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenPollReturnsError", func(tt *testing.T) {
		CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return cvpapi.Cvp{Async: fakeAsyncClient{err: assert.AnError}}
		}
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		done := false
		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{
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
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{
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
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

		_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{
			AccountName:    "acct",
			LocationID:     "loc",
			XCorrelationID: "corr",
		}, &cvpModels.OperationV1beta{Name: "operations/test-op"})
		assert.NoError(tt, err)
	})
}

func TestPollSdeCreateADActivity_OperationCompletedWithError(t *testing.T) {
	originalCvpClient := CvpClient
	originalGetSignedJwtToken := getSignedJwtToken
	defer func() {
		CvpClient = originalCvpClient
		getSignedJwtToken = originalGetSignedJwtToken
	}()
	getSignedJwtToken = func(string) (string, error) { return "test-token", nil }

	done := true
	code := float64(409)
	CvpClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return cvpapi.Cvp{Async: fakeAsyncClient{resp: &cvpModels.OperationV1beta{
			Done:  &done,
			Error: &cvpModels.StatusV1Beta{Code: code, Message: "conflict"},
			Name:  "operations/test-op",
		}}}
	}

	activityObj := ActiveDirectorySyncActivity{}
	env := newTestActivityEnv()
	env.RegisterActivity(activityObj.PollPushPasswordOperationActivity)

	notDone := false
	_, err := env.ExecuteActivity(activityObj.PollPushPasswordOperationActivity, &SyncActiveDirectoryParams{
		AccountName:    "acct",
		LocationID:     "loc",
		XCorrelationID: "corr",
	}, &cvpModels.OperationV1beta{Done: &notDone, Name: "operations/test-op"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "synchronizing Active Directory")
}

func TestCreateActiveDirectoryInVCPActivity(t *testing.T) {
	t.Run("ReturnsErrorWhenADIsNil", func(tt *testing.T) {
		activityObj := ActiveDirectorySyncActivity{}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.CreateActiveDirectoryInVCPActivity)

		_, err := env.ExecuteActivity(activityObj.CreateActiveDirectoryInVCPActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
		}, "secret-path")
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenGetAccountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(nil, assert.AnError)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.CreateActiveDirectoryInVCPActivity)

		_, err := env.ExecuteActivity(activityObj.CreateActiveDirectoryInVCPActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}, "secret-path")
		assert.Error(tt, err)
	})

	t.Run("ReturnsErrorWhenCreateADFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil)
		mockStorage.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-id", int64(1)).Return(nil, nil)
		mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.CreateActiveDirectoryInVCPActivity)

		_, err := env.ExecuteActivity(activityObj.CreateActiveDirectoryInVCPActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			LocationID:        "loc",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}, "secret-path")
		assert.Error(tt, err)
	})

	t.Run("ReturnsExistingADWhenFoundByUUID", func(tt *testing.T) {
		existing := &datamodel.ActiveDirectory{BaseModel: datamodel.BaseModel{ID: 99}}

		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil)
		mockStorage.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-id", int64(1)).Return(existing, nil)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.CreateActiveDirectoryInVCPActivity)

		val, err := env.ExecuteActivity(activityObj.CreateActiveDirectoryInVCPActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}, "secret-path")
		assert.NoError(tt, err)

		var ad datamodel.ActiveDirectory
		assert.NoError(tt, val.Get(&ad))
		assert.Equal(tt, int64(99), ad.ID)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("ReturnsErrorWhenFetchExistingADFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil)
		mockStorage.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-id", int64(1)).Return(nil, assert.AnError)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.CreateActiveDirectoryInVCPActivity)

		_, err := env.ExecuteActivity(activityObj.CreateActiveDirectoryInVCPActivity, &SyncActiveDirectoryParams{
			ActiveDirectoryID: "ad-id",
			AccountName:       "acct",
			ActiveDirectory:   &models.ActiveDirectory{AdName: "ad-name"},
		}, "secret-path")
		assert.Error(tt, err)
	})

	t.Run("ReturnsCreatedADWithAttributes", func(tt *testing.T) {
		originalSecretProject := env.SecretManagerProjectID
		env.SecretManagerProjectID = "secret-proj"
		defer func() { env.SecretManagerProjectID = originalSecretProject }()

		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetAccount", mock.Anything, "acct").Return(&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}}, nil)
		mockStorage.On("GetActiveDirectoryByUuidAndAccountId", mock.Anything, "ad-id", int64(1)).Return(nil, nil)
		mockStorage.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{
			BaseModel: datamodel.BaseModel{ID: 10},
			AdName:    "ad-name",
		}, nil)
		mockStorage.On("UpdateActiveDirectory", mock.Anything, mock.Anything).Return(&datamodel.ActiveDirectory{}, nil)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		testEnv := newTestActivityEnv()
		testEnv.RegisterActivity(activityObj.CreateActiveDirectoryInVCPActivity)

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

		val, err := testEnv.ExecuteActivity(activityObj.CreateActiveDirectoryInVCPActivity, params, "secret-path")
		assert.NoError(tt, err)

		var created datamodel.ActiveDirectory
		assert.NoError(tt, val.Get(&created))
		assert.Equal(tt, int64(10), created.ID)
		assert.Equal(tt, "secret-path", created.CredentialPath)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdatePoolActiveDirectoryIDActivity(t *testing.T) {
	t.Run("ReturnsErrorOnUpdateFailure", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-uuid", mock.Anything).Return(assert.AnError)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.UpdatePoolActiveDirectoryIDActivity)

		_, err := env.ExecuteActivity(activityObj.UpdatePoolActiveDirectoryIDActivity, &SyncActiveDirectoryParams{PoolUUID: "pool-uuid"}, int64(11))
		assert.Error(tt, err)
	})

	t.Run("ReturnsNilOnSuccess", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-uuid", mock.Anything).Return(nil)

		activityObj := ActiveDirectorySyncActivity{SE: mockStorage}
		env := newTestActivityEnv()
		env.RegisterActivity(activityObj.UpdatePoolActiveDirectoryIDActivity)

		_, err := env.ExecuteActivity(activityObj.UpdatePoolActiveDirectoryIDActivity, &SyncActiveDirectoryParams{PoolUUID: "pool-uuid"}, int64(11))
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}
