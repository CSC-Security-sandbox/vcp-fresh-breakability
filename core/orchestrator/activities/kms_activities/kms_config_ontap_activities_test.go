package kms_activities

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestConfigureKmsForSvmActivity(t *testing.T) {
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenProviderNotFound", func(t *testing.T) {
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}

		// Patch activities.GetProviderByNode to return nil
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return nil, errors.New("provider not found")
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "provider not found")
	})
	t.Run("ConfigureKmsForSvmActivityWhenEmptyKmsConfigId", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: ""}
		// Patch activities.GetProviderByNode to return a dummy provider
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()
		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenGetKmsConfigFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}
		// Patch activities.GetProviderByNode to return a dummy provider
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(nil, errors.New("db error")).Once()
		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "db error")
	})
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenDecryptPasswordFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}

		origDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(_ log.Secret) (*string, error) { return nil, errors.New("decrypt error") }
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		kmsConfig := &datamodel.KmsConfig{ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(kmsConfig, nil).Once()
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()
		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "decrypt error")
	})
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenBase64DecodeFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}

		origDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := "not-base64"
			return &s, nil
		}
		defer func() { utils.DecryptPassword = origDecryptPassword }()

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()
		kmsConfig := &datamodel.KmsConfig{ServiceAccount: &datamodel.ServiceAccount{}}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(kmsConfig, nil).Once()
		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenCreateKmsConfigFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}

		origDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := base64.StdEncoding.EncodeToString([]byte("key"))
			return &s, nil
		}
		defer func() { utils.DecryptPassword = origDecryptPassword }()
		kmsConfig := &datamodel.KmsConfig{ServiceAccount: &datamodel.ServiceAccount{}, KmsAttributes: &datamodel.KmsAttributes{}}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(kmsConfig, nil).Once()
		mockProvider.On("CreateKmsConfig", mock.Anything).Return(nil, errors.New("provider error")).Once()
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "provider error")
	})
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenUpdateSvmWithKmsConfigIDsFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}
		resp := &vsa.CreateKmsConfigResponse{}
		kmsConfig := &datamodel.KmsConfig{ServiceAccount: &datamodel.ServiceAccount{}, KmsAttributes: &datamodel.KmsAttributes{}}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(kmsConfig, nil).Once()
		mockProvider.On("CreateKmsConfig", mock.Anything).Return(resp, nil).Once()
		mockSE.On("UpdateSvmWithKmsConfigIDs", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("update error")).Once()

		origDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := base64.StdEncoding.EncodeToString([]byte("key"))
			return &s, nil
		}
		defer func() { utils.DecryptPassword = origDecryptPassword }()

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "update error")
	})
	t.Run("ConfigureKmsForSvmActivityReturnsErrorWhenUpdateKmsConfigStateFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}
		resp := &vsa.CreateKmsConfigResponse{}
		kmsConfig := &datamodel.KmsConfig{ServiceAccount: &datamodel.ServiceAccount{}, KmsAttributes: &datamodel.KmsAttributes{}}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(kmsConfig, nil).Once()
		mockProvider.On("CreateKmsConfig", mock.Anything).Return(resp, nil).Once()
		mockSE.On("UpdateSvmWithKmsConfigIDs", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("state error")).Once()
		origDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := base64.StdEncoding.EncodeToString([]byte("key"))
			return &s, nil
		}
		defer func() { utils.DecryptPassword = origDecryptPassword }()

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "state error")
	})
	t.Run("ConfigureKmsForSvmActivityReturnsUpdatedSvmOnSuccess", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockSE := database.NewMockStorage(t)
		mockActivity := &KmsConfigActivity{SE: mockSE}
		ctx := context.Background()
		svm := &datamodel.Svm{Name: "svm1"}
		node := &coreModels.Node{}
		params := commonparams.CreatePoolParams{KmsConfigId: "kms-uuid"}
		resp := &vsa.CreateKmsConfigResponse{}
		kmsConfig := &datamodel.KmsConfig{ServiceAccount: &datamodel.ServiceAccount{}, KmsAttributes: &datamodel.KmsAttributes{}}
		mockSE.On("GetKmsConfig", mock.Anything, mock.Anything).Return(kmsConfig, nil).Once()
		mockProvider.On("CreateKmsConfig", mock.Anything).Return(resp, nil).Once()
		mockSE.On("UpdateSvmWithKmsConfigIDs", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(svm, nil).Once()
		mockSE.On("UpdateKmsConfigState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
		origDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(_ log.Secret) (*string, error) {
			s := base64.StdEncoding.EncodeToString([]byte("key"))
			return &s, nil
		}
		defer func() { utils.DecryptPassword = origDecryptPassword }()

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		result, err := mockActivity.ConfigureKmsForSvmActivity(ctx, svm, node, params)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestCheckVsaKmsConfigReachableActivity(t *testing.T) {
	t.Run("CheckVsaKmsConfigReachableActivityReturnsErrorWhenProviderNotFound", func(t *testing.T) {
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{}
		node := &coreModels.Node{}

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return nil, errors.New("provider not found")
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := mockActivity.CheckVsaKmsConfigReachableActivity(ctx, svm, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider not found")
	})
	t.Run("CheckVsaKmsConfigReachableActivityReturnsNoErrorWhenKmsIsReachable", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{SvmDetails: &datamodel.SvmDetails{ExternalKmsConfigUUID: "uuid"}}
		node := &coreModels.Node{}

		mockProvider.On("IsGcpKmsReachable", mock.Anything).Return(true, nil).Once()
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := mockActivity.CheckVsaKmsConfigReachableActivity(ctx, svm, node)
		assert.NoError(t, err)
	})
	t.Run("CheckVsaKmsConfigReachableActivityReturnsPermissionDeniedError", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{SvmDetails: &datamodel.SvmDetails{ExternalKmsConfigUUID: "uuid"}}
		node := &coreModels.Node{}

		mockProvider.On("IsGcpKmsReachable", mock.Anything).Return(false, errors.New("permission_denied")).Once()
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := mockActivity.CheckVsaKmsConfigReachableActivity(ctx, svm, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lacks permission")
	})
	t.Run("CheckVsaKmsConfigReachableActivityReturnsInvalidJwtSignatureError", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{SvmDetails: &datamodel.SvmDetails{ExternalKmsConfigUUID: "uuid"}}
		node := &coreModels.Node{}

		mockProvider.On("IsGcpKmsReachable", mock.Anything).Return(false, errors.New("Invalid JWT Signature")).Once()
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := mockActivity.CheckVsaKmsConfigReachableActivity(ctx, svm, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to establish connectivity")
	})
	t.Run("CheckVsaKmsConfigReachableActivityReturnsNonRetryableErrorForOtherErrors", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{SvmDetails: &datamodel.SvmDetails{ExternalKmsConfigUUID: "uuid"}}
		node := &coreModels.Node{}

		mockProvider.On("IsGcpKmsReachable", mock.Anything).Return(false, errors.New("some other error")).Once()
		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) { return mockProvider, nil }
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := mockActivity.CheckVsaKmsConfigReachableActivity(ctx, svm, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "GCP KMS key is not reachable from VSA Clusters")
	})
}

func TestGetOntapRestProviderForPoolActivity(t *testing.T) {
	t.Run("WhenGetOntapRestProviderForPoolReturnsError", func(t *testing.T) {
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		pool := &datamodel.Pool{}

		origGetOntapRestProviderForPool := backgroundactivities.GetOntapRestProviderForPool
		backgroundactivities.GetOntapRestProviderForPool = func(_ context.Context, _ database.Storage, _ *datamodel.Pool) (vsa.Provider, error) {
			return nil, errors.New("Unable to provision provider")
		}
		defer func() { backgroundactivities.GetOntapRestProviderForPool = origGetOntapRestProviderForPool }()

		provider, err := mockActivity.GetOntapRestProviderForPoolActivity(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Unable to provision provider")
		assert.Nil(t, provider)
	})
	t.Run("WhenGetOntapRestProviderForPoolReturnsProvider", func(t *testing.T) {
		mockActivity := &KmsConfigActivity{}
		ctx := context.Background()
		pool := &datamodel.Pool{}
		providerVSA := vsa.NewProvider(ctx, vsa.ProviderDetails{})

		origGetOntapRestProviderForPool := backgroundactivities.GetOntapRestProviderForPool
		backgroundactivities.GetOntapRestProviderForPool = func(_ context.Context, _ database.Storage, _ *datamodel.Pool) (vsa.Provider, error) {
			return providerVSA, nil
		}
		defer func() { backgroundactivities.GetOntapRestProviderForPool = origGetOntapRestProviderForPool }()

		provider, err := mockActivity.GetOntapRestProviderForPoolActivity(ctx, pool)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})
}

func TestDeleteEkmConfigActivity(t *testing.T) {
	t.Run("WhenProviderNotFound", func(tt *testing.T) {
		kmsConfigActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{}
		node := &coreModels.Node{}

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return nil, errors.New("provider not found")
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := kmsConfigActivity.DeleteEkmConfigActivity(ctx, node, svm)
		assert.Error(tt, err)
		assert.Errorf(tt, err, "provider not found")
	})
	t.Run("WhenSvmDetailsIsNil", func(tt *testing.T) {
		kmsConfigActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{}
		node := &coreModels.Node{}
		mockProvider := new(vsa.MockProvider)

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()
		err := kmsConfigActivity.DeleteEkmConfigActivity(ctx, node, svm)
		assert.Error(tt, err)
		assert.Errorf(tt, err, "Unable to determine External-UUID of EKM since SvmDetails field of Svm DataModel is nil")
	})
	t.Run("WhenDeleteEkmConfigReturnsError", func(tt *testing.T) {
		kmsConfigActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{SvmDetails: &datamodel.SvmDetails{ExternalKmsConfigUUID: "externalUUID1"}}
		node := &coreModels.Node{}
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("DeleteEkmConfig", mock.Anything).Return(errors.New("ekm deletion failed"))

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()

		err := kmsConfigActivity.DeleteEkmConfigActivity(ctx, node, svm)

		assert.Error(tt, err)
		assert.Errorf(tt, err, "ekm deletion failed")
	})
	t.Run("WhenActivityIsSuccessful", func(tt *testing.T) {
		kmsConfigActivity := &KmsConfigActivity{}
		ctx := context.Background()
		svm := &datamodel.Svm{SvmDetails: &datamodel.SvmDetails{ExternalKmsConfigUUID: "externalUUID1"}}
		node := &coreModels.Node{}
		mockProvider := new(vsa.MockProvider)

		origGetProviderByNode := hyperscaler.GetProviderByNode
		hyperscaler.GetProviderByNode = func(_ context.Context, _ *coreModels.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { hyperscaler.GetProviderByNode = origGetProviderByNode }()
		mockProvider.On("DeleteEkmConfig", mock.Anything).Return(nil)

		err := kmsConfigActivity.DeleteEkmConfigActivity(ctx, node, svm)

		assert.NoError(tt, err)
		assert.Nil(tt, err)
	})
}
