package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"go.temporal.io/sdk/testsuite"
)

func TestFetchConstituentCountForLargeVolume(t *testing.T) {
	t.Run("Success_ReturnsConstituentCount", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchConstituentCountForLargeVolume)

		vol := &datamodel.Volume{
			Name:             "fg-vol",
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "ext-uuid-1"},
			Svm:              &datamodel.Svm{Name: "svm1"},
		}
		node := &models.Node{Name: "node1"}
		constCount := int32(4)
		mockProvider.On("GetVolume", vsa.GetVolumeParams{
			UUID:    "ext-uuid-1",
			SvmName: "svm1",
		}).Return(&vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{Name: "fg-vol", ExternalUUID: "ext-uuid-1"},
			ConstituentCount: &constCount,
		}, nil)

		val, err := env.ExecuteActivity(activity.FetchConstituentCountForLargeVolume, vol, node)
		assert.NoError(tt, err)
		var count int32
		_ = val.Get(&count)
		assert.Equal(tt, int32(4), count)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("Success_NoConstituentCount_ReturnsZero", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchConstituentCountForLargeVolume)

		vol := &datamodel.Volume{
			Name:             "flexvol",
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "ext-uuid-2"},
			Svm:              &datamodel.Svm{Name: "svm1"},
		}
		node := &models.Node{Name: "node1"}
		mockProvider.On("GetVolume", vsa.GetVolumeParams{UUID: "ext-uuid-2", SvmName: "svm1"}).
			Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{Name: "flexvol"}}, nil)

		val, err := env.ExecuteActivity(activity.FetchConstituentCountForLargeVolume, vol, node)
		assert.NoError(tt, err)
		var count int32
		_ = val.Get(&count)
		assert.Equal(tt, int32(0), count)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("Success_NilVolumeResponse_ReturnsZero", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchConstituentCountForLargeVolume)

		vol := &datamodel.Volume{
			Name:             "vol",
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "ext-uuid-3"},
			Svm:              &datamodel.Svm{Name: "svm1"},
		}
		node := &models.Node{Name: "node1"}
		mockProvider.On("GetVolume", mock.Anything).Return(nil, nil)

		val, err := env.ExecuteActivity(activity.FetchConstituentCountForLargeVolume, vol, node)
		assert.NoError(tt, err)
		var count int32
		_ = val.Get(&count)
		assert.Equal(tt, int32(0), count)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("GetProviderByNodeFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchConstituentCountForLargeVolume)

		vol := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "ext-uuid"},
			Svm:              &datamodel.Svm{Name: "svm1"},
		}
		node := &models.Node{}

		_, err := env.ExecuteActivity(activity.FetchConstituentCountForLargeVolume, vol, node)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get provider")
	})

	t.Run("GetVolumeFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchConstituentCountForLargeVolume)

		vol := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "ext-uuid"},
			Svm:              &datamodel.Svm{Name: "svm1"},
		}
		node := &models.Node{}
		mockProvider.On("GetVolume", mock.Anything).Return(nil, errors.New("get volume failed"))

		_, err := env.ExecuteActivity(activity.FetchConstituentCountForLargeVolume, vol, node)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "get volume failed")
		mockProvider.AssertExpectations(tt)
	})
}

func TestVerifyCVCountForLargeVolume(t *testing.T) {
	t.Run("Match_ReturnsNil", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.VerifyCVCountForLargeVolume)

		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{ConstituentCountOfBackup: 4},
		}
		restoreTargetCount := int32(4)

		_, err := env.ExecuteActivity(activity.VerifyCVCountForLargeVolume, backup, restoreTargetCount)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Mismatch_ReturnsError", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := OntapModeRestoreActivity{SE: mockStorage}
		env.RegisterActivity(activity.VerifyCVCountForLargeVolume)

		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{ConstituentCountOfBackup: 4},
		}
		restoreTargetCount := int32(2)

		_, err := env.ExecuteActivity(activity.VerifyCVCountForLargeVolume, backup, restoreTargetCount)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "restore target volume constituent count (2) does not match backup constituent count (4)")
		mockStorage.AssertExpectations(tt)
	})
}
