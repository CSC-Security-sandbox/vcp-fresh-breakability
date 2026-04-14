package expertmodeactivities

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"go.temporal.io/sdk/testsuite"
)

func TestResolveExpertModeFlexcloneSharedBytes(t *testing.T) {
	ctx := context.Background()
	parentVolUUID := "parent-ontap-uuid"
	snapUUID := "snap-ontap-uuid"

	t.Run("FromParentSnapshotUUIDViaGetSnapshot", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockProvider.On("GetSnapshot", snapUUID, parentVolUUID).Return(&vsa.SnapshotProviderResponse{
			LogicalSizeInBytes: 4096,
		}, nil)

		n, err := resolveExpertModeFlexcloneSharedBytes(ctx, mockProvider, parentVolUUID, snapUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(4096), n)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("MissingParentSnapshotUUIDReturnsZero", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		n, err := resolveExpertModeFlexcloneSharedBytes(ctx, mockProvider, parentVolUUID, "")
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), n)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("MissingParentVolumeUUIDReturnsZero", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		n, err := resolveExpertModeFlexcloneSharedBytes(ctx, mockProvider, "", snapUUID)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), n)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("EmptyParentUUIDsReturnZero", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		n, err := resolveExpertModeFlexcloneSharedBytes(ctx, mockProvider, "", "")
		assert.NoError(tt, err)
		assert.Equal(tt, int64(0), n)
		mockProvider.AssertExpectations(tt)
	})
}

// containsIgnoreCase checks if a string contains a substring (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func TestFetchOntapVolumeByName(t *testing.T) {
	t.Run("WhenVolumeIsFoundInONTAP", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByName)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776, // 1TB
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume",
				ExternalUUID: "ontap-uuid-123",
			},
			Size:  2199023255552, // 2TB
			Style: "flexgroup",
			State: "online",
		}

		// Mock GetVolumeForExpertMode
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(expectedVolumeResponse, nil)

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *datamodel.ExpertModeVolumes
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-volume", result.Name)
		assert.Equal(tt, int64(2199023255552), result.SizeInBytes)
		assert.Equal(tt, "flexgroup", result.Style)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.State)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsNotFoundInONTAP", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByName)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "non-existent-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		notFoundError := utilErrors.NewNotFoundErr("volume", nil)

		// Mock GetVolumeForExpertMode to return not found error
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "non-existent-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(nil, notFoundError)

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)

		// Assert
		// ExecuteActivity may return nil error, but the actual error is in the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// Verify it's wrapped as TemporalApplicationError with ErrResourceNotFound
		// The error message should contain "Resource not found" (capital R) or "resource not found"
		errMsg := err.Error()
		assert.True(tt,
			containsIgnoreCase(errMsg, "resource not found") || containsIgnoreCase(errMsg, "not found"),
			"Expected error to contain 'resource not found' or 'not found', got: %v", err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return an error
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByName)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "original-uuid",
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)

		// Assert
		// When ExecuteActivity returns an error, it's returned directly
		// If no error from ExecuteActivity, check the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when GetProviderByNode fails")
		assert.Nil(tt, result)
		// Verify the error message contains the expected text
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "failed to get provider"),
				"Expected error to contain 'failed to get provider', got: %v", err)
		}
	})

	t.Run("WhenGetVolumeForExpertModeReturnsNonNotFoundError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByName)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		otherError := errors.New("internal server error")

		// Mock GetVolumeForExpertMode to return a non-not-found error
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(nil, otherError)

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)

		// Assert
		// When ExecuteActivity returns an error, it's returned directly
		// If no error from ExecuteActivity, check the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when GetVolumeForExpertMode returns error")
		assert.Nil(tt, result)
		// Verify the error message contains the expected text
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "internal server error"),
				"Expected error to contain 'internal server error', got: %v", err)
		}
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeHasNoSvm", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByName)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "original-uuid",
			Svm:          nil, // No SVM
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume",
				ExternalUUID: "ontap-uuid-123",
			},
			Size:  1099511627776,
			Style: "flexvol",
			State: "online",
		}

		// Mock GetVolumeForExpertMode with empty SvmName
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "",
			IsRestore:  false,
		}).Return(expectedVolumeResponse, nil)

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *datamodel.ExpertModeVolumes
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-volume", result.Name)
		assert.Equal(tt, int64(1099511627776), result.SizeInBytes)
		assert.Equal(tt, "flexvol", result.Style)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.State)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeNotFoundErrorContainsNotfoundString", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByName)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "non-existent-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Error message contains "not found" but is not a utilErrors.NotFoundErr
		notFoundStringError := errors.New("volume not found in ONTAP")

		// Mock GetVolumeForExpertMode to return error with "not found" string
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "non-existent-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(nil, notFoundStringError)

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)

		// Assert
		// ExecuteActivity may return nil error, but the actual error is in the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// Should be treated as not found error - wrapped as TemporalApplicationError with ErrResourceNotFound
		// The error message should contain "Resource not found" (capital R) or "resource not found"
		errMsg := err.Error()
		assert.True(tt,
			containsIgnoreCase(errMsg, "resource not found") || containsIgnoreCase(errMsg, "not found"),
			"Expected error to contain 'resource not found' or 'not found', got: %v", err)
		mockProvider.AssertExpectations(tt)
	})
}

func TestFetchOntapVolumeByName_FlexcloneSharedBytesFromParentSnapshot(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := ExpertModeVolumeActivity{SE: mockStorage}
	env.RegisterActivity(activity.FetchOntapVolumeByName)

	parentVolUUID := "parent-ontap-uuid"
	snapUUID := "snap-ontap-uuid"
	volume := &datamodel.ExpertModeVolumes{
		Name:         "clone-vol",
		Style:        "flexvol",
		ExternalUUID: "original-uuid",
		VolumeAttributes: &datamodel.ExpertModeVolumeAttributes{
			IsFlexclone: true,
			Clone: &datamodel.ExpertModeCloneInfo{
				ParentVolume:   &datamodel.ExpertModeCloneParent{UUID: parentVolUUID},
				ParentSnapshot: &datamodel.ExpertModeCloneParent{UUID: snapUUID},
			},
		},
		Svm: &datamodel.Svm{Name: "svm-a"},
	}
	node := &models.Node{Name: "n1"}

	mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
		VolumeName: "clone-vol",
		SvmName:    "svm-a",
		IsRestore:  false,
	}).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "clone-vol",
			ExternalUUID: "ontap-uuid-1",
		},
		Size:  1234,
		Style: "flexvol",
		State: "online",
	}, nil).Once()

	mockProvider.On("GetCloneVolumeForExpertMode", vsa.GetVolumeParams{
		UUID:      "ontap-uuid-1",
		SvmName:   "svm-a",
		IsRestore: false,
	}).Return(&vsa.VolumeResponse{
		CloneParentVolumeUUID:   parentVolUUID,
		CloneParentSnapshotUUID: snapUUID,
	}, nil).Once()

	mockProvider.On("GetSnapshot", snapUUID, parentVolUUID).Return(&vsa.SnapshotProviderResponse{
		LogicalSizeInBytes: 8192,
	}, nil).Once()

	encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)
	assert.NoError(t, err)

	var result *datamodel.ExpertModeVolumes
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, int64(8192), result.SharedBytes)
	mockProvider.AssertExpectations(t)
}

func TestFetchOntapVolumeByName_GetCloneVolumeForExpertModeError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	orig := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = orig }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activity := ExpertModeVolumeActivity{SE: mockStorage}
	env.RegisterActivity(activity.FetchOntapVolumeByName)

	volume := &datamodel.ExpertModeVolumes{
		Name: "clone-vol",
		VolumeAttributes: &datamodel.ExpertModeVolumeAttributes{
			IsFlexclone: true,
			Clone: &datamodel.ExpertModeCloneInfo{
				ParentVolume:   &datamodel.ExpertModeCloneParent{UUID: "pv"},
				ParentSnapshot: &datamodel.ExpertModeCloneParent{UUID: "ps"},
			},
		},
		Svm: &datamodel.Svm{Name: "svm-a"},
	}
	node := &models.Node{Name: "n1"}

	mockProvider.On("GetVolumeForExpertMode", mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{Name: "clone-vol", ExternalUUID: "ontap-uuid-1"},
		Size:             100,
		Style:            "flexvol",
		State:            "online",
	}, nil)
	cloneErr := errors.New("clone get failed")
	mockProvider.On("GetCloneVolumeForExpertMode", mock.Anything).Return(nil, cloneErr)

	_, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestFetchOntapVolumeByName_ResolveSharedBytesError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	orig := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = orig }()
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activity := ExpertModeVolumeActivity{SE: mockStorage}
	env.RegisterActivity(activity.FetchOntapVolumeByName)

	parentVolUUID := "parent-ontap-uuid"
	snapUUID := "snap-ontap-uuid"
	volume := &datamodel.ExpertModeVolumes{
		Name: "clone-vol",
		VolumeAttributes: &datamodel.ExpertModeVolumeAttributes{
			IsFlexclone: true,
			Clone: &datamodel.ExpertModeCloneInfo{
				ParentVolume:   &datamodel.ExpertModeCloneParent{UUID: parentVolUUID},
				ParentSnapshot: &datamodel.ExpertModeCloneParent{UUID: snapUUID},
			},
		},
		Svm: &datamodel.Svm{Name: "svm-a"},
	}
	node := &models.Node{Name: "n1"}

	mockProvider.On("GetVolumeForExpertMode", mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{Name: "clone-vol", ExternalUUID: "ontap-uuid-1"},
		Size:             100,
		Style:            "flexvol",
		State:            "online",
	}, nil)
	mockProvider.On("GetCloneVolumeForExpertMode", mock.Anything).Return(&vsa.VolumeResponse{
		CloneParentVolumeUUID:   parentVolUUID,
		CloneParentSnapshotUUID: snapUUID,
	}, nil)
	snapErr := errors.New("snapshot read failed")
	mockProvider.On("GetSnapshot", snapUUID, parentVolUUID).Return(nil, snapErr)

	_, err := env.ExecuteActivity(activity.FetchOntapVolumeByName, volume, node)
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestResolveExpertModeFlexcloneSharedBytes_NilSnapshotResponse(t *testing.T) {
	ctx := context.Background()
	mockProvider := new(vsa.MockProvider)
	mockProvider.On("GetSnapshot", "snap-1", "vol-1").Return(nil, nil)

	n, err := resolveExpertModeFlexcloneSharedBytes(ctx, mockProvider, "vol-1", "snap-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil snapshot response")
	assert.Equal(t, int64(0), n)
	mockProvider.AssertExpectations(t)
}

func TestUpdateExpertModeVolumeInDB(t *testing.T) {
	t.Run("WhenVolumeIsUpdatedSuccessfully", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeInDB)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:        "test-volume",
			SizeInBytes: 2199023255552, // 2TB
			Style:       "flexgroup",
			State:       models.LifeCycleStateAvailable,
		}

		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:        "test-volume",
			SizeInBytes: 2199023255552,
			Style:       "flexgroup",
			State:       models.LifeCycleStateAvailable,
		}

		// Mock UpdateExpertModeVolume
		mockStorage.On("UpdateExpertModeVolume", mock.Anything, volume).Return(updatedVolume, nil)

		// Act
		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeInDB, volume)

		// Assert
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateExpertModeVolumeFails", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeInDB)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:        "test-volume",
			SizeInBytes: 2199023255552,
			Style:       "flexgroup",
			State:       models.LifeCycleStateAvailable,
		}

		expectedError := errors.New("database update failed")

		// Mock UpdateExpertModeVolume to return error
		mockStorage.On("UpdateExpertModeVolume", mock.Anything, volume).Return(nil, expectedError)

		// Act
		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeInDB, volume)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database update failed")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsUpdatedWithDifferentStates", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeInDB)

		states := []string{
			models.LifeCycleStateCreating,
			models.LifeCycleStateAvailable,
			models.LifeCycleStateDeleted,
		}

		for _, state := range states {
			tt.Run(state, func(ttt *testing.T) {
				volume := &datamodel.ExpertModeVolumes{
					BaseModel: datamodel.BaseModel{
						UUID: "volume-uuid-123",
					},
					Name:        "test-volume",
					SizeInBytes: 1099511627776,
					Style:       "flexvol",
					State:       state,
				}

				updatedVolume := &datamodel.ExpertModeVolumes{
					BaseModel: datamodel.BaseModel{
						UUID: "volume-uuid-123",
					},
					Name:        "test-volume",
					SizeInBytes: 1099511627776,
					Style:       "flexvol",
					State:       state,
				}

				// Mock UpdateExpertModeVolume
				mockStorage.On("UpdateExpertModeVolume", mock.Anything, volume).Return(updatedVolume, nil)

				// Act
				_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeInDB, volume)

				// Assert
				assert.NoError(ttt, err)
			})
		}

		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsUpdatedWithDifferentStyles", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeInDB)

		styles := []string{"flexvol", "flexgroup", "flexcache"}

		for _, style := range styles {
			tt.Run(style, func(ttt *testing.T) {
				volume := &datamodel.ExpertModeVolumes{
					BaseModel: datamodel.BaseModel{
						UUID: "volume-uuid-123",
					},
					Name:        "test-volume",
					SizeInBytes: 1099511627776,
					Style:       style,
					State:       models.LifeCycleStateAvailable,
				}

				updatedVolume := &datamodel.ExpertModeVolumes{
					BaseModel: datamodel.BaseModel{
						UUID: "volume-uuid-123",
					},
					Name:        "test-volume",
					SizeInBytes: 1099511627776,
					Style:       style,
					State:       models.LifeCycleStateAvailable,
				}

				// Mock UpdateExpertModeVolume
				mockStorage.On("UpdateExpertModeVolume", mock.Anything, volume).Return(updatedVolume, nil)

				// Act
				_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeInDB, volume)

				// Assert
				assert.NoError(ttt, err)
			})
		}

		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateExpertModeVolumeStateInDB(t *testing.T) {
	t.Run("WhenVolumeStateIsUpdatedSuccessfully", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeStateInDB)

		volumeUUID := "emv-uuid-123"
		state := models.LifeCycleStateREADY

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: volumeUUID},
			ExternalUUID: "ext-uuid-123",
			Name:         "test-vol",
			State:        models.LifeCycleStateCreating,
			Style:        "flexvol",
		}
		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: volumeUUID},
			ExternalUUID: "ext-uuid-123",
			Name:         "test-vol",
			State:        state,
			Style:        volume.Style, // Style is unchanged
		}

		mockStorage.On("GetExpertModeVolumeByUUID", mock.Anything, volumeUUID).Return(volume, nil)
		mockStorage.On("UpdateExpertModeVolume", mock.Anything, mock.MatchedBy(func(v *datamodel.ExpertModeVolumes) bool {
			return v.UUID == volumeUUID && v.State == state && v.Style == volume.Style
		})).Return(updatedVolume, nil)

		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeStateInDB, volumeUUID, state)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetExpertModeVolumeByUUIDFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeStateInDB)

		volumeUUID := "emv-uuid-456"
		getErr := errors.New("volume not found")
		mockStorage.On("GetExpertModeVolumeByUUID", mock.Anything, volumeUUID).Return(nil, getErr)

		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeStateInDB, volumeUUID, models.LifeCycleStateREADY)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "volume not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateExpertModeVolumeFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeStateInDB)

		volumeUUID := "emv-uuid-789"
		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: volumeUUID},
			ExternalUUID: "ext-uuid-789",
			Name:         "test-vol",
			State:        models.LifeCycleStateCreating,
		}
		updateErr := errors.New("database update failed")
		mockStorage.On("GetExpertModeVolumeByUUID", mock.Anything, volumeUUID).Return(volume, nil)
		mockStorage.On("UpdateExpertModeVolume", mock.Anything, mock.Anything).Return(nil, updateErr)

		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeStateInDB, volumeUUID, models.LifeCycleStateREADY)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database update failed")
		mockStorage.AssertExpectations(tt)
	})
}

func TestCheckVolumeDeletedInOntap(t *testing.T) {
	t.Run("WhenVolumeIsNotFound_IsNotFoundErr", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		notFoundError := utilErrors.NewNotFoundErr("volume", nil)

		// Mock GetVolumeForExpertMode to return not found error
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(nil, notFoundError)

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		// When volume is not found, deletion is complete, should return nil (success)
		assert.NoError(tt, err, "Expected no error when volume is not found (deletion complete)")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsNotFound_ContainsNotfoundString", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Error message contains "not found" but is not a utilErrors.NotFoundErr
		notFoundStringError := errors.New("volume not found in ONTAP")

		// Mock GetVolumeForExpertMode to return error with "not found" string
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(nil, notFoundStringError)

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		// When volume is not found (even if error contains "not found" string), deletion is complete, should return nil (success)
		assert.NoError(tt, err, "Expected no error when volume is not found (deletion complete)")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsStillFound", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume",
				ExternalUUID: "ontap-uuid-123",
			},
			Size:  1099511627776,
			Style: "flexvol",
			State: "online",
		}

		// Mock GetVolumeForExpertMode to return volume (still exists)
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(expectedVolumeResponse, nil)

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		// When volume is still found, should return error to trigger activity retry
		assert.Error(tt, err, "Expected error when volume is still found")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt,
				containsIgnoreCase(errMsg, "still exists") || containsIgnoreCase(errMsg, "deletion may be in progress") || containsIgnoreCase(errMsg, "resource state conflict") || containsIgnoreCase(errMsg, "invalid state"),
				"Expected error to contain 'still exists', 'deletion may be in progress', 'resource state conflict', or 'invalid state', got: %v", err)
		}
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return an error
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		assert.Error(tt, err, "Expected error when GetProviderByNode fails")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "failed to get provider"),
				"Expected error to contain 'failed to get provider', got: %v", err)
		}
	})

	t.Run("WhenGetVolumeForExpertModeReturnsNonNotFoundError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		otherError := errors.New("network timeout error")

		// Mock GetVolumeForExpertMode to return a non-not-found error
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			IsRestore:  false,
		}).Return(nil, otherError)

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		// Other errors (network, etc.) should be retried - should return error
		assert.Error(tt, err, "Expected error when GetVolumeForExpertMode returns non-not-found error")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "network timeout error"),
				"Expected error to contain 'network timeout error', got: %v", err)
		}
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeHasNoSvm", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
			Svm:          nil, // No SVM
		}

		node := &models.Node{
			Name: "test-node",
		}

		notFoundError := utilErrors.NewNotFoundErr("volume", nil)

		// Mock GetVolumeForExpertMode with empty SvmName
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "",
			IsRestore:  false,
		}).Return(nil, notFoundError)

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		// When volume is not found, deletion is complete, should return nil (success)
		assert.NoError(tt, err, "Expected no error when volume is not found (deletion complete)")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeHasNoSvm_StillExists", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(tt)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.CheckVolumeDeletedInOntap)

		volume := &datamodel.ExpertModeVolumes{
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateDeleting,
			ExternalUUID: "original-uuid",
			Svm:          nil, // No SVM
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume",
				ExternalUUID: "ontap-uuid-123",
			},
			Size:  1099511627776,
			Style: "flexvol",
			State: "online",
		}

		// Mock GetVolumeForExpertMode with empty SvmName, volume still exists
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			VolumeName: "test-volume",
			SvmName:    "",
			IsRestore:  false,
		}).Return(expectedVolumeResponse, nil)

		// Act
		_, err := env.ExecuteActivity(activity.CheckVolumeDeletedInOntap, volume, node)

		// Assert
		// When volume is still found, should return error to trigger activity retry
		assert.Error(tt, err, "Expected error when volume is still found")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt,
				containsIgnoreCase(errMsg, "still exists") || containsIgnoreCase(errMsg, "deletion may be in progress") || containsIgnoreCase(errMsg, "resource state conflict") || containsIgnoreCase(errMsg, "invalid state"),
				"Expected error to contain 'still exists', 'deletion may be in progress', 'resource state conflict', or 'invalid state', got: %v", err)
		}
		mockProvider.AssertExpectations(tt)
	})
}

func TestDeleteExpertModeVolumeInDB(t *testing.T) {
	t.Run("WhenVolumeIsDeletedSuccessfully", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.DeleteExpertModeVolumeInDB)

		volumeUUID := "volume-uuid-123"

		// Mock DeleteExpertModeVolume to return success
		mockStorage.On("DeleteExpertModeVolume", mock.Anything, volumeUUID).Return(nil)

		// Act
		_, err := env.ExecuteActivity(activity.DeleteExpertModeVolumeInDB, volumeUUID)

		// Assert
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenDeleteExpertModeVolumeFails", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.DeleteExpertModeVolumeInDB)

		volumeUUID := "volume-uuid-123"

		expectedError := errors.New("database delete failed")

		// Mock DeleteExpertModeVolume to return error
		mockStorage.On("DeleteExpertModeVolume", mock.Anything, volumeUUID).Return(expectedError)

		// Act
		_, err := env.ExecuteActivity(activity.DeleteExpertModeVolumeInDB, volumeUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database delete failed")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenVolumeNotFound", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.DeleteExpertModeVolumeInDB)

		volumeUUID := "non-existent-uuid"

		notFoundError := errors.New("record not found")

		// Mock DeleteExpertModeVolume to return not found error
		mockStorage.On("DeleteExpertModeVolume", mock.Anything, volumeUUID).Return(notFoundError)

		// Act
		_, err := env.ExecuteActivity(activity.DeleteExpertModeVolumeInDB, volumeUUID)

		// Assert
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "record not found")
		mockStorage.AssertExpectations(tt)
	})
}

func TestValidateONTAPVolumeUpdate(t *testing.T) {
	t.Run("WhenVolumeUpdateIsValidatedSuccessfully", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.ValidateONTAPVolumeUpdate)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552, // 2TB
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "external-uuid-123",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Mock fetchOntapVolumeByUUID to return volume with matching size and name
		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		ontapVolume := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{Name: "test-volume"},
			Size:             2199023255552, // Same size
		}

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return ontapVolume, nil
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.ValidateONTAPVolumeUpdate, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *datamodel.ExpertModeVolumes
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-volume", result.Name)
		assert.Equal(tt, int64(2199023255552), result.SizeInBytes)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.State)
	})

	t.Run("WhenVolumeUpdateIsNotComplete_DifferentSize", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.ValidateONTAPVolumeUpdate)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552, // 2TB
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "external-uuid-123",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Mock fetchOntapVolumeByUUID to return volume with different size
		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		ontapVolume := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{Name: "test-volume"},
			Size:             1099511627776, // Different size (1TB)
		}

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return ontapVolume, nil
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.ValidateONTAPVolumeUpdate, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when volume update is not complete")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt,
				containsIgnoreCase(errMsg, "still not updated") || containsIgnoreCase(errMsg, "update may be in progress") || containsIgnoreCase(errMsg, "resource state conflict") || containsIgnoreCase(errMsg, "invalid state"),
				"Expected error to contain 'still not updated', 'update may be in progress', 'resource state conflict', or 'invalid state', got: %v", err)
		}
		// Should still return the ontapVolume even though there's an error
		if result != nil {
			assert.Equal(tt, int64(1099511627776), result.SizeInBytes)
		}
	})

	t.Run("WhenVolumeUpdateIsNotComplete_DifferentName", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.ValidateONTAPVolumeUpdate)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552, // 2TB
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "external-uuid-123",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Mock fetchOntapVolumeByUUID to return volume with different name
		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		ontapVolume := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{Name: "different-volume-name"},
			Size:             2199023255552, // Same size
		}

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return ontapVolume, nil
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.ValidateONTAPVolumeUpdate, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when volume update is not complete")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt,
				containsIgnoreCase(errMsg, "still not updated") || containsIgnoreCase(errMsg, "update may be in progress") || containsIgnoreCase(errMsg, "resource state conflict") || containsIgnoreCase(errMsg, "invalid state"),
				"Expected error to contain 'still not updated', 'update may be in progress', 'resource state conflict', or 'invalid state', got: %v", err)
		}
		// Should still return the ontapVolume even though there's an error
		if result != nil {
			assert.Equal(tt, "different-volume-name", result.Name)
		}
	})

	t.Run("WhenFetchOntapVolumeByUUIDReturnsError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.ValidateONTAPVolumeUpdate)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552,
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "external-uuid-123",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Mock fetchOntapVolumeByUUID to return an error
		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		fetchError := errors.New("failed to fetch volume from ONTAP")

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return nil, fetchError
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.ValidateONTAPVolumeUpdate, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when fetchOntapVolumeByUUID returns error")
		assert.Nil(tt, result)
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "failed to fetch volume from ONTAP"),
				"Expected error to contain 'failed to fetch volume from ONTAP', got: %v", err)
		}
	})

	t.Run("WhenFetchOntapVolumeByUUIDReturnsNotFoundError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.ValidateONTAPVolumeUpdate)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552,
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "external-uuid-123",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Mock fetchOntapVolumeByUUID to return a not found error
		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		notFoundError := utilErrors.NewNotFoundErr("volume", nil)

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return nil, notFoundError
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.ValidateONTAPVolumeUpdate, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when fetchOntapVolumeByUUID returns not found error")
		assert.Nil(tt, result)
		if err != nil {
			errMsg := err.Error()
			assert.True(tt,
				containsIgnoreCase(errMsg, "resource not found") || containsIgnoreCase(errMsg, "not found"),
				"Expected error to contain 'resource not found' or 'not found', got: %v", err)
		}
	})

	t.Run("WhenVolumeUpdateIsNotComplete_DifferentSizeAndName", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.ValidateONTAPVolumeUpdate)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552, // 2TB
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
			ExternalUUID: "external-uuid-123",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Mock fetchOntapVolumeByUUID to return volume with different size and name
		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		ontapVolume := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{Name: "different-volume-name"},
			Size:             1099511627776, // Same size
		}

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return ontapVolume, nil
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.ValidateONTAPVolumeUpdate, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when volume update is not complete")
		if err != nil {
			errMsg := err.Error()
			assert.True(tt,
				containsIgnoreCase(errMsg, "still not updated") || containsIgnoreCase(errMsg, "update may be in progress") || containsIgnoreCase(errMsg, "resource state conflict") || containsIgnoreCase(errMsg, "invalid state"),
				"Expected error to contain 'still not updated', 'update may be in progress', 'resource state conflict', or 'invalid state', got: %v", err)
		}
		// Should still return the ontapVolume even though there's an error
		if result != nil {
			assert.Equal(tt, "different-volume-name", result.Name)
			assert.Equal(tt, int64(1099511627776), result.SizeInBytes)
		}
	})
}

func TestFetchOntapVolumeByUUID(t *testing.T) {
	t.Run("WhenVolumeIsFoundInONTAP", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		env.RegisterActivity(fetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  1099511627776, // 1TB
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "external-uuid-456",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume-updated",
				ExternalUUID: "external-uuid-456",
			},
			Size:  2199023255552, // 2TB
			Style: "flexgroup",
			State: "online",
		}

		// Mock GetVolumeForExpertMode with UUID
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			UUID:      "external-uuid-456",
			SvmName:   "test-svm",
			IsRestore: false,
		}).Return(expectedVolumeResponse, nil)

		// Act
		encodedValue, err := env.ExecuteActivity(fetchOntapVolumeByUUID, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *vsa.VolumeResponse
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-volume-updated", result.Name)
		assert.Equal(tt, int64(2199023255552), result.Size)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeIsNotFoundInONTAP", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		env.RegisterActivity(fetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "non-existent-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "non-existent-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		notFoundError := utilErrors.NewNotFoundErr("volume", nil)

		// Mock GetVolumeForExpertMode to return not found error
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			UUID:      "non-existent-uuid",
			SvmName:   "test-svm",
			IsRestore: false,
		}).Return(nil, notFoundError)

		// Act
		encodedValue, err := env.ExecuteActivity(fetchOntapVolumeByUUID, volume, node)

		// Assert
		// ExecuteActivity may return nil error, but the actual error is in the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// Verify it's wrapped as TemporalApplicationError with ErrResourceNotFound
		errMsg := err.Error()
		assert.True(tt,
			containsIgnoreCase(errMsg, "resource not found") || containsIgnoreCase(errMsg, "not found"),
			"Expected error to contain 'resource not found' or 'not found', got: %v", err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return an error
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		env.RegisterActivity(fetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "external-uuid-456",
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Act
		encodedValue, err := env.ExecuteActivity(fetchOntapVolumeByUUID, volume, node)

		// Assert
		// When ExecuteActivity returns an error, it's returned directly
		// If no error from ExecuteActivity, check the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when GetProviderByNode fails")
		assert.Nil(tt, result)
		// Verify the error message contains the expected text
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "failed to get provider"),
				"Expected error to contain 'failed to get provider', got: %v", err)
		}
	})

	t.Run("WhenGetVolumeForExpertModeReturnsNonNotFoundError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		env.RegisterActivity(fetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "external-uuid-456",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		otherError := errors.New("internal server error")

		// Mock GetVolumeForExpertMode to return a non-not-found error
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			UUID:      "external-uuid-456",
			SvmName:   "test-svm",
			IsRestore: false,
		}).Return(nil, otherError)

		// Act
		encodedValue, err := env.ExecuteActivity(fetchOntapVolumeByUUID, volume, node)

		// Assert
		// When ExecuteActivity returns an error, it's returned directly
		// If no error from ExecuteActivity, check the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err, "Expected error when GetVolumeForExpertMode returns error")
		assert.Nil(tt, result)
		// Verify the error message contains the expected text
		if err != nil {
			errMsg := err.Error()
			assert.True(tt, containsIgnoreCase(errMsg, "internal server error"),
				"Expected error to contain 'internal server error', got: %v", err)
		}
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeHasNoSvm", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		env.RegisterActivity(fetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "external-uuid-456",
			Svm:          nil, // No SVM
		}

		node := &models.Node{
			Name: "test-node",
		}

		expectedVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume",
				ExternalUUID: "external-uuid-456",
			},
			Size:  1099511627776,
			Style: "flexvol",
			State: "online",
		}

		// Mock GetVolumeForExpertMode with empty SvmName
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			UUID:      "external-uuid-456",
			SvmName:   "",
			IsRestore: false,
		}).Return(expectedVolumeResponse, nil)

		// Act
		encodedValue, err := env.ExecuteActivity(fetchOntapVolumeByUUID, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *vsa.VolumeResponse
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-volume", result.Name)
		assert.Equal(tt, int64(1099511627776), result.Size)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenVolumeNotFoundErrorContainsNotfoundString", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		// Mock GetProviderByNode to return the mock provider
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		env.RegisterActivity(fetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "non-existent-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "non-existent-uuid",
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		// Error message contains "not found" but is not a utilErrors.NotFoundErr
		notFoundStringError := errors.New("volume not found in ONTAP")

		// Mock GetVolumeForExpertMode to return error with "not found" string
		mockProvider.On("GetVolumeForExpertMode", vsa.GetVolumeParams{
			UUID:      "non-existent-uuid",
			SvmName:   "test-svm",
			IsRestore: false,
		}).Return(nil, notFoundStringError)

		// Act
		encodedValue, err := env.ExecuteActivity(fetchOntapVolumeByUUID, volume, node)

		// Assert
		// ExecuteActivity may return nil error, but the actual error is in the encoded value
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// Should be treated as not found error - wrapped as TemporalApplicationError with ErrResourceNotFound
		errMsg := err.Error()
		assert.True(tt,
			containsIgnoreCase(errMsg, "resource not found") || containsIgnoreCase(errMsg, "not found"),
			"Expected error to contain 'resource not found' or 'not found', got: %v", err)
		mockProvider.AssertExpectations(tt)
	})
}

// TestExpertModeVolumeActivity_FetchOntapVolumeByUUID tests the public method
// (a *ExpertModeVolumeActivity) FetchOntapVolumeByUUID which calls fetchOntapVolumeByUUID
// and converts the result via convertOntapToONTAPModeVol.
func TestExpertModeVolumeActivity_FetchOntapVolumeByUUID(t *testing.T) {
	t.Run("WhenFetchSucceeds_ReturnsConvertedVolume", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  1099511627776,
			Style:        "flexvol",
			State:        models.LifeCycleStateCreating,
			ExternalUUID: "external-uuid-456",
			Description:  "my volume",
			AccountID:    1,
			PoolID:       2,
			SvmID:        3,
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name: "test-node",
		}

		ontapVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "test-volume-updated",
				ExternalUUID: "external-uuid-456",
			},
			Size:  2199023255552, // 2TB
			Style: "flexgroup",
			State: "online",
		}

		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return ontapVolumeResponse, nil
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByUUID, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *datamodel.ExpertModeVolumes
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// From ontap response
		assert.Equal(tt, "test-volume-updated", result.Name)
		assert.Equal(tt, int64(2199023255552), result.SizeInBytes)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.State)
		// From input volume (db)
		assert.Equal(tt, "volume-uuid-123", result.UUID)
		assert.Equal(tt, "external-uuid-456", result.ExternalUUID)
		assert.Equal(tt, "flexvol", result.Style) // convertOntapToONTAPModeVol uses dbVolume.Style
		assert.Equal(tt, "my volume", result.Description)
		assert.Equal(tt, int64(1), result.AccountID)
		assert.Equal(tt, int64(2), result.PoolID)
		assert.Equal(tt, int64(3), result.SvmID)
		assert.Equal(tt, volume.Svm, result.Svm)
	})

	t.Run("WhenFetchReturnsError_ReturnsError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-123"},
			Name:         "test-volume",
			ExternalUUID: "external-uuid-456",
			Svm:          &datamodel.Svm{Name: "test-svm"},
		}

		node := &models.Node{Name: "test-node"}

		fetchErr := errors.New("failed to fetch volume from ONTAP")

		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return nil, fetchErr
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByUUID, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.True(tt, containsIgnoreCase(err.Error(), "failed to fetch volume from ONTAP"),
			"Expected error to contain 'failed to fetch volume from ONTAP', got: %v", err)
	})

	t.Run("WhenFetchReturnsNotFoundError_ReturnsWrappedError", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-123"},
			Name:         "test-volume",
			ExternalUUID: "non-existent-uuid",
			Svm:          &datamodel.Svm{Name: "test-svm"},
		}

		node := &models.Node{Name: "test-node"}

		notFoundErr := utilErrors.NewNotFoundErr("volume", nil)

		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			return nil, notFoundErr
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByUUID, volume, node)

		// Assert
		var result *datamodel.ExpertModeVolumes
		if err == nil {
			err = encodedValue.Get(&result)
		}
		assert.Error(tt, err)
		assert.Nil(tt, result)
		errMsg := err.Error()
		assert.True(tt,
			containsIgnoreCase(errMsg, "resource not found") || containsIgnoreCase(errMsg, "not found"),
			"Expected error to contain 'resource not found' or 'not found', got: %v", err)
	})

	t.Run("WhenVolumeHasNilSvm_Succeeds", func(tt *testing.T) {
		// Arrange
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.FetchOntapVolumeByUUID)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel:    datamodel.BaseModel{UUID: "volume-uuid-123"},
			Name:         "test-volume",
			ExternalUUID: "external-uuid-456",
			Svm:          nil, // No SVM
		}

		node := &models.Node{Name: "test-node"}

		ontapVolumeResponse := &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{
				Name:         "ontap-volume-name",
				ExternalUUID: "external-uuid-456",
			},
			Size:  1099511627776,
			Style: "flexvol",
			State: "online",
		}

		originalFetchOntapVolumeByUUID := fetchOntapVolumeByUUID
		defer func() { fetchOntapVolumeByUUID = originalFetchOntapVolumeByUUID }()

		fetchOntapVolumeByUUID = func(ctx context.Context, vol *datamodel.ExpertModeVolumes, n *models.Node) (*vsa.VolumeResponse, error) {
			assert.Nil(tt, vol.Svm, "volume.Svm should be nil")
			return ontapVolumeResponse, nil
		}

		// Act
		encodedValue, err := env.ExecuteActivity(activity.FetchOntapVolumeByUUID, volume, node)

		// Assert
		assert.NoError(tt, err)
		var result *datamodel.ExpertModeVolumes
		err = encodedValue.Get(&result)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "ontap-volume-name", result.Name)
		assert.Equal(tt, int64(1099511627776), result.SizeInBytes)
		assert.Equal(tt, models.LifeCycleStateAvailable, result.State)
		assert.Equal(tt, "volume-uuid-123", result.UUID)
		assert.Nil(tt, result.Svm)
	})
}

func TestUpdateExpertModeVolumeBackupConfigInDB(t *testing.T) {
	t.Run("WhenUpdateFails_ReturnsError", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeBackupConfigInDB)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		}

		mockStorage.EXPECT().UpdateExpertModeVolumeDataProtection(mock.Anything, volume).
			Return(errors.New("db write error"))

		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeBackupConfigInDB, volume)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "db write error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateSucceeds_ReturnsNil", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		activity := ExpertModeVolumeActivity{SE: mockStorage}
		env.RegisterActivity(activity.UpdateExpertModeVolumeBackupConfigInDB)

		volume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		}

		mockStorage.EXPECT().UpdateExpertModeVolumeDataProtection(mock.Anything, volume).
			Return(nil)

		_, err := env.ExecuteActivity(activity.UpdateExpertModeVolumeBackupConfigInDB, volume)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}
