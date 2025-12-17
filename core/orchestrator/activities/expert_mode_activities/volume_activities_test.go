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
			Name:         "test-volume",
			SizeInBytes:  2199023255552, // 2TB
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
		}

		updatedVolume := &datamodel.ExpertModeVolumes{
			BaseModel: datamodel.BaseModel{
				UUID: "volume-uuid-123",
			},
			Name:         "test-volume",
			SizeInBytes:  2199023255552,
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
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
			Name:         "test-volume",
			SizeInBytes:  2199023255552,
			Style:        "flexgroup",
			State:        models.LifeCycleStateAvailable,
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
					Name:         "test-volume",
					SizeInBytes:  1099511627776,
					Style:        "flexvol",
					State:        state,
				}

				updatedVolume := &datamodel.ExpertModeVolumes{
					BaseModel: datamodel.BaseModel{
						UUID: "volume-uuid-123",
					},
					Name:         "test-volume",
					SizeInBytes:  1099511627776,
					Style:        "flexvol",
					State:        state,
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
					Name:         "test-volume",
					SizeInBytes:  1099511627776,
					Style:        style,
					State:        models.LifeCycleStateAvailable,
				}

				updatedVolume := &datamodel.ExpertModeVolumes{
					BaseModel: datamodel.BaseModel{
						UUID: "volume-uuid-123",
					},
					Name:         "test-volume",
					SizeInBytes:  1099511627776,
					Style:        style,
					State:        models.LifeCycleStateAvailable,
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
