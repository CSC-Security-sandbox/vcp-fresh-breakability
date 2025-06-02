package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1betaDeleteHostGroup(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeleteHostGroupParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
			HostGroupId:   "hg-id",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeleteHostGroup(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaDeleteHostGroupBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaDeleteHostGroupBadRequest).Message)
	})
	t.Run("WhenHostGroupDoesNotExist", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeleteHostGroupParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			HostGroupId:   "hg-id",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetHostGroup(mock.Anything, params.HostGroupId, params.ProjectNumber).Return(nil, errors.NewNotFoundErr("not found", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeleteHostGroup(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})
	t.Run("WhenHostGroupHasActiveVolumes", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeleteHostGroupParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			HostGroupId:   "hg-id",
		}

		existingHG := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID: "deletable-hg-id",
			},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetHostGroup(mock.Anything, params.HostGroupId, params.ProjectNumber).Return(existingHG, nil)
		mockOrchestrator.EXPECT().DeleteHostGroup(mock.Anything, params.ProjectNumber, params.HostGroupId).Return(nil, errors.NewUserInputValidationErr("host group is attached to volumes"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeleteHostGroup(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaDeleteHostGroupBadRequest).Code)
		assert.Equal(tt, "host group is attached to volumes", result.(*gcpgenserver.V1betaDeleteHostGroupBadRequest).Message)
	})
	t.Run("WhenHostGroupGetFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeleteHostGroupParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			HostGroupId:   "hg-id",
		}

		existingHG := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID: "deletable-hg-id",
			},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetHostGroup(mock.Anything, params.HostGroupId, params.ProjectNumber).Return(existingHG, nil)
		mockOrchestrator.EXPECT().DeleteHostGroup(mock.Anything, params.ProjectNumber, params.HostGroupId).Return(nil, errors.New("some error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeleteHostGroup(context.Background(), params)

		assert.NotNil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaDeleteHostGroupInternalServerError).Code)
		assert.Equal(tt, "Internal server error", result.(*gcpgenserver.V1betaDeleteHostGroupInternalServerError).Message)
	})
	t.Run("WhenHostGroupDeleteFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeleteHostGroupParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			HostGroupId:   "hg-id",
		}

		existingHG := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID: "deletable-hg-id",
			},
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetHostGroup(mock.Anything, params.HostGroupId, params.ProjectNumber).Return(existingHG, errors.New("some error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeleteHostGroup(context.Background(), params)

		assert.NotNil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaDeleteHostGroupInternalServerError).Code)
		assert.Equal(tt, "Internal server error", result.(*gcpgenserver.V1betaDeleteHostGroupInternalServerError).Message)
	})
	t.Run("WhenHostGroupDeletionSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaDeleteHostGroupParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
			HostGroupId:   "hg-id",
		}

		existingHG := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID: "deletable-hg-id",
			},
		}
		deletedPool := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID: "deletable-pool-id",
			},
			State: models.LifeCycleStateDeleted,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().GetHostGroup(mock.Anything, params.HostGroupId, params.ProjectNumber).Return(existingHG, nil)
		mockOrchestrator.EXPECT().DeleteHostGroup(mock.Anything, params.ProjectNumber, params.HostGroupId).Return(deletedPool, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaDeleteHostGroup(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})
}

func TestV1betaGetMultipleHostGroups(t *testing.T) {
	t.Run("WhenGetMultipleHostGroupsFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleHostGroupsParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.HostGroupIdListV1beta{
			HostGroupUuids: []string{"hg-id-1", "hg-id-2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleHostGroups(mock.Anything, params.ProjectNumber, req.HostGroupUuids).Return(nil, errors.New("some error"))
		result, err := handler.V1betaGetMultipleHostGroups(context.Background(), req, params)

		assert.NotNil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaGetMultipleHostGroupsInternalServerError).Code)
		assert.Equal(tt, "Internal server error", result.(*gcpgenserver.V1betaGetMultipleHostGroupsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleHostGroupsReturnsZeroHGs", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleHostGroupsParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.HostGroupIdListV1beta{
			HostGroupUuids: []string{"hg-id-1", "hg-id-2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		resp := []*models.HostGroup{}
		mockOrchestrator.EXPECT().GetMultipleHostGroups(mock.Anything, params.ProjectNumber, req.HostGroupUuids).Return(resp, nil)
		result, err := handler.V1betaGetMultipleHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.(*gcpgenserver.V1betaGetMultipleHostGroupsOK).HostGroups, 0)
	})
	t.Run("WhenGetMultipleHostGroupsReturns2HGs", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleHostGroupsParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.HostGroupIdListV1beta{
			HostGroupUuids: []string{"hg-id-1", "hg-id-2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		resp := []*models.HostGroup{
			{
				BaseModel: models.BaseModel{
					UUID: "hg-id-1",
				},
			},
			{
				BaseModel: models.BaseModel{
					UUID: "hg-id-2",
				},
			},
		}
		mockOrchestrator.EXPECT().GetMultipleHostGroups(mock.Anything, params.ProjectNumber, req.HostGroupUuids).Return(resp, nil)
		result, err := handler.V1betaGetMultipleHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.(*gcpgenserver.V1betaGetMultipleHostGroupsOK).HostGroups, 2)
	})
}

func TestV1betaCreateHostGroup(t *testing.T) {
	t.Run("WhenCreateHostGroupFailsWithConflict", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreateHostGroupParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.HostGroupV1beta{
			ResourceId:  "test-host-group",
			Description: gcpgenserver.NewOptString("test description"),
			OsType:      gcpgenserver.HostGroupV1betaOsTypeLINUX,
			Type:        gcpgenserver.NewOptHostGroupV1betaType(gcpgenserver.HostGroupV1betaTypeISCSIINITIATOR),
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		createHGParams := &orchestrator.CreateHostGroupParams{Name: "test-host-group", Description: "test description", HostGroupType: "test description", Hosts: []string(nil), OSType: "LINUX", AccountID: "project-number"}
		mockOrchestrator.EXPECT().CreateHostGroup(mock.Anything, createHGParams).Return(nil, errors.NewConflictErr("Host group already exists"))

		result, err := handler.V1betaCreateHostGroup(context.Background(), req, params)

		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(409), result.(*gcpgenserver.V1betaCreateHostGroupConflict).Code)
		assert.Equal(tt, "Host group already exists", result.(*gcpgenserver.V1betaCreateHostGroupConflict).Message)
	})
	t.Run("WhenCreateHostGroupFailsWithISE", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreateHostGroupParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.HostGroupV1beta{
			ResourceId:  "test-host-group",
			Description: gcpgenserver.NewOptString("test description"),
			OsType:      gcpgenserver.HostGroupV1betaOsTypeLINUX,
			Type:        gcpgenserver.NewOptHostGroupV1betaType(gcpgenserver.HostGroupV1betaTypeISCSIINITIATOR),
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		createHGParams := &orchestrator.CreateHostGroupParams{Name: "test-host-group", Description: "test description", HostGroupType: "test description", Hosts: []string(nil), OSType: "LINUX", AccountID: "project-number"}
		mockOrchestrator.EXPECT().CreateHostGroup(mock.Anything, createHGParams).Return(nil, errors.New("some error"))

		result, err := handler.V1betaCreateHostGroup(context.Background(), req, params)

		assert.NotNil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaCreateHostGroupInternalServerError).Code)
		assert.Equal(tt, "Internal server error", result.(*gcpgenserver.V1betaCreateHostGroupInternalServerError).Message)
	})
	t.Run("WhenCreateHostGroupWithSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaCreateHostGroupParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}

		req := &gcpgenserver.HostGroupV1beta{
			ResourceId:  "test-host-group",
			Description: gcpgenserver.NewOptString("test description"),
			OsType:      gcpgenserver.HostGroupV1betaOsTypeLINUX,
			Type:        gcpgenserver.NewOptHostGroupV1betaType(gcpgenserver.HostGroupV1betaTypeISCSIINITIATOR),
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		createHGParams := &orchestrator.CreateHostGroupParams{Name: "test-host-group", Description: "test description", HostGroupType: "test description", Hosts: []string(nil), OSType: "LINUX", AccountID: "project-number"}
		mockOrchestrator.EXPECT().CreateHostGroup(mock.Anything, createHGParams).Return(&models.HostGroup{
			BaseModel:     models.BaseModel{},
			Name:          "",
			Description:   "",
			State:         "",
			StateDetails:  "",
			OSType:        "",
			Hosts:         nil,
			HostGroupType: "",
			AccountName:   "",
		}, nil)

		result, err := handler.V1betaCreateHostGroup(context.Background(), req, params)

		assert.Nil(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, true, result.(*gcpgenserver.V1betaCreateHostGroupOK).Done.Value)
	})
}
