package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1betaBatchListHostGroups(t *testing.T) {
	validLocation := env.Region
	if validLocation == "" {
		validLocation = "us-east4"
	}

	t.Run("WhenLocationIdIsInvalid", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: "invalid-location-123"}

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq := result.(*gcpgenserver.V1betaBatchListHostGroupsBadRequest)
		assert.Equal(tt, float64(400), badReq.Code)
	})

	t.Run("WhenLocationIdIsWrongRegion", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: "europe-west1"}

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq := result.(*gcpgenserver.V1betaBatchListHostGroupsBadRequest)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Contains(tt, badReq.Message, "Invalid region")
	})

	t.Run("WhenUUIDsListIsEmpty", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq := result.(*gcpgenserver.V1betaBatchListHostGroupsBadRequest)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "hostGroupUuids must not be empty", badReq.Message)
	})

	t.Run("WhenUUIDsExceedMaxBatchSize", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		uuids := make([]string, maxBatchHostGroupUUIDs+1)
		for i := range uuids {
			uuids[i] = "uuid"
		}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: uuids,
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq := result.(*gcpgenserver.V1betaBatchListHostGroupsBadRequest)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, fmt.Sprintf("hostGroupUuids must not exceed %d entries", maxBatchHostGroupUUIDs), badReq.Message)
	})

	t.Run("WhenUUIDsExceedCustomMaxBatchSize", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		origMax := maxBatchHostGroupUUIDs
		maxBatchHostGroupUUIDs = 10
		defer func() { maxBatchHostGroupUUIDs = origMax }()

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		uuids := make([]string, 11)
		for i := range uuids {
			uuids[i] = "uuid"
		}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: uuids,
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badReq := result.(*gcpgenserver.V1betaBatchListHostGroupsBadRequest)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "hostGroupUuids must not exceed 10 entries", badReq.Message)
	})

	t.Run("WhenOrchestratorReturnsError", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1", "uuid-2"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		mockOrchestrator.EXPECT().GetHostGroupsByUUIDs(mock.Anything, req.HostGroupUuids).
			Return(nil, errors.New("database error"))

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		ise := result.(*gcpgenserver.V1betaBatchListHostGroupsInternalServerError)
		assert.Equal(tt, float64(500), ise.Code)
		assert.Equal(tt, "Internal server error", ise.Message)
	})

	t.Run("WhenNoHostGroupsFound", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		mockOrchestrator.EXPECT().GetHostGroupsByUUIDs(mock.Anything, req.HostGroupUuids).
			Return([]*models.HostGroup{}, nil)

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		ok := result.(*gcpgenserver.V1betaBatchListHostGroupsOK)
		assert.Len(tt, ok.HostGroups, 0)
	})

	t.Run("WhenHostGroupsFound", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1", "uuid-2"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		now := time.Now()
		hostGroups := []*models.HostGroup{
			{
				BaseModel: models.BaseModel{
					UUID:      "uuid-1",
					CreatedAt: now,
				},
				Name:          "hg-one",
				Description:   "First host group",
				State:         "READY",
				OSType:        "LINUX",
				Hosts:         []string{"iqn.1998-01.com.vmware:host1"},
				HostGroupType: "ISCSI_INITIATOR",
			},
			{
				BaseModel: models.BaseModel{
					UUID:      "uuid-2",
					CreatedAt: now,
				},
				Name:          "hg-two",
				Description:   "Second host group",
				State:         "READY",
				OSType:        "WINDOWS",
				Hosts:         []string{"iqn.1998-01.com.vmware:host2"},
				HostGroupType: "ISCSI_INITIATOR",
			},
		}

		mockOrchestrator.EXPECT().GetHostGroupsByUUIDs(mock.Anything, req.HostGroupUuids).
			Return(hostGroups, nil)

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		ok := result.(*gcpgenserver.V1betaBatchListHostGroupsOK)
		assert.Len(tt, ok.HostGroups, 2)

		assert.Equal(tt, "uuid-1", ok.HostGroups[0].HostGroupId.Value)
		assert.Equal(tt, "hg-one", ok.HostGroups[0].Name.Value)
		assert.Equal(tt, "hg-one", ok.HostGroups[0].ResourceId.Value)
		assert.Equal(tt, "First host group", ok.HostGroups[0].Description.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaState("READY"), ok.HostGroups[0].State.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsType("LINUX"), ok.HostGroups[0].OsType.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaType("ISCSI_INITIATOR"), ok.HostGroups[0].Type.Value)
		assert.Equal(tt, []string{"iqn.1998-01.com.vmware:host1"}, ok.HostGroups[0].Hosts.Value)

		assert.Equal(tt, "uuid-2", ok.HostGroups[1].HostGroupId.Value)
		assert.Equal(tt, "hg-two", ok.HostGroups[1].Name.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsType("WINDOWS"), ok.HostGroups[1].OsType.Value)
	})
}

func TestConvertToBatchHostGroupV1Beta(t *testing.T) {
	t.Run("ConvertsAllFieldsWhenPopulated", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID:      "test-uuid",
				CreatedAt: now,
			},
			Name:          "my-host-group",
			Description:   "test description",
			State:         "READY",
			OSType:        "LINUX",
			Hosts:         []string{"host1", "host2"},
			HostGroupType: "ISCSI_INITIATOR",
		}

		result := convertToBatchHostGroupV1Beta(hg)

		assert.Equal(tt, "test-uuid", result.HostGroupId.Value)
		assert.True(tt, result.Name.Set)
		assert.False(tt, result.Name.Null)
		assert.Equal(tt, "my-host-group", result.Name.Value)
		assert.Equal(tt, "my-host-group", result.ResourceId.Value)
		assert.Equal(tt, "test description", result.Description.Value)
		assert.Equal(tt, now, result.Created.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaState("READY"), result.State.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaType("ISCSI_INITIATOR"), result.Type.Value)
		assert.Equal(tt, []string{"host1", "host2"}, result.Hosts.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsType("LINUX"), result.OsType.Value)
	})

	t.Run("ReturnsNullForOptionalEmptyFields", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID:      "test-uuid",
				CreatedAt: now,
			},
			Name:          "",
			Description:   "",
			State:         "READY",
			OSType:        "LINUX",
			Hosts:         []string{"host1"},
			HostGroupType: "ISCSI_INITIATOR",
		}

		result := convertToBatchHostGroupV1Beta(hg)

		assert.Equal(tt, "test-uuid", result.HostGroupId.Value)

		assert.True(tt, result.Name.Set)
		assert.True(tt, result.Name.Null)

		assert.True(tt, result.ResourceId.Set)
		assert.True(tt, result.ResourceId.Null)

		assert.True(tt, result.Description.Set)
		assert.True(tt, result.Description.Null)

		assert.Equal(tt, now, result.Created.Value)
		assert.False(tt, result.Created.Null)

		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaState("READY"), result.State.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsType("LINUX"), result.OsType.Value)
		assert.Equal(tt, []string{"host1"}, result.Hosts.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaType("ISCSI_INITIATOR"), result.Type.Value)
	})

	t.Run("ReturnsDefaultEnumValuesWhenEmpty", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel: models.BaseModel{
				UUID:      "test-uuid",
				CreatedAt: now,
			},
			Name:          "my-hg",
			Description:   "desc",
			State:         "",
			OSType:        "",
			Hosts:         []string{"host1"},
			HostGroupType: "",
		}

		result := convertToBatchHostGroupV1Beta(hg)

		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED, result.State.Value)
		assert.True(tt, result.State.Set)
		assert.False(tt, result.State.Null)

		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED, result.Type.Value)
		assert.True(tt, result.Type.Set)
		assert.False(tt, result.Type.Null)

		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED, result.OsType.Value)
		assert.True(tt, result.OsType.Set)
		assert.False(tt, result.OsType.Null)
	})
}
