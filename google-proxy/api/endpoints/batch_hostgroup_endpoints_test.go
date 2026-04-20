package api

import (
	"context"
	"encoding/json"
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

	t.Run("Success_WithFields", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		now := time.Now()
		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{
			LocationId: validLocation,
			Fields:     []gcpgenserver.V1betaBatchListHostGroupsFieldsItem{"name", "state"},
		}

		hostGroups := []*models.HostGroup{
			{
				BaseModel:     models.BaseModel{UUID: "uuid-1", CreatedAt: now},
				Name:          "hg-one",
				Description:   "First host group",
				State:         "READY",
				OSType:        "LINUX",
				Hosts:         []string{"iqn.1998-01.com.vmware:host1"},
				HostGroupType: "ISCSI_INITIATOR",
			},
		}

		mockOrchestrator.EXPECT().GetHostGroupsByUUIDs(mock.Anything, req.HostGroupUuids).
			Return(hostGroups, nil)

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		ok := result.(*gcpgenserver.V1betaBatchListHostGroupsOK)
		assert.Len(tt, ok.HostGroups, 1)

		hg := ok.HostGroups[0]
		assert.Equal(tt, "uuid-1", hg.HostGroupId.Value)
		assert.True(tt, hg.Name.Set, "name should be present when requested")
		assert.Equal(tt, "hg-one", hg.Name.Value)
		assert.True(tt, hg.State.Set, "state should be present when requested")
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaState("READY"), hg.State.Value)

		assert.False(tt, hg.ResourceId.Set, "resourceId should be absent when not requested")
		assert.False(tt, hg.Description.Set, "description should be absent when not requested")
		assert.False(tt, hg.Created.Set, "created should be absent when not requested")
		assert.False(tt, hg.Type.Set, "type should be absent when not requested")
		assert.False(tt, hg.Hosts.Set, "hosts should be absent when not requested")
		assert.False(tt, hg.OsType.Set, "osType should be absent when not requested")
	})

	t.Run("NoFieldsRequested_ReturnsOnlyHostGroupId", func(tt *testing.T) {
		origFn := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = origFn }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return locationID, "", nil
		}

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		now := time.Now()
		req := &gcpgenserver.BatchHostGroupUUIDListV1beta{
			HostGroupUuids: []string{"uuid-1"},
		}
		params := gcpgenserver.V1betaBatchListHostGroupsParams{LocationId: validLocation}

		hostGroups := []*models.HostGroup{
			{
				BaseModel:     models.BaseModel{UUID: "uuid-1", CreatedAt: now},
				Name:          "hg-one",
				Description:   "First host group",
				State:         "READY",
				OSType:        "LINUX",
				Hosts:         []string{"iqn.1998-01.com.vmware:host1"},
				HostGroupType: "ISCSI_INITIATOR",
			},
		}

		mockOrchestrator.EXPECT().GetHostGroupsByUUIDs(mock.Anything, req.HostGroupUuids).
			Return(hostGroups, nil)

		result, err := handler.V1betaBatchListHostGroups(context.Background(), req, params)

		assert.NoError(tt, err)
		ok := result.(*gcpgenserver.V1betaBatchListHostGroupsOK)
		assert.Len(tt, ok.HostGroups, 1)

		hg := ok.HostGroups[0]
		assert.Equal(tt, "uuid-1", hg.HostGroupId.Value, "hostGroupId always present")
		assert.False(tt, hg.Name.Set, "name should be absent when no fields requested")
		assert.False(tt, hg.ResourceId.Set, "resourceId should be absent when no fields requested")
		assert.False(tt, hg.Description.Set, "description should be absent when no fields requested")
		assert.False(tt, hg.Created.Set, "created should be absent when no fields requested")
		assert.False(tt, hg.State.Set, "state should be absent when no fields requested")
		assert.False(tt, hg.Type.Set, "type should be absent when not requested")
		assert.False(tt, hg.Hosts.Set, "hosts should be absent when no fields requested")
		assert.False(tt, hg.OsType.Set, "osType should be absent when no fields requested")
	})

	t.Run("WhenHostGroupsFound_AllFields", func(tt *testing.T) {
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
		params := gcpgenserver.V1betaBatchListHostGroupsParams{
			LocationId: validLocation,
			Fields: []gcpgenserver.V1betaBatchListHostGroupsFieldsItem{
				"name", "resourceId", "description", "created", "state", "type", "hosts", "osType",
			},
		}

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

func TestBuildHostGroupFieldSet(t *testing.T) {
	t.Run("NilFields_ReturnsNil", func(tt *testing.T) {
		result := buildHostGroupFieldSet(nil)
		assert.Nil(tt, result)
	})

	t.Run("EmptyFields_ReturnsNil", func(tt *testing.T) {
		result := buildHostGroupFieldSet([]gcpgenserver.V1betaBatchListHostGroupsFieldsItem{})
		assert.Nil(tt, result)
	})

	t.Run("WithFields_ReturnsMap", func(tt *testing.T) {
		fields := []gcpgenserver.V1betaBatchListHostGroupsFieldsItem{"name", "state", "osType"}
		result := buildHostGroupFieldSet(fields)
		assert.NotNil(tt, result)
		assert.True(tt, result["name"])
		assert.True(tt, result["state"])
		assert.True(tt, result["osType"])
		assert.False(tt, result["description"])
	})
}

func TestConvertToBatchHostGroupV1Beta(t *testing.T) {
	t.Run("NoFields_ReturnsOnlyHostGroupId", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel: models.BaseModel{UUID: "test-uuid", CreatedAt: now},
			Name:      "my-host-group",
			State:     "READY",
			OSType:    "LINUX",
		}

		result := convertToBatchHostGroupV1Beta(hg, nil)

		assert.Equal(tt, "test-uuid", result.HostGroupId.Value)
		assert.False(tt, result.Name.Set)
		assert.False(tt, result.ResourceId.Set)
		assert.False(tt, result.Description.Set)
		assert.False(tt, result.Created.Set)
		assert.False(tt, result.State.Set)
		assert.False(tt, result.Type.Set)
		assert.False(tt, result.Hosts.Set)
		assert.False(tt, result.OsType.Set)
	})

	t.Run("AllFieldsRequested_ConvertsAllFields", func(tt *testing.T) {
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

		fieldSet := map[string]bool{
			"name": true, "resourceId": true, "description": true,
			"created": true, "state": true, "type": true,
			"hosts": true, "osType": true,
		}

		result := convertToBatchHostGroupV1Beta(hg, fieldSet)

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

	t.Run("PartialFields_OnlyRequestedFieldsPresent", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel: models.BaseModel{UUID: "test-uuid", CreatedAt: now},
			Name:      "my-host-group",
			State:     "READY",
			OSType:    "LINUX",
			Hosts:     []string{"host1"},
		}

		fieldSet := map[string]bool{"name": true, "state": true}

		result := convertToBatchHostGroupV1Beta(hg, fieldSet)

		assert.Equal(tt, "test-uuid", result.HostGroupId.Value)
		assert.True(tt, result.Name.Set)
		assert.Equal(tt, "my-host-group", result.Name.Value)
		assert.True(tt, result.State.Set)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaState("READY"), result.State.Value)

		assert.False(tt, result.ResourceId.Set)
		assert.False(tt, result.Description.Set)
		assert.False(tt, result.Created.Set)
		assert.False(tt, result.Type.Set)
		assert.False(tt, result.Hosts.Set)
		assert.False(tt, result.OsType.Set)
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

		fieldSet := map[string]bool{
			"name": true, "resourceId": true, "description": true,
			"created": true, "state": true, "type": true,
			"hosts": true, "osType": true,
		}

		result := convertToBatchHostGroupV1Beta(hg, fieldSet)

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

		fieldSet := map[string]bool{
			"name": true, "resourceId": true, "description": true,
			"created": true, "state": true, "type": true,
			"hosts": true, "osType": true,
		}

		result := convertToBatchHostGroupV1Beta(hg, fieldSet)

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

	t.Run("EmptyHosts_ReturnsNullWhenHostsRequested", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel:     models.BaseModel{UUID: "test-uuid", CreatedAt: now},
			Name:          "hg",
			State:         "READY",
			OSType:        "LINUX",
			Hosts:         []string{},
			HostGroupType: "ISCSI_INITIATOR",
		}
		fieldSet := map[string]bool{"hosts": true, "state": true}
		result := convertToBatchHostGroupV1Beta(hg, fieldSet)
		assert.True(tt, result.Hosts.Set)
		assert.True(tt, result.Hosts.Null)
	})

	t.Run("NilHostsSlice_ReturnsNullWhenHostsRequested", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel:     models.BaseModel{UUID: "test-uuid", CreatedAt: now},
			Name:          "hg",
			State:         "READY",
			OSType:        "LINUX",
			Hosts:         nil,
			HostGroupType: "ISCSI_INITIATOR",
		}
		fieldSet := map[string]bool{"hosts": true}
		result := convertToBatchHostGroupV1Beta(hg, fieldSet)
		assert.True(tt, result.Hosts.Set)
		assert.True(tt, result.Hosts.Null)
	})

	t.Run("InternalDBStrings_MapToMarshalableAPIEnums", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel:     models.BaseModel{UUID: "test-uuid", CreatedAt: now},
			Name:          "hg",
			State:         models.LifeCycleStateDeleted,
			OSType:        "not-a-real-os",
			Hosts:         []string{"iqn.example"},
			HostGroupType: "FC_INITIATOR",
		}
		fieldSet := map[string]bool{"state": true, "type": true, "osType": true}
		result := convertToBatchHostGroupV1Beta(hg, fieldSet)

		_, err := result.State.Value.MarshalText()
		assert.NoError(tt, err)
		_, err = result.Type.Value.MarshalText()
		assert.NoError(tt, err)
		_, err = result.OsType.Value.MarshalText()
		assert.NoError(tt, err)

		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaStateDELETING, result.State.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED, result.Type.Value)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED, result.OsType.Value)
	})

	t.Run("ZeroCreatedAt_RequestedCreated_EmitsNullOpt", func(tt *testing.T) {
		hg := &models.HostGroup{
			BaseModel: models.BaseModel{UUID: "test-uuid"},
			Name:      "hg",
			State:     "READY",
		}
		fieldSet := map[string]bool{"created": true, "state": true}
		result := convertToBatchHostGroupV1Beta(hg, fieldSet)
		assert.True(tt, result.Created.Set)
		assert.True(tt, result.Created.Null)
	})

	t.Run("InvalidUTF8InStrings_JSONMarshalSucceeds", func(tt *testing.T) {
		now := time.Now()
		hg := &models.HostGroup{
			BaseModel:     models.BaseModel{UUID: "test-uuid\x80", CreatedAt: now},
			Name:          "n\x80",
			Description:   "d\x80",
			State:         "READY",
			OSType:        "LINUX",
			Hosts:         []string{"iqn\x80bad"},
			HostGroupType: "ISCSI_INITIATOR",
		}
		fieldSet := map[string]bool{
			"name": true, "resourceId": true, "description": true,
			"created": true, "state": true, "type": true,
			"hosts": true, "osType": true,
		}
		result := convertToBatchHostGroupV1Beta(hg, fieldSet)
		_, err := json.Marshal(&result)
		assert.NoError(tt, err)
	})
}

func TestApplyBatchHostGroupFieldSelection(t *testing.T) {
	t.Run("WithRequestedFields_ReturnsOnlyRequestedFields", func(tt *testing.T) {
		bp := gcpgenserver.BatchHostGroupV1beta{
			HostGroupId: gcpgenserver.NewOptString("uuid-1"),
			Name:        gcpgenserver.NewOptNilString("hg-one"),
			ResourceId:  gcpgenserver.NewOptNilString("hg-one"),
			Description: gcpgenserver.NewOptNilString("desc"),
			State:       gcpgenserver.NewOptNilBatchHostGroupV1betaState("READY"),
		}

		applyBatchHostGroupFieldSelection(&bp, map[string]bool{"name": true})

		assert.True(tt, bp.Name.Set)
		assert.Equal(tt, "hg-one", bp.Name.Value)
		assert.False(tt, bp.ResourceId.Set)
		assert.False(tt, bp.Description.Set)
		assert.False(tt, bp.State.Set)
	})

	t.Run("WithoutFields_KeepsOnlyHostGroupId", func(tt *testing.T) {
		bp := gcpgenserver.BatchHostGroupV1beta{
			HostGroupId: gcpgenserver.NewOptString("uuid-1"),
			Name:        gcpgenserver.NewOptNilString("hg-one"),
			ResourceId:  gcpgenserver.NewOptNilString("hg-one"),
			Description: gcpgenserver.NewOptNilString("desc"),
			State:       gcpgenserver.NewOptNilBatchHostGroupV1betaState("READY"),
			Type:        gcpgenserver.NewOptNilBatchHostGroupV1betaType("ISCSI_INITIATOR"),
			OsType:      gcpgenserver.NewOptNilBatchHostGroupV1betaOsType("LINUX"),
		}

		applyBatchHostGroupFieldSelection(&bp, nil)

		assert.True(tt, bp.HostGroupId.Set)
		assert.Equal(tt, "uuid-1", bp.HostGroupId.Value)
		assert.False(tt, bp.Name.Set)
		assert.False(tt, bp.ResourceId.Set)
		assert.False(tt, bp.Description.Set)
		assert.False(tt, bp.Created.Set)
		assert.False(tt, bp.State.Set)
		assert.False(tt, bp.Type.Set)
		assert.False(tt, bp.Hosts.Set)
		assert.False(tt, bp.OsType.Set)
	})

	t.Run("AllFields_KeepsEverything", func(tt *testing.T) {
		bp := gcpgenserver.BatchHostGroupV1beta{
			HostGroupId: gcpgenserver.NewOptString("uuid-1"),
			Name:        gcpgenserver.NewOptNilString("hg-one"),
			ResourceId:  gcpgenserver.NewOptNilString("hg-one"),
			Description: gcpgenserver.NewOptNilString("desc"),
			Created:     gcpgenserver.NewOptNilDateTime(time.Now()),
			State:       gcpgenserver.NewOptNilBatchHostGroupV1betaState("READY"),
			Type:        gcpgenserver.NewOptNilBatchHostGroupV1betaType("ISCSI_INITIATOR"),
			Hosts:       gcpgenserver.NewOptNilStringArray([]string{"host1"}),
			OsType:      gcpgenserver.NewOptNilBatchHostGroupV1betaOsType("LINUX"),
		}

		allFields := map[string]bool{
			"name": true, "resourceId": true, "description": true,
			"created": true, "state": true, "type": true,
			"hosts": true, "osType": true,
		}

		applyBatchHostGroupFieldSelection(&bp, allFields)

		assert.True(tt, bp.Name.Set)
		assert.True(tt, bp.ResourceId.Set)
		assert.True(tt, bp.Description.Set)
		assert.True(tt, bp.Created.Set)
		assert.True(tt, bp.State.Set)
		assert.True(tt, bp.Type.Set)
		assert.True(tt, bp.Hosts.Set)
		assert.True(tt, bp.OsType.Set)
	})
}

func TestEnsureRequestedHostGroupFieldsPresent(t *testing.T) {
	t.Run("SetsRequestedUnsetFieldsToNull", func(tt *testing.T) {
		bp := gcpgenserver.BatchHostGroupV1beta{
			HostGroupId: gcpgenserver.NewOptString("uuid-1"),
		}

		fields := map[string]bool{
			"name": true, "resourceId": true, "description": true,
			"created": true, "state": true, "type": true,
			"hosts": true, "osType": true,
		}

		ensureRequestedHostGroupFieldsPresent(&bp, fields)

		assert.True(tt, bp.Name.Set)
		assert.True(tt, bp.Name.Null)
		assert.True(tt, bp.ResourceId.Set)
		assert.True(tt, bp.ResourceId.Null)
		assert.True(tt, bp.Description.Set)
		assert.True(tt, bp.Description.Null)
		assert.True(tt, bp.Created.Set)
		assert.True(tt, bp.Created.Null)
		assert.True(tt, bp.Hosts.Set)
		assert.True(tt, bp.Hosts.Null)

		assert.True(tt, bp.State.Set)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaStateSTATEUNSPECIFIED, bp.State.Value)
		assert.True(tt, bp.Type.Set)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaTypeUNSPECIFIED, bp.Type.Value)
		assert.True(tt, bp.OsType.Set)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaOsTypeOSTYPEUNSPECIFIED, bp.OsType.Value)
	})

	t.Run("NilFieldSet_DoesNothing", func(tt *testing.T) {
		bp := gcpgenserver.BatchHostGroupV1beta{
			HostGroupId: gcpgenserver.NewOptString("uuid-1"),
		}

		ensureRequestedHostGroupFieldsPresent(&bp, nil)

		assert.False(tt, bp.Name.Set)
		assert.False(tt, bp.State.Set)
	})

	t.Run("DoesNotOverwriteExistingValues", func(tt *testing.T) {
		bp := gcpgenserver.BatchHostGroupV1beta{
			HostGroupId: gcpgenserver.NewOptString("uuid-1"),
			Name:        gcpgenserver.NewOptNilString("existing-name"),
			State:       gcpgenserver.NewOptNilBatchHostGroupV1betaState("READY"),
		}

		fields := map[string]bool{"name": true, "state": true, "description": true}

		ensureRequestedHostGroupFieldsPresent(&bp, fields)

		assert.Equal(tt, "existing-name", bp.Name.Value)
		assert.False(tt, bp.Name.Null)
		assert.Equal(tt, gcpgenserver.BatchHostGroupV1betaState("READY"), bp.State.Value)
		assert.True(tt, bp.Description.Set)
		assert.True(tt, bp.Description.Null)
	})
}
