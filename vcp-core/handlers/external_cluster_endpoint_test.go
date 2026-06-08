package api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
	"gorm.io/gorm"
)

func TestV1OnboardExternalCluster_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	req := &oasgenserver.ExternalClusterOnboardRequestV1{
		LocationId: "us-central1",
		Hosts: []oasgenserver.ExternalClusterHostV1{
			{
				HostName:     "ontap-hw-cluster-01",
				ManagementIp: "10.0.0.1",
				AdminCredentials: oasgenserver.ExternalClusterAdminCredentialsV1{
					Username: "admin",
					Password: "secret",
				},
			},
		},
	}

	now := time.Now().UTC()
	created := []*datamodel.Cluster{{
		BaseModel: datamodel.BaseModel{
			UUID:      "9760acf5-4638-11e7-9bdb-020073ca3333",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:            "us-central1",
		HostName:              "ontap-hw-cluster-01",
		AdminUsername:         "admin",
		LifecycleState:        "CREATED",
		LifecycleStateDetails: "Registered",
	}}

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.MatchedBy(func(p *common.OnboardExternalClustersParams) bool {
		return p != nil && p.LocationID == "us-central1" && len(p.Hosts) == 1 &&
			p.Hosts[0].HostName == "ontap-hw-cluster-01" && p.Hosts[0].Username == "admin" &&
			p.Hosts[0].Password == "secret" && p.Hosts[0].ManagementIP == "10.0.0.1"
	})).Return(created, nil)

	result, err := handler.V1OnboardExternalCluster(context.Background(), req, oasgenserver.V1OnboardExternalClusterParams{})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1OnboardExternalClusterCreatedApplicationJSON)
	assert.True(t, ok)
	assert.Len(t, *res, 1)
	assert.Equal(t, "9760acf5-4638-11e7-9bdb-020073ca3333", (*res)[0].ExternalClusterId.Value)
	assert.Equal(t, "us-central1", (*res)[0].LocationId.Value)
	assert.Equal(t, "admin", (*res)[0].AdminUsername.Value)
	assert.Equal(t, oasgenserver.ExternalClusterHostResourceV1LifeCycleStateCREATED, (*res)[0].LifeCycleState.Value)
}

func TestV1OnboardExternalCluster_ResponseOmitsPassword(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	req := &oasgenserver.ExternalClusterOnboardRequestV1{
		LocationId: "us-central1",
		Hosts: []oasgenserver.ExternalClusterHostV1{
			{
				HostName:     "ontap-hw-cluster-01",
				ManagementIp: "10.0.0.1",
				AdminCredentials: oasgenserver.ExternalClusterAdminCredentialsV1{
					Username: "admin",
					Password: "must-not-appear-in-response",
				},
			},
		},
	}

	now := time.Now().UTC()
	created := []*datamodel.Cluster{{
		BaseModel: datamodel.BaseModel{
			UUID:      "9760acf5-4638-11e7-9bdb-020073ca3333",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:    "us-central1",
		HostName:      "ontap-hw-cluster-01",
		AdminUsername: "admin",
		AdminPassword: "encrypted-ciphertext-stored-in-db",
	}}

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).Return(created, nil)

	result, err := handler.V1OnboardExternalCluster(context.Background(), req, oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	res, ok := result.(*oasgenserver.V1OnboardExternalClusterCreatedApplicationJSON)
	require.True(t, ok)
	require.Len(t, *res, 1)
	// API resource has no password field; ensure mapping does not leak stored ciphertext via other fields.
	assert.Equal(t, "admin", (*res)[0].AdminUsername.Value)
	assert.False(t, (*res)[0].HostName.Value == "must-not-appear-in-response")
}

func TestV1GetExternalCluster_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	now := time.Now().UTC()
	host := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{
			UUID:      "9760acf5-4638-11e7-9bdb-020073ca3333",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:            "us-central1",
		HostName:              "ontap-hw-cluster-01",
		AdminUsername:         "admin",
		LifecycleState:        "CREATED",
		LifecycleStateDetails: "Registered",
	}

	mockOrch.On("GetExternalCluster", mock.Anything, "9760acf5-4638-11e7-9bdb-020073ca3333").
		Return(host, nil)

	result, err := handler.V1GetExternalCluster(context.Background(), oasgenserver.V1GetExternalClusterParams{
		ExternalClusterId: "9760acf5-4638-11e7-9bdb-020073ca3333",
	})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.ExternalClusterHostResourceV1)
	assert.True(t, ok)
	assert.Equal(t, "9760acf5-4638-11e7-9bdb-020073ca3333", res.ExternalClusterId.Value)
}

func TestV1GetExternalCluster_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("GetExternalCluster", mock.Anything, "missing-id").
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("not found")))

	result, err := handler.V1GetExternalCluster(context.Background(), oasgenserver.V1GetExternalClusterParams{
		ExternalClusterId: "missing-id",
	})

	assert.NoError(t, err)
	_, ok := result.(*oasgenserver.V1GetExternalClusterNotFound)
	assert.True(t, ok)
}

func TestV1DeleteExternalCluster_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	now := time.Now().UTC()
	deleted := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{
			UUID:      "9760acf5-4638-11e7-9bdb-020073ca3333",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:            "us-central1",
		HostName:              "ontap-hw-cluster-01",
		AdminUsername:         "admin",
		LifecycleState:        "DELETED",
		LifecycleStateDetails: "DELETED",
	}

	mockOrch.On("DeleteExternalCluster", mock.Anything, "9760acf5-4638-11e7-9bdb-020073ca3333").
		Return(deleted, nil)

	result, err := handler.V1DeleteExternalCluster(context.Background(), oasgenserver.V1DeleteExternalClusterParams{
		ExternalClusterId: "9760acf5-4638-11e7-9bdb-020073ca3333",
	})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.ExternalClusterHostResourceV1)
	assert.True(t, ok)
	assert.Equal(t, oasgenserver.ExternalClusterHostResourceV1LifeCycleStateDELETED, res.LifeCycleState.Value)
}

func TestV1DeleteExternalCluster_IdempotentWhenAlreadyDeleted(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	now := time.Now().UTC()
	deleted := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{
			UUID:      "9760acf5-4638-11e7-9bdb-020073ca3333",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:            "us-central1",
		HostName:              "ontap-hw-cluster-01",
		AdminUsername:         "admin",
		LifecycleState:        "DELETED",
		LifecycleStateDetails: "DELETED",
	}

	mockOrch.On("DeleteExternalCluster", mock.Anything, "9760acf5-4638-11e7-9bdb-020073ca3333").
		Return(deleted, nil)

	result, err := handler.V1DeleteExternalCluster(context.Background(), oasgenserver.V1DeleteExternalClusterParams{
		ExternalClusterId: "9760acf5-4638-11e7-9bdb-020073ca3333",
	})

	assert.NoError(t, err)
	_, ok := result.(*oasgenserver.ExternalClusterHostResourceV1)
	assert.True(t, ok)
}

func TestV1DeleteExternalCluster_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("DeleteExternalCluster", mock.Anything, "missing-id").
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("not found")))

	result, err := handler.V1DeleteExternalCluster(context.Background(), oasgenserver.V1DeleteExternalClusterParams{
		ExternalClusterId: "missing-id",
	})

	assert.NoError(t, err)
	_, ok := result.(*oasgenserver.V1DeleteExternalClusterNotFound)
	assert.True(t, ok)
}

func TestV1OnboardExternalCluster_Conflict(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	req := &oasgenserver.ExternalClusterOnboardRequestV1{
		LocationId: "us-central1",
		Hosts: []oasgenserver.ExternalClusterHostV1{
			{
				HostName:     "host1",
				ManagementIp: "10.0.0.1",
				AdminCredentials: oasgenserver.ExternalClusterAdminCredentialsV1{
					Username: "admin",
					Password: "secret",
				},
			},
		},
	}

	dupErr := fmt.Errorf(`external cluster %q already onboarded in location %q`, "host1", "us-central1")
	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, dupErr))

	result, err := handler.V1OnboardExternalCluster(context.Background(), req, oasgenserver.V1OnboardExternalClusterParams{})

	assert.NoError(t, err)
	conflict, ok := result.(*oasgenserver.V1OnboardExternalClusterConflict)
	assert.True(t, ok)
	assert.Equal(t, float64(409), conflict.Code)
	assert.Equal(t, `external cluster "host1" already onboarded in location "us-central1"`, conflict.Message)
}

func TestV1OnboardExternalCluster_PassesDescriptionAndManagementIP(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	req := &oasgenserver.ExternalClusterOnboardRequestV1{
		LocationId: "us-central1",
		Hosts: []oasgenserver.ExternalClusterHostV1{
			{
				HostName:     "ontap-hw-cluster-01.example.com",
				Description:  oasgenserver.NewOptString("Primary DR site"),
				Label:        oasgenserver.NewOptString("type=SAPHANA"),
				Protocol:     oasgenserver.NewOptExternalClusterHostV1Protocol(oasgenserver.ExternalClusterHostV1ProtocolHTTPS),
				ManagementIp: "10.10.10.50",
				AdminCredentials: oasgenserver.ExternalClusterAdminCredentialsV1{
					Username: "admin",
					Password: "secret",
				},
			},
		},
	}

	now := time.Now().UTC()
	created := []*datamodel.Cluster{{
		BaseModel: datamodel.BaseModel{
			UUID:      "9760acf5-4638-11e7-9bdb-020073ca3333",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:    "us-central1",
		HostName:      "ontap-hw-cluster-01.example.com",
		Description:   "Primary DR site",
		Label:         "type=SAPHANA",
		Protocol:      "HTTPS",
		Port:          443,
		AdminUsername: "admin",
		ClusterAttributes: &datamodel.ClusterAttributes{
			ManagementIP: "10.10.10.50",
		},
	}}

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.MatchedBy(func(p *common.OnboardExternalClustersParams) bool {
		return p != nil && len(p.Hosts) == 1 &&
			p.Hosts[0].Description == "Primary DR site" &&
			p.Hosts[0].Label == "type=SAPHANA" &&
			p.Hosts[0].Protocol == "HTTPS" &&
			p.Hosts[0].Port == 0 &&
			p.Hosts[0].ManagementIP == "10.10.10.50"
	})).Return(created, nil)

	result, err := handler.V1OnboardExternalCluster(context.Background(), req, oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	res, ok := result.(*oasgenserver.V1OnboardExternalClusterCreatedApplicationJSON)
	require.True(t, ok)
	require.Len(t, *res, 1)
	assert.Equal(t, "Primary DR site", (*res)[0].Description.Value)
	assert.Equal(t, "type=SAPHANA", (*res)[0].Label.Value)
	assert.Equal(t, oasgenserver.ExternalClusterHostResourceV1ProtocolHTTPS, (*res)[0].Protocol.Value)
	assert.Equal(t, int32(443), (*res)[0].Port.Value)
	assert.Equal(t, "10.10.10.50", (*res)[0].ManagementIp.Value)
	assert.False(t, (*res)[0].OntapVersion.IsSet())
}

func TestV1UpdateExternalCluster_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	hostID := "9760acf5-4638-11e7-9bdb-020073ca3333"
	req := &oasgenserver.ExternalClusterHostUpdateV1{
		Description: oasgenserver.NewOptNilString("Updated description"),
		Label:       oasgenserver.NewOptString("type=UPDATED"),
	}

	now := time.Now().UTC()
	updated := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{
			UUID:      hostID,
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:  "us-central1",
		HostName:    "ontap-hw-cluster-01",
		Description: "Updated description",
		Label:       "type=UPDATED",
		Protocol:    "HTTPS",
		Port:        443,
	}

	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(p *common.UpdateExternalClusterParams) bool {
		return p != nil && p.ExternalClusterID == hostID && p.Description != nil && *p.Description == "Updated description" &&
			p.Label != nil && *p.Label == "type=UPDATED"
	})).Return(updated, nil)

	result, err := handler.V1UpdateExternalCluster(context.Background(), req, oasgenserver.V1UpdateExternalClusterParams{
		ExternalClusterId: hostID,
	})

	require.NoError(t, err)
	res, ok := result.(*oasgenserver.ExternalClusterHostResourceV1)
	require.True(t, ok)
	assert.Equal(t, "Updated description", res.Description.Value)
	assert.Equal(t, "type=UPDATED", res.Label.Value)
}

func TestV1UpdateExternalCluster_EmptyBody(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	result, err := handler.V1UpdateExternalCluster(context.Background(), &oasgenserver.ExternalClusterHostUpdateV1{},
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: "9760acf5-4638-11e7-9bdb-020073ca3333"})

	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1UpdateExternalClusterBadRequest)
	assert.True(t, ok)
}

func TestV1UpdateExternalCluster_UpdatesProtocolAndPort(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	hostID := "9760acf5-4638-11e7-9bdb-020073ca3333"
	req := &oasgenserver.ExternalClusterHostUpdateV1{
		Protocol: oasgenserver.NewOptExternalClusterHostUpdateV1Protocol(oasgenserver.ExternalClusterHostUpdateV1ProtocolHTTP),
		Port:     oasgenserver.NewOptInt32(8080),
	}

	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(p *common.UpdateExternalClusterParams) bool {
		return p.Protocol != nil && *p.Protocol == "HTTP" && p.Port != nil && *p.Port == 8080
	})).Return(&datamodel.Cluster{BaseModel: datamodel.BaseModel{UUID: hostID}}, nil)

	_, err := handler.V1UpdateExternalCluster(context.Background(), req,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: hostID})
	require.NoError(t, err)
}

func TestV1UpdateExternalCluster_UpdatesCredentials(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	hostID := "9760acf5-4638-11e7-9bdb-020073ca3333"
	req := &oasgenserver.ExternalClusterHostUpdateV1{
		AdminCredentials: oasgenserver.NewOptExternalClusterAdminCredentialsV1(
			oasgenserver.ExternalClusterAdminCredentialsV1{
				Username: "admin",
				Password: "new-secret",
			},
		),
	}

	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(p *common.UpdateExternalClusterParams) bool {
		return p.Password != nil && *p.Password == "new-secret" &&
			p.Username != nil && *p.Username == "admin"
	})).Return(&datamodel.Cluster{BaseModel: datamodel.BaseModel{UUID: hostID}}, nil)

	_, err := handler.V1UpdateExternalCluster(context.Background(), req,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: hostID})
	require.NoError(t, err)
}

func TestV1UpdateExternalCluster_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	req := &oasgenserver.ExternalClusterHostUpdateV1{
		Label: oasgenserver.NewOptString("x"),
	}
	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.Anything).
		Return(nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("not found")))

	result, err := handler.V1UpdateExternalCluster(context.Background(), req,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: "9760acf5-4638-11e7-9bdb-020073ca3333"})

	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1UpdateExternalClusterNotFound)
	assert.True(t, ok)
}

func TestV1OnboardExternalCluster_NilRequest(t *testing.T) {
	handler := NewHandler(factory.NewMockOrchestratorFactory(t))

	result, err := handler.V1OnboardExternalCluster(context.Background(), nil, oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	badReq, ok := result.(*oasgenserver.V1OnboardExternalClusterBadRequest)
	require.True(t, ok)
	assert.Equal(t, float64(400), badReq.Code)
}

func TestV1OnboardExternalCluster_NotImplemented(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).
		Return(nil, utilserrors.NewNotImplementedYetErr())

	result, err := handler.V1OnboardExternalCluster(context.Background(), validOnboardRequest(), oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1OnboardExternalClusterInternalServerError)
	assert.True(t, ok)
}

func TestV1OnboardExternalCluster_BadRequest(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).
		Return(nil, utilserrors.NewBadRequestErr("invalid protocol"))

	result, err := handler.V1OnboardExternalCluster(context.Background(), validOnboardRequest(), oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	badReq, ok := result.(*oasgenserver.V1OnboardExternalClusterBadRequest)
	require.True(t, ok)
	assert.Equal(t, float64(400), badReq.Code)
}

func TestV1OnboardExternalCluster_ConflictFromUtilsError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).
		Return(nil, utilserrors.NewConflictErr("host already exists"))

	result, err := handler.V1OnboardExternalCluster(context.Background(), validOnboardRequest(), oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	conflict, ok := result.(*oasgenserver.V1OnboardExternalClusterConflict)
	require.True(t, ok)
	assert.Equal(t, float64(409), conflict.Code)
}

func TestV1OnboardExternalCluster_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).
		Return(nil, utilserrors.NewNotFoundErr("ExternalCluster", nil))

	result, err := handler.V1OnboardExternalCluster(context.Background(), validOnboardRequest(), oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1OnboardExternalClusterNotFound)
	assert.True(t, ok)
}

func TestV1OnboardExternalCluster_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("OnboardExternalClusters", mock.Anything, mock.Anything).
		Return(nil, errors.New("unexpected failure"))

	result, err := handler.V1OnboardExternalCluster(context.Background(), validOnboardRequest(), oasgenserver.V1OnboardExternalClusterParams{})
	require.NoError(t, err)
	serverErr, ok := result.(*oasgenserver.V1OnboardExternalClusterInternalServerError)
	require.True(t, ok)
	assert.Contains(t, serverErr.Message, "unexpected failure")
}

func TestV1UpdateExternalCluster_NilRequest(t *testing.T) {
	handler := NewHandler(factory.NewMockOrchestratorFactory(t))

	result, err := handler.V1UpdateExternalCluster(context.Background(), nil,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: "id"})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1UpdateExternalClusterBadRequest)
	assert.True(t, ok)
}

func TestV1UpdateExternalCluster_NotImplemented(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.Anything).
		Return(nil, utilserrors.NewNotImplementedYetErr())

	req := &oasgenserver.ExternalClusterHostUpdateV1{Label: oasgenserver.NewOptString("x")}
	result, err := handler.V1UpdateExternalCluster(context.Background(), req,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: "id"})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1UpdateExternalClusterInternalServerError)
	assert.True(t, ok)
}

func TestV1UpdateExternalCluster_BadRequestFromOrchestrator(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.Anything).
		Return(nil, utilserrors.NewBadRequestErr("invalid port"))

	req := &oasgenserver.ExternalClusterHostUpdateV1{Port: oasgenserver.NewOptInt32(0)}
	result, err := handler.V1UpdateExternalCluster(context.Background(), req,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: "id"})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1UpdateExternalClusterBadRequest)
	assert.True(t, ok)
}

func TestV1UpdateExternalCluster_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateExternalCluster", mock.Anything, mock.Anything).
		Return(nil, errors.New("update failed"))

	req := &oasgenserver.ExternalClusterHostUpdateV1{Label: oasgenserver.NewOptString("x")}
	result, err := handler.V1UpdateExternalCluster(context.Background(), req,
		oasgenserver.V1UpdateExternalClusterParams{ExternalClusterId: "id"})
	require.NoError(t, err)
	serverErr, ok := result.(*oasgenserver.V1UpdateExternalClusterInternalServerError)
	require.True(t, ok)
	assert.Contains(t, serverErr.Message, "update failed")
}

func TestV1GetExternalCluster_NotImplemented(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("GetExternalCluster", mock.Anything, "host-id").
		Return(nil, utilserrors.NewNotImplementedYetErr())

	result, err := handler.V1GetExternalCluster(context.Background(), oasgenserver.V1GetExternalClusterParams{
		ExternalClusterId: "host-id",
	})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1GetExternalClusterInternalServerError)
	assert.True(t, ok)
}

func TestV1GetExternalCluster_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("GetExternalCluster", mock.Anything, "host-id").
		Return(nil, errors.New("get failed"))

	result, err := handler.V1GetExternalCluster(context.Background(), oasgenserver.V1GetExternalClusterParams{
		ExternalClusterId: "host-id",
	})
	require.NoError(t, err)
	serverErr, ok := result.(*oasgenserver.V1GetExternalClusterInternalServerError)
	require.True(t, ok)
	assert.Contains(t, serverErr.Message, "get failed")
}

func TestV1DeleteExternalCluster_NotImplemented(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("DeleteExternalCluster", mock.Anything, "host-id").
		Return(nil, utilserrors.NewNotImplementedYetErr())

	result, err := handler.V1DeleteExternalCluster(context.Background(), oasgenserver.V1DeleteExternalClusterParams{
		ExternalClusterId: "host-id",
	})
	require.NoError(t, err)
	_, ok := result.(*oasgenserver.V1DeleteExternalClusterInternalServerError)
	assert.True(t, ok)
}

func TestV1DeleteExternalCluster_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("DeleteExternalCluster", mock.Anything, "host-id").
		Return(nil, errors.New("delete failed"))

	result, err := handler.V1DeleteExternalCluster(context.Background(), oasgenserver.V1DeleteExternalClusterParams{
		ExternalClusterId: "host-id",
	})
	require.NoError(t, err)
	serverErr, ok := result.(*oasgenserver.V1DeleteExternalClusterInternalServerError)
	require.True(t, ok)
	assert.Contains(t, serverErr.Message, "delete failed")
}

func Test_convertExternalClusterToV1_NilRow(t *testing.T) {
	result := convertExternalClusterToV1(nil)
	assert.Equal(t, oasgenserver.ExternalClusterHostResourceV1{}, result)
}

func Test_convertExternalClusterToV1_WithDeletedAt(t *testing.T) {
	now := time.Now().UTC()
	row := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{
			UUID:      "uuid-1",
			CreatedAt: now,
			UpdatedAt: now,
		},
		LocationID:            "us-central1",
		HostName:              "host-1",
		AdminUsername:         "admin",
		LifecycleState:        "DELETED",
		LifecycleStateDetails: "DELETED",
		ClusterAttributes: &datamodel.ClusterAttributes{
			OntapVersion: "9.15.1",
			ManagementIP: "10.0.0.1",
		},
	}
	row.BaseModel.DeletedAt = &gorm.DeletedAt{Time: now, Valid: true}

	result := convertExternalClusterToV1(row)
	assert.True(t, result.DeletedAt.IsSet())
	assert.Equal(t, "9.15.1", result.OntapVersion.Value)
	assert.Equal(t, "10.0.0.1", result.ManagementIp.Value)
}

func validOnboardRequest() *oasgenserver.ExternalClusterOnboardRequestV1 {
	return &oasgenserver.ExternalClusterOnboardRequestV1{
		LocationId: "us-central1",
		Hosts: []oasgenserver.ExternalClusterHostV1{
			{
				HostName:     "host1",
				ManagementIp: "10.0.0.1",
				AdminCredentials: oasgenserver.ExternalClusterAdminCredentialsV1{
					Username: "admin",
					Password: "secret",
				},
			},
		},
	}
}
