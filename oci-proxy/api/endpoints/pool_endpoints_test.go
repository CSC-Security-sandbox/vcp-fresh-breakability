package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	ociserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/oci-proxy/api/oci-servergen"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/workflowquery"
	"go.temporal.io/sdk/client"
)

// defaultTestOPC matches middleware: a stable UUID used when tests do not care about the exact value.
const defaultTestOPC = "aaaaaaaa-bbbb-4ccc-dddd-eeeeeeeeeeee"

const defaultTestTenancyOCID = "ocid1.tenancy.oc1..aaaaaaaatestaaa"

func defaultCreatePoolParams() ociserver.CreatePoolParams {
	return ociserver.CreatePoolParams{TenancyOcid: defaultTestTenancyOCID}
}

// validCreatePoolRequest returns a request that passes validateCreatePoolRequest:
// shared HA (no secondary AD set) so the non-shared-HA rule does not apply, and
// DataEndpointCount=2 to satisfy the parity check. Tests that exercise individual
// validation branches mutate exactly one field at a time.
func validCreatePoolRequest() *ociserver.CreatePoolRequest {
	return &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..valid",
		CompartmentOCID:           "ocid1.compartment.oc1..valid",
		DisplayName:               "valid-pool",
		SubnetId:                  "ocid1.subnet.oc1.iad.valid",
		DataNicSubnetId:           "ocid1.subnet.oc1.iad.validdata",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 1024,
		ThroughputGBps:            1.0,
		DataEndpointCount:         2,
		OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..valid", Version: "1"},
	}
}

// gbpsToMibps converts GBps (SI, 10^9 bytes/s) to MiBps (2^20 bytes/s).
func gbpsToMibps(gbps float64) int64 {
	return int64(gbps * workflowquery.MiBpsPerGBps)
}

func contextWithOpcRequestID(parent context.Context, opc string) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	h := make(http.Header)
	h.Set(string(middleware.OPCRequestIDHeaderName), opc)
	return context.WithValue(parent, middleware.HeaderContextKey, h)
}

func TestNewHandler(t *testing.T) {
	t.Run("NewHandler returns non-nil handler with nil ServerState when passed nil", func(tt *testing.T) {
		h := NewHandler(nil)
		assert.NotNil(tt, h)
		assert.Nil(tt, h.ServerState)
	})
	t.Run("NewHandler returns handler with given ServerState", func(tt *testing.T) {
		serverState := NewServerState()
		h := NewHandler(serverState)
		assert.NotNil(tt, h)
		assert.Same(tt, serverState, h.ServerState)
	})
}

func TestCreatePool(t *testing.T) {
	t.Run("CreatePool returns 202 with workflow body when orchestrator returns success", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().CreatePool(
			mock.Anything,
			mock.MatchedBy(func(p *commonparams.CreatePoolParams) bool {
				return p != nil &&
					p.SizeInBytes == 1024*1024*1024*1024 && // 1024 GiB
					p.HAPairs == 1 && // 2 dataEndpoints / 2
					p.CustomPerformanceParams != nil &&
					p.CustomPerformanceParams.ThroughputMibps == gbpsToMibps(1.0) &&
					p.PrimaryZone == "ad1" &&
					p.SecondaryZone == "ad2" &&
					p.IsRegionalHA == true // ad1 != ad2 → derived as regional HA
			}),
		).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-uuid-1", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           "test-pool",
			AccountName:    "ocid1.compartment.oc1..aaa",
			Region:         "us-phoenix-1",
			VendorSubNetID: "ocid1.subnet.oc1..aaa",
			SizeInBytes:    1099511627776,
			State:          "CREATING",
			PoolAttributes: &models.PoolAttributes{PrimaryZone: "ad1", SecondaryZone: "ad2"},
		}, "work-request-id", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                    "ocid1.pool.oc1..aaa",
			CompartmentOCID:             "ocid1.compartment.oc1..aaa",
			DisplayName:                 "test-pool",
			SubnetId:                    "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:             "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain:   "ad1",
			SecondaryAvailabilityDomain: ociserver.NewOptString("ad2"),
			SizeInGiB:                   1024,
			ThroughputGBps:              1.0,
			DataEndpointCount:           2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolAcceptedResponseHeaders")
		assert.Equal(tt, defaultTestOPC, headers.OpcRequestID)
		assert.Equal(tt, "in_progress", headers.Response.Status)
		assert.Equal(tt, "work-request-id", headers.Response.WorkflowId)
		assert.Equal(tt, req.PoolOCID, headers.Response.PoolOCID)
	})

	t.Run("CreatePool echoes opc-request-id when provided", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..bbb",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-echo-id", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           req.DisplayName,
			AccountName:    req.CompartmentOCID,
			VendorSubNetID: req.SubnetId,
			SizeInBytes:    1024 * 1024 * 1024 * 1024,
			State:          "ACTIVE",
		}, "work-req-echo", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()

		res, err := h.CreatePool(contextWithOpcRequestID(nil, "client-request-id-123"), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolAcceptedResponseHeaders")
		assert.Equal(tt, "client-request-id-123", headers.OpcRequestID)
		assert.Equal(tt, "in_progress", headers.Response.Status)
		assert.Equal(tt, "work-req-echo", headers.Response.WorkflowId)
		assert.Equal(tt, req.PoolOCID, headers.Response.PoolOCID)
	})

	t.Run("CreatePool populates pool from request", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..ccc",
			CompartmentOCID:           "ocid1.compartment.oc1..testcomp",
			DisplayName:               "my-pool",
			SubnetId:                  "ocid1.subnet.oc1.iad.testsubnet",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 1024, ThroughputGBps: 0.5},
				{SizeInGiB: 1024, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-from-req", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           req.DisplayName,
			AccountName:    req.CompartmentOCID,
			VendorSubNetID: req.SubnetId,
			SizeInBytes:    2 * 1024 * 1024 * 1024 * 1024,
			State:          "ACTIVE",
		}, "work-req-populate", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolAcceptedResponseHeaders")
		assert.Equal(tt, "in_progress", headers.Response.Status)
		assert.Equal(tt, "work-req-populate", headers.Response.WorkflowId)
		assert.Equal(tt, req.PoolOCID, headers.Response.PoolOCID)
	})

	t.Run("CreatePool forwards required DataNicSubnetId to orchestrator params", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..datanic",
			CompartmentOCID:           "ocid1.compartment.oc1..testcomp",
			DisplayName:               "pool-with-data-nic",
			SubnetId:                  "ocid1.subnet.oc1.iad.testsubnet",
			DataNicSubnetId:           "ocid1.subnet.oc1.iad.testdatasubnet",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}
		mockOrchestrator.EXPECT().
			CreatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.CreatePoolParams) bool {
				return p != nil && p.DataNICSubnetID == req.DataNicSubnetId
			})).
			Return(&models.Pool{
				BaseModel:      models.BaseModel{UUID: "pool-data-nic", CreatedAt: time.Now(), UpdatedAt: time.Now()},
				Name:           req.DisplayName,
				AccountName:    req.CompartmentOCID,
				VendorSubNetID: req.SubnetId,
				SizeInBytes:    1024 * 1024 * 1024 * 1024,
				State:          "CREATING",
			}, "work-req-data-nic", nil)
		h := Handler{Orchestrator: mockOrchestrator}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolAcceptedResponseHeaders")
		assert.Equal(tt, "work-req-data-nic", headers.Response.WorkflowId)
		assert.Equal(tt, req.PoolOCID, headers.Response.PoolOCID)
	})

	t.Run("CreatePool returns 400 when orchestrator returns validation error", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewUserInputValidationErr("invalid region"))
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		badReq, ok := res.(*ociserver.CreatePoolBadRequest)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolBadRequest")
		assert.Equal(tt, defaultTestOPC, badReq.OpcRequestID)
		assert.Equal(tt, "failed", badReq.Response.Status)
		assert.Equal(tt, req.PoolOCID, badReq.Response.PoolOCID)
		assert.Contains(tt, badReq.Response.ErrorMessage, "invalid region")
	})

	t.Run("CreatePool conflict uses unwrapped error message when wrapped", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		inner := utilserrors.NewConflictErr("inner detail")
		wrapped := fmt.Errorf("outer: %w", inner)
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(nil, "", wrapped)
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		conflict, ok := res.(*ociserver.CreatePoolConflict)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolConflict")
		assert.Equal(tt, inner.Error(), conflict.Response.ErrorMessage)
	})

	t.Run("CreatePool returns 409 when orchestrator returns conflict", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewConflictErr("pool already exists"))
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		conflict, ok := res.(*ociserver.CreatePoolConflict)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolConflict")
		assert.Equal(tt, defaultTestOPC, conflict.OpcRequestID)
		assert.Equal(tt, "failed", conflict.Response.Status)
		assert.Equal(tt, req.PoolOCID, conflict.Response.PoolOCID)
		assert.Contains(tt, conflict.Response.ErrorMessage, "pool already exists")
	})

	t.Run("CreatePool returns 500 when orchestrator returns generic error", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(nil, "", errors.New("internal failure"))
		h := Handler{Orchestrator: mockOrchestrator}
		params := defaultCreatePoolParams()
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			PrimaryAvailabilityDomain: "ad1",
			SizeInGiB:                 1024,
			ThroughputGBps:            1.0,
			DataEndpointCount:         2,
			DataEndpointConfig: []ociserver.DataEndpointConfig{
				{SizeInGiB: 512, ThroughputGBps: 0.5},
				{SizeInGiB: 512, ThroughputGBps: 0.5},
			},
			OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		serverErr, ok := res.(*ociserver.CreatePoolInternalServerError)
		assert.True(tt, ok, "response should be *ociserver.CreatePoolInternalServerError")
		assert.Equal(tt, defaultTestOPC, serverErr.OpcRequestID)
		assert.Equal(tt, "failed", serverErr.Response.Status)
		assert.Equal(tt, req.PoolOCID, serverErr.Response.PoolOCID)
		assert.Equal(tt, "Internal server error", serverErr.Response.ErrorMessage)
	})
}

func TestGetHealth(t *testing.T) {
	t.Run("GetHealth returns healthy when ServerState is nil", func(tt *testing.T) {
		h := NewHandler(nil)
		res, err := h.GetHealth(context.Background())
		assert.NoError(tt, err)
		health, ok := res.(*ociserver.Health)
		assert.True(tt, ok, "response should be *ociserver.Health")
		assert.True(tt, health.Status.IsSet())
		status, _ := health.Status.Get()
		assert.Equal(tt, "healthy", status)
	})

	t.Run("GetHealth returns healthy when not shutting down", func(tt *testing.T) {
		serverState := NewServerState()
		h := NewHandler(serverState)
		res, err := h.GetHealth(context.Background())
		assert.NoError(tt, err)
		health, ok := res.(*ociserver.Health)
		assert.True(tt, ok)
		status, _ := health.Status.Get()
		assert.Equal(tt, "healthy", status)
	})

	t.Run("GetHealth returns 500 when shutting down", func(tt *testing.T) {
		serverState := NewServerState()
		serverState.SetShuttingDown()
		h := NewHandler(serverState)
		res, err := h.GetHealth(context.Background())
		assert.NoError(tt, err)
		errRes, ok := res.(*ociserver.StandardError500Headers)
		assert.True(tt, ok, "response should be *ociserver.StandardError500Headers")
		assert.NotEmpty(tt, errRes.OpcRequestID)
		assert.Equal(tt, float64(500), errRes.Response.Code)
		assert.Equal(tt, "Server is shutting down", errRes.Response.Message)
	})
}

func TestGetWorkflow(t *testing.T) {
	const opcWf = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

	t.Run("GetWorkflow maps metadata VMs when query succeeds", func(tt *testing.T) {
		orig := workflowQueryFn
		workflowQueryFn = func(ctx context.Context, c client.Client, workflowID, runID string) (workflowquery.Result, error) {
			return workflowquery.Result{
				Status:       workflowquery.WorkflowStatusCompleted,
				WorkflowType: "OCICreatePoolWorkflow",
				PoolMetadata: &workflowquery.OCICreatePoolMetadata{
					Vms: []workflowquery.OCICreatePoolVMMetadata{
						{
							Name:            "vm-01",
							SerialNumber:    "1234501",
							VSAManagementIP: "10.0.0.3",
							InterclusterIP:  "10.0.0.1",
							NodeIP:          "10.0.0.2",
							HAPair:          "ha_pair-0",
						},
					},
				},
			}, nil
		}
		tt.Cleanup(func() { workflowQueryFn = orig })

		h := Handler{}
		res, err := h.GetWorkflow(contextWithOpcRequestID(nil, opcWf), ociserver.GetWorkflowParams{WorkRequestId: "wf-1"})
		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.GetWorkflowStatusResponseHeaders)
		assert.True(tt, ok, "response should be *ociserver.GetWorkflowStatusResponseHeaders")
		assert.Equal(tt, opcWf, headers.OpcRequestID)
		assert.Equal(tt, "completed", headers.Response.Status)
		assert.True(tt, headers.Response.PoolMetadata.IsSet())
		meta, ok := headers.Response.PoolMetadata.Get()
		assert.True(tt, ok)
		if assert.Len(tt, meta.Vms, 1) {
			assert.Equal(tt, "vm-01", meta.Vms[0].Name)
			assert.Equal(tt, "1234501", meta.Vms[0].SerialNumber)
			assert.Equal(tt, "10.0.0.3", meta.Vms[0].VsaManagementIP)
			assert.Equal(tt, "10.0.0.1", meta.Vms[0].InterclusterIP)
			assert.Equal(tt, "10.0.0.2", meta.Vms[0].NodeIP)
			assert.Equal(tt, "ha_pair-0", meta.Vms[0].HaPair,
				"haPair must be propagated from internal metadata to the OAS response")
		}
	})

	t.Run("GetWorkflow surfaces credentials.secret from completed pool workflow metadata", func(tt *testing.T) {
		orig := workflowQueryFn
		workflowQueryFn = func(ctx context.Context, c client.Client, workflowID, runID string) (workflowquery.Result, error) {
			return workflowquery.Result{
				Status:       workflowquery.WorkflowStatusCompleted,
				WorkflowType: "OCICreatePoolWorkflow",
				PoolMetadata: &workflowquery.OCICreatePoolMetadata{
					Credentials: &workflowquery.OCICreatePoolCredentialsMetadata{
						Secret: &workflowquery.OCICredentialRefMetadata{
							Ocid:    "FsnIdocnv-2bbd1dd79fa45f97-ontap-admin",
							Version: "3",
						},
					},
				},
			}, nil
		}
		tt.Cleanup(func() { workflowQueryFn = orig })

		h := Handler{}
		res, err := h.GetWorkflow(contextWithOpcRequestID(nil, opcWf), ociserver.GetWorkflowParams{WorkRequestId: "wf-2"})
		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.GetWorkflowStatusResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, "completed", headers.Response.Status)
		assert.True(tt, headers.Response.PoolMetadata.IsSet())
		meta, ok := headers.Response.PoolMetadata.Get()
		assert.True(tt, ok)
		assert.Empty(tt, meta.Vms)
		assert.Equal(tt, "FsnIdocnv-2bbd1dd79fa45f97-ontap-admin", meta.Credentials.Secret.Ocid)
		assert.Equal(tt, "3", meta.Credentials.Secret.Version)
		// Certificate is reserved for future PRs and must collapse to empty placeholder.
		assert.Equal(tt, "", meta.Credentials.Certificate.Ocid)
		assert.Equal(tt, "", meta.Credentials.Certificate.Version)
	})

	t.Run("GetWorkflow emits empty credentials when pool metadata has none", func(tt *testing.T) {
		orig := workflowQueryFn
		workflowQueryFn = func(ctx context.Context, c client.Client, workflowID, runID string) (workflowquery.Result, error) {
			return workflowquery.Result{
				Status:       workflowquery.WorkflowStatusCompleted,
				WorkflowType: "OCICreatePoolWorkflow",
				PoolMetadata: &workflowquery.OCICreatePoolMetadata{
					Vms: []workflowquery.OCICreatePoolVMMetadata{{Name: "vm-x"}},
				},
			}, nil
		}
		tt.Cleanup(func() { workflowQueryFn = orig })

		h := Handler{}
		res, err := h.GetWorkflow(contextWithOpcRequestID(nil, opcWf), ociserver.GetWorkflowParams{WorkRequestId: "wf-3"})
		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.GetWorkflowStatusResponseHeaders)
		assert.True(tt, ok)
		meta, ok := headers.Response.PoolMetadata.Get()
		assert.True(tt, ok)
		assert.Equal(tt, "", meta.Credentials.Secret.Ocid)
		assert.Equal(tt, "", meta.Credentials.Secret.Version)
		assert.Equal(tt, "", meta.Credentials.Certificate.Ocid)
		assert.Equal(tt, "", meta.Credentials.Certificate.Version)
	})

	t.Run("GetWorkflow maps SVM metadata when query returns SVM result", func(tt *testing.T) {
		orig := workflowQueryFn
		workflowQueryFn = func(ctx context.Context, c client.Client, workflowID, runID string) (workflowquery.Result, error) {
			return workflowquery.Result{
				Status:       workflowquery.WorkflowStatusCompleted,
				WorkflowType: "OCICreateSVMWorkflow",
				SvmMetadata: &workflowquery.OCICreateSVMMetadata{
					Name:    "svm-1",
					SvmOCID: "ocid1.svm",
					Lifs: []workflowquery.OCICreateSVMLifMetadata{
						{Name: "lif1", IP: "10.0.0.1", Node: "node1", Protocols: []string{"nfs", "cifs", "s3"}},
					},
				},
			}, nil
		}
		tt.Cleanup(func() { workflowQueryFn = orig })

		h := Handler{}
		res, err := h.GetWorkflow(contextWithOpcRequestID(nil, opcWf), ociserver.GetWorkflowParams{WorkRequestId: "wf-svm"})
		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.GetWorkflowStatusResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, "completed", headers.Response.Status)
		assert.True(tt, headers.Response.SvmMetadata.IsSet())
		svmMeta, ok := headers.Response.SvmMetadata.Get()
		assert.True(tt, ok)
		name, _ := svmMeta.Name.Get()
		assert.Equal(tt, "svm-1", name)
		svmOCID, _ := svmMeta.SvmOCID.Get()
		assert.Equal(tt, "ocid1.svm", svmOCID)
		assert.Len(tt, svmMeta.Lifs, 1)
	})

	t.Run("mapGetWorkflowQueryError returns 404 for not found error", func(tt *testing.T) {
		res := mapGetWorkflowQueryError(opcWf, errors.New("workflow not found"))
		errRes, ok := res.(*ociserver.GetWorkflowNotFound)
		assert.True(tt, ok, "response should be *ociserver.GetWorkflowNotFound")
		assert.Equal(tt, opcWf, errRes.OpcRequestID)
		assert.Equal(tt, "workflow not found", errRes.Response.ErrorMessage)
	})

	t.Run("mapGetWorkflowQueryError returns 500 for other errors", func(tt *testing.T) {
		res := mapGetWorkflowQueryError(opcWf, errors.New("temporal unavailable"))
		errRes, ok := res.(*ociserver.GetWorkflowInternalServerError)
		assert.True(tt, ok, "response should be *ociserver.GetWorkflowInternalServerError")
		assert.Equal(tt, opcWf, errRes.OpcRequestID)
		assert.Equal(tt, "temporal unavailable", errRes.Response.ErrorMessage)
	})
}

func TestDeletePool(t *testing.T) {
	t.Run("DeletePool returns 202 with OperationV1beta and headers when orchestrator returns in-progress", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "550e8400-e29b-41d4-a716-446655440000", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           "mypool",
			State:          models.LifeCycleStateDeleting,
			PoolAttributes: &models.PoolAttributes{PrimaryZone: "ad1", SecondaryZone: "ad2"},
		}, "op-123", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1.iad.testdelete", TenancyOcid: "ocid1.tenancy.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.DeletePoolAcceptedResponseHeaders)
		assert.True(tt, ok, "response should be *ociserver.DeletePoolAcceptedResponseHeaders")
		assert.Equal(tt, defaultTestOPC, headers.OpcRequestID)
		assert.Equal(tt, "in_progress", headers.Response.Status)
		assert.Equal(tt, "op-123", headers.Response.WorkflowId)
		assert.Equal(tt, params.PoolOCID, headers.Response.PoolOCID)
	})

	t.Run("DeletePool returns 204 NoContent when delete completed", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "550e8400-e29b-41d4-a716-446655440000", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           "mypool",
			State:          models.LifeCycleStateDeleted,
			PoolAttributes: &models.PoolAttributes{PrimaryZone: "ad1", SecondaryZone: "ad2"},
		}, "op-456", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1.iad.testdelete", TenancyOcid: "ocid1.tenancy.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)

		assert.NoError(tt, err)
		noContent, ok := res.(*ociserver.DeletePoolNoContent)
		assert.True(tt, ok, "response should be *ociserver.DeletePoolNoContent")
		assert.Equal(tt, defaultTestOPC, noContent.OpcRequestID)
	})

	t.Run("DeletePool echoes opc-request-id when provided", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-uuid", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           "mypool",
			State:          models.LifeCycleStateDeleting,
			PoolAttributes: &models.PoolAttributes{PrimaryZone: "ad1", SecondaryZone: "ad2"},
		}, "op-echo", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1.iad.testecho", TenancyOcid: "ocid1.tenancy.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, "delete-request-id"), params)

		assert.NoError(tt, err)
		headers := res.(*ociserver.DeletePoolAcceptedResponseHeaders)
		assert.Equal(tt, "delete-request-id", headers.OpcRequestID)
	})

	t.Run("DeletePool returns 404 style body when pool not found", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewNotFoundErr("pool not found", nil))
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1.iad.testdelete", TenancyOcid: "ocid1.tenancy.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.DeletePoolNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, headers.OpcRequestID)
		assert.Equal(tt, "failed", headers.Response.Status)
		assert.Equal(tt, params.PoolOCID, headers.Response.PoolOCID)
		assert.Contains(tt, headers.Response.ErrorMessage, "not found")
	})

	t.Run("DeletePool returns 400 when orchestrator returns bad request", func(tt *testing.T) {
		// Handler echoes err.Error(); assert against the same error so mock text and expectation cannot drift.
		orchestratorErr := utilserrors.NewBadRequestErr("pool cannot be deleted with active volumes")
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", orchestratorErr)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..x", TenancyOcid: "ocid1.compartment.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)
		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.DeletePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, bad.OpcRequestID)
		assert.Equal(tt, orchestratorErr.Error(), bad.Response.ErrorMessage)
	})

	t.Run("DeletePool returns 409 when orchestrator returns conflict", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewConflictErr("in transition"))
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..x", TenancyOcid: "ocid1.tenancy.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)
		assert.NoError(tt, err)
		conf, ok := res.(*ociserver.DeletePoolConflict)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, conf.OpcRequestID)
		assert.Equal(tt, params.PoolOCID, conf.Response.PoolOCID)
		assert.Contains(tt, conf.Response.ErrorMessage, "Error deleting pool",
			"handler must prefix the wrapped orchestrator conflict error so callers can distinguish from validation 409s")
		assert.Contains(tt, conf.Response.ErrorMessage, "in transition",
			"original orchestrator error must be preserved (not flattened) when wrapping")
	})

	t.Run("DeletePool returns 500 on generic orchestrator error", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", errors.New("upstream failure"))
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..x", TenancyOcid: "ocid1.compartment.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)
		assert.NoError(tt, err)
		srv, ok := res.(*ociserver.DeletePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, "Internal server error", srv.Response.ErrorMessage)
	})

	t.Run("DeletePool returns 202 when pool is still CREATING", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel: models.BaseModel{UUID: "u1"},
			State:     models.LifeCycleStateCreating,
		}, "wf-del", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..x", TenancyOcid: "ocid1.compartment.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)
		assert.NoError(tt, err)
		acc, ok := res.(*ociserver.DeletePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, "wf-del", acc.Response.WorkflowId)
	})
}

func TestOpcRequestIDFromContext(t *testing.T) {
	t.Run("no header map in context", func(t *testing.T) {
		_, err := opcRequestIDFromContext(context.Background())
		assert.Error(t, err)
	})
	t.Run("empty opc-request-id after trim", func(t *testing.T) {
		h := make(http.Header)
		h.Set(string(middleware.OPCRequestIDHeaderName), "   ")
		ctx := context.WithValue(context.Background(), middleware.HeaderContextKey, h)
		_, err := opcRequestIDFromContext(ctx)
		assert.Error(t, err)
	})
	t.Run("returns trimmed id", func(t *testing.T) {
		h := make(http.Header)
		h.Set(string(middleware.OPCRequestIDHeaderName), "  my-id  ")
		ctx := context.WithValue(context.Background(), middleware.HeaderContextKey, h)
		id, err := opcRequestIDFromContext(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "my-id", id)
	})
}

func TestCreatePool_MissingOPCRequestID(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	h := Handler{Orchestrator: mockOrchestrator}
	req := &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..aaa",
		CompartmentOCID:           "ocid1.compartment.oc1..aaa",
		DisplayName:               "p",
		SubnetId:                  "ocid1.subnet.oc1..aaa",
		DataNicSubnetId:           "ocid1.subnet.oc1..data",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 1024,
		ThroughputGBps:            1.0,
		DataEndpointCount:         2,
		DataEndpointConfig: []ociserver.DataEndpointConfig{
			{SizeInGiB: 512, ThroughputGBps: 0.5},
			{SizeInGiB: 512, ThroughputGBps: 0.5},
		},
		OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
	}
	res, err := h.CreatePool(context.Background(), req, defaultCreatePoolParams())
	assert.NoError(t, err)
	bad, ok := res.(*ociserver.CreatePoolBadRequest)
	assert.True(t, ok)
	assert.Equal(t, invalidOPCRequestID, bad.Response.ErrorMessage)
}

func TestCreatePool_EmptyWorkflowID(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
		BaseModel: models.BaseModel{UUID: "pool-uuid-1", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		Name:      "test-pool",
		State:     "CREATING",
	}, "", nil)
	h := Handler{Orchestrator: mockOrchestrator}
	req := &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..aaa",
		CompartmentOCID:           "ocid1.compartment.oc1..aaa",
		DisplayName:               "test-pool",
		SubnetId:                  "ocid1.subnet.oc1..aaa",
		DataNicSubnetId:           "ocid1.subnet.oc1..data",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 1024,
		ThroughputGBps:            1.0,
		DataEndpointCount:         2,
		DataEndpointConfig: []ociserver.DataEndpointConfig{
			{SizeInGiB: 512, ThroughputGBps: 0.5},
			{SizeInGiB: 512, ThroughputGBps: 0.5},
		},
		OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
	}
	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())
	assert.NoError(t, err)
	headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
	assert.True(t, ok)
	assert.Equal(t, "", headers.Response.WorkflowId)
}

func TestCreatePool_OddDataEndpointCountRejected(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	h := Handler{Orchestrator: mockOrchestrator}
	req := &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..odd-count",
		CompartmentOCID:           "ocid1.compartment.oc1..aaa",
		DisplayName:               "odd-count-pool",
		SubnetId:                  "ocid1.subnet.oc1..aaa",
		DataNicSubnetId:           "ocid1.subnet.oc1..data",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 512,
		ThroughputGBps:            1.0,
		DataEndpointCount:         3, // odd: must be rejected
		OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
	}

	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

	assert.NoError(t, err)
	bad, ok := res.(*ociserver.CreatePoolBadRequest)
	assert.True(t, ok, "response should be *ociserver.CreatePoolBadRequest")
	assert.Equal(t, defaultTestOPC, bad.OpcRequestID)
	assert.Equal(t, req.PoolOCID, bad.Response.PoolOCID)
	assert.Equal(t, string(workflowquery.WorkflowStatusFailed), bad.Response.Status)
	assert.Equal(t, errMsgOddDataEndpointCount, bad.Response.ErrorMessage)
}

func TestGetWorkflow_MissingOPCRequestID(t *testing.T) {
	h := Handler{}
	res, err := h.GetWorkflow(context.Background(), ociserver.GetWorkflowParams{WorkRequestId: "wf-1"})
	assert.NoError(t, err)
	bad, ok := res.(*ociserver.GetWorkflowBadRequest)
	assert.True(t, ok)
	assert.Equal(t, invalidOPCRequestID, bad.Response.ErrorMessage)
}

func TestCreatePool_AdminPasswordVersionNotParseable(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	h := Handler{Orchestrator: mockOrchestrator}
	req := &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..aaa",
		CompartmentOCID:           "ocid1.compartment.oc1..aaa",
		DisplayName:               "p",
		SubnetId:                  "ocid1.subnet.oc1..aaa",
		DataNicSubnetId:           "ocid1.subnet.oc1..data",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 1024,
		ThroughputGBps:            1.0,
		DataEndpointCount:         2,
		DataEndpointConfig: []ociserver.DataEndpointConfig{
			{SizeInGiB: 512, ThroughputGBps: 1.0},
			{SizeInGiB: 512, ThroughputGBps: 1.0},
		},
		OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "not-a-number"},
	}

	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

	assert.NoError(t, err)
	bad, ok := res.(*ociserver.CreatePoolBadRequest)
	assert.True(t, ok, "response should be *ociserver.CreatePoolBadRequest")
	assert.Equal(t, defaultTestOPC, bad.OpcRequestID)
	assert.Equal(t, req.PoolOCID, bad.Response.PoolOCID)
	assert.Equal(t, errMsgInvalidAdminPasswordVersion, bad.Response.ErrorMessage)
}

func TestCreatePool_AdminPasswordVersionLessThanOne(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	h := Handler{Orchestrator: mockOrchestrator}
	req := &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..aaa",
		CompartmentOCID:           "ocid1.compartment.oc1..aaa",
		DisplayName:               "p",
		SubnetId:                  "ocid1.subnet.oc1..aaa",
		DataNicSubnetId:           "ocid1.subnet.oc1..data",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 1024,
		ThroughputGBps:            1.0,
		DataEndpointCount:         2,
		DataEndpointConfig: []ociserver.DataEndpointConfig{
			{SizeInGiB: 512, ThroughputGBps: 1.0},
			{SizeInGiB: 512, ThroughputGBps: 1.0},
		},
		OciAdminPassword: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "0"},
	}

	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

	assert.NoError(t, err)
	bad, ok := res.(*ociserver.CreatePoolBadRequest)
	assert.True(t, ok, "response should be *ociserver.CreatePoolBadRequest")
	assert.Equal(t, defaultTestOPC, bad.OpcRequestID)
	assert.Equal(t, req.PoolOCID, bad.Response.PoolOCID)
	assert.Equal(t, errMsgAdminPasswordVersionLessThanOne, bad.Response.ErrorMessage)
}

func TestGetWorkflow_NodeUUIDEnrichment(t *testing.T) {
	const opcWf = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"

	t.Run("NodeUUIDPopulatedWhenOrchestratorReturnsNodes", func(tt *testing.T) {
		orig := workflowQueryFn
		workflowQueryFn = func(ctx context.Context, c client.Client, workflowID, runID string) (workflowquery.Result, error) {
			return workflowquery.Result{
				Status:       workflowquery.WorkflowStatusCompleted,
				WorkflowType: "OCICreatePoolWorkflow",
				PoolMetadata: &workflowquery.OCICreatePoolMetadata{
					PoolUUID: "pool-uuid-enriched",
					Vms: []workflowquery.OCICreatePoolVMMetadata{
						{Name: "vm-01"},
					},
				},
			}, nil
		}
		tt.Cleanup(func() { workflowQueryFn = orig })

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().
			GetNodesByPoolUUID(mock.Anything, "pool-uuid-enriched").
			Return([]*datamodel.Node{
				{BaseModel: datamodel.BaseModel{UUID: "node-uuid-1"}, Name: "vm-01"},
			}, nil)

		h := Handler{Orchestrator: mockOrchestrator}
		res, err := h.GetWorkflow(contextWithOpcRequestID(nil, opcWf), ociserver.GetWorkflowParams{WorkRequestId: "wf-enrich"})

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.GetWorkflowStatusResponseHeaders)
		assert.True(tt, ok)
		meta, ok := headers.Response.PoolMetadata.Get()
		assert.True(tt, ok)
		if assert.Len(tt, meta.Vms, 1) {
			assert.Equal(tt, "vm-01", meta.Vms[0].Name)
			assert.Equal(tt, "node-uuid-1", meta.Vms[0].NodeUUID)
		}
	})

	t.Run("NodeUUIDEmptyWhenOrchestratorLookupFails", func(tt *testing.T) {
		orig := workflowQueryFn
		workflowQueryFn = func(ctx context.Context, c client.Client, workflowID, runID string) (workflowquery.Result, error) {
			return workflowquery.Result{
				Status:       workflowquery.WorkflowStatusCompleted,
				WorkflowType: "OCICreatePoolWorkflow",
				PoolMetadata: &workflowquery.OCICreatePoolMetadata{
					PoolUUID: "pool-uuid-fail",
					Vms: []workflowquery.OCICreatePoolVMMetadata{
						{Name: "vm-01"},
					},
				},
			}, nil
		}
		tt.Cleanup(func() { workflowQueryFn = orig })

		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().
			GetNodesByPoolUUID(mock.Anything, "pool-uuid-fail").
			Return(nil, fmt.Errorf("db down"))

		h := Handler{Orchestrator: mockOrchestrator}
		res, err := h.GetWorkflow(contextWithOpcRequestID(nil, opcWf), ociserver.GetWorkflowParams{WorkRequestId: "wf-fail"})

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.GetWorkflowStatusResponseHeaders)
		assert.True(tt, ok)
		meta, ok := headers.Response.PoolMetadata.Get()
		assert.True(tt, ok)
		if assert.Len(tt, meta.Vms, 1) {
			assert.Equal(tt, "vm-01", meta.Vms[0].Name)
			assert.Empty(tt, meta.Vms[0].NodeUUID, "nodeUUID should be empty when lookup fails")
		}
	})
}

func TestDeletePool_MissingOPCRequestID(t *testing.T) {
	h := Handler{}
	res, err := h.DeletePool(context.Background(), ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..aaa"})
	assert.NoError(t, err)
	bad, ok := res.(*ociserver.DeletePoolBadRequest)
	assert.True(t, ok)
	assert.Equal(t, invalidOPCRequestID, bad.Response.ErrorMessage)
}

func TestCreatePool_MapsTopLevelFieldsToOrchestratorParams(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	// Use shared HA (secondary unset) so DataEndpointCount=4 is allowed; this test
	// pins the SizeInGiB→bytes, DataEndpointCount→HAPairs, and ThroughputGBps→MiBps
	// mappings. Non-shared HA forces DataEndpointCount=2 (covered separately).
	req := &ociserver.CreatePoolRequest{
		PoolOCID:                  "ocid1.pool.oc1..top-level",
		CompartmentOCID:           "ocid1.compartment.oc1..testcomp",
		DisplayName:               "top-level-pool",
		SubnetId:                  "ocid1.subnet.oc1.iad.testsubnet",
		DataNicSubnetId:           "ocid1.subnet.oc1.iad.testdatasubnet",
		PrimaryAvailabilityDomain: "ad1",
		SizeInGiB:                 200,
		ThroughputGBps:            2,
		DataEndpointCount:         4,
		OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
	}
	mockOrchestrator.EXPECT().
		CreatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.CreatePoolParams) bool {
			return p != nil &&
				p.SizeInBytes == 200*1024*1024*1024 &&
				p.HAPairs == 2 && // DataEndpointCount(4) / 2
				p.CustomPerformanceParams != nil &&
				p.CustomPerformanceParams.ThroughputMibps == gbpsToMibps(2) &&
				p.CustomPerformanceParams.Iops == nil &&
				p.PrimaryZone == "ad1" &&
				p.SecondaryZone == "ad1" && // shared HA → secondary defaults to primary
				p.IsRegionalHA == false // shared HA
		})).
		Return(&models.Pool{
			BaseModel: models.BaseModel{UUID: "pool-top-level", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:      req.DisplayName,
			State:     "CREATING",
		}, "work-req-top-level", nil)
	h := Handler{Orchestrator: mockOrchestrator}

	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

	assert.NoError(t, err)
	headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
	assert.True(t, ok, "response should be *ociserver.CreatePoolAcceptedResponseHeaders")
	assert.Equal(t, "work-req-top-level", headers.Response.WorkflowId)
}

// TestCreatePool_AvailabilityDomainDerivation pins the post-validation derivation
// of SecondaryZone, MediatorZone and IsRegionalHA on the orchestrator params.
//
// Semantics under the current handler (mediator handling is uniform across both
// HA modes; only secondary/IsRegionalHA differ):
//   - SecondaryZone:
//   - Shared HA (secondary unset, empty, or equal to primary): defaults to primary.
//   - Non-shared HA: SecondaryZone = the supplied secondary value.
//   - IsRegionalHA: false when shared HA, true when non-shared HA.
//   - MediatorZone (same rule in both branches):
//   - = MediatorAvailabilityDomain.Value when non-empty (passed through), otherwise
//   - = primary (defaulted) when MediatorAvailabilityDomain is unset or empty.
func TestCreatePool_AvailabilityDomainDerivation(t *testing.T) {
	cases := []struct {
		name          string
		primary       string
		secondary     ociserver.OptString
		mediator      ociserver.OptString
		endpointCount int64
		wantSecondary string // expected SecondaryZone on the orchestrator-bound params
		wantMediator  string // expected MediatorZone on the orchestrator-bound params
		wantRegHA     bool
		description   string
	}{
		{
			name:          "NonSharedHA_MediatorProvided_PassesThrough",
			primary:       "ad1",
			secondary:     ociserver.OptString{Value: "ad2", Set: true},
			mediator:      ociserver.OptString{Value: "ad3", Set: true},
			endpointCount: 2,
			wantSecondary: "ad2",
			wantMediator:  "ad3", // non-empty mediator passed through verbatim
			wantRegHA:     true,
			description:   "non-shared HA with mediator provided → mediatorAD = supplied value",
		},
		{
			name:          "NonSharedHA_MediatorUnset_DefaultsToPrimary",
			primary:       "ad1",
			secondary:     ociserver.OptString{Value: "ad2", Set: true},
			mediator:      ociserver.OptString{},
			endpointCount: 2,
			wantSecondary: "ad2",
			wantMediator:  "ad1", // mediator unset → defaulted to primary
			wantRegHA:     true,
			description:   "non-shared HA without mediator → mediatorAD = primary",
		},
		{
			name:          "NonSharedHA_MediatorEmptyButSet_DefaultsToPrimary",
			primary:       "ad1",
			secondary:     ociserver.OptString{Value: "ad2", Set: true},
			mediator:      ociserver.OptString{Value: "", Set: true},
			endpointCount: 2,
			wantSecondary: "ad2",
			wantMediator:  "ad1", // empty value treated identically to unset
			wantRegHA:     true,
			description:   "non-shared HA with empty-but-set mediator → mediatorAD = primary",
		},
		{
			name:          "SharedHA_MediatorProvided_PassesThrough",
			primary:       "ad1",
			secondary:     ociserver.OptString{Value: "ad1", Set: true},
			mediator:      ociserver.OptString{Value: "ad9", Set: true}, // distinct value: proves pass-through, not coincidence
			endpointCount: 2,
			wantSecondary: "ad1",
			wantMediator:  "ad9", // shared HA does NOT clear mediator; supplied value flows through
			wantRegHA:     false,
			description:   "shared HA with mediator provided → mediatorAD = supplied value (not cleared)",
		},
		{
			name:          "SharedHA_SecondaryAndMediatorUnset_MediatorDefaultsToPrimary",
			primary:       "ad1",
			secondary:     ociserver.OptString{},
			mediator:      ociserver.OptString{},
			endpointCount: 4, // shared HA imposes no DataEndpointCount==2 rule
			wantSecondary: "ad1",
			wantMediator:  "ad1", // defaulted to primary because mediator was empty
			wantRegHA:     false,
			description:   "secondary unset → shared HA → secondary = primary, mediator defaulted to primary; count > 2 allowed",
		},
		{
			name:          "SharedHA_SecondaryAndMediatorEmptyButSet_MediatorDefaultsToPrimary",
			primary:       "ad1",
			secondary:     ociserver.OptString{Value: "", Set: true},
			mediator:      ociserver.OptString{Value: "", Set: true},
			endpointCount: 2,
			wantSecondary: "ad1",
			wantMediator:  "ad1", // defaulted to primary because mediator was empty-but-set
			wantRegHA:     false,
			description:   "secondary and mediator explicitly empty → shared HA, mediator defaulted to primary",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
			req := validCreatePoolRequest()
			req.PoolOCID = "ocid1.pool.oc1..ad-deriv"
			req.PrimaryAvailabilityDomain = tc.primary
			req.SecondaryAvailabilityDomain = tc.secondary
			req.MediatorAvailabilityDomain = tc.mediator
			req.DataEndpointCount = tc.endpointCount

			mockOrchestrator.EXPECT().
				CreatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.CreatePoolParams) bool {
					return p != nil &&
						p.PrimaryZone == tc.primary &&
						p.SecondaryZone == tc.wantSecondary &&
						p.MediatorZone == tc.wantMediator &&
						p.IsRegionalHA == tc.wantRegHA
				})).
				Return(&models.Pool{
					BaseModel: models.BaseModel{UUID: "pool-ad-deriv", CreatedAt: time.Now(), UpdatedAt: time.Now()},
					Name:      req.DisplayName,
					State:     "CREATING",
				}, "work-req-ad-deriv", nil)
			h := Handler{Orchestrator: mockOrchestrator}

			res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

			assert.NoError(tt, err, tc.description)
			_, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
			assert.True(tt, ok, "expected accepted response for %s", tc.description)
		})
	}
}

func TestNewError(t *testing.T) {
	t.Run("NewError returns 500 when err is nil", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), nil)
		assert.Equal(tt, http.StatusInternalServerError, res.StatusCode)
		assert.Equal(tt, float64(500), res.Response.Code)
		assert.Equal(tt, "internal error", res.Response.Message)
	})

	t.Run("NewError returns 499 for context.Canceled", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), context.Canceled)
		assert.Equal(tt, 499, res.StatusCode)
		assert.Equal(tt, float64(499), res.Response.Code)
	})

	t.Run("NewError returns 504 for context.DeadlineExceeded", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), context.DeadlineExceeded)
		assert.Equal(tt, http.StatusGatewayTimeout, res.StatusCode)
		assert.Equal(tt, float64(504), res.Response.Code)
	})

	t.Run("NewError returns 404 for not found message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("resource not found"))
		assert.Equal(tt, http.StatusNotFound, res.StatusCode)
		assert.Equal(tt, float64(404), res.Response.Code)
		assert.Contains(tt, res.Response.Message, "not found")
	})

	t.Run("NewError returns 401 for unauthorized message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("unauthorized"))
		assert.Equal(tt, http.StatusUnauthorized, res.StatusCode)
		assert.Equal(tt, float64(401), res.Response.Code)
	})

	t.Run("NewError returns 401 for invalid token message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("invalid token"))
		assert.Equal(tt, http.StatusUnauthorized, res.StatusCode)
	})

	t.Run("NewError returns 403 for forbidden message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("access denied"))
		assert.Equal(tt, http.StatusForbidden, res.StatusCode)
		assert.Equal(tt, float64(403), res.Response.Code)
	})

	t.Run("NewError returns 409 for conflict message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("resource already exists"))
		assert.Equal(tt, http.StatusConflict, res.StatusCode)
		assert.Equal(tt, float64(409), res.Response.Code)
	})

	t.Run("NewError returns 400 for bad request message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("invalid input"))
		assert.Equal(tt, http.StatusBadRequest, res.StatusCode)
		assert.Equal(tt, float64(400), res.Response.Code)
	})

	t.Run("NewError returns 429 for rate limit message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("too many requests"))
		assert.Equal(tt, http.StatusTooManyRequests, res.StatusCode)
		assert.Equal(tt, float64(429), res.Response.Code)
	})

	t.Run("NewError returns 422 for unprocessable message", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("unprocessable entity"))
		assert.Equal(tt, http.StatusUnprocessableEntity, res.StatusCode)
		assert.Equal(tt, float64(422), res.Response.Code)
	})

	t.Run("NewError returns 500 for unknown error", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New("something went wrong"))
		assert.Equal(tt, http.StatusInternalServerError, res.StatusCode)
		assert.Equal(tt, float64(500), res.Response.Code)
		assert.Equal(tt, "something went wrong", res.Response.Message)
	})

	t.Run("NewError uses status text when error message is empty", func(tt *testing.T) {
		var h Handler
		res := h.NewError(context.Background(), errors.New(""))
		assert.Equal(tt, http.StatusInternalServerError, res.StatusCode)
		assert.Equal(tt, http.StatusText(http.StatusInternalServerError), res.Response.Message)
	})
}

func TestUpdatePool(t *testing.T) {
	const poolOCID = "ocid1.pool.oc1..update-test"
	const tenancy = "ocid1.tenancy.oc1..aaa"

	t.Run("returns 202 when orchestrator succeeds", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				p.AccountName == tenancy &&
				p.TotalThroughputMibps == gbpsToMibps(2) &&
				p.CustomPerformanceEnabled
		})).Return(&models.Pool{}, "wf-update-1", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: 2, Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, headers.OpcRequestID)
		assert.Equal(tt, "in_progress", headers.Response.Status)
		assert.Equal(tt, "wf-update-1", headers.Response.WorkflowId)
		assert.Equal(tt, poolOCID, headers.Response.PoolOCID)
	})

	t.Run("returns 400 when orchestrator returns validation error", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewUserInputValidationErr("size too small"))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Contains(tt, bad.Response.ErrorMessage, "size too small")
	})

	t.Run("returns 400 when orchestrator returns bad request error", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewBadRequestErr("PoolExternalIdentifier is required"))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Contains(tt, bad.Response.ErrorMessage, "PoolExternalIdentifier is required")
	})

	t.Run("returns 404 when orchestrator returns not found", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewNotFoundErr("pool not found", nil))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		notFound, ok := res.(*ociserver.UpdatePoolNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, notFound.OpcRequestID)
		assert.Contains(tt, notFound.Response.ErrorMessage, "not found")
	})

	t.Run("returns 409 when orchestrator returns conflict", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewConflictErr("pool in transition"))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		conflict, ok := res.(*ociserver.UpdatePoolConflict)
		assert.True(tt, ok)
		assert.Contains(tt, conflict.Response.ErrorMessage, "transitioning")
	})

	t.Run("returns 500 on generic orchestrator error", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", errors.New("unexpected"))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		srv, ok := res.(*ociserver.UpdatePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, "Internal server error", srv.Response.ErrorMessage)
	})

	t.Run("missing opc-request-id returns 400", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(context.Background(), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, invalidOPCRequestID, bad.Response.ErrorMessage)
	})

	t.Run("returns 202 with throughputGBps and dataEndpointCount update", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				p.TotalThroughputMibps == gbpsToMibps(4) &&
				p.CustomPerformanceEnabled &&
				p.SizeInBytes == 0
		})).Return(&models.Pool{}, "wf-throughput-only", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: 4, Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, "wf-throughput-only", headers.Response.WorkflowId)
		assert.Equal(tt, poolOCID, headers.Response.PoolOCID)
	})

	t.Run("returns 202 echoes custom opc-request-id", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(&models.Pool{}, "wf-echo", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, "custom-opc-id-123"), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, "custom-opc-id-123", headers.OpcRequestID)
		assert.Equal(tt, poolOCID, headers.Response.PoolOCID)
	})

	t.Run("returns 400 verifies full response body when neither nodeCapacities nor dataEndpointCount provided", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, bad.OpcRequestID)
		assert.Equal(tt, poolOCID, bad.Response.PoolOCID)
		assert.Equal(tt, "failed", bad.Response.Status)
		assert.Equal(tt, errMsgNeitherNodeCapacitiesNorDEC, bad.Response.ErrorMessage)
	})

	t.Run("returns 404 verifies full response body", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewNotFoundErr("pool not found", nil))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		notFound, ok := res.(*ociserver.UpdatePoolNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, notFound.OpcRequestID)
		assert.Equal(tt, poolOCID, notFound.Response.PoolOCID)
		assert.Equal(tt, "failed", notFound.Response.Status)
		assert.Contains(tt, notFound.Response.ErrorMessage, "not found")
	})

	t.Run("returns 409 verifies full response body and fixed message", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewConflictErr("pool in transition"))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		conflict, ok := res.(*ociserver.UpdatePoolConflict)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, conflict.OpcRequestID)
		assert.Equal(tt, poolOCID, conflict.Response.PoolOCID)
		assert.Equal(tt, "failed", conflict.Response.Status)
		assert.Equal(tt, "Error updating pool - Pool is already transitioning between states", conflict.Response.ErrorMessage)
	})

	t.Run("returns 500 verifies full response body", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", errors.New("unexpected"))
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		srv, ok := res.(*ociserver.UpdatePoolInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, defaultTestOPC, srv.OpcRequestID)
		assert.Equal(tt, poolOCID, srv.Response.PoolOCID)
		assert.Equal(tt, "failed", srv.Response.Status)
		assert.Equal(tt, "Internal server error", srv.Response.ErrorMessage)
	})

	t.Run("returns 409 with wrapped conflict error", func(tt *testing.T) {
		inner := utilserrors.NewConflictErr("inner conflict")
		wrapped := fmt.Errorf("outer: %w", inner)
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.Anything).Return(nil, "", wrapped)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		conflict, ok := res.(*ociserver.UpdatePoolConflict)
		assert.True(tt, ok)
		assert.Equal(tt, "Error updating pool - Pool is already transitioning between states", conflict.Response.ErrorMessage)
	})

	t.Run("passes PoolId from URL path param to orchestrator", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				p.PoolExternalIdentifier == poolOCID &&
				p.AccountName == tenancy
		})).Return(&models.Pool{}, "wf-poolid", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		headers, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
		assert.Equal(tt, "wf-poolid", headers.Response.WorkflowId)
	})

	t.Run("returns 400 when both dataEndpointCount and nodeCapacities are set", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			NodeCapacities:    []ociserver.NodeCapacity{{SizeInGiB: 100}},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgBothNodeCapacitiesAndDEC, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when neither nodeCapacities nor dataEndpointCount provided", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps: ociserver.OptFloat64{Value: 2, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgNeitherNodeCapacitiesNorDEC, bad.Response.ErrorMessage)
	})

	t.Run("maps nodeCapacities to UpdatePoolParams", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				len(p.NodeCapacities) == 2 &&
				p.NodeCapacities[0].Name == "node-a" &&
				p.NodeCapacities[0].NodeUUID == "uuid-a" &&
				p.NodeCapacities[0].SizeInGiB == 100 &&
				p.NodeCapacities[1].Name == "node-b" &&
				p.NodeCapacities[1].NodeUUID == "uuid-b" &&
				p.NodeCapacities[1].SizeInGiB == 200
		})).Return(&models.Pool{}, "wf-nc", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "node-a", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-a", Set: true},
					SizeInGiB: 100,
				},
				{
					Name:      ociserver.OptString{Value: "node-b", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-b", Set: true},
					SizeInGiB: 200,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		_, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
	})

	t.Run("maps kmsKeyId to UpdatePoolParams", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil && p.KmsKeyId == "ocid1.key.oc1..kkk"
		})).Return(&models.Pool{}, "wf-kms", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			KmsKeyId:          ociserver.OptString{Value: "ocid1.key.oc1..kkk", Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		_, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
	})

	t.Run("maps nsgIds to UpdatePoolParams", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				len(p.NsgIds) == 2 &&
				p.NsgIds[0] == "nsg-1" &&
				p.NsgIds[1] == "nsg-2"
		})).Return(&models.Pool{}, "wf-nsg", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			NsgIds:            []string{"nsg-1", "nsg-2"},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		_, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
	})

	t.Run("maps securityAttributes to UpdatePoolParams", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				p.SecurityAttributes["env"] == "prod" &&
				p.SecurityAttributes["tier"] == "gold"
		})).Return(&models.Pool{}, "wf-sa", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			SecurityAttributes: ociserver.OptUpdatePoolRequestSecurityAttributes{
				Value: ociserver.UpdatePoolRequestSecurityAttributes{"env": "prod", "tier": "gold"},
				Set:   true,
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		_, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
	})

	t.Run("maps ociAdminPassword to UpdatePoolParams", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().UpdatePool(mock.Anything, mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
			return p != nil &&
				p.OciAdminPassword != nil &&
				p.OciAdminPassword.Ocid == "ocid1.secret.oc1..sss" &&
				p.OciAdminPassword.Version == 7
		})).Return(&models.Pool{}, "wf-pwd", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			OciAdminPassword: ociserver.OptOCIOCIDVersionRef{
				Value: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..sss", Version: "7"},
				Set:   true,
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		_, ok := res.(*ociserver.UpdatePoolAcceptedResponseHeaders)
		assert.True(tt, ok)
	})

	t.Run("returns 400 when ociAdminPassword version is not parseable", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			OciAdminPassword: ociserver.OptOCIOCIDVersionRef{
				Value: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..sss", Version: "not-a-number"},
				Set:   true,
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgInvalidAdminPasswordVersion, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when ociAdminPassword version is below 1", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
			OciAdminPassword: ociserver.OptOCIOCIDVersionRef{
				Value: ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..sss", Version: "0"},
				Set:   true,
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgAdminPasswordVersionLessThanOne, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when throughputGBps is zero", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: 0, Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgThroughputGBpsNotPositive, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when throughputGBps is negative", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: -2.5, Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgThroughputGBpsNotPositive, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when throughputGBps is NaN", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: math.NaN(), Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgThroughputGBpsNotFinite, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when throughputGBps is +Inf", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: math.Inf(1), Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, errMsgThroughputGBpsNotFinite, bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when a nodeCapacity sizeInGiB is zero", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{Name: ociserver.OptString{Value: "n1", Set: true}, SizeInGiB: 1024},
				{Name: ociserver.OptString{Value: "n2", Set: true}, SizeInGiB: 0},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacitySizeNotPositive, 1), bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when a nodeCapacity sizeInGiB is negative", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{Name: ociserver.OptString{Value: "n1", Set: true}, SizeInGiB: -10},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacitySizeNotPositive, 0), bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when a nodeCapacity name is missing", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					NodeUUID:  ociserver.OptString{Value: "uuid-a", Set: true},
					SizeInGiB: 100,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNameRequired, 0), bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when a nodeCapacity name is blank", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "   ", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-a", Set: true},
					SizeInGiB: 100,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNameRequired, 0), bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when a nodeCapacity nodeUUID is missing", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "node-a", Set: true},
					SizeInGiB: 100,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDRequired, 0), bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when a nodeCapacity nodeUUID is blank", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "node-a", Set: true},
					NodeUUID:  ociserver.OptString{Value: "", Set: true},
					SizeInGiB: 100,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDRequired, 0), bad.Response.ErrorMessage)
	})

	t.Run("reports the offending nodeCapacity index when a later entry is missing nodeUUID", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "node-a", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-a", Set: true},
					SizeInGiB: 100,
				},
				{
					Name:      ociserver.OptString{Value: "node-b", Set: true},
					SizeInGiB: 200,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDRequired, 1), bad.Response.ErrorMessage)
	})

	t.Run("returns 400 when nodeCapacities contains duplicate node_uuid (orchestrator not called)", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		h := Handler{Orchestrator: mockOrchestrator}
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "node-a", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-dup", Set: true},
					SizeInGiB: 100,
				},
				{
					Name:      ociserver.OptString{Value: "node-b", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-dup", Set: true},
					SizeInGiB: 200,
				},
			},
		}
		params := ociserver.UpdatePoolParams{PoolOCID: poolOCID, TenancyOcid: tenancy}

		res, err := h.UpdatePool(contextWithOpcRequestID(context.Background(), defaultTestOPC), req, params)

		assert.NoError(tt, err)
		bad, ok := res.(*ociserver.UpdatePoolBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDDuplicate, 1, "uuid-dup"),
			bad.Response.ErrorMessage)
		assert.Equal(tt, poolOCID, bad.Response.PoolOCID)
	})
}

func TestValidateUpdatePoolUnitConversionInputs(t *testing.T) {
	t.Run("accepts a valid request with throughput and DEC", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps:    ociserver.OptFloat64{Value: 2.5, Set: true},
			DataEndpointCount: ociserver.OptInt64{Value: 4, Set: true},
		}
		assert.Equal(tt, "", validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("accepts an empty request (no fields set)", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{}
		assert.Equal(tt, "", validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("accepts unset optional fields without inspecting Value", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			ThroughputGBps: ociserver.OptFloat64{Value: -1, Set: false},
		}
		assert.Equal(tt, "", validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("rejects zero throughputGBps", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{ThroughputGBps: ociserver.OptFloat64{Value: 0, Set: true}}
		assert.Equal(tt, errMsgThroughputGBpsNotPositive, validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("rejects negative throughputGBps", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{ThroughputGBps: ociserver.OptFloat64{Value: -0.5, Set: true}}
		assert.Equal(tt, errMsgThroughputGBpsNotPositive, validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("rejects NaN throughputGBps", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{ThroughputGBps: ociserver.OptFloat64{Value: math.NaN(), Set: true}}
		assert.Equal(tt, errMsgThroughputGBpsNotFinite, validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("rejects +Inf throughputGBps", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{ThroughputGBps: ociserver.OptFloat64{Value: math.Inf(1), Set: true}}
		assert.Equal(tt, errMsgThroughputGBpsNotFinite, validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("rejects -Inf throughputGBps", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{ThroughputGBps: ociserver.OptFloat64{Value: math.Inf(-1), Set: true}}
		assert.Equal(tt, errMsgThroughputGBpsNotFinite, validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("rejects a non-positive node capacity and reports its index", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{SizeInGiB: 100},
				{SizeInGiB: 200},
				{SizeInGiB: -1},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacitySizeNotPositive, 2), validateUpdatePoolUnitConversionInputs(req))
	})

	t.Run("accepts node capacities that are all positive", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{SizeInGiB: 100},
				{SizeInGiB: 200},
			},
		}
		assert.Equal(tt, "", validateUpdatePoolUnitConversionInputs(req))
	})
}

func TestValidateUpdatePoolNodeCapacityRequiredFields(t *testing.T) {
	full := func(name, uuid string, size int64) ociserver.NodeCapacity {
		return ociserver.NodeCapacity{
			Name:      ociserver.OptString{Value: name, Set: true},
			NodeUUID:  ociserver.OptString{Value: uuid, Set: true},
			SizeInGiB: size,
		}
	}

	t.Run("accepts an empty nodeCapacities array", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{}
		assert.Equal(tt, "", validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("accepts entries with all three fields populated", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				full("node-a", "uuid-a", 512),
				full("node-b", "uuid-b", 1024),
			},
		}
		assert.Equal(tt, "", validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("rejects when name is not set", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{NodeUUID: ociserver.OptString{Value: "uuid-a", Set: true}, SizeInGiB: 512},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNameRequired, 0),
			validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("rejects when name is set but empty/whitespace", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "  \t  ", Set: true},
					NodeUUID:  ociserver.OptString{Value: "uuid-a", Set: true},
					SizeInGiB: 512,
				},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNameRequired, 0),
			validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("rejects when nodeUUID is not set", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{Name: ociserver.OptString{Value: "node-a", Set: true}, SizeInGiB: 512},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDRequired, 0),
			validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("rejects when nodeUUID is set but empty", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{
					Name:      ociserver.OptString{Value: "node-a", Set: true},
					NodeUUID:  ociserver.OptString{Value: "", Set: true},
					SizeInGiB: 512,
				},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDRequired, 0),
			validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("reports the index of the first offending entry", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				full("node-a", "uuid-a", 512),
				full("node-b", "uuid-b", 1024),
				{Name: ociserver.OptString{Value: "node-c", Set: true}, SizeInGiB: 256},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDRequired, 2),
			validateUpdatePoolNodeCapacityRequiredFields(req))
	})

	t.Run("name is checked before nodeUUID for the same entry", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				{SizeInGiB: 100},
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNameRequired, 0),
			validateUpdatePoolNodeCapacityRequiredFields(req))
	})
}

func TestValidateUpdatePoolNodeCapacityUniqueness(t *testing.T) {
	entry := func(name, uuid string, size int64) ociserver.NodeCapacity {
		return ociserver.NodeCapacity{
			Name:      ociserver.OptString{Value: name, Set: true},
			NodeUUID:  ociserver.OptString{Value: uuid, Set: true},
			SizeInGiB: size,
		}
	}

	t.Run("accepts empty nodeCapacities", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{}
		assert.Equal(tt, "", validateUpdatePoolNodeCapacityUniqueness(req))
	})

	t.Run("accepts a single entry", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{entry("n1", "uuid-1", 100)},
		}
		assert.Equal(tt, "", validateUpdatePoolNodeCapacityUniqueness(req))
	})

	t.Run("accepts distinct UUIDs across multiple entries", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				entry("n1", "uuid-1", 100),
				entry("n2", "uuid-2", 200),
				entry("n3", "uuid-3", 300),
			},
		}
		assert.Equal(tt, "", validateUpdatePoolNodeCapacityUniqueness(req))
	})

	t.Run("rejects a duplicate UUID and reports the second occurrence index", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				entry("n1", "uuid-dup", 100),
				entry("n2", "uuid-dup", 200),
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDDuplicate, 1, "uuid-dup"),
			validateUpdatePoolNodeCapacityUniqueness(req))
	})

	t.Run("trims whitespace before comparing UUIDs", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				entry("n1", "uuid-a", 100),
				entry("n2", "  uuid-a  ", 200),
			},
		}
		assert.Equal(tt, fmt.Sprintf(errMsgNodeCapacityNodeUUIDDuplicate, 1, "uuid-a"),
			validateUpdatePoolNodeCapacityUniqueness(req))
	})

	t.Run("skips entries with unset or blank nodeUUID (delegated to required-fields validator)", func(tt *testing.T) {
		req := &ociserver.UpdatePoolRequest{
			NodeCapacities: []ociserver.NodeCapacity{
				entry("n1", "uuid-1", 100),
				{Name: ociserver.OptString{Value: "n2", Set: true}, SizeInGiB: 200},
				{Name: ociserver.OptString{Value: "n3", Set: true},
					NodeUUID: ociserver.OptString{Value: "   ", Set: true}, SizeInGiB: 300},
			},
		}
		assert.Equal(tt, "", validateUpdatePoolNodeCapacityUniqueness(req))
	})
}

// TestIsSharedHA pins the shared-HA classification on the (primary, secondary) pair.
// "Shared HA" means the pool runs in a single AD: the secondary is either unset
// (empty) or identical to the primary. Anything else is non-shared (regional) HA.
func TestIsSharedHA(t *testing.T) {
	cases := []struct {
		name      string
		primary   string
		secondary string
		want      bool
	}{
		{name: "empty secondary is shared", primary: "ad1", secondary: "", want: true},
		{name: "secondary equals primary is shared", primary: "ad1", secondary: "ad1", want: true},
		{name: "secondary differs from primary is non-shared", primary: "ad1", secondary: "ad2", want: false},
		{name: "both empty is shared", primary: "", secondary: "", want: true},
		{name: "primary empty but secondary set is non-shared", primary: "", secondary: "ad1", want: false},
		{name: "secondary differs by whitespace is non-shared (no trim)", primary: "ad1", secondary: " ad1", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			assert.Equal(tt, tc.want, isSharedHA(tc.primary, tc.secondary))
		})
	}
}

// TestCreatePool_NonSharedHARequiresDataEndpointCount2 covers the new validation
// rule: when the request describes a non-shared (regional) HA pool, DataEndpointCount
// must be exactly 2. Any other (even) value must be rejected with a 400.
func TestCreatePool_NonSharedHARequiresDataEndpointCount2(t *testing.T) {
	mockOrchestrator := factory.NewMockOrchestratorFactory(t)
	h := Handler{Orchestrator: mockOrchestrator}
	req := validCreatePoolRequest()
	req.PoolOCID = "ocid1.pool.oc1..non-shared-bad-count"
	req.SecondaryAvailabilityDomain = ociserver.OptString{Value: "ad2", Set: true} // non-shared
	req.DataEndpointCount = 4                                                      // even but != 2

	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, defaultCreatePoolParams())

	assert.NoError(t, err)
	bad, ok := res.(*ociserver.CreatePoolBadRequest)
	assert.True(t, ok, "response should be *ociserver.CreatePoolBadRequest")
	assert.Equal(t, defaultTestOPC, bad.OpcRequestID)
	assert.Equal(t, req.PoolOCID, bad.Response.PoolOCID)
	assert.Equal(t, string(workflowquery.WorkflowStatusFailed), bad.Response.Status)
	assert.Equal(t, errMsgInvalidConfigForNonSharedHA, bad.Response.ErrorMessage)
	// Orchestrator must not be invoked when validation fails up front.
	mockOrchestrator.AssertNotCalled(t, "CreatePool", mock.Anything, mock.Anything)
}

// TestValidateCreatePoolRequest exercises every branch of validateCreatePoolRequest
// directly (no Handler, no mock). Each non-nil-request case starts from a known-good
// base and mutates exactly one field so the failing branch is unambiguous.
func TestValidateCreatePoolRequest(t *testing.T) {
	t.Run("nil request returns error", func(tt *testing.T) {
		params := defaultCreatePoolParams()
		err := validateCreatePoolRequest(nil, &params)
		assert.EqualError(tt, err, "request body is required")
	})

	t.Run("nil params returns error", func(tt *testing.T) {
		err := validateCreatePoolRequest(validCreatePoolRequest(), nil)
		assert.EqualError(tt, err, "request params are required")
	})

	cases := []struct {
		name    string
		mutate  func(*ociserver.CreatePoolRequest, *ociserver.CreatePoolParams)
		wantErr string // empty string means expect nil
	}{
		{
			name:    "valid request passes",
			mutate:  nil,
			wantErr: "",
		},
		{
			name:    "empty pool OCID rejected with empty error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.PoolOCID = "" },
			wantErr: errMsgEmptyPoolOCID,
		},
		{
			name:    "whitespace-only pool OCID rejected as empty after TrimSpace",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.PoolOCID = "   " },
			wantErr: errMsgEmptyPoolOCID,
		},
		{
			name:    "non-empty malformed pool OCID rejected with structural error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.PoolOCID = "not-an-ocid" },
			wantErr: errMsgInvalidPoolOCID,
		},
		{
			name:    "empty compartment OCID rejected with empty error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.CompartmentOCID = "" },
			wantErr: errMsgEmptyCompartmentOCID,
		},
		{
			name:    "non-empty malformed compartment OCID rejected with structural error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.CompartmentOCID = "x" },
			wantErr: errMsgInvalidCompartmentOCID,
		},
		{
			name:    "empty subnet OCID rejected with empty error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.SubnetId = "" },
			wantErr: errMsgEmptySubnetID,
		},
		{
			name:    "non-empty malformed subnet OCID rejected with structural error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.SubnetId = "subnet-123" },
			wantErr: errMsgInvalidSubnetID,
		},
		{
			name:    "zero size rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.SizeInGiB = 0 },
			wantErr: errMsgNonPositiveSizeInGiB,
		},
		{
			name:    "negative size rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.SizeInGiB = -1 },
			wantErr: errMsgNonPositiveSizeInGiB,
		},
		{
			name:    "empty data NIC subnet OCID rejected with empty error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.DataNicSubnetId = "" },
			wantErr: errMsgEmptyDataNicSubnetID,
		},
		{
			name: "non-empty malformed data NIC subnet OCID rejected with structural error",
			mutate: func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) {
				r.DataNicSubnetId = "data-subnet-1"
			},
			wantErr: errMsgInvalidDataNicSubnetID,
		},
		{
			name:    "empty admin password OCID rejected with empty error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.OciAdminPassword.Ocid = "" },
			wantErr: errMsgEmptyAdminPasswordOCID,
		},
		{
			name:    "non-empty malformed admin password OCID rejected with structural error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.OciAdminPassword.Ocid = "x" },
			wantErr: errMsgInvalidAdminPasswordOCID,
		},
		{
			name:    "empty admin password version rejected with empty error",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.OciAdminPassword.Version = "" },
			wantErr: errMsgEmptyAdminPasswordVersion,
		},
		{
			name:    "zero throughput rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.ThroughputGBps = 0 },
			wantErr: errMsgNonPositiveThroughputGBps,
		},
		{
			name:    "negative throughput rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.ThroughputGBps = -0.5 },
			wantErr: errMsgNonPositiveThroughputGBps,
		},
		{
			name:    "odd DataEndpointCount rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.DataEndpointCount = 3 },
			wantErr: errMsgOddDataEndpointCount,
		},
		{
			name:    "empty tenancy OCID (from params) rejected with empty error",
			mutate:  func(_ *ociserver.CreatePoolRequest, p *ociserver.CreatePoolParams) { p.TenancyOcid = "" },
			wantErr: errMsgEmptyTenancyOcid,
		},
		{
			name:    "non-empty malformed tenancy OCID (from params) rejected with structural error",
			mutate:  func(_ *ociserver.CreatePoolRequest, p *ociserver.CreatePoolParams) { p.TenancyOcid = "not-an-ocid" },
			wantErr: errMsgInvalidTenancyOcid,
		},
		{
			name:    "empty displayName rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.DisplayName = "" },
			wantErr: errMsgEmptyDisplayName,
		},
		{
			name:    "whitespace-only displayName rejected (TrimSpace)",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.DisplayName = "   \t\n " },
			wantErr: errMsgEmptyDisplayName,
		},
		{
			name:    "empty primaryAvailabilityDomain rejected",
			mutate:  func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) { r.PrimaryAvailabilityDomain = "" },
			wantErr: errMsgEmptyPrimaryAD,
		},
		{
			name: "whitespace-only primaryAvailabilityDomain rejected (TrimSpace)",
			mutate: func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) {
				r.PrimaryAvailabilityDomain = " \t"
			},
			wantErr: errMsgEmptyPrimaryAD,
		},
		{
			name: "non-shared HA with DataEndpointCount=4 rejected",
			mutate: func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) {
				r.SecondaryAvailabilityDomain = ociserver.OptString{Value: "ad2", Set: true}
				r.DataEndpointCount = 4
			},
			wantErr: errMsgInvalidConfigForNonSharedHA,
		},
		{
			name: "non-shared HA with DataEndpointCount=2 passes",
			mutate: func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) {
				r.SecondaryAvailabilityDomain = ociserver.OptString{Value: "ad2", Set: true}
				r.DataEndpointCount = 2
			},
			wantErr: "",
		},
		{
			name: "shared HA via secondary equal to primary with DataEndpointCount=4 passes",
			mutate: func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) {
				r.SecondaryAvailabilityDomain = ociserver.OptString{Value: "ad1", Set: true}
				r.DataEndpointCount = 4
			},
			wantErr: "",
		},
		{
			name: "shared HA via empty secondary with DataEndpointCount=4 passes",
			mutate: func(r *ociserver.CreatePoolRequest, _ *ociserver.CreatePoolParams) {
				r.DataEndpointCount = 4
			},
			wantErr: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			req := validCreatePoolRequest()
			params := defaultCreatePoolParams()
			if tc.mutate != nil {
				tc.mutate(req, &params)
			}
			err := validateCreatePoolRequest(req, &params)
			if tc.wantErr == "" {
				assert.NoError(tt, err)
				return
			}
			assert.EqualError(tt, err, tc.wantErr)
		})
	}
}

func TestValidateCreatePoolRequest_TrimsInPlace(t *testing.T) {
	req := validCreatePoolRequest()
	req.PoolOCID = "  ocid1.pool.oc1..valid  "
	req.CompartmentOCID = "\tocid1.compartment.oc1..valid\n"
	req.DisplayName = "  My Pool  "
	req.SubnetId = " ocid1.subnet.oc1.iad.valid "
	req.DataNicSubnetId = "\nocid1.subnet.oc1.iad.validdata\t"
	req.PrimaryAvailabilityDomain = "  ad1\t"
	req.SecondaryAvailabilityDomain = ociserver.OptString{Value: " ad1 ", Set: true}
	req.MediatorAvailabilityDomain = ociserver.OptString{Value: "  ad9  ", Set: true}
	req.OciAdminPassword = ociserver.OCIOCIDVersionRef{
		Ocid:    " ocid1.secret.oc1..valid ",
		Version: " 1 ",
	}
	req.Description = ociserver.OptString{Value: "  some desc  ", Set: true}
	req.DataEndpointCount = 2

	params := ociserver.CreatePoolParams{TenancyOcid: "  ocid1.tenancy.oc1..aaaaaaaatestaaa  "}

	err := validateCreatePoolRequest(req, &params)
	require.NoError(t, err, "padded-but-otherwise-valid request must validate after the in-place trim")

	assert.Equal(t, "ocid1.pool.oc1..valid", req.PoolOCID,
		"PoolOCID must be trimmed in place so the orchestrator and DB store the canonical form")
	assert.Equal(t, "ocid1.compartment.oc1..valid", req.CompartmentOCID)
	assert.Equal(t, "My Pool", req.DisplayName,
		"DisplayName trim is critical: human-readable name must not carry copy/paste whitespace into the DB")
	assert.Equal(t, "ocid1.subnet.oc1.iad.valid", req.SubnetId)
	assert.Equal(t, "ocid1.subnet.oc1.iad.validdata", req.DataNicSubnetId)
	assert.Equal(t, "ad1", req.PrimaryAvailabilityDomain)
	assert.Equal(t, "ad1", req.SecondaryAvailabilityDomain.Value,
		"OptString-wrapped optional fields must also be trimmed so isSharedHA's equality check is not defeated by whitespace")
	assert.Equal(t, "ad9", req.MediatorAvailabilityDomain.Value)
	assert.Equal(t, "ocid1.secret.oc1..valid", req.OciAdminPassword.Ocid)
	assert.Equal(t, "1", req.OciAdminPassword.Version,
		"Version is parsed by strconv.ParseInt downstream, which rejects ' 1 '; trim before validation lets the parse succeed")
	assert.Equal(t, "some desc", req.Description.Value)
	assert.Equal(t, "ocid1.tenancy.oc1..aaaaaaaatestaaa", params.TenancyOcid,
		"path/query param TenancyOcid must be trimmed via the *params handle so the orchestrator AccountName matches the DB key")
}

// TestValidateDeletePoolRequest covers each validation branch for the Delete
// path (no request body, only path/query params) and pins that nil params is
// rejected before any field access.
func TestValidateDeletePoolRequest(t *testing.T) {
	t.Run("nil params returns error", func(tt *testing.T) {
		err := validateDeletePoolRequest(nil)
		assert.EqualError(tt, err, "request params are required")
	})

	t.Run("valid params pass", func(tt *testing.T) {
		params := ociserver.DeletePoolParams{
			PoolOCID:    "ocid1.pool.oc1.iad.valid",
			TenancyOcid: defaultTestTenancyOCID,
		}
		assert.NoError(tt, validateDeletePoolRequest(&params))
	})

	t.Run("non-empty malformed PoolOCID rejected with structural error", func(tt *testing.T) {
		params := ociserver.DeletePoolParams{
			PoolOCID:    "not-an-ocid",
			TenancyOcid: defaultTestTenancyOCID,
		}
		assert.EqualError(tt, validateDeletePoolRequest(&params), errMsgInvalidPoolOCID)
	})

	t.Run("empty PoolOCID rejected with empty error (not structural)", func(tt *testing.T) {
		params := ociserver.DeletePoolParams{
			PoolOCID:    "",
			TenancyOcid: defaultTestTenancyOCID,
		}
		// errMsgEmptyPoolOCID, NOT errMsgInvalidPoolOCID — distinct caller mistake,
		// distinct diagnostic.
		assert.EqualError(tt, validateDeletePoolRequest(&params), errMsgEmptyPoolOCID)
	})

	t.Run("whitespace-only PoolOCID is empty-after-trim", func(tt *testing.T) {
		params := ociserver.DeletePoolParams{
			PoolOCID:    "  \t",
			TenancyOcid: defaultTestTenancyOCID,
		}
		// The normalize step turns "  \t" into "" before validation,
		// so this collapses to the empty-error path (not the structural one).
		assert.EqualError(tt, validateDeletePoolRequest(&params), errMsgEmptyPoolOCID)
	})

	t.Run("empty TenancyOcid rejected with empty error", func(tt *testing.T) {
		params := ociserver.DeletePoolParams{
			PoolOCID:    "ocid1.pool.oc1.iad.valid",
			TenancyOcid: "",
		}
		assert.EqualError(tt, validateDeletePoolRequest(&params), errMsgEmptyTenancyOcid)
	})

	t.Run("non-empty malformed TenancyOcid rejected with structural error", func(tt *testing.T) {
		params := ociserver.DeletePoolParams{
			PoolOCID:    "ocid1.pool.oc1.iad.valid",
			TenancyOcid: "not-an-ocid",
		}
		assert.EqualError(tt, validateDeletePoolRequest(&params), errMsgInvalidTenancyOcid)
	})
}

func TestValidateDeletePoolRequest_TrimsInPlace(t *testing.T) {
	params := ociserver.DeletePoolParams{
		PoolOCID:    " ocid1.pool.oc1.iad.valid\t",
		TenancyOcid: "  ocid1.tenancy.oc1..aaaaaaaatestaaa  ",
	}

	require.NoError(t, validateDeletePoolRequest(&params),
		"padded-but-otherwise-valid params must validate after the in-place trim")

	assert.Equal(t, "ocid1.pool.oc1.iad.valid", params.PoolOCID,
		"PoolOCID must be trimmed in place so the orchestrator's pool lookup matches the create-time value")
	assert.Equal(t, "ocid1.tenancy.oc1..aaaaaaaatestaaa", params.TenancyOcid,
		"TenancyOcid must be trimmed in place so AccountName matches the DB key")
}
