package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
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
		params := ociserver.CreatePoolParams{}
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
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
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-echo-id", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           req.DisplayName,
			AccountName:    req.CompartmentOCID,
			VendorSubNetID: req.SubnetId,
			SizeInBytes:    uint64(req.SizeInGiB) * 1024 * 1024 * 1024,
			State:          "ACTIVE",
		}, "work-req-echo", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.CreatePoolParams{}

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
			CompartmentOCID:           "comp-1",
			DisplayName:               "my-pool",
			SubnetId:                  "subnet-1",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			SizeInGiB:                 2048,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
		}
		mockOrchestrator.EXPECT().CreatePool(mock.Anything, mock.Anything).Return(&models.Pool{
			BaseModel:      models.BaseModel{UUID: "pool-from-req", CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Name:           req.DisplayName,
			AccountName:    req.CompartmentOCID,
			VendorSubNetID: req.SubnetId,
			SizeInBytes:    uint64(req.SizeInGiB) * 1024 * 1024 * 1024,
			State:          "ACTIVE",
		}, "work-req-populate", nil)
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.CreatePoolParams{}

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
			CompartmentOCID:           "comp-1",
			DisplayName:               "pool-with-data-nic",
			SubnetId:                  "subnet-1",
			DataNicSubnetId:           "subnet-data-1",
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
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
				SizeInBytes:    uint64(req.SizeInGiB) * 1024 * 1024 * 1024,
				State:          "CREATING",
			}, "work-req-data-nic", nil)
		h := Handler{Orchestrator: mockOrchestrator}

		res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, ociserver.CreatePoolParams{})

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
		params := ociserver.CreatePoolParams{}
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
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
		params := ociserver.CreatePoolParams{}
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
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
		params := ociserver.CreatePoolParams{}
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
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
		params := ociserver.CreatePoolParams{}
		req := &ociserver.CreatePoolRequest{
			PoolOCID:                  "ocid1.pool.oc1..aaa",
			CompartmentOCID:           "ocid1.compartment.oc1..aaa",
			DisplayName:               "test-pool",
			SubnetId:                  "ocid1.subnet.oc1..aaa",
			DataNicSubnetId:           "ocid1.subnet.oc1..data",
			SizeInGiB:                 1024,
			PrimaryAvailabilityDomain: "ad1",
			ThroughputGBps:            1,
			Iops:                      1000,
			OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
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
		}
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
		params := ociserver.DeletePoolParams{PoolOCID: "550e8400-e29b-41d4-a716-446655440000", TenancyOcid: "ocid1.compartment.oc1..aaa"}
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
		params := ociserver.DeletePoolParams{PoolOCID: "550e8400-e29b-41d4-a716-446655440000", TenancyOcid: "ocid1.compartment.oc1..aaa"}
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
		params := ociserver.DeletePoolParams{PoolOCID: "pool-uuid", TenancyOcid: "ocid1.compartment.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, "delete-request-id"), params)

		assert.NoError(tt, err)
		headers := res.(*ociserver.DeletePoolAcceptedResponseHeaders)
		assert.Equal(tt, "delete-request-id", headers.OpcRequestID)
	})

	t.Run("DeletePool returns 404 style body when pool not found", func(tt *testing.T) {
		mockOrchestrator := factory.NewMockOrchestratorFactory(tt)
		mockOrchestrator.EXPECT().DeletePool(mock.Anything, mock.Anything).Return(nil, "", utilserrors.NewNotFoundErr("pool not found", nil))
		h := Handler{Orchestrator: mockOrchestrator}
		params := ociserver.DeletePoolParams{PoolOCID: "550e8400-e29b-41d4-a716-446655440000", TenancyOcid: "ocid1.compartment.oc1..aaa"}
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
		params := ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..x", TenancyOcid: "ocid1.compartment.oc1..aaa"}
		res, err := h.DeletePool(contextWithOpcRequestID(nil, defaultTestOPC), params)
		assert.NoError(tt, err)
		conf, ok := res.(*ociserver.DeletePoolConflict)
		assert.True(tt, ok)
		assert.Contains(tt, conf.Response.ErrorMessage, "transitioning")
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
		SizeInGiB:                 1024,
		PrimaryAvailabilityDomain: "ad1",
		ThroughputGBps:            1,
		Iops:                      1000,
		OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
	}
	res, err := h.CreatePool(context.Background(), req, ociserver.CreatePoolParams{})
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
		SizeInGiB:                 1024,
		PrimaryAvailabilityDomain: "ad1",
		ThroughputGBps:            1,
		Iops:                      1000,
		OciAdminPassword:          ociserver.OCIOCIDVersionRef{Ocid: "ocid1.secret.oc1..aaa", Version: "1"},
	}
	res, err := h.CreatePool(contextWithOpcRequestID(nil, defaultTestOPC), req, ociserver.CreatePoolParams{})
	assert.NoError(t, err)
	headers, ok := res.(*ociserver.CreatePoolAcceptedResponseHeaders)
	assert.True(t, ok)
	assert.Equal(t, "", headers.Response.WorkflowId)
}

func TestGetWorkflow_MissingOPCRequestID(t *testing.T) {
	h := Handler{}
	res, err := h.GetWorkflow(context.Background(), ociserver.GetWorkflowParams{WorkRequestId: "wf-1"})
	assert.NoError(t, err)
	bad, ok := res.(*ociserver.GetWorkflowBadRequest)
	assert.True(t, ok)
	assert.Equal(t, invalidOPCRequestID, bad.Response.ErrorMessage)
}

func TestDeletePool_MissingOPCRequestID(t *testing.T) {
	h := Handler{}
	res, err := h.DeletePool(context.Background(), ociserver.DeletePoolParams{PoolOCID: "ocid1.pool.oc1..aaa"})
	assert.NoError(t, err)
	bad, ok := res.(*ociserver.DeletePoolBadRequest)
	assert.True(t, ok)
	assert.Equal(t, invalidOPCRequestID, bad.Response.ErrorMessage)
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
