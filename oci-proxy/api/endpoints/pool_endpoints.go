package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	ociserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/oci-proxy/api/oci-servergen"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/workflowquery"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
)

const (
	invalidOPCRequestID                   = "Opc-Request-Id not set"
	errMsgInvalidAdminPasswordVersion     = "ociAdminPassword.version must be a valid integer"
	errMsgAdminPasswordVersionLessThanOne = "ociAdminPassword.version must be greater than or equal to 1"
	errMsgOddDataEndpointConfig           = "dataEndpointConfig must have an even number of entries (one pair per HA pair)"
	errMsgOddDataEndpointCount            = "dataEndpointCount must be a multiple of 2"
)

// Handler implements the OCI API Handler interface.
// Embedding UnimplementedHandler provides default 501 for any operation not overridden here.
type Handler struct {
	ociserver.UnimplementedHandler
	Orchestrator   factory.OrchestratorFactory
	ServerState    *ServerState
	TemporalClient client.Client
}

var workflowQueryFn = workflowquery.Query

func NewHandler(serverState *ServerState) *Handler {
	return &Handler{ServerState: serverState}
}

// opcRequestIDFromContext returns opc-request-id from the request headers stored in context.
// ociPrepareRequestMiddleware sets HeaderContextKey (and opc on r.Header); auth may refresh the same map.
func opcRequestIDFromContext(ctx context.Context) (string, error) {
	opcHeader := string(middleware.OPCRequestIDHeaderName)
	h, ok := ctx.Value(middleware.HeaderContextKey).(http.Header)
	if !ok || h == nil {
		return "", errors.New("request headers not in context")
	}
	if id := strings.TrimSpace(h.Get(opcHeader)); id != "" {
		return id, nil
	}
	return "", errors.New("opc-request-id header missing")
}

// GetHealth returns the server health status.
// During graceful shutdown, this returns an error to fail readiness probes,
// allowing the load balancer to stop routing traffic to this pod before it terminates.
func (h *Handler) GetHealth(ctx context.Context) (ociserver.GetHealthRes, error) {
	if h.ServerState != nil && h.ServerState.IsShuttingDown() {
		return &ociserver.StandardError500Headers{
			OpcRequestID: uuid.NewString(),
			Response: ociserver.Error{
				Code:    500,
				Message: "Server is shutting down",
			},
		}, nil
	}
	return &ociserver.Health{Status: ociserver.NewOptString("healthy")}, nil
}

func newCreatePoolBadRequest(opcRequestID, poolOCID, errMsg string) *ociserver.CreatePoolBadRequest {
	return &ociserver.CreatePoolBadRequest{
		OpcRequestID: opcRequestID,
		Response: ociserver.PoolOperationErrorResponse{
			Status:       string(workflowquery.WorkflowStatusFailed),
			PoolOCID:     poolOCID,
			ErrorMessage: errMsg,
		},
	}
}

// CreatePool returns 202 Accepted with opc-request-id, opc-work-request-id, and the created Pool.
func (h *Handler) CreatePool(ctx context.Context, req *ociserver.CreatePoolRequest, params ociserver.CreatePoolParams) (ociserver.CreatePoolRes, error) {
	logger := util.GetLogger(ctx)

	opcRequestID, err := opcRequestIDFromContext(ctx)
	if err != nil {
		logger.Error("missing opc-request-id in context", "error", err)
		return newCreatePoolBadRequest(uuid.NewString(), req.PoolOCID, invalidOPCRequestID), nil
	}

	ociAdminPasswordVersion, err := strconv.ParseInt(req.OciAdminPassword.Version, 10, 64)
	if err != nil {
		logger.Error("invalid ociAdminPassword version", "error", err)
		return newCreatePoolBadRequest(opcRequestID, req.PoolOCID, errMsgInvalidAdminPasswordVersion), nil
	}

	if ociAdminPasswordVersion < 1 {
		logger.Error("invalid ociAdminPassword version", "version", ociAdminPasswordVersion)
		return newCreatePoolBadRequest(opcRequestID, req.PoolOCID, errMsgAdminPasswordVersionLessThanOne), nil
	}

	if len(req.DataEndpointConfig)%2 != 0 {
		logger.Error("dataEndpointConfig has odd number of entries", "count", len(req.DataEndpointConfig))
		return newCreatePoolBadRequest(opcRequestID, req.PoolOCID, errMsgOddDataEndpointConfig), nil
	}

	if req.DataEndpointCount%2 != 0 {
		logger.Error("dataEndpointCount is not a multiple of 2", "count", req.DataEndpointCount)
		return newCreatePoolBadRequest(opcRequestID, req.PoolOCID, errMsgOddDataEndpointCount), nil
	}

	secondaryAD := req.SecondaryAvailabilityDomain.Value
	if secondaryAD == "" {
		secondaryAD = req.PrimaryAvailabilityDomain
	}
	isRegionalHA := secondaryAD != req.PrimaryAvailabilityDomain

	mediatorAD := req.MediatorAvailabilityDomain.Value
	if mediatorAD == "" {
		mediatorAD = req.PrimaryAvailabilityDomain
	}

	// Map the provided config to CreatePoolParams
	createPoolParams := &commonparams.CreatePoolParams{
		AccountName:    params.TenancyOcid,                         // compartment_id from ociconfig
		PrimaryZone:    req.PrimaryAvailabilityDomain,              // availability_domain1
		SecondaryZone:  secondaryAD,                                // availability_domain2
		MediatorZone:   mediatorAD,                                 // mediator_availability_domain
		IsRegionalHA:   isRegionalHA,                               // derived from primary vs secondary AD
		Name:           req.DisplayName,                            // deployment_id
		Description:    req.Description.Value,                      // Will be generated by orchestrator
		VendorSubNetID: req.SubnetId,                               // subnet_id from ociconfig
		SizeInBytes:    uint64(req.SizeInGiB) * 1024 * 1024 * 1024, // convert GiB to bytes
		PoolOCID:       req.PoolOCID,                               // OCI pool OCID - used to generate deployment name
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			Enabled:         true,
			ThroughputMibps: int64(req.ThroughputGBps * workflowquery.MiBpsPerGBps), // convert GBps from API contract to MiBps for orchestrator
		},
		LargeCapacity:   false,
		CompartmentOCID: req.CompartmentOCID,
		OciAdminPassword: &commonparams.OciAdminPassword{
			Ocid:    req.OciAdminPassword.Ocid,
			Version: ociAdminPasswordVersion,
		},
		DataNICSubnetID: req.DataNicSubnetId,
		HAPairs:         uint64(req.DataEndpointCount / 2),
	}

	_, workflowID, err := h.Orchestrator.CreatePool(ctx, createPoolParams)
	if err != nil {
		if utilserrors.IsUserInputValidationErr(err) {
			return newCreatePoolBadRequest(opcRequestID, req.PoolOCID, err.Error()), nil
		}
		if utilserrors.IsConflictErr(err) {
			msg := err.Error()
			if unwrapped := errors.Unwrap(err); unwrapped != nil {
				msg = unwrapped.Error()
			}
			return &ociserver.CreatePoolConflict{
				OpcRequestID: opcRequestID,
				Response: ociserver.PoolOperationErrorResponse{
					Status:       string(workflowquery.WorkflowStatusFailed),
					PoolOCID:     req.PoolOCID,
					ErrorMessage: msg,
				},
			}, nil
		}
		logger.Error("CreatePool orchestrator error", "error", err)
		return &ociserver.CreatePoolInternalServerError{
			OpcRequestID: opcRequestID,
			Response: ociserver.PoolOperationErrorResponse{
				Status:       string(workflowquery.WorkflowStatusFailed),
				PoolOCID:     req.PoolOCID,
				ErrorMessage: "Internal server error",
			},
		}, nil
	}
	if workflowID != "" {
		return &ociserver.CreatePoolAcceptedResponseHeaders{
			OpcRequestID: opcRequestID,
			Response: ociserver.CreatePoolAcceptedResponse{
				Status:     string(workflowquery.WorkflowStatusInProgress),
				WorkflowId: workflowID,
				PoolOCID:   req.PoolOCID,
			},
		}, nil
	}
	return &ociserver.CreatePoolAcceptedResponseHeaders{
		OpcRequestID: opcRequestID,
		Response: ociserver.CreatePoolAcceptedResponse{
			Status:     string(workflowquery.WorkflowStatusInProgress),
			WorkflowId: "",
			PoolOCID:   req.PoolOCID,
		},
	}, nil
}

func (h *Handler) GetWorkflow(ctx context.Context, params ociserver.GetWorkflowParams) (ociserver.GetWorkflowRes, error) {
	logger := util.GetLogger(ctx)

	opcRequestID, err := opcRequestIDFromContext(ctx)
	if err != nil {
		logger.Error("missing opc-request-id", "error", err)
		return &ociserver.GetWorkflowBadRequest{
			OpcRequestID: uuid.NewString(),
			Response: ociserver.GetWorkflowErrorResponse{
				ErrorMessage: invalidOPCRequestID,
			},
		}, nil
	}

	res, err := workflowQueryFn(ctx, h.TemporalClient, params.WorkRequestId, "")
	if err != nil {
		return mapGetWorkflowQueryError(opcRequestID, err), nil
	}
	resp := ociserver.GetWorkflowStatusResponse{
		WorkflowType: res.WorkflowType,
		Status:       string(res.Status),
	}
	if res.Error != nil {
		workflowErr := ociserver.WorkflowStatusError{
			Message: ociserver.NewOptString(res.Error.Message),
		}
		if res.Error.Cause != "" {
			workflowErr.Cause = ociserver.NewOptString(res.Error.Cause)
		}
		resp.Error = ociserver.NewOptWorkflowStatusError(workflowErr)
	}
	if res.PoolMetadata != nil {
		nodeUUIDByName := h.nodeUUIDsByName(ctx, res.PoolMetadata.PoolUUID)
		vms := make([]ociserver.OCICreatePoolWorkflowVM, 0, len(res.PoolMetadata.Vms))
		for _, vm := range res.PoolMetadata.Vms {
			vms = append(vms, ociserver.OCICreatePoolWorkflowVM{
				Name:            vm.Name,
				SerialNumber:    vm.SerialNumber,
				VsaManagementIP: vm.VSAManagementIP,
				InterclusterIP:  vm.InterclusterIP,
				NodeIP:          vm.NodeIP,
				NodeUUID:        nodeUUIDByName[vm.Name],
				HaPair:          vm.HAPair,
				SizeInGiB:       vm.SizeInGiB,
				Iops:            vm.IOPS,
				ThroughputGBps:  vm.ThroughputGBps,
			})
		}
		resp.PoolMetadata = ociserver.NewOptOCICreatePoolWorkflowMetadata(
			ociserver.OCICreatePoolWorkflowMetadata{
				Vms:         vms,
				Credentials: ociserver.OCICreatePoolWorkflowCredentials{},
			},
		)
	}
	if res.SvmMetadata != nil {
		lifs := make([]ociserver.SvmLif, 0, len(res.SvmMetadata.Lifs))
		for _, l := range res.SvmMetadata.Lifs {
			protocols := make([]ociserver.SvmLifProtocolsItem, 0, len(l.Protocols))
			for _, p := range l.Protocols {
				protocols = append(protocols, ociserver.SvmLifProtocolsItem(p))
			}
			lifs = append(lifs, ociserver.SvmLif{
				Name:      ociserver.NewOptString(l.Name),
				IpAddress: ociserver.NewOptString(l.IP),
				Node:      ociserver.NewOptString(l.Node),
				Protocols: protocols,
			})
		}
		resp.SvmMetadata = ociserver.NewOptOCICreateSVMWorkflowMetadata(
			ociserver.OCICreateSVMWorkflowMetadata{
				Name:    ociserver.NewOptString(res.SvmMetadata.Name),
				SvmOCID: ociserver.NewOptString(res.SvmMetadata.SvmOCID),
				Lifs:    lifs,
			},
		)
	}
	return &ociserver.GetWorkflowStatusResponseHeaders{
		OpcRequestID: opcRequestID,
		Response:     resp,
	}, nil
}

func (h *Handler) nodeUUIDsByName(ctx context.Context, poolUUID string) map[string]string {
	if h.Orchestrator == nil || strings.TrimSpace(poolUUID) == "" {
		return nil
	}
	nodes, err := h.Orchestrator.GetNodesByPoolUUID(ctx, poolUUID)
	if err != nil {
		util.GetLogger(ctx).Warn("nodeUUID enrichment skipped: GetNodesByPoolUUID failed", "poolUUID", poolUUID, "error", err)
		return nil
	}
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		if n != nil && n.Name != "" {
			out[n.Name] = n.UUID
		}
	}
	return out
}

func mapGetWorkflowQueryError(opcRequestID string, err error) ociserver.GetWorkflowRes {
	code := statusFromError(err)
	if code == http.StatusNotFound {
		return &ociserver.GetWorkflowNotFound{
			OpcRequestID: opcRequestID,
			Response: ociserver.GetWorkflowErrorResponse{
				ErrorMessage: err.Error(),
			},
		}
	}
	return &ociserver.GetWorkflowInternalServerError{
		OpcRequestID: opcRequestID,
		Response: ociserver.GetWorkflowErrorResponse{
			ErrorMessage: err.Error(),
		},
	}
}

func (h *Handler) DeletePool(ctx context.Context, params ociserver.DeletePoolParams) (ociserver.DeletePoolRes, error) {
	logger := util.GetLogger(ctx)
	poolOCID := params.PoolOCID

	opcRequestID, err := opcRequestIDFromContext(ctx)
	if err != nil {
		return &ociserver.DeletePoolBadRequest{
			OpcRequestID: uuid.NewString(),
			Response: ociserver.PoolOperationErrorResponse{
				Status:       string(workflowquery.WorkflowStatusFailed),
				PoolOCID:     poolOCID,
				ErrorMessage: invalidOPCRequestID,
			},
		}, nil
	}

	// Map to DeletePoolParams - for OCI, AccountName is compartment OCID and PoolName is deployment name
	deletePoolParams := &commonparams.DeletePoolParams{
		AccountName: params.TenancyOcid, // account name (compartment OCID)
		PoolOCID:    poolOCID,           // path parameter
	}

	// Delete the pool
	deleted, workflowID, err := h.Orchestrator.DeletePool(ctx, deletePoolParams)
	if err != nil {
		logger.Error("Failed to delete pool", "error", err.Error())
		if utilserrors.IsNotFoundErr(err) {
			logger.Info("Pool not found", "poolOCID", deletePoolParams.PoolOCID)
			return &ociserver.DeletePoolNotFound{
				OpcRequestID: opcRequestID,
				Response: ociserver.PoolOperationErrorResponse{
					Status:       string(workflowquery.WorkflowStatusFailed),
					PoolOCID:     poolOCID,
					ErrorMessage: err.Error(),
				},
			}, nil
		}
		if utilserrors.IsBadRequestErr(err) {
			return &ociserver.DeletePoolBadRequest{
				OpcRequestID: opcRequestID,
				Response: ociserver.PoolOperationErrorResponse{
					Status:       string(workflowquery.WorkflowStatusFailed),
					PoolOCID:     poolOCID,
					ErrorMessage: err.Error(),
				},
			}, nil
		}
		if utilserrors.IsConflictErr(err) {
			return &ociserver.DeletePoolConflict{
				OpcRequestID: opcRequestID,
				Response: ociserver.PoolOperationErrorResponse{
					Status:       string(workflowquery.WorkflowStatusFailed),
					PoolOCID:     poolOCID,
					ErrorMessage: "Error deleting pool - Pool is already transitioning between states",
				},
			}, nil
		}
		logger.Error("Failed to delete pool", "error", err.Error())
		return &ociserver.DeletePoolInternalServerError{
			OpcRequestID: opcRequestID,
			Response: ociserver.PoolOperationErrorResponse{
				Status:       string(workflowquery.WorkflowStatusFailed),
				PoolOCID:     poolOCID,
				ErrorMessage: "Internal server error",
			},
		}, nil
	}
	if deleted.State == models.LifeCycleStateDeleting || deleted.State == models.LifeCycleStateCreating {
		return &ociserver.DeletePoolAcceptedResponseHeaders{
			OpcRequestID: opcRequestID,
			Response: ociserver.DeletePoolAcceptedResponse{
				Status:     string(workflowquery.WorkflowStatusInProgress),
				WorkflowId: workflowID,
				PoolOCID:   poolOCID,
			},
		}, nil
	}

	logger.Info("Pool deleted successfully", "poolOCID", deletePoolParams.PoolOCID)
	return &ociserver.DeletePoolNoContent{OpcRequestID: opcRequestID}, nil
}

// NewError maps handler errors to HTTP status and returns *ErrorStatusCode. Defaults to 500 when status cannot be determined.
func (*Handler) NewError(ctx context.Context, err error) *ociserver.ErrorStatusCode {
	if err == nil {
		return &ociserver.ErrorStatusCode{StatusCode: http.StatusInternalServerError, Response: ociserver.Error{Code: 500, Message: "internal error"}}
	}
	code := statusFromError(err)
	msg := err.Error()
	if msg == "" {
		msg = http.StatusText(code)
	}
	return &ociserver.ErrorStatusCode{
		StatusCode: code,
		Response:   ociserver.Error{Code: float64(code), Message: msg},
	}
}

// statusFromError maps known error types to HTTP status codes; returns 500 for unknown errors.
func statusFromError(err error) int {
	switch {
	case errors.Is(err, context.Canceled):
		return 499 // Client Closed Request (non-standard but common)
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout // 504
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound // 404
	case strings.Contains(msg, "unauthorized") || strings.Contains(msg, "invalid token"):
		return http.StatusUnauthorized // 401
	case strings.Contains(msg, "forbidden") || strings.Contains(msg, "access denied"):
		return http.StatusForbidden // 403
	case strings.Contains(msg, "conflict") || strings.Contains(msg, "already exists"):
		return http.StatusConflict // 409
	case strings.Contains(msg, "validation") || strings.Contains(msg, "bad request") || strings.Contains(msg, "invalid"):
		return http.StatusBadRequest // 400
	case strings.Contains(msg, "too many requests") || strings.Contains(msg, "rate limit"):
		return http.StatusTooManyRequests // 429
	case strings.Contains(msg, "unprocessable"):
		return http.StatusUnprocessableEntity // 422
	default:
		return http.StatusInternalServerError // 500
	}
}
