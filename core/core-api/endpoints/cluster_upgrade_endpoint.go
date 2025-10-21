package api

import (
	"context"
	"fmt"
	"strings"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// V1UpgradeCluster handles the POST /v1/clusters/{clusterId}/upgrade endpoint
func (h *Handler) V1UpgradeCluster(ctx context.Context, req *oasgenserver.ClusterUpgradeRequestV1, params oasgenserver.V1UpgradeClusterParams) (oasgenserver.V1UpgradeClusterRes, error) {
	log := util.GetLogger(ctx)
	log.Infof("Received cluster upgrade request: clusterId=%s", params.ClusterId)

	// Convert API request to orchestrator parameters
	orchestratorParams := &commonparams.UpgradeClusterParams{
		ClusterID:          params.ClusterId,
		VSABuildImage:      req.VsaBuildImage.Value,
		MediatorBuildImage: req.MediatorBuildImage.Value,
		ForceUpgrade:       req.ForceUpgrade.Value,
		Metadata:           req.Metadata.Value,
	}

	// Call orchestrator to upgrade cluster
	response, jobID, err := h.Orchestrator.UpgradeCluster(ctx, orchestratorParams)
	if err != nil {
		log.Errorf("Failed to upgrade cluster: clusterId=%s error=%v", params.ClusterId, err)
		// Handle different error types and return appropriate API error responses
		if errors.IsNotFoundErr(err) {
			return &oasgenserver.V1UpgradeClusterNotFound{
				Message: fmt.Sprintf("Cluster not found: %s", params.ClusterId),
				Code:    404,
			}, nil
		}
		if strings.Contains(err.Error(), "bad request") || strings.Contains(err.Error(), "invalid request") {
			return &oasgenserver.V1UpgradeClusterBadRequest{
				Message: fmt.Sprintf("Invalid request: %v", err),
				Code:    400,
			}, nil
		}
		if strings.Contains(err.Error(), "conflict") {
			return &oasgenserver.V1UpgradeClusterConflict{
				Message: err.Error(),
				Code:    409,
			}, nil
		}
		if strings.Contains(err.Error(), "forbidden") {
			return &oasgenserver.V1UpgradeClusterForbidden{
				Message: err.Error(),
				Code:    403,
			}, nil
		}
		if strings.Contains(err.Error(), "unavailable") || strings.Contains(err.Error(), "Failed to retrieve cluster information") {
			return &oasgenserver.V1UpgradeClusterInternalServerError{
				Message: fmt.Sprintf("Service temporarily unavailable: %v", err),
				Code:    503,
			}, nil
		}

		// Default to internal server error
		return &oasgenserver.V1UpgradeClusterInternalServerError{
			Message: fmt.Sprintf("Upgrade operation failed: %v", err),
			Code:    500,
		}, nil
	}

	// Convert response to API format
	apiResponse := &oasgenserver.ClusterUpgradeResponseV1{
		ClusterId: response.ClusterID,
		Status:    oasgenserver.ClusterUpgradeResponseV1Status(response.Status),
		JobId:     response.JobID,
		CreatedAt: response.CreatedAt,
		UpdatedAt: response.UpdatedAt,
	}

	log.Info("Cluster upgrade initiated successfully", "clusterId", params.ClusterId, "jobId", jobID)
	return apiResponse, nil
}

// V1GetClusterUpgradeStatus handles the GET /v1/clusters/upgrade/{jobId} endpoint
func (h *Handler) V1GetClusterUpgradeStatus(ctx context.Context, params oasgenserver.V1GetClusterUpgradeStatusParams) (oasgenserver.V1GetClusterUpgradeStatusRes, error) {
	log := util.GetLogger(ctx)
	log.Info("Received cluster upgrade status request", "jobId", params.JobId)

	// Call orchestrator to get upgrade status
	progress, err := h.Orchestrator.GetClusterUpgradeStatus(ctx, params.JobId)
	if err != nil {
		log.Error("Failed to get cluster upgrade status", "jobId", params.JobId, "error", err)

		// Handle different error types and return appropriate API error responses
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "record not found") {
			return &oasgenserver.V1GetClusterUpgradeStatusNotFound{
				Message: fmt.Sprintf("Upgrade job not found: %s", params.JobId),
				Code:    404,
			}, nil
		}
		if strings.Contains(err.Error(), "bad request") || strings.Contains(err.Error(), "invalid request") {
			return &oasgenserver.V1GetClusterUpgradeStatusBadRequest{
				Message: fmt.Sprintf("Invalid request: %v", err),
				Code:    400,
			}, nil
		}

		// Default to internal server error
		return &oasgenserver.V1GetClusterUpgradeStatusInternalServerError{
			Message: fmt.Sprintf("Failed to get upgrade status: %v", err),
			Code:    500,
		}, nil
	}

	// Convert response to API format
	apiResponse := &oasgenserver.UpgradeProgressV1{
		JobId:    progress.JobID,
		Status:   oasgenserver.UpgradeProgressV1Status(progress.Status),
		Clusters: convertClusterStatusesToAPI(progress.Clusters),
		Errors:   convertUpgradeErrorsToAPI(progress.Errors),
		Warnings: progress.Warnings,
	}

	log.Info("Cluster upgrade status retrieved successfully", "jobId", params.JobId, "status", progress.Status)
	return apiResponse, nil
}

// convertClusterStatusesToAPI converts cluster statuses to API format
func convertClusterStatusesToAPI(clusters []models.ClusterUpgradeStatus) []oasgenserver.ClusterUpgradeStatusV1 {
	if clusters == nil {
		return nil
	}

	apiClusters := make([]oasgenserver.ClusterUpgradeStatusV1, len(clusters))
	for i, cluster := range clusters {
		apiClusters[i] = oasgenserver.ClusterUpgradeStatusV1{
			ClusterId:   cluster.ClusterID,
			Status:      oasgenserver.ClusterUpgradeStatusV1Status(cluster.Status),
			StartTime:   utils.ConvertTimeToOptDateTime(cluster.StartTime),
			EndTime:     utils.ConvertTimeToOptDateTime(cluster.EndTime),
			CurrentStep: utils.ConvertStringToOptString(cluster.CurrentStep),
		}
	}
	return apiClusters
}

// convertUpgradeErrorsToAPI converts upgrade errors to API format
func convertUpgradeErrorsToAPI(errors []models.UpgradeError) []oasgenserver.UpgradeErrorV1 {
	if errors == nil {
		return nil
	}

	apiErrors := make([]oasgenserver.UpgradeErrorV1, len(errors))
	for i, err := range errors {
		apiErrors[i] = oasgenserver.UpgradeErrorV1{
			Code:      err.Code,
			Message:   err.Message,
			Type:      err.Type,
			Retryable: err.Retryable,
			ClusterId: utils.ConvertStringToOptString(err.ClusterID),
		}
	}
	return apiErrors
}

// V1ListAvailableVersions handles the GET /v1/clusters/versions endpoint
func (h *Handler) V1ListAvailableVersions(ctx context.Context, params oasgenserver.V1ListAvailableVersionsParams) (oasgenserver.V1ListAvailableVersionsRes, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Listing available ONTAP versions")

	// Call orchestrator to get available versions
	response, err := h.Orchestrator.ListAvailableVersions(ctx)
	if err != nil {
		logger.Error("Failed to list available versions", "error", err)

		// Convert error to appropriate API error response
		return &oasgenserver.V1ListAvailableVersionsInternalServerError{
			Code:    500,
			Message: "Failed to retrieve available versions",
		}, nil
	}

	// Convert response to API format
	var versions []oasgenserver.AvailableVersionV1
	for _, version := range response.Versions {
		versions = append(versions, oasgenserver.AvailableVersionV1{
			OntapVersion: version.OntapVersion,
			VsaImagePath: version.VSAImagePath,
			VsaName:      version.VSAName,
			MediatorName: version.MediatorName,
			IsCurrent:    version.IsCurrent,
			IsActive:     version.IsActive,
		})
	}

	apiResponse := &oasgenserver.ListAvailableVersionsResponseV1{
		Versions: versions,
		Current:  response.Current,
	}

	logger.Info("Successfully listed available versions", "count", len(versions), "current", response.Current)
	return apiResponse, nil
}
