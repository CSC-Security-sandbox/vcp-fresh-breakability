package oci

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

const (
	// minNodesForSVM is the minimum number of cluster nodes required to create an SVM (needed for HA LIF placement).
	minNodesForSVM = 2
	// svmNameMaxLength is the ONTAP maximum length for an SVM name.
	svmNameMaxLength = 47
	// svmNameMinLength is the minimum length for an SVM name.
	svmNameMinLength = 1
)

// SvmNameRegex: alphanumeric, hyphen, underscore only (ONTAP-compatible).
var svmNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// validateCreateSvmParams checks that all required OCI-specific input fields are present.
func validateCreateSvmParams(params *commonparams.CreateSvmParams) error {
	if params.PoolOCID == "" {
		return customerrors.NewBadRequestErr("PoolOCID is required")
	}
	if params.SvmExternalIdentifier == "" {
		return customerrors.NewBadRequestErr("SvmOCID is required")
	}
	if params.Name == "" {
		return customerrors.NewBadRequestErr("Name is required")
	}
	if strings.TrimSpace(params.AccountName) == "" {
		return customerrors.NewBadRequestErr("Tenancy-Ocid is required")
	}
	return nil
}

// validateDeleteSvmParams checks that all required input fields for SVM deletion are present.
func validateDeleteSvmParams(params *commonparams.DeleteSvmParams) error {
	if params.SvmID == "" {
		return customerrors.NewBadRequestErr("svmOCID is required")
	}
	if params.AccountName == "" {
		return customerrors.NewBadRequestErr("Tenancy-Ocid is required")
	}
	if strings.TrimSpace(params.PoolOCID) == "" {
		return customerrors.NewBadRequestErr("PoolOCID is required")
	}
	return nil
}

// validateCreateSvm runs all pre-create checks: required params, cluster state/capacity, SVM name (convention + uniqueness),
// and IP requirements (data LIFs per node based on protocols).
func validateCreateSvm(ctx context.Context, se database.Storage, params *commonparams.CreateSvmParams, pool *datamodel.Pool) error {
	if err := validateCreateSvmClusterStateAndCapacity(ctx, se, pool); err != nil {
		return err
	}
	if err := validateSvmName(params.Name); err != nil {
		return err
	}
	if err := validateSvmNameUniqueness(ctx, se, params.Name, pool.ID); err != nil {
		return err
	}
	return validateCreateSvmIPRequirements(ctx, se, params, pool)
}

// validateCreateSvmClusterStateAndCapacity ensures the cluster (pool) is in a valid state and has capacity for a new SVM.
func validateCreateSvmClusterStateAndCapacity(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
	if pool.State != string(models.LifeCycleStateREADY) {
		return customerrors.NewConflictErr("pool is not available for SVM creation")
	}
	if pool.VLMConfig == "" {
		return customerrors.NewUserInputValidationErr("pool does not have cluster config; ensure pool is available")
	}

	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}
	if len(nodes) < minNodesForSVM {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("cluster must have at least %d nodes to create an SVM", minNodesForSVM))
	}
	for _, node := range nodes {
		if node.State != models.LifeCycleStateREADY && node.State != models.LifeCycleStateAvailable {
			return customerrors.NewUserInputValidationErr(fmt.Sprintf("node %s is not ready (state: %s)", node.Name, node.State))
		}
	}
	return nil
}

// validateSvmName checks naming convention: length and allowed characters.
func validateSvmName(name string) error {
	if name == "" {
		return customerrors.NewUserInputValidationErr("SVM name is required")
	}
	if len(name) < svmNameMinLength {
		return customerrors.NewUserInputValidationErr("SVM name is too short")
	}
	if len(name) > svmNameMaxLength {
		return customerrors.NewUserInputValidationErr(fmt.Sprintf("SVM name must be at most %d characters", svmNameMaxLength))
	}
	if !svmNameRegex.MatchString(name) {
		return customerrors.NewUserInputValidationErr("SVM name must contain only letters, numbers, hyphens, and underscores")
	}
	return nil
}

// validateSvmNameUniqueness checks that no SVM with the same name exists in the pool (DB).
func validateSvmNameUniqueness(ctx context.Context, se database.Storage, name string, poolID int64) error {
	existing, err := se.GetSvmByNameAndPoolID(ctx, name, poolID)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			return nil // no existing SVM with this name
		}
		return err
	}
	if existing != nil {
		return customerrors.NewConflictErr("SVM with this name already exists in the cluster")
	}
	return nil
}

// requiredDataLifCount returns the number of data LIFs (IPs) needed for the new SVM based on protocols and node count.
// 1 data LIF per node for SAN (iSCSI); 1 per node for NAS (NFS). So e.g. 2 nodes + iSCSI+NFS = 4 LIFs.
func requiredDataLifCount(enableIscsi, enableNfs bool, nodeCount int) int {
	if nodeCount <= 0 {
		return 0
	}
	lifsPerNode := 0
	if enableIscsi {
		lifsPerNode++
	}
	if enableNfs {
		lifsPerNode++
	}
	if lifsPerNode == 0 {
		lifsPerNode = 1 // default at least one data path (e.g. iSCSI)
	}
	return lifsPerNode * nodeCount
}

// validateCreateSvmIPRequirements checks that if Ips are provided, the count matches required data LIFs.
func validateCreateSvmIPRequirements(ctx context.Context, se database.Storage, params *commonparams.CreateSvmParams, pool *datamodel.Pool) error {
	nodes, err := se.GetNodesByPoolID(ctx, pool.ID)
	if err != nil {
		return err
	}
	required := requiredDataLifCount(params.EnableIscsi, params.EnableNfs, len(nodes))
	if len(params.Ips) == 0 {
		return nil // no IPs provided; allocation may happen later or elsewhere
	}
	if len(params.Ips) != required {
		return customerrors.NewUserInputValidationErr(
			fmt.Sprintf("Ips count must be %d (data LIFs for %d nodes with selected protocols), got %d",
				required, len(nodes), len(params.Ips)))
	}
	return nil
}
