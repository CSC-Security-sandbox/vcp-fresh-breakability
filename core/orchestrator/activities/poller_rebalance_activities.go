package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Poller rebalance verify defaults (read once at process init).
var (
	rebalancePollerVerifyMaxAttempts = env.GetInt("REBALANCE_POLLER_VERIFY_MAX_ATTEMPTS", 24)
	rebalancePollerVerifyIntervalSec = env.GetInt("REBALANCE_POLLER_VERIFY_INTERVAL_SEC", 5)
	// Harvest management HTTP (prometheus-targets lists active pollers as localhost:<poller-port> on the holder).
	harvestPollerVerifyManagementPort        = env.GetString("HARVEST_POLLER_VERIFY_MANAGEMENT_PORT", "3000")
	harvestPollerVerifyPrometheusTargetsPath = env.GetString("HARVEST_POLLER_VERIFY_PROMETHEUS_TARGETS_PATH", "/pollers/prometheus-targets")
	harvestPollerVerifyScheme                = env.GetString("HARVEST_POLLER_VERIFY_SCHEME", "http")
	harvestPollerLeaseNamespace              = env.GetString("LEASE_NAMESPACE", "vcp")
	harvestPollerVerifyPodNamespace          = env.GetString("HARVEST_POLLER_VERIFY_POD_NAMESPACE", harvestPollerLeaseNamespace)
	// Harvest DELETE /config/delete JSON body { "leaseName": "..." } removes the lease folder on the farm PVC.
	harvestDeleteLeaseConfigPath = env.GetString("HARVEST_DELETE_LEASE_CONFIG_PATH", "/config/delete")
)

// getPodIPForKubernetesLeaseHolder resolves the harvest-farm pod IP from the target k8s lease (overridable in tests).
var getPodIPForKubernetesLeaseHolder = utils.GetPodIPForKubernetesLeaseHolder

// deleteKubernetesLeaseForEmptyHarvestPollers removes coordination.k8s.io leases for drained node groups (overridable in tests).
var deleteKubernetesLeaseForEmptyHarvestPollers = utils.DeleteKubernetesLease

// harvestPrometheusTargetEntry matches JSON from GET .../pollers/prometheus-targets on the harvest management port.
type harvestPrometheusTargetEntry struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func prometheusTargetStringPort(target string) (port string, ok bool) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", false
	}
	_, port, err := net.SplitHostPort(target)
	if err == nil {
		return port, true
	}
	if i := strings.LastIndex(target, ":"); i > 0 && i < len(target)-1 {
		return target[i+1:], true
	}
	return "", false
}

func verifyStagedPortsInPrometheusTargetsJSON(body []byte, leaseName string, requiredPorts map[string]struct{}) error {
	var entries []harvestPrometheusTargetEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return fmt.Errorf("unmarshal prometheus-targets for lease %s: %w", leaseName, err)
	}
	seen := make(map[string]struct{})
	for _, e := range entries {
		for _, t := range e.Targets {
			if p, ok := prometheusTargetStringPort(t); ok && p != "" {
				seen[p] = struct{}{}
			}
		}
	}
	var missing []string
	for p := range requiredPorts {
		if _, ok := seen[p]; !ok {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("lease %s: poller port(s) not in prometheus-targets: %s", leaseName, strings.Join(missing, ","))
	}
	return nil
}

// deleteHarvestLeaseConfigFolder removes the Harvest PVC lease directory for an empty lease via the farm REST API.
// Harvest DELETE /config/delete (configurable via HARVEST_DELETE_LEASE_CONFIG_PATH) expects JSON body
// { "leaseName": "<lease>" }. Harvest returns: 200 when the empty folder was removed; 404 when the folder
// was already absent (treated as success here); 400 when leaseName is missing; 409 when the folder still
// has config files; 500 on filesystem errors.
// Per-poller files use DELETE /config/{leaseName}/{prefix}{nodeID}.
// harvestRestProtocol and harvestEndPoint come from sibling activity files in package activities.
func deleteHarvestLeaseConfigFolder(ctx context.Context, leaseName string) error {
	leaseName = strings.TrimSpace(leaseName)
	if leaseName == "" {
		return nil
	}
	body, err := json.Marshal(map[string]string{"leaseName": leaseName})
	if err != nil {
		return err
	}
	u := fmt.Sprintf("%s://%s%s", harvestRestProtocol, harvestEndPoint, harvestDeleteLeaseConfigPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	var respBody []byte
	if resp.Body != nil {
		respBody, _ = io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}
	respSnippet := strings.TrimSpace(string(respBody))
	if len(respSnippet) > 512 {
		respSnippet = respSnippet[:512] + "..."
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	case http.StatusBadRequest:
		return fmt.Errorf("delete harvest lease folder %s: bad request (missing leaseName?): %s", leaseName, respSnippet)
	case http.StatusConflict:
		return fmt.Errorf("delete harvest lease folder %s: folder not empty (HTTP 409); remove poller config files first: %s", leaseName, respSnippet)
	default:
		return fmt.Errorf("delete harvest lease folder %s: %s: %s", leaseName, resp.Status, respSnippet)
	}
}

// deleteRebalanceTargetHarvestPoller removes a poller YAML from Harvest for the given lease and node id
// (same REST path as unregister). Used for target rollback and for deleting the copy on the source lease after commit.
// harvestRestProtocol, harvestEndPoint, and leasePrefix come from sibling activity files in package activities.
func deleteRebalanceTargetHarvestPoller(ctx context.Context, leaseName string, nodeID int64) error {
	if leaseName == "" {
		return nil
	}
	url := fmt.Sprintf(harvestRestProtocol+"://"+harvestEndPoint+"/config/%s/%s%d", leaseName, leasePrefix, nodeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete harvest poller node %d lease %s: %s", nodeID, leaseName, resp.Status)
	}
	return nil
}

func rollbackTargetHarvestUploads(ctx context.Context, staged []RebalanceStagedNode) {
	logger := util.GetLogger(ctx)
	for i := len(staged) - 1; i >= 0; i-- {
		s := staged[i]
		if s.TargetLeaseName == "" {
			continue
		}
		if err := deleteRebalanceTargetHarvestPoller(ctx, s.TargetLeaseName, s.NodeID); err != nil {
			logger.Warnf("rollback: failed to delete target harvest poller node %d lease %s: %v", s.NodeID, s.TargetLeaseName, err)
		} else {
			logger.Infof("rollback: deleted target harvest poller node %d lease %s", s.NodeID, s.TargetLeaseName)
		}
	}
}

// PollerRebalanceActivities implements snapshot and plan for harvest poller rebalancing.
type PollerRebalanceActivities struct {
	SE database.Storage
}

// HarvestNodeGroupsSnapshotResult is the DB snapshot of all node groups and poller counts.
type HarvestNodeGroupsSnapshotResult struct {
	Groups []datamodel.NodeGroupPollerCount
}

// GetNodeGroupsWithPollerCountsActivity returns aggregate poller counts per node group (ordered by count ascending).
func (a *PollerRebalanceActivities) GetNodeGroupsWithPollerCountsActivity(ctx context.Context) (*HarvestNodeGroupsSnapshotResult, error) {
	activity.RecordHeartbeat(ctx, "GetNodeGroupsWithPollerCountsActivity")
	rows, err := a.SE.ListNodeGroupsWithPollerCounts(ctx)
	if err != nil {
		return nil, err
	}
	return &HarvestNodeGroupsSnapshotResult{Groups: rows}, nil
}

// EmptyHarvestLeaseCandidate is a node group with zero pollers but a non-empty Kubernetes/Harvest lease name.
type EmptyHarvestLeaseCandidate struct {
	NodeGroupID int64
	LeaseName   string
}

// ListEmptyHarvestLeasesForCleanupActivity returns node groups that still reference a lease after all pollers moved away.
func (a *PollerRebalanceActivities) ListEmptyHarvestLeasesForCleanupActivity(ctx context.Context) ([]EmptyHarvestLeaseCandidate, error) {
	activity.RecordHeartbeat(ctx, "ListEmptyHarvestLeasesForCleanupActivity")
	logger := util.GetLogger(ctx)
	rows, err := a.SE.ListNodeGroupsWithPollerCounts(ctx)
	if err != nil {
		return nil, err
	}
	var out []EmptyHarvestLeaseCandidate
	for _, row := range rows {
		if row.Count != 0 {
			continue
		}
		lease := strings.TrimSpace(row.LeaseName)
		if lease == "" {
			continue
		}
		out = append(out, EmptyHarvestLeaseCandidate{NodeGroupID: row.NodeGroupID, LeaseName: lease})
	}
	logger.Infof("ListEmptyHarvestLeasesForCleanupActivity: %d empty lease candidate(s)", len(out))
	return out, nil
}

// CleanupEmptyLeaseParams identifies one node group lease to remove from Harvest, Kubernetes, and the DB.
type CleanupEmptyLeaseParams struct {
	NodeGroupID int64
	LeaseName   string
}

// CleanupEmptyLeaseActivity removes the Harvest lease folder via DELETE /config/delete (JSON body leaseName), deletes the
// coordination.k8s.io lease, then soft-deletes the node_groups row when the group still has zero pollers.
func (a *PollerRebalanceActivities) CleanupEmptyLeaseActivity(ctx context.Context, params *CleanupEmptyLeaseParams) error {
	activity.RecordHeartbeat(ctx, "CleanupEmptyLeaseActivity")
	logger := util.GetLogger(ctx)
	if params == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("cleanup empty lease params required"))
	}
	if params.NodeGroupID == 0 || strings.TrimSpace(params.LeaseName) == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("node group id and lease name required"))
	}

	n, err := a.SE.GetNodeGroupMapNodeCount(ctx, params.NodeGroupID)
	if err != nil {
		return err
	}
	if n > 0 {
		logger.Infof("CleanupEmptyLeaseActivity: skip node group %d lease %s (%d poller(s) assigned)", params.NodeGroupID, params.LeaseName, n)
		return nil
	}

	// Pre-check under lock before external deletes so we don't remove lease artifacts for a group that is already repopulated.
	shouldProceedCleanup := false
	err = a.SE.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		g := tx.GORM().WithContext(ctx)
		var lockedGroup datamodel.NodeGroup
		if err := g.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND deleted_at IS NULL", params.NodeGroupID).
			First(&lockedGroup).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				logger.Warnf("CleanupEmptyLeaseActivity: node group %d already deleted, skip", params.NodeGroupID)
				return nil
			}
			return err
		}
		if strings.TrimSpace(lockedGroup.LeaseName) != params.LeaseName {
			logger.Infof("CleanupEmptyLeaseActivity: skip node group %d (lease drift before external cleanup: db %q params %q)", params.NodeGroupID, lockedGroup.LeaseName, params.LeaseName)
			return nil
		}
		var cnt int64
		if err := g.Model(&datamodel.NodeNodeGroupMap{}).
			Where("node_group_id = ? AND deleted_at IS NULL", params.NodeGroupID).
			Count(&cnt).Error; err != nil {
			return err
		}
		if cnt > 0 {
			logger.Infof("CleanupEmptyLeaseActivity: skip node group %d; %d poller(s) assigned before external cleanup", params.NodeGroupID, cnt)
			return nil
		}
		shouldProceedCleanup = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("precheck empty lease cleanup for node group %d: %w", params.NodeGroupID, err)
	}
	if !shouldProceedCleanup {
		return nil
	}

	if err := deleteHarvestLeaseConfigFolder(ctx, params.LeaseName); err != nil {
		return fmt.Errorf("delete harvest lease folder for lease %q: %w", params.LeaseName, err)
	}
	logger.Infof("CleanupEmptyLeaseActivity: removed harvest config folder for lease %s (node group %d)", params.LeaseName, params.NodeGroupID)

	leaseNS := strings.TrimSpace(harvestPollerLeaseNamespace)
	if leaseNS == "" {
		leaseNS = "vcp"
	}
	if err := deleteKubernetesLeaseForEmptyHarvestPollers(ctx, leaseNS, params.LeaseName); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			logger.Warnf("CleanupEmptyLeaseActivity: k8s lease %s/%s already gone: %v", leaseNS, params.LeaseName, err)
		} else {
			return fmt.Errorf("delete k8s lease %s/%s: %w", leaseNS, params.LeaseName, err)
		}
	} else {
		logger.Infof("CleanupEmptyLeaseActivity: deleted k8s lease %s/%s for node group %d", leaseNS, params.LeaseName, params.NodeGroupID)
	}

	// Lock source node_group row again before final count+delete to avoid races with concurrent assigners.
	err = a.SE.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		g := tx.GORM().WithContext(ctx)
		var lockedGroup datamodel.NodeGroup
		if err := g.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND deleted_at IS NULL", params.NodeGroupID).
			First(&lockedGroup).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				logger.Warnf("CleanupEmptyLeaseActivity: node group %d already deleted", params.NodeGroupID)
				return nil
			}
			return err
		}
		if strings.TrimSpace(lockedGroup.LeaseName) != params.LeaseName {
			logger.Infof("CleanupEmptyLeaseActivity: skip deleting node group %d (lease drift after lock: db %q params %q)", params.NodeGroupID, lockedGroup.LeaseName, params.LeaseName)
			return nil
		}
		var cnt int64
		if err := g.Model(&datamodel.NodeNodeGroupMap{}).
			Where("node_group_id = ? AND deleted_at IS NULL", params.NodeGroupID).
			Count(&cnt).Error; err != nil {
			return err
		}
		if cnt > 0 {
			logger.Infof("CleanupEmptyLeaseActivity: skip deleting node group %d; %d poller(s) assigned after external cleanup", params.NodeGroupID, cnt)
			return nil
		}
		if err := g.Where("id = ? AND deleted_at IS NULL", params.NodeGroupID).
			Delete(&datamodel.NodeGroup{}).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("soft-delete node group %d: %w", params.NodeGroupID, err)
	}
	logger.Infof("CleanupEmptyLeaseActivity: soft-deleted node group %d", params.NodeGroupID)
	return nil
}

// BuildPollerRebalancePlanParams configures tier thresholds for the greedy plan.
type BuildPollerRebalancePlanParams struct {
	NodeGroups       []datamodel.NodeGroupPollerCount
	MaxNodesPerGroup int
	EvictThreshold   int
	SoftThreshold    int
	IncludeTier2     bool
}

// HarvestRebalancePlanOutput is the planned atomic batches for one planning run.
type HarvestRebalancePlanOutput struct {
	Moves []HarvestRebalanceMove
}

// BuildPollerRebalancePlanActivity builds tiered fill-first moves from node group stats (HA-aware).
func (a *PollerRebalanceActivities) BuildPollerRebalancePlanActivity(ctx context.Context, params *BuildPollerRebalancePlanParams) (*HarvestRebalancePlanOutput, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "BuildPollerRebalancePlanActivity")
	if params == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("plan params required"))
	}
	nodeGroups := params.NodeGroups
	if len(nodeGroups) == 0 {
		return &HarvestRebalancePlanOutput{}, nil
	}

	nodesByGroup := make(map[int64][]int64)
	partnerOf := make(map[int64]int64)
	for _, nodeGroup := range nodeGroups {
		if nodeGroup.Count == 0 {
			nodesByGroup[nodeGroup.NodeGroupID] = nil
			continue
		}
		maps, err := a.SE.ListNodeNodeGroupMapsByNodeGroupID(ctx, nodeGroup.NodeGroupID)
		if err != nil {
			return nil, err
		}
		for _, m := range maps {
			nodesByGroup[nodeGroup.NodeGroupID] = append(nodesByGroup[nodeGroup.NodeGroupID], m.NodeID)
			if _, ok := partnerOf[m.NodeID]; !ok {
				pid, err := a.SE.GetHarvestHaSiblingNodeID(ctx, m.NodeID)
				if err != nil {
					return nil, err
				}
				partnerOf[m.NodeID] = pid
			}
		}
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partnerOf,
		params.MaxNodesPerGroup, params.EvictThreshold, params.SoftThreshold,
		params.IncludeTier2)
	logger.Infof("BuildPollerRebalancePlanActivity: %d atomic batches planned", len(moves))
	return &HarvestRebalancePlanOutput{Moves: moves}, nil
}

// RebalanceStagedNode records target port chosen at upload time; commit must use the same values.
// TargetLeaseName is used to DELETE uploaded YAML from the target lease on rollback.
// SourceLeaseName is the pre-move Harvest lease; after a successful DB commit we DELETE that YAML so the poller exists only on the target lease.
type RebalanceStagedNode struct {
	NodeID          int64
	SourceGroupID   int64
	TargetGroupID   int64
	SourceLeaseName string
	TargetLeaseName string
	Port            string
}

// RebalanceUploadStageResult is returned after YAML upload (before DB commit).
type RebalanceUploadStageResult struct {
	Staged []RebalanceStagedNode
}

// UploadRebalanceMovesParams uploads Harvest configs for planned moves while DB still shows source groups.
type UploadRebalanceMovesParams struct {
	Moves     []HarvestRebalanceMove
	UploadURL string
}

func cloneHarvestConfig(h *datamodel.HarvestConfig) (*datamodel.HarvestConfig, error) {
	if h == nil {
		return nil, errors.New("nil harvest config")
	}
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	var out datamodel.HarvestConfig
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UploadRebalanceMovesToHarvestActivity picks target ports, renders YAML for the target lease, and uploads to Harvest
// without updating node_node_group_map (persist=false on uploadHarvestNodeMapping). Returns staged rows for verify+commit.
// On any failure after at least one successful upload, target lease files uploaded in this run are removed (Harvest rollback).
func (a *PollerRebalanceActivities) UploadRebalanceMovesToHarvestActivity(ctx context.Context, params *UploadRebalanceMovesParams) (res *RebalanceUploadStageResult, err error) {
	logger := util.GetLogger(ctx)
	var staged []RebalanceStagedNode
	defer func() {
		if err != nil && len(staged) > 0 {
			logger.Warnf("UploadRebalanceMovesToHarvestActivity rolling back %d partial target upload(s): %v", len(staged), err)
			rollbackTargetHarvestUploads(ctx, staged)
		}
	}()

	activity.RecordHeartbeat(ctx, "UploadRebalanceMovesToHarvestActivity started")
	if params == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("upload params required"))
	}
	if params.UploadURL == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("upload URL required"))
	}
	if len(params.Moves) == 0 {
		return &RebalanceUploadStageResult{}, nil
	}

	db := a.SE.DB().WithContext(ctx)
	seen := make(map[int64]struct{})

	for mi, move := range params.Moves {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("upload rebalance move %d/%d source=%d target=%d", mi+1, len(params.Moves), move.SourceGroupID, move.TargetGroupID))
		targetGroup, err := a.SE.GetNodeGroup(ctx, move.TargetGroupID)
		if err != nil {
			return nil, err
		}
		if targetGroup == nil || targetGroup.LeaseName == "" {
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("target node group %d missing or has empty lease name", move.TargetGroupID),
				"HarvestRebalanceInvalidTargetGroup", nil)
		}

		sourceLeaseName := strings.TrimSpace(move.SourceLeaseName)
		if sourceLeaseName == "" {
			sourceGroup, err := a.SE.GetNodeGroup(ctx, move.SourceGroupID)
			if err != nil {
				return nil, err
			}
			if sourceGroup != nil {
				sourceLeaseName = strings.TrimSpace(sourceGroup.LeaseName)
			}
		}

		for _, nodeID := range move.NodeIDs {
			if _, dup := seen[nodeID]; dup {
				continue
			}
			seen[nodeID] = struct{}{}
			activity.RecordHeartbeat(ctx, fmt.Sprintf("upload rebalance poller node %d", nodeID))

			mapping, err := a.SE.GetActiveNodeNodeGroupMapByNodeID(ctx, nodeID, nil)
			if err != nil {
				return nil, err
			}
			if mapping.NodeGroupID != move.SourceGroupID {
				msg := fmt.Sprintf("rebalance upload: node %d on group %d, move expects source %d",
					nodeID, mapping.NodeGroupID, move.SourceGroupID)
				logger.Error(msg)
				return nil, temporal.NewNonRetryableApplicationError(msg, "HarvestRebalancePrepareSourceMismatch", nil)
			}
			if mapping.HarvestConfig == nil {
				return nil, temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("node %d has no harvest_config", nodeID), "HarvestRebalanceMissingHarvestConfig", nil)
			}

			port, err := database.GetFirstAvailablePort(db, move.TargetGroupID)
			if err != nil {
				return nil, err
			}

			hc, err := cloneHarvestConfig(mapping.HarvestConfig)
			if err != nil {
				return nil, err
			}
			hc.LEASE_NAME = targetGroup.LeaseName
			hc.PORT = port

			uploadMapping := &datamodel.NodeNodeGroupMap{
				NodeID:        mapping.NodeID,
				NodeGroupID:   move.TargetGroupID,
				NodeGroup:     targetGroup,
				HarvestConfig: hc,
			}

			node, err := a.SE.GetNodeByID(ctx, nodeID)
			if err != nil {
				return nil, err
			}
			pool, err := a.SE.GetPoolByID(ctx, node.PoolID)
			if err != nil {
				return nil, err
			}

			var credentials *vlm.OntapCredentials
			if !smHarvestAuthEnabled {
				credentials, err = fetchOnTapCredentials(ctx, pool)
				if err != nil {
					return nil, err
				}
				if credentials == nil {
					return nil, fmt.Errorf("failed to get credentials for pool %d", pool.ID)
				}
			}

			if err := uploadHarvestNodeMapping(ctx, a.SE, uploadMapping, params.UploadURL, pool, credentials, utils.RenderHarvestTemplate, false); err != nil {
				return nil, err
			}

			staged = append(staged, RebalanceStagedNode{
				NodeID:          nodeID,
				SourceGroupID:   move.SourceGroupID,
				TargetGroupID:   move.TargetGroupID,
				SourceLeaseName: sourceLeaseName,
				TargetLeaseName: targetGroup.LeaseName,
				Port:            port,
			})
		}
	}

	logger.Infof("UploadRebalanceMovesToHarvestActivity: staged %d poller upload(s)", len(staged))
	return &RebalanceUploadStageResult{Staged: staged}, nil
}

// RollbackRebalanceTargetHarvestParams removes poller YAML files from target leases after a failed verify/commit.
type RollbackRebalanceTargetHarvestParams struct {
	Staged []RebalanceStagedNode
}

// RollbackRebalanceTargetHarvestActivity deletes uploaded poller configs on target leases (compensation).
func (a *PollerRebalanceActivities) RollbackRebalanceTargetHarvestActivity(ctx context.Context, params *RollbackRebalanceTargetHarvestParams) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "RollbackRebalanceTargetHarvestActivity started")
	if params == nil || len(params.Staged) == 0 {
		return nil
	}
	rollbackTargetHarvestUploads(ctx, params.Staged)
	logger.Infof("RollbackRebalanceTargetHarvestActivity: processed rollback for %d staged node(s)", len(params.Staged))
	return nil
}

// VerifyRebalancePollersParams checks poller HTTP reachability after upload.
type VerifyRebalancePollersParams struct {
	Staged []RebalanceStagedNode
}

// VerifyRebalancePollersUpActivity checks that staged pollers appear in the lease holder's
// GET /pollers/prometheus-targets (management port, default 3000): one HTTP call per target lease instead
// of per-poller /metrics. The host is the coordination lease holder pod IP (see utils.GetPodIPForKubernetesLeaseHolder).
// On host/connectivity failures, the cached holder IP for that lease is cleared so the next attempt re-queries
// the lease holder. For "ports not visible yet" failures, keep the same host to avoid excess lease API calls.
func (a *PollerRebalanceActivities) VerifyRebalancePollersUpActivity(ctx context.Context, params *VerifyRebalancePollersParams) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "VerifyRebalancePollersUpActivity started")
	if params == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("verify params required"))
	}
	if len(params.Staged) == 0 {
		return nil
	}

	podNS := strings.TrimSpace(harvestPollerVerifyPodNamespace)
	if podNS == "" {
		podNS = harvestPollerLeaseNamespace
	}

	interval := time.Duration(rebalancePollerVerifyIntervalSec) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	maxAttempts := rebalancePollerVerifyMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 24
	}

	client := &http.Client{Timeout: 30 * time.Second}

	stagedByLease := make(map[string][]RebalanceStagedNode)
	for _, s := range params.Staged {
		if s.TargetLeaseName == "" {
			return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError,
				fmt.Errorf("verify: staged node %d has empty target lease name", s.NodeID))
		}
		p := strings.TrimSpace(s.Port)
		if p == "" {
			return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError,
				fmt.Errorf("verify: staged node %d has empty poller port", s.NodeID))
		}
		stagedByLease[s.TargetLeaseName] = append(stagedByLease[s.TargetLeaseName], s)
	}
	leaseNames := make([]string, 0, len(stagedByLease))
	for name := range stagedByLease {
		leaseNames = append(leaseNames, name)
	}
	sort.Strings(leaseNames)

	leaseHolderHostByLease := make(map[string]string)

	for _, leaseName := range leaseNames {
		batch := stagedByLease[leaseName]
		requiredPorts := make(map[string]struct{})
		for _, s := range batch {
			requiredPorts[strings.TrimSpace(s.Port)] = struct{}{}
		}

		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			activity.RecordHeartbeat(ctx, fmt.Sprintf("verify lease %s attempt %d/%d (%d poller(s))",
				leaseName, attempt, maxAttempts, len(batch)))

			host, ok := leaseHolderHostByLease[leaseName]
			if !ok {
				ip, err := getPodIPForKubernetesLeaseHolder(ctx, harvestPollerLeaseNamespace, leaseName, podNS)
				if err != nil {
					return fmt.Errorf("verify: resolve pod IP for lease %q: %w", leaseName, err)
				}
				leaseHolderHostByLease[leaseName] = ip
				host = ip
				logger.Infof("VerifyRebalancePollersUpActivity: lease %s holder pod IP %s", leaseName, ip)
			}

			mgmtHostPort := net.JoinHostPort(host, harvestPollerVerifyManagementPort)
			url := fmt.Sprintf("%s://%s%s", harvestPollerVerifyScheme, mgmtHostPort, harvestPollerVerifyPrometheusTargetsPath)

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return err
			}
			resp, err := client.Do(req)
			var body []byte
			if resp != nil && resp.Body != nil {
				body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
				_ = resp.Body.Close()
			}
			refreshLeaseHolderHost := false
			if err != nil {
				lastErr = err
				refreshLeaseHolderHost = true
			} else if resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if err := verifyStagedPortsInPrometheusTargetsJSON(body, leaseName, requiredPorts); err != nil {
					lastErr = err
				} else {
					logger.Infof("VerifyRebalancePollersUpActivity: lease %s ok (%s) ports %v", leaseName, url, keysSorted(requiredPorts))
					lastErr = nil
					break
				}
			} else if resp != nil {
				lastErr = fmt.Errorf("GET %s: status %s", url, resp.Status)
				refreshLeaseHolderHost = true
			} else {
				lastErr = fmt.Errorf("GET %s: empty response", url)
				refreshLeaseHolderHost = true
			}

			if lastErr != nil && refreshLeaseHolderHost {
				delete(leaseHolderHostByLease, leaseName)
			}

			if lastErr == nil {
				break
			}

			if attempt < maxAttempts {
				select {
				case <-time.After(interval):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		if lastErr != nil {
			msg := fmt.Sprintf("poller verify failed for lease %s after %d attempts: %v", leaseName, maxAttempts, lastErr)
			logger.Error(msg)
			return fmt.Errorf("%s", msg)
		}
	}

	logger.Infof("VerifyRebalancePollersUpActivity: all %d poller(s) verified across %d lease(s)", len(params.Staged), len(leaseNames))
	return nil
}

func keysSorted(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// CommitRebalanceMovesParams applies DB updates after successful upload and verify.
type CommitRebalanceMovesParams struct {
	Staged           []RebalanceStagedNode
	MaxNodesPerGroup int
}

// CommitRebalanceMovesInDBActivity repoints node_node_group_map to target groups using ports from the upload stage.
// All updates run in a single DB transaction: any failure rolls back the entire batch.
//
// Before applying updates, target node_groups are row-locked (FOR UPDATE) and capacity is revalidated against
// current DB state. If concurrent writers fill a target after planning, commit fails with retryable
// ApplicationError type HarvestRebalanceCapacityChanged so workflow retries with a fresh plan.
//
// After the transaction succeeds, poller YAML is removed from each node's source Harvest lease (HTTP, outside the DB tx).
// If a source delete fails after the DB commit, the activity returns an error and Temporal retries. The DB update path is
// idempotent for nodes already on the target with matching lease/port; source deletes are safe to repeat (Harvest DELETE
// tolerates missing files). Until retries succeed, Harvest may briefly retain YAML on both source and target for affected nodes.
//
// Rows already on the target with matching lease/port are skipped (idempotent) so the activity can retry after a partial source delete.
func (a *PollerRebalanceActivities) CommitRebalanceMovesInDBActivity(ctx context.Context, params *CommitRebalanceMovesParams) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "CommitRebalanceMovesInDBActivity started")
	if params == nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("commit params required"))
	}
	if len(params.Staged) == 0 {
		return nil
	}
	if params.MaxNodesPerGroup <= 0 {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, errors.New("max nodes per group must be > 0"))
	}

	err := a.SE.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		g := tx.GORM().WithContext(ctx)
		targetIDsSet := make(map[int64]struct{})
		for _, s := range params.Staged {
			targetIDsSet[s.TargetGroupID] = struct{}{}
		}
		targetGroupIDs := make([]int64, 0, len(targetIDsSet))
		for id := range targetIDsSet {
			targetGroupIDs = append(targetGroupIDs, id)
		}
		sort.Slice(targetGroupIDs, func(i, j int) bool { return targetGroupIDs[i] < targetGroupIDs[j] })

		var lockedTargets []datamodel.NodeGroup
		if err := g.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id IN ? AND deleted_at IS NULL", targetGroupIDs).
			Order("id ASC").
			Find(&lockedTargets).Error; err != nil {
			return err
		}
		targetGroupByID := make(map[int64]*datamodel.NodeGroup, len(lockedTargets))
		for i := range lockedTargets {
			target := &lockedTargets[i]
			if target.LeaseName == "" {
				return temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("target node group %d missing or has empty lease name", target.ID),
					"HarvestRebalanceInvalidTargetGroup", nil)
			}
			targetGroupByID[target.ID] = target
		}
		for _, id := range targetGroupIDs {
			if _, ok := targetGroupByID[id]; !ok {
				return temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("target node group %d missing or deleted", id),
					"HarvestRebalanceInvalidTargetGroup", nil)
			}
		}

		type groupCountRow struct {
			NodeGroupID int64 `gorm:"column:node_group_id"`
			Cnt         int64 `gorm:"column:cnt"`
		}
		var rows []groupCountRow
		if err := g.Model(&datamodel.NodeNodeGroupMap{}).
			Select("node_group_id, COUNT(*)::bigint AS cnt").
			Where("deleted_at IS NULL AND node_group_id IN ?", targetGroupIDs).
			Group("node_group_id").
			Scan(&rows).Error; err != nil {
			return err
		}
		currentByTarget := make(map[int64]int64, len(targetGroupIDs))
		for _, id := range targetGroupIDs {
			currentByTarget[id] = 0
		}
		for _, row := range rows {
			currentByTarget[row.NodeGroupID] = row.Cnt
		}

		type pendingUpdate struct {
			nodeID    int64
			port      string
			targetID  int64
			targetRef *datamodel.NodeGroup
			mapping   *datamodel.NodeNodeGroupMap
		}
		var updates []pendingUpdate
		incomingByTarget := make(map[int64]int64)
		seenNodeIDs := make(map[int64]struct{})

		for i, s := range params.Staged {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("commit rebalance node %d (%d/%d)", s.NodeID, i+1, len(params.Staged)))
			if _, dup := seenNodeIDs[s.NodeID]; dup {
				continue
			}
			seenNodeIDs[s.NodeID] = struct{}{}

			targetGroup := targetGroupByID[s.TargetGroupID]
			mapping, err := a.SE.GetActiveNodeNodeGroupMapByNodeID(ctx, s.NodeID, tx)
			if err != nil {
				return err
			}
			if mapping.NodeGroupID == s.TargetGroupID {
				if mapping.HarvestConfig != nil &&
					mapping.HarvestConfig.LEASE_NAME == targetGroup.LeaseName &&
					mapping.HarvestConfig.PORT == s.Port {
					continue
				}
				msg := fmt.Sprintf("rebalance commit: node %d already on target group %d but harvest_config does not match expected lease/port",
					s.NodeID, s.TargetGroupID)
				logger.Error(msg)
				return temporal.NewNonRetryableApplicationError(msg, "HarvestRebalanceCommitDrift", nil)
			}
			if mapping.NodeGroupID != s.SourceGroupID {
				msg := fmt.Sprintf("rebalance commit: node %d on group %d, expected source %d (drift or duplicate commit)",
					s.NodeID, mapping.NodeGroupID, s.SourceGroupID)
				logger.Error(msg)
				return temporal.NewNonRetryableApplicationError(msg, "HarvestRebalanceCommitSourceMismatch", nil)
			}
			if mapping.HarvestConfig == nil {
				return temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("node %d has no harvest_config", s.NodeID), "HarvestRebalanceMissingHarvestConfig", nil)
			}
			incomingByTarget[s.TargetGroupID]++
			updates = append(updates, pendingUpdate{
				nodeID:    s.NodeID,
				port:      s.Port,
				targetID:  s.TargetGroupID,
				targetRef: targetGroup,
				mapping:   mapping,
			})
		}

		for targetID, incoming := range incomingByTarget {
			current := currentByTarget[targetID]
			if current+incoming > int64(params.MaxNodesPerGroup) {
				msg := fmt.Sprintf("rebalance commit capacity changed: target group %d has %d assigned, incoming %d exceeds max %d",
					targetID, current, incoming, params.MaxNodesPerGroup)
				logger.Warnf(msg)
				return temporal.NewApplicationError(msg, "HarvestRebalanceCapacityChanged")
			}
		}

		for i, u := range updates {
			activity.RecordHeartbeat(ctx, fmt.Sprintf("commit rebalance DB update node %d (%d/%d)", u.nodeID, i+1, len(updates)))
			u.mapping.NodeGroupID = u.targetID
			u.mapping.NodeGroup = u.targetRef
			u.mapping.HarvestConfig.LEASE_NAME = u.targetRef.LeaseName
			u.mapping.HarvestConfig.PORT = u.port
			if err := g.Save(u.mapping).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	for i, s := range params.Staged {
		activity.RecordHeartbeat(ctx, fmt.Sprintf("delete source harvest poller node %d (%d/%d)", s.NodeID, i+1, len(params.Staged)))
		if s.SourceLeaseName == "" {
			logger.Warnf("CommitRebalanceMovesInDBActivity: skip source harvest delete for node %d (empty source lease name)", s.NodeID)
			continue
		}
		if s.SourceLeaseName == s.TargetLeaseName {
			continue
		}
		if err := deleteRebalanceTargetHarvestPoller(ctx, s.SourceLeaseName, s.NodeID); err != nil {
			return fmt.Errorf("delete source harvest poller after rebalance commit node %d lease %s: %w", s.NodeID, s.SourceLeaseName, err)
		}
		logger.Infof("CommitRebalanceMovesInDBActivity: removed poller YAML for node %d from source lease %s", s.NodeID, s.SourceLeaseName)
	}

	logger.Infof("CommitRebalanceMovesInDBActivity: committed %d node mapping(s)", len(params.Staged))
	return nil
}
