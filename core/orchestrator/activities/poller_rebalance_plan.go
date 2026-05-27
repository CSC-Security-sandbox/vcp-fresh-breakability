package activities

import (
	"sort"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

// HarvestRebalanceMove is one atomic drain: all NodeIDs move from SourceGroupID to TargetGroupID together.
type HarvestRebalanceMove struct {
	SourceGroupID   int64
	TargetGroupID   int64
	SourceLeaseName string
	NodeIDs         []int64
}

// buildHarvestRebalanceMoves produces a plan for one planning round. Each source drain is atomic:
// all pollers from that source go to one target chosen in fullest-first order among targets with
// len(nodes) > evictThreshold that can fit all source nodes and pass HA.
// It mutates nodesByGroup in place (simulated moves reassign nodes between groups); the caller
// must not rely on it staying unchanged. Tier-1 sources are derived from live membership in nodesByGroup.
// partnerNodeID maps each node id to its HA partner node id (0 if none). HA is enforced against the
// simulated layout: a batch may not move to a target that already contains any source node's partner.
func buildHarvestRebalanceMoves(nodeGroups []datamodel.NodeGroupPollerCount, nodesByGroup map[int64][]int64, partnerNodeID map[int64]int64, maxNodes, evictThreshold, softThreshold int, includeTier2 bool) []HarvestRebalanceMove {
	var moves []HarvestRebalanceMove
	leaseNameByGroup := make(map[int64]string)
	nodeGroupByNodeID := make(map[int64]int64)
	for _, ng := range nodeGroups {
		leaseNameByGroup[ng.NodeGroupID] = ng.LeaseName
	}
	for groupID, nodes := range nodesByGroup {
		for _, nodeID := range nodes {
			nodeGroupByNodeID[nodeID] = groupID
		}
	}

	sourceGroupIDs := availableTier1SourcesNotYetMoved(nodeGroups, nodesByGroup, evictThreshold)
	moves = append(moves, planTierMoves(nodeGroups, nodesByGroup, nodeGroupByNodeID, partnerNodeID, leaseNameByGroup, maxNodes, evictThreshold, sourceGroupIDs)...)

	if includeTier2 {
		tier2Sources := tier2SourceGroups(nodesByGroup, evictThreshold, softThreshold)
		moves = append(moves, planTierMoves(nodeGroups, nodesByGroup, nodeGroupByNodeID, partnerNodeID, leaseNameByGroup, maxNodes, evictThreshold, tier2Sources)...)
	}

	return moves
}

// availableTier1SourcesNotYetMoved returns node group IDs that qualify as tier-1 from live membership
// in nodesByGroup: 0 < len < evictThreshold. Use at planning start (initial layout) and after simulated moves;
// drained sources and groups that have crossed evictThreshold are omitted.
func availableTier1SourcesNotYetMoved(nodeGroups []datamodel.NodeGroupPollerCount, nodesByGroup map[int64][]int64, evictThreshold int) []int64 {
	var ids []int64
	for _, ng := range nodeGroups {
		live := len(nodesByGroup[ng.NodeGroupID])
		if live > 0 && live < evictThreshold {
			ids = append(ids, ng.NodeGroupID)
		}
	}
	sortGroupIDsByLoad(nodesByGroup, ids, true)
	return ids
}

// tier2SourceGroups returns groups in the mid band: evictThreshold <= len(nodes) < softThreshold.
// Uses current membership in nodesByGroup (after tier 1 when called from buildHarvestRebalanceMoves).
func tier2SourceGroups(nodesByGroup map[int64][]int64, evictThreshold, softThreshold int) []int64 {
	var sourceGroupIDs []int64
	for groupID, nodes := range nodesByGroup {
		c := int64(len(nodes))
		if c > 0 && int(c) >= evictThreshold && int(c) < softThreshold {
			sourceGroupIDs = append(sourceGroupIDs, groupID)
		}
	}
	sortGroupIDsByLoad(nodesByGroup, sourceGroupIDs, true)
	return sourceGroupIDs
}

func planTierMoves(nodeGroups []datamodel.NodeGroupPollerCount, nodesByGroup map[int64][]int64, nodeGroupByNodeID, partnerNodeID map[int64]int64, leaseNameByGroup map[int64]string, maxNodes, evictThreshold int, sourceGroupOrder []int64) []HarvestRebalanceMove {
	var moves []HarvestRebalanceMove
	for _, sourceGroupID := range sourceGroupOrder {
		sourceNodes := nodesByGroup[sourceGroupID]
		if len(sourceNodes) == 0 {
			continue
		}
		targetGroupID := pickAtomicOffloadTarget(nodeGroups, nodesByGroup, nodeGroupByNodeID, sourceGroupID, sourceNodes, partnerNodeID, maxNodes, evictThreshold)
		if targetGroupID == 0 {
			continue
		}
		nodeIDs := append([]int64(nil), sourceNodes...)
		moves = append(moves, HarvestRebalanceMove{
			SourceGroupID:   sourceGroupID,
			TargetGroupID:   targetGroupID,
			SourceLeaseName: leaseNameByGroup[sourceGroupID],
			NodeIDs:         nodeIDs,
		})
		nodesByGroup[targetGroupID] = append(nodesByGroup[targetGroupID], sourceNodes...)
		nodesByGroup[sourceGroupID] = nil
		for _, nodeID := range sourceNodes {
			nodeGroupByNodeID[nodeID] = targetGroupID
		}
	}
	return moves
}

// pickAtomicOffloadTarget chooses where to send an entire source batch: try warm targets in fullest-first order
// (len > evictThreshold, headroom, HA). If none qualify, fall back to the fullest *other* tier-1 lease that is
// still available (see availableTier1SourcesNotYetMoved) and can accept the batch under stagingPeerAccepts.
func pickAtomicOffloadTarget(nodeGroups []datamodel.NodeGroupPollerCount, nodesByGroup map[int64][]int64, nodeGroupByNodeID map[int64]int64, sourceGroupID int64, sourceNodeIDs []int64, partnerNodeID map[int64]int64, maxNodes, evictThreshold int) int64 {
	if len(sourceNodeIDs) == 0 {
		return 0
	}
	for _, targetID := range targetGroupsFullestFirst(nodesByGroup, sourceGroupID) {
		if targetAcceptsAtomicBatch(nodesByGroup, nodeGroupByNodeID, targetID, sourceNodeIDs, maxNodes, evictThreshold, partnerNodeID) {
			return targetID
		}
	}
	tier1Other := availableTier1SourcesNotYetMoved(nodeGroups, nodesByGroup, evictThreshold)
	var candidates []int64
	for _, gid := range tier1Other {
		if gid == sourceGroupID {
			continue
		}
		if stagingPeerAccepts(nodesByGroup, nodeGroupByNodeID, gid, sourceNodeIDs, maxNodes, partnerNodeID) {
			candidates = append(candidates, gid)
		}
	}
	if len(candidates) == 0 {
		return 0
	}
	sortGroupIDsByLoad(nodesByGroup, candidates, false)
	return candidates[0]
}

func stagingPeerAccepts(nodesByGroup map[int64][]int64, nodeGroupByNodeID map[int64]int64, targetGroupID int64, sourceNodeIDs []int64, maxNodes int, partnerNodeID map[int64]int64) bool {
	if len(nodesByGroup[targetGroupID]) == 0 {
		return false
	}
	if len(nodesByGroup[targetGroupID])+len(sourceNodeIDs) > maxNodes {
		return false
	}
	for _, nodeID := range sourceNodeIDs {
		partner := partnerNodeID[nodeID]
		if partner == 0 {
			continue
		}
		if nodeGroupByNodeID[partner] == targetGroupID {
			return false
		}
	}
	return true
}

// targetGroupsFullestFirst returns all groups except sourceGroupID, ordered by current membership size descending,
// then by group ID ascending when sizes match.
func targetGroupsFullestFirst(nodesByGroup map[int64][]int64, sourceGroupID int64) []int64 {
	var ids []int64
	for gid := range nodesByGroup {
		if gid == sourceGroupID {
			continue
		}
		ids = append(ids, gid)
	}
	sortGroupIDsByLoad(nodesByGroup, ids, false)
	return ids
}

// sortGroupIDsByLoad sorts groupIDs by len(nodesByGroup[id]). ascending: lightest groups first; false: fullest first.
// Equal lengths: lower group ID first.
func sortGroupIDsByLoad(nodesByGroup map[int64][]int64, groupIDs []int64, ascending bool) {
	sort.Slice(groupIDs, func(i, j int) bool {
		ni, nj := len(nodesByGroup[groupIDs[i]]), len(nodesByGroup[groupIDs[j]])
		if ni != nj {
			if ascending {
				return ni < nj
			}
			return ni > nj
		}
		return groupIDs[i] < groupIDs[j]
	})
}

// targetAcceptsAtomicBatch is true when the target has more than evictThreshold nodes, room for the batch,
// and no source node's HA partner is already on this target in the simulated layout.
func targetAcceptsAtomicBatch(nodesByGroup map[int64][]int64, nodeGroupByNodeID map[int64]int64, targetGroupID int64, sourceNodeIDs []int64, maxNodes, evictThreshold int, partnerNodeID map[int64]int64) bool {
	if len(nodesByGroup[targetGroupID]) <= evictThreshold {
		return false
	}
	batchSize := len(sourceNodeIDs)
	if len(nodesByGroup[targetGroupID])+batchSize > maxNodes {
		return false
	}
	for _, nodeID := range sourceNodeIDs {
		partner := partnerNodeID[nodeID]
		if partner == 0 {
			continue
		}
		if nodeGroupByNodeID[partner] == targetGroupID {
			return false
		}
	}
	return true
}
