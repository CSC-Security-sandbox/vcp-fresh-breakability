package activities

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

func TestBuildHarvestRebalanceMoves_Tier1FillFirst(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 10},
		{NodeGroupID: 2, LeaseName: "l2", Count: 25},
		{NodeGroupID: 3, LeaseName: "l3", Count: 25},
	}
	nodesByGroup := map[int64][]int64{
		1: {100, 101},
		2: makeNodeIDs(200, 25),
		3: makeNodeIDs(300, 25),
	}
	partner := map[int64]int64{}
	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)
	require.Len(t, moves, 1)
	// Both nodes in one batch: fullest tie, then lowest group ID (2 before 3).
	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(2), moves[0].TargetGroupID)
	require.Equal(t, []int64{100, 101}, moves[0].NodeIDs)
}

func TestBuildHarvestRebalanceMoves_AtomicOffloadNoTargetFitsAllSourceNodes(t *testing.T) {
	// Source tier-1 with 30 nodes; targets at 180/180; max 200 → no single target has 30 headroom.
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 30},
		{NodeGroupID: 2, LeaseName: "l2", Count: 180},
		{NodeGroupID: 3, LeaseName: "l3", Count: 180},
	}
	nodes1 := make([]int64, 30)
	for i := range nodes1 {
		nodes1[i] = int64(1000 + i)
	}
	nodes2 := make([]int64, 180)
	for i := range nodes2 {
		nodes2[i] = int64(2000 + i)
	}
	nodes3 := make([]int64, 180)
	for i := range nodes3 {
		nodes3[i] = int64(3000 + i)
	}
	partner := map[int64]int64{}
	nodesByGroup := map[int64][]int64{
		1: nodes1,
		2: nodes2,
		3: nodes3,
	}
	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)
	require.Empty(t, moves)
}

func TestBuildHarvestRebalanceMoves_AtomicOffloadSkipsFullestWhenHABlocks(t *testing.T) {
	// Group 2 is fullest (150 nodes) but HA forbids it for both source nodes; group 3 fits all and is allowed.
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 2},
		{NodeGroupID: 2, LeaseName: "l2", Count: 150},
		{NodeGroupID: 3, LeaseName: "l3", Count: 100},
	}
	nodesByGroup := map[int64][]int64{
		1: {100, 101},
		2: make([]int64, 150),
		3: make([]int64, 100),
	}
	for i := range nodesByGroup[2] {
		nodesByGroup[2][i] = int64(2000 + i)
	}
	for i := range nodesByGroup[3] {
		nodesByGroup[3][i] = int64(3000 + i)
	}
	// Partners live in group 2 (HA cannot share the offload target with those nodes).
	partner := map[int64]int64{
		100: 2000,
		101: 2001,
	}
	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)
	require.Len(t, moves, 1)
	require.Equal(t, int64(3), moves[0].TargetGroupID)
	require.Equal(t, []int64{100, 101}, moves[0].NodeIDs)
}

// TestBuildHarvestRebalanceMoves_MultipleSourcesSingleTarget tests multiple tier-1 sources
// all draining to the same target (the fullest one that can fit them).
func TestBuildHarvestRebalanceMoves_MultipleSourcesSingleTarget(t *testing.T) {
	// Groups 1, 2, 3 are tier-1 sources (count < evictThreshold=20), group 4 is the target.
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 5},
		{NodeGroupID: 2, LeaseName: "l2", Count: 8},
		{NodeGroupID: 3, LeaseName: "l3", Count: 3},
		{NodeGroupID: 4, LeaseName: "l4", Count: 100},
	}
	nodesByGroup := map[int64][]int64{
		1: {100, 101, 102, 103, 104},                // 5 nodes
		2: {200, 201, 202, 203, 204, 205, 206, 207}, // 8 nodes
		3: {300, 301, 302},                          // 3 nodes
		4: makeNodeIDs(4000, 100),                   // 100 nodes (target)
	}
	partner := map[int64]int64{} // no HA constraints

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// All 3 sources should move to group 4 (the only non-tier-1 group).
	// Order: sources processed by lightest first (3 nodes, then 5 nodes, then 8 nodes).
	require.Len(t, moves, 3)

	// First move: group 3 (3 nodes) -> group 4
	require.Equal(t, int64(3), moves[0].SourceGroupID)
	require.Equal(t, int64(4), moves[0].TargetGroupID)
	require.Len(t, moves[0].NodeIDs, 3)

	// Second move: group 1 (5 nodes) -> group 4
	require.Equal(t, int64(1), moves[1].SourceGroupID)
	require.Equal(t, int64(4), moves[1].TargetGroupID)
	require.Len(t, moves[1].NodeIDs, 5)

	// Third move: group 2 (8 nodes) -> group 4
	require.Equal(t, int64(2), moves[2].SourceGroupID)
	require.Equal(t, int64(4), moves[2].TargetGroupID)
	require.Len(t, moves[2].NodeIDs, 8)
}

// TestBuildHarvestRebalanceMoves_MultipleSourcesMultipleTargets tests sources distributing
// across multiple targets when capacity constraints apply.
func TestBuildHarvestRebalanceMoves_MultipleSourcesMultipleTargets(t *testing.T) {
	// Two tier-1 sources, two targets. Target 4 can only fit one source due to capacity.
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 10},
		{NodeGroupID: 2, LeaseName: "l2", Count: 15},
		{NodeGroupID: 3, LeaseName: "l3", Count: 180}, // target, near capacity
		{NodeGroupID: 4, LeaseName: "l4", Count: 190}, // target, very near capacity
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 10),   // 10 nodes
		2: makeNodeIDs(200, 15),   // 15 nodes
		3: makeNodeIDs(3000, 180), // 180 nodes
		4: makeNodeIDs(4000, 190), // 190 nodes
	}
	partner := map[int64]int64{}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// Source order: lightest first (group 1 with 10, then group 2 with 15).
	// Target order: fullest first (group 4 at 190, then group 3 at 180).
	// Group 4 has headroom for 10 nodes (200-190=10), so group 1 fits.
	// After group 1 moves, group 4 has 200 (full). Group 2 (15 nodes) must go to group 3 (headroom 20).
	require.Len(t, moves, 2)

	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(4), moves[0].TargetGroupID)
	require.Len(t, moves[0].NodeIDs, 10)

	require.Equal(t, int64(2), moves[1].SourceGroupID)
	require.Equal(t, int64(3), moves[1].TargetGroupID)
	require.Len(t, moves[1].NodeIDs, 15)
}

// TestBuildHarvestRebalanceMoves_HAConstraintsDistributeAcrossTargets tests that HA siblings
// prevent certain moves, causing sources to use different targets.
func TestBuildHarvestRebalanceMoves_HAConstraintsDistributeAcrossTargets(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 4},   // source
		{NodeGroupID: 2, LeaseName: "l2", Count: 6},   // source
		{NodeGroupID: 3, LeaseName: "l3", Count: 100}, // target
		{NodeGroupID: 4, LeaseName: "l4", Count: 120}, // target (fullest)
	}
	nodesByGroup := map[int64][]int64{
		1: {100, 101, 102, 103},
		2: {200, 201, 202, 203, 204, 205},
		3: makeNodeIDs(3000, 100),
		4: makeNodeIDs(4000, 120),
	}
	// Partners sit on the opposite target: g1↔g4, g2↔g3 (cannot share that target with the partner).
	partner := map[int64]int64{
		100: 4000, 101: 4001, 102: 4002, 103: 4003,
		4000: 100, 4001: 101, 4002: 102, 4003: 103,
		200: 3000, 201: 3001, 202: 3002, 203: 3003, 204: 3004, 205: 3005,
		3000: 200, 3001: 201, 3002: 202, 3003: 203, 3004: 204, 3005: 205,
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	require.Len(t, moves, 2)

	// Group 1 (4 nodes) can't go to group 4 (HA blocked), goes to group 3.
	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(3), moves[0].TargetGroupID)

	// Group 2 (6 nodes) can't go to group 3 (HA blocked), goes to group 4.
	require.Equal(t, int64(2), moves[1].SourceGroupID)
	require.Equal(t, int64(4), moves[1].TargetGroupID)
}

// TestBuildHarvestRebalanceMoves_Tier2IncludedWithMultipleSources tests tier-2 processing
// with multiple sources from both tiers.
func TestBuildHarvestRebalanceMoves_Tier2IncludedWithMultipleSources(t *testing.T) {
	// evictThreshold=20, softThreshold=50
	// Tier-1: count < 20 (groups 1, 2)
	// Tier-2: 20 <= count < 50 (groups 3, 4)
	// Targets: count >= 50 (group 5)
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 5},   // tier-1
		{NodeGroupID: 2, LeaseName: "l2", Count: 10},  // tier-1
		{NodeGroupID: 3, LeaseName: "l3", Count: 25},  // tier-2
		{NodeGroupID: 4, LeaseName: "l4", Count: 40},  // tier-2
		{NodeGroupID: 5, LeaseName: "l5", Count: 100}, // target
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 5),
		2: makeNodeIDs(200, 10),
		3: makeNodeIDs(300, 25),
		4: makeNodeIDs(400, 40),
		5: makeNodeIDs(5000, 100),
	}
	partner := map[int64]int64{}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, true)

	// Tier-1 moves first (groups 1, 2), then tier-2 (groups 3, 4).
	// All should go to group 5 since it has plenty of capacity.
	require.Len(t, moves, 4)

	// Tier-1: lightest first
	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(5), moves[0].TargetGroupID)

	require.Equal(t, int64(2), moves[1].SourceGroupID)
	require.Equal(t, int64(5), moves[1].TargetGroupID)

	// Tier-2: lightest first
	require.Equal(t, int64(3), moves[2].SourceGroupID)
	require.Equal(t, int64(5), moves[2].TargetGroupID)

	require.Equal(t, int64(4), moves[3].SourceGroupID)
	require.Equal(t, int64(5), moves[3].TargetGroupID)
}

// TestBuildHarvestRebalanceMoves_PartialMovesWhenCapacityLimited tests tier-1 sources draining in order
// into a single target that stays above evictThreshold and has enough headroom for each atomic batch.
func TestBuildHarvestRebalanceMoves_PartialMovesWhenCapacityLimited(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 10},
		{NodeGroupID: 2, LeaseName: "l2", Count: 15},
		{NodeGroupID: 3, LeaseName: "l3", Count: 12},
		{NodeGroupID: 4, LeaseName: "l4", Count: 150}, // > evictThreshold, 50 slots under maxNodes=200
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 10),
		2: makeNodeIDs(200, 15),
		3: makeNodeIDs(300, 12),
		4: makeNodeIDs(4000, 150),
	}
	partner := map[int64]int64{}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// Tier-1 source order by load: 10, 12, 15 → groups 1, 3, 2. All drain to group 4.
	require.Len(t, moves, 3)
	require.Equal(t, int64(4), moves[0].TargetGroupID)
	require.Equal(t, int64(4), moves[1].TargetGroupID)
	require.Equal(t, int64(4), moves[2].TargetGroupID)
}

// TestBuildHarvestRebalanceMoves_NoSourcesAllAboveThreshold tests when all groups are above
// eviction threshold - no moves needed.
func TestBuildHarvestRebalanceMoves_NoSourcesAllAboveThreshold(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 50},
		{NodeGroupID: 2, LeaseName: "l2", Count: 75},
		{NodeGroupID: 3, LeaseName: "l3", Count: 100},
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 50),
		2: makeNodeIDs(200, 75),
		3: makeNodeIDs(300, 100),
	}
	partner := map[int64]int64{}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// All groups have count >= evictThreshold (20), so no tier-1 sources.
	require.Empty(t, moves)
}

// TestBuildHarvestRebalanceMoves_MixedHAConstraintsSomeBlocked tests a mix where some
// nodes in a source are HA-blocked from the fullest target but others aren't.
// Since moves are atomic (all-or-nothing), the entire source must find a valid target.
func TestBuildHarvestRebalanceMoves_MixedHAConstraintsSomeBlocked(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 4},
		{NodeGroupID: 2, LeaseName: "l2", Count: 150},
		{NodeGroupID: 3, LeaseName: "l3", Count: 100},
	}
	nodesByGroup := map[int64][]int64{
		1: {100, 101, 102, 103},
		2: makeNodeIDs(2000, 150),
		3: makeNodeIDs(3000, 100),
	}
	// Only node 100 has its partner in group 2; atomic batch cannot use that target.
	partner := map[int64]int64{
		100: 2000,
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	require.Len(t, moves, 1)
	// Must go to group 3 since group 2 is blocked by node 100's HA constraint.
	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(3), moves[0].TargetGroupID)
	require.Equal(t, []int64{100, 101, 102, 103}, moves[0].NodeIDs)
}

// TestBuildHarvestRebalanceMoves_FiveGroupsComplexScenario tests a complex scenario with
// 5 groups, mixed tiers, HA constraints, and capacity limits.
func TestBuildHarvestRebalanceMoves_FiveGroupsComplexScenario(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 8},   // tier-1, can move
		{NodeGroupID: 2, LeaseName: "l2", Count: 12},  // tier-1, can move
		{NodeGroupID: 3, LeaseName: "l3", Count: 20},  // tier-2 (fits on g4 after tier-1; emptied tier-1 cannot be targets)
		{NodeGroupID: 4, LeaseName: "l4", Count: 170}, // target
		{NodeGroupID: 5, LeaseName: "l5", Count: 185}, // target (fullest)
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 8),
		2: makeNodeIDs(200, 12),
		3: makeNodeIDs(300, 20),
		4: makeNodeIDs(4000, 170),
		5: makeNodeIDs(5000, 185),
	}
	// Group 1 nodes pair with distinct nodes in group 5 (cannot offload onto g5).
	partner := map[int64]int64{}
	for i, id := range nodesByGroup[1] {
		p := nodesByGroup[5][i]
		partner[id], partner[p] = p, id
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, true)

	// Tier-1: group 1 (8) → 4 (HA blocks 5). Tier-1: group 2 (12) → 5 (fullest with headroom).
	// Tier-2: group 3 (20) → 4 (170+8=178, +20=198 ≤ 200; g5 too full for 20 nodes).
	require.Len(t, moves, 3)

	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(4), moves[0].TargetGroupID) // HA blocks group 5

	require.Equal(t, int64(2), moves[1].SourceGroupID)
	require.Equal(t, int64(5), moves[1].TargetGroupID)

	require.Equal(t, int64(3), moves[2].SourceGroupID)
	require.Equal(t, int64(4), moves[2].TargetGroupID)
}

// TestBuildHarvestRebalanceMoves_MultipleHAPairsWithFourLeases tests a more complex scenario
// with 4 tier-1 leases forming HA pair relationships and 2 target leases.
func TestBuildHarvestRebalanceMoves_MultipleHAPairsWithFourLeases(t *testing.T) {
	// Groups 1-4 are tier-1 sources. HA pairs: 1↔3, 2↔4.
	// Groups 5-6 are targets.
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 5},
		{NodeGroupID: 2, LeaseName: "l2", Count: 5},
		{NodeGroupID: 3, LeaseName: "l3", Count: 5},
		{NodeGroupID: 4, LeaseName: "l4", Count: 5},
		{NodeGroupID: 5, LeaseName: "l5", Count: 150}, // target
		{NodeGroupID: 6, LeaseName: "l6", Count: 150}, // target
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 5),
		2: makeNodeIDs(200, 5),
		3: makeNodeIDs(300, 5),
		4: makeNodeIDs(400, 5),
		5: makeNodeIDs(5000, 150),
		6: makeNodeIDs(6000, 150),
	}
	// HA pairs: g1[i]↔g3[i], g2[i]↔g4[i]. After g1 and g2 drain to g5, partners on g3/g4 cannot use g5.
	partner := map[int64]int64{}
	for i := 0; i < 5; i++ {
		a, b := int64(100+i), int64(300+i)
		partner[a], partner[b] = b, a
		c, d := int64(200+i), int64(400+i)
		partner[c], partner[d] = d, c
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	require.Len(t, moves, 4)

	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(5), moves[0].TargetGroupID)
	require.Len(t, moves[0].NodeIDs, 5)

	require.Equal(t, int64(2), moves[1].SourceGroupID)
	require.Equal(t, int64(5), moves[1].TargetGroupID)
	require.Len(t, moves[1].NodeIDs, 5)

	require.Equal(t, int64(3), moves[2].SourceGroupID)
	require.Equal(t, int64(6), moves[2].TargetGroupID)
	require.Len(t, moves[2].NodeIDs, 5)

	require.Equal(t, int64(4), moves[3].SourceGroupID)
	require.Equal(t, int64(6), moves[3].TargetGroupID)
	require.Len(t, moves[3].NodeIDs, 5)
}

// TestBuildHarvestRebalanceMoves_FourSourcesTwoNearCapacityTargets tests tier-1 sources
// distributing across two targets that are both near capacity (195/200 each).
// Each target can only accept 5 more nodes, fitting exactly 1 source of 5 nodes each.
// HA pairs: g1[i]↔g2[i], g3[i]↔g4[i]. After g1 and g3 drain to g5/g6, partners g2/g4 cannot use those targets.
func TestBuildHarvestRebalanceMoves_FourSourcesTwoNearCapacityTargets(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 5},
		{NodeGroupID: 2, LeaseName: "l2", Count: 5},
		{NodeGroupID: 3, LeaseName: "l3", Count: 5},
		{NodeGroupID: 4, LeaseName: "l4", Count: 5},
		{NodeGroupID: 5, LeaseName: "l5", Count: 195}, // target
		{NodeGroupID: 6, LeaseName: "l6", Count: 195}, // target
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(100, 5),
		2: makeNodeIDs(200, 5),
		3: makeNodeIDs(300, 5),
		4: makeNodeIDs(400, 5),
		5: makeNodeIDs(5000, 195),
		6: makeNodeIDs(6000, 195),
	}
	// HA pairs: g1[i]↔g2[i], g3[i]↔g4[i]. After g1 and g3 drain to g5/g6, partners g2/g4 cannot use those targets.
	partner := map[int64]int64{}
	for i := 0; i < 5; i++ {
		a, b := int64(100+i), int64(200+i)
		partner[a], partner[b] = b, a
		c, d := int64(300+i), int64(400+i)
		partner[c], partner[d] = d, c
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// All 4 sources have the same count (5), so processed by lowest group ID: 1, 2, 3, 4.
	// Targets are fullest-first; both at 195, so group 5 (lower ID) is tried first.
	// Group 5 headroom = 5, fits group 1 (5 nodes).
	// After group 1 moves, group 5 is full (200).
	// Group 2 cannot use g5 (HA blocked by g1 nodes already there), goes to g6.
	// After group 2 moves to g6, group 6 is full (200).
	// Group 3 cannot move: g5 is full, g6 is full → no move.
	// Group 4 cannot move: g5 is full, g6 is full → no move.
	require.Len(t, moves, 2)

	// Group 1 -> Group 5
	require.Equal(t, int64(1), moves[0].SourceGroupID)
	require.Equal(t, int64(5), moves[0].TargetGroupID)
	require.Len(t, moves[0].NodeIDs, 5)

	// Group 2 -> Group 6 (Group 5 is HA blocked by g1)
	require.Equal(t, int64(2), moves[1].SourceGroupID)
	require.Equal(t, int64(6), moves[1].TargetGroupID)
	require.Len(t, moves[1].NodeIDs, 5)
}

// TestBuildHarvestRebalanceMoves_FullTargetsNoMoves tests when warm targets g1,g2 are full: pickAtomicOffloadTarget
// falls back via availableTier1SourcesNotYetMoved to the fullest other tier-1 lease (g3→g5; g4→g6 after g5 is no longer tier-1).
// l1,l2 at 200 (full), l3,l4 at 17 nodes (tier-1), l5,l6 at 19 nodes (tier-1).
// HA pairs: g3[i]↔g4[i]. After g3 drains to g5, partner g4 cannot use g5.
func TestBuildHarvestRebalanceMoves_FullTargetsNoMoves(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 200}, // target (full)
		{NodeGroupID: 2, LeaseName: "l2", Count: 200}, // target (full)
		{NodeGroupID: 3, LeaseName: "l3", Count: 17},  // tier-1
		{NodeGroupID: 4, LeaseName: "l4", Count: 17},  // tier-1
		{NodeGroupID: 5, LeaseName: "l5", Count: 19},  // tier-1
		{NodeGroupID: 6, LeaseName: "l6", Count: 19},  // tier-1
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(1000, 200),
		2: makeNodeIDs(2000, 200),
		3: makeNodeIDs(300, 17),
		4: makeNodeIDs(400, 17),
		5: makeNodeIDs(500, 19),
		6: makeNodeIDs(600, 19),
	}
	// HA pairs: g3[i]↔g4[i]. After g3 drains to g5, partner g4 cannot use g5.
	partner := map[int64]int64{}
	for i := 0; i < 17; i++ {
		a, b := int64(300+i), int64(400+i)
		partner[a], partner[b] = b, a
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// Tier-1 sources: g3, g4, g5, g6 (all < evictThreshold=20). Processed lightest first: g3, g4 (17 each), then g5, g6 (19 each).
	// Targets (count > evictThreshold): g1, g2 are full (200). No headroom.
	// Fallback to tier-1 targets: g5, g6 (fullest tier-1 = 19 nodes each).
	// g3 (17) → g5 (fullest tier-1, becomes 36 nodes, now > evictThreshold).
	// g4 (17) cannot use g5 (HA blocked by g3 nodes) → g6 (becomes 36 nodes).
	// g5 and g6 are now > evictThreshold, so they don't move.
	require.Len(t, moves, 2)
	require.Equal(t, int64(3), moves[0].SourceGroupID)
	require.Equal(t, int64(5), moves[0].TargetGroupID)
	require.Equal(t, int64(4), moves[1].SourceGroupID)
	require.Equal(t, int64(6), moves[1].TargetGroupID)
}

// TestBuildHarvestRebalanceMoves_EightGroupsWithSiblings tests a more complex scenario with 8 groups:
// l1,l2 at 200 (full), l3,l4 at 17 nodes (tier-1, siblings), l5,l6 at 19 nodes (tier-1), l7,l8 at 10 nodes (tier-1, siblings).
// HA pairs: g3[i]↔g4[i], g7[i]↔g8[i].
func TestBuildHarvestRebalanceMoves_EightGroupsWithSiblings(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 200}, // target (full)
		{NodeGroupID: 2, LeaseName: "l2", Count: 200}, // target (full)
		{NodeGroupID: 3, LeaseName: "l3", Count: 17},  // tier-1
		{NodeGroupID: 4, LeaseName: "l4", Count: 17},  // tier-1
		{NodeGroupID: 5, LeaseName: "l5", Count: 19},  // tier-1
		{NodeGroupID: 6, LeaseName: "l6", Count: 19},  // tier-1
		{NodeGroupID: 7, LeaseName: "l7", Count: 10},  // tier-1
		{NodeGroupID: 8, LeaseName: "l8", Count: 10},  // tier-1
	}
	nodesByGroup := map[int64][]int64{
		1: makeNodeIDs(1000, 200),
		2: makeNodeIDs(2000, 200),
		3: makeNodeIDs(300, 17),
		4: makeNodeIDs(400, 17),
		5: makeNodeIDs(500, 19),
		6: makeNodeIDs(600, 19),
		7: makeNodeIDs(700, 10),
		8: makeNodeIDs(800, 10),
	}
	// HA pairs: g3[i]↔g4[i], g7[i]↔g8[i].
	partner := map[int64]int64{}
	for i := 0; i < 17; i++ {
		a, b := int64(300+i), int64(400+i)
		partner[a], partner[b] = b, a
	}
	for i := 0; i < 10; i++ {
		a, b := int64(700+i), int64(800+i)
		partner[a], partner[b] = b, a
	}

	moves := buildHarvestRebalanceMoves(nodeGroups, nodesByGroup, partner, 200, 20, 50, false)

	// Tier-1 sources (all < 20): g3,g4 (17), g5,g6 (19), g7,g8 (10).
	// Processed lightest first: g7 (10), g8 (10), g3 (17), g4 (17), g5 (19), g6 (19).
	// Targets (count > evictThreshold): g1, g2 are full (200). No headroom.
	// Fallback to fullest tier-1 targets: g5, g6 (19 nodes each).
	// g7 (10) → g5 (fullest tier-1, becomes 29 nodes, now > evictThreshold).
	// g8 (10) cannot use g5 (HA blocked by g7 nodes) → g6 (becomes 29 nodes).
	// g3 (17) → g5 (now fullest at 29, has headroom 200-29=171 for 17 nodes, becomes 46).
	// g4 (17) cannot use g5 (HA blocked by g3 nodes) → g6 (becomes 46).
	// g5 and g6 are no longer tier-1 sources (now 46 nodes each), so they don't move.
	require.Len(t, moves, 4)

	// g7 (10 nodes) -> g5
	require.Equal(t, int64(7), moves[0].SourceGroupID)
	require.Equal(t, int64(5), moves[0].TargetGroupID)
	require.Len(t, moves[0].NodeIDs, 10)

	// g8 (10 nodes) -> g6 (HA blocked from g5)
	require.Equal(t, int64(8), moves[1].SourceGroupID)
	require.Equal(t, int64(6), moves[1].TargetGroupID)
	require.Len(t, moves[1].NodeIDs, 10)

	// g3 (17 nodes) -> g5
	require.Equal(t, int64(3), moves[2].SourceGroupID)
	require.Equal(t, int64(5), moves[2].TargetGroupID)
	require.Len(t, moves[2].NodeIDs, 17)

	// g4 (17 nodes) -> g6 (HA blocked from g5)
	require.Equal(t, int64(4), moves[3].SourceGroupID)
	require.Equal(t, int64(6), moves[3].TargetGroupID)
	require.Len(t, moves[3].NodeIDs, 17)
}

// Helper functions

func makeNodeIDs(startID, count int) []int64 {
	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(startID + i)
	}
	return ids
}

func TestPlanTierMoves_SkipsEmptySourceGroup(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 0},
		{NodeGroupID: 2, LeaseName: "l2", Count: 100},
	}
	nodesByGroup := map[int64][]int64{
		1: nil,
		2: makeNodeIDs(2000, 100),
	}
	partner := map[int64]int64{}
	leaseBy := map[int64]string{1: "l1", 2: "l2"}
	nodeBy := map[int64]int64{}
	moves := planTierMoves(nodeGroups, nodesByGroup, nodeBy, partner, leaseBy, 200, 20, []int64{1, 2})
	require.Empty(t, moves)
}

func TestPickAtomicOffloadTarget_EmptySource(t *testing.T) {
	id := pickAtomicOffloadTarget(nil, map[int64][]int64{1: {1}}, map[int64]int64{}, 1, nil, map[int64]int64{}, 200, 20)
	require.Zero(t, id)
}

func TestStagingPeerAccepts_Branches(t *testing.T) {
	nodes := map[int64][]int64{
		10: {1},
		20: {2, 3},
	}
	byNode := map[int64]int64{1: 10, 2: 20, 3: 20, 99: 20}
	partner := map[int64]int64{2: 99}

	require.False(t, stagingPeerAccepts(nodes, byNode, 99, []int64{5}, 200, partner))

	require.False(t, stagingPeerAccepts(nodes, byNode, 20, []int64{1, 1}, 3, partner))

	require.False(t, stagingPeerAccepts(nodes, byNode, 20, []int64{2}, 200, partner))

	require.True(t, stagingPeerAccepts(nodes, byNode, 20, []int64{3}, 200, partner))
}

func TestPickAtomicOffloadTarget_NoTier1FallbackCandidates(t *testing.T) {
	nodeGroups := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "l1", Count: 2},
		{NodeGroupID: 2, LeaseName: "l2", Count: 100},
	}
	nodesByGroup := map[int64][]int64{
		1: {10, 11},
		2: makeNodeIDs(2000, 100),
	}
	nodeBy := map[int64]int64{10: 1, 11: 1}
	for _, id := range nodesByGroup[2] {
		nodeBy[id] = 2
	}
	partner := map[int64]int64{}
	// maxNodes=100: warm target has 100 nodes, batch of 2 does not fit (100+2>100); no other tier-1 lease to fall back to.
	id := pickAtomicOffloadTarget(nodeGroups, nodesByGroup, nodeBy, 1, []int64{10, 11}, partner, 100, 20)
	require.Zero(t, id)
}
