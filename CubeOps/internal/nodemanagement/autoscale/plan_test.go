// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package autoscale

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 11, 10, 3, 0, 0, 0, time.UTC)

// serving builds a 16-core / 32 GiB node using usedMilli CPU and the same
// amount of MB of memory.
func serving(name string, usedMilli int64, mvm int64) PoolNode {
	return PoolNode{Name: name, State: StateServing, HasPlaceholder: true, PlaceholderName: "cube-capacity-" + name,
		InKubernetes: true, InCube: true, CubeHealthy: true,
		QuotaMilliCPU: 16000, QuotaMemoryMB: 32000, UsedMilliCPU: usedMilli, UsedMemoryMB: usedMilli, MvmNum: mvm}
}

func bound(nodes ...PoolNode) []Placeholder {
	var out []Placeholder
	for _, n := range nodes {
		if n.HasPlaceholder {
			out = append(out, Placeholder{Name: n.PlaceholderName, NodeName: n.Name, CreatedAt: t0.Add(-time.Hour)})
		}
	}
	return out
}

// sizing: keep room for 2 sandboxes of 4 CPU / 8 GiB, fill to 75%.
func sizing() Sizing {
	return Sizing{MinNodes: 1, MaxNodes: 5, TargetUtilization: 0.75, HeadroomSandboxes: 2, SandboxMilliCPU: 4000, SandboxMemoryMB: 8192}
}

func kinds(p Plan) []ActionKind {
	var out []ActionKind
	for _, a := range p.Actions {
		out = append(out, a.Kind)
	}
	return out
}

func TestSlotsUseTheSchedulersStrictFit(t *testing.T) {
	assert.Equal(t, 1, slots(8000, 4000), "8 free: one 4-core fits, then 4 is not > 4")
	assert.Equal(t, 2, slots(9000, 4000))
	assert.Equal(t, 0, slots(4000, 4000))
	assert.Equal(t, 0, slots(-1, 4000))
}

func TestComputeDemandCountsFragmentation(t *testing.T) {
	// 3 cores free on each of two nodes: 6 cores in total, but no 4-core
	// sandbox fits anywhere -> scale out.
	nodes := []PoolNode{serving("a", 13000, 3), serving("b", 13000, 3)}
	d := ComputeDemand(nodes, sizing())
	assert.Equal(t, 0, d.FreeSlots)
	assert.Equal(t, 3, d.DesiredNodes)
}

func TestComputeDemandScalesOutOnUtilization(t *testing.T) {
	// Plenty of slots on paper, but 80% CPU fill exceeds the 75% target.
	nodes := []PoolNode{serving("a", 12800, 6), serving("b", 12800, 6)}
	s := sizing()
	s.SandboxMilliCPU, s.SandboxMemoryMB = 500, 500
	d := ComputeDemand(nodes, s)
	assert.Greater(t, d.FreeSlots, s.HeadroomSandboxes)
	assert.Equal(t, 3, d.DesiredNodes, "25600m / (16000m*0.75) = 2.13 -> 3")
}

func TestComputeDemandClampsToMax(t *testing.T) {
	nodes := []PoolNode{serving("a", 16000, 8), serving("b", 16000, 8)}
	s := sizing()
	s.MaxNodes = 3
	assert.Equal(t, 3, ComputeDemand(nodes, s).DesiredNodes)
}

func TestComputeDemandRetiresOnlyWhenTheRestKeepsHeadroom(t *testing.T) {
	nodes := []PoolNode{serving("a", 2000, 1), serving("b", 0, 0), serving("c", 3000, 2)}
	d := ComputeDemand(nodes, sizing())
	assert.Equal(t, 2, d.DesiredNodes)
	assert.Equal(t, "b", d.ScaleInVictim, "fewest MicroVMs")

	// Two nodes, each with room for exactly one more: removing either leaves
	// one slot, below the headroom of two.
	nodes = []PoolNode{serving("a", 8000, 2), serving("b", 8000, 2)}
	d = ComputeDemand(nodes, sizing())
	assert.Equal(t, 2, d.DesiredNodes)
	assert.Contains(t, d.ScaleInBlocked, "headroom")
}

func TestComputeDemandSkipsBlockedVictims(t *testing.T) {
	b := serving("b", 0, 0)
	b.Blocked = "only READY replica of template t1"
	nodes := []PoolNode{serving("a", 1000, 1), b, serving("c", 1000, 1)}
	d := ComputeDemand(nodes, sizing())
	assert.Equal(t, "a", d.ScaleInVictim)
}

func TestDecideAdoptsEveryExistingNodeBeforeScalingIn(t *testing.T) {
	nodes := []PoolNode{serving("a", 0, 0), serving("b", 0, 0), serving("c", 0, 0)}
	for i := range nodes {
		nodes[i].HasPlaceholder, nodes[i].PlaceholderName = false, ""
	}
	p := Decide(Input{Now: t0, Nodes: nodes, Sizing: sizing(), ScaleInCooldown: time.Minute})
	require.Equal(t, []ActionKind{ActCreatePlaceholders}, kinds(p))
	assert.Equal(t, 3, p.Actions[0].Count)
}

func TestDecideRequestsCapacityWhenHeadroomIsGone(t *testing.T) {
	nodes := []PoolNode{serving("a", 13000, 6), serving("b", 13000, 6)}
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing()})
	require.Equal(t, []ActionKind{ActCreatePlaceholders}, kinds(p))
	assert.Equal(t, 1, p.Actions[0].Count)
}

func TestDecideCountsWarmingNodesAsCapacityOnTheWay(t *testing.T) {
	w := PoolNode{Name: "c", State: StateWarming, InKubernetes: true, InCube: true, CubeHealthy: true,
		HasPlaceholder: true, PlaceholderName: "cube-capacity-c", MissingTemplates: []string{"t1", "t2"}}
	nodes := []PoolNode{serving("a", 13000, 6), serving("b", 13000, 6), w}
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing()})
	require.Equal(t, []ActionKind{ActWarmTemplates}, kinds(p), "no second node while c warms")
	assert.Equal(t, "c", p.Actions[0].Node)
	assert.Equal(t, []string{"t1", "t2"}, p.Actions[0].Templates)
}

func TestDecideFillsGapsOnEstablishedNodesWithoutHoldingThem(t *testing.T) {
	a := serving("a", 0, 0)
	a.FillTemplates, a.MissingTemplates = true, []string{"t9"}
	nodes := []PoolNode{a}
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing()})
	require.Equal(t, []ActionKind{ActWarmTemplates}, kinds(p))
	assert.Equal(t, 1, p.Demand.ServingNodes)
}

func TestDecideWithdrawsNewestPendingRequestWhenDemandFalls(t *testing.T) {
	nodes := []PoolNode{serving("a", 1000, 1), serving("b", 1000, 1)}
	phs := append(bound(nodes...),
		Placeholder{Name: "cube-capacity-old", CreatedAt: t0.Add(-10 * time.Minute)},
		Placeholder{Name: "cube-capacity-new", CreatedAt: t0.Add(-time.Minute)})
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: phs, Sizing: sizing()})
	require.Equal(t, []ActionKind{ActDeletePending, ActDeletePending}, kinds(p))
	assert.Equal(t, "cube-capacity-new", p.Actions[0].Pod)
}

func TestDecideScaleInWaitsForCooldownThenIsolatesVictim(t *testing.T) {
	nodes := []PoolNode{serving("a", 2000, 2), serving("b", 1000, 1), serving("c", 3000, 3)}
	in := Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing(), ScaleInCooldown: 15 * time.Minute}

	p := Decide(in)
	assert.Empty(t, p.Actions, "first observation only starts the cooldown")
	assert.Equal(t, t0, p.BelowSince)

	in.Now, in.BelowSince = t0.Add(10*time.Minute), p.BelowSince
	assert.Empty(t, Decide(in).Actions)

	in.Now = t0.Add(16 * time.Minute)
	p = Decide(in)
	require.Equal(t, []ActionKind{ActIsolate}, kinds(p))
	assert.Equal(t, "b", p.Actions[0].Node)
}

func TestDecideCooldownResetsWhenDemandReturns(t *testing.T) {
	nodes := []PoolNode{serving("a", 11000, 6), serving("b", 11000, 6)}
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing(),
		BelowSince: t0.Add(-time.Hour), ScaleInCooldown: time.Minute})
	assert.Empty(t, p.Actions)
	assert.True(t, p.BelowSince.IsZero())
}

func TestDecideNeverScalesBelowMinimum(t *testing.T) {
	nodes := []PoolNode{serving("a", 0, 0), serving("b", 0, 0)}
	s := sizing()
	s.MinNodes = 2
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: s,
		BelowSince: t0.Add(-time.Hour), ScaleInCooldown: time.Minute})
	assert.Empty(t, p.Actions)
}

func draining(name string, isolatedAgo time.Duration) PoolNode {
	n := serving(name, 500, 1)
	n.State, n.IsolatedSince, n.OwnedIsolation, n.CubeIsolated = StateDraining, t0.Add(-isolatedAgo), true, true
	return n
}

func TestDecideWaitsDrainGraceBeforeCheckingEmptiness(t *testing.T) {
	dr := draining("b", 30*time.Second)
	nodes := []PoolNode{serving("a", 500, 1), dr, serving("c", 0, 0)}
	in := Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing(),
		DrainGrace: 2 * time.Minute, DrainTimeout: time.Hour}
	assert.Empty(t, Decide(in).Actions, "in-flight creates may still land during the grace period")

	in.Nodes[1] = draining("b", 3*time.Minute)
	p := Decide(in)
	require.Equal(t, []ActionKind{ActCheckDrained}, kinds(p), "no second isolation while one drains")
	assert.Equal(t, "cube-capacity-b", p.Actions[0].Pod)
}

func TestDecideReassertsLostIsolationAndDoesNotReleaseMeanwhile(t *testing.T) {
	dr := draining("b", 10*time.Minute)
	dr.CubeIsolated, dr.IsolationLost = false, true
	nodes := []PoolNode{serving("a", 500, 1), dr, serving("c", 0, 0)}
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing(),
		DrainGrace: 2 * time.Minute, DrainTimeout: time.Hour})
	require.Equal(t, []ActionKind{ActReassertIsolation}, kinds(p))
}

func TestDecideCancelsDrainOnTimeoutOrRisingDemand(t *testing.T) {
	dr := draining("b", 2*time.Hour)
	nodes := []PoolNode{serving("a", 4000, 2), dr}
	p := Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing(), DrainTimeout: time.Hour})
	require.Equal(t, []ActionKind{ActCancelDrain}, kinds(p))

	dr = draining("b", time.Minute)
	nodes = []PoolNode{serving("a", 13000, 6), dr} // a alone lacks headroom
	p = Decide(Input{Now: t0, Nodes: nodes, Placeholders: bound(nodes...), Sizing: sizing(), DrainTimeout: time.Hour})
	require.Contains(t, kinds(p), ActCancelDrain)
	for _, a := range p.Actions {
		if a.Kind == ActCancelDrain {
			assert.Contains(t, a.Reason, "demand rose")
		}
	}
}

func TestDecideReleasedNodeGetsNoPlaceholderAndIsCleanedUpOnceGone(t *testing.T) {
	a := serving("a", 0, 0)
	rel := PoolNode{Name: "b", State: StateReleased, InKubernetes: true, InCube: true, CubeHealthy: true, OwnedIsolation: true, CubeIsolated: true}
	p := Decide(Input{Now: t0, Nodes: []PoolNode{a, rel}, Placeholders: bound(a), Sizing: sizing()})
	assert.Empty(t, p.Actions, "released node is not re-pinned; machine still present")

	rel.InKubernetes, rel.CubeHealthy = false, false
	p = Decide(Input{Now: t0, Nodes: []PoolNode{a, rel}, Placeholders: bound(a), Sizing: sizing()})
	require.Equal(t, []ActionKind{ActDeleteCubeNode}, kinds(p))
	assert.Equal(t, "b", p.Actions[0].Node)
}

func TestDecideKeepsOperatorIsolatedNodePinned(t *testing.T) {
	a := serving("a", 0, 0)
	f := PoolNode{Name: "b", State: StateForeign, InKubernetes: true, InCube: true, CubeHealthy: true}
	p := Decide(Input{Now: t0, Nodes: []PoolNode{a, f}, Placeholders: bound(a), Sizing: sizing(),
		BelowSince: t0.Add(-time.Hour), ScaleInCooldown: time.Minute})
	require.Equal(t, []ActionKind{ActCreatePlaceholders}, kinds(p))
	assert.Equal(t, 1, p.Actions[0].Count)
}
