// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

// Package autoscale sizes the compute-node pool through the Kubernetes Cluster
// Autoscaler (issue #2193).
//
// CubeOps decides how many nodes to keep and which node to retire; the Cluster
// Autoscaler adds and removes machines. The handshake is a placeholder Pod:
//
//   - every node CubeOps wants to keep carries one placeholder Pod, so the
//     Cluster Autoscaler never sees it as empty (Cube MicroVMs are not Pods);
//   - a Pending placeholder asks the Cluster Autoscaler for one more node;
//   - a placeholder is deleted only after its node was isolated in Cube, holds
//     no running or paused sandbox, and holds no data that exists only there,
//     so the machine the Cluster Autoscaler removes is exactly the drained one;
//   - once the machine is gone, the Cube node metadata is deleted so lookups
//     of sandboxes that lived there return "not found" instead of an error.
//
// A new node counts as capacity only after the templates the pool serves are
// READY on it; the reconciler asks CubeMaster to redo missing replicas on the
// node, because replica backfill does not complete AppSnapshot templates.
//
// All durable state is derived on every pass from Cube node metadata, the Cube
// node operation log, the template inventory and the placeholder Pods, so a
// restart resumes where the previous leader stopped.
package autoscale

import (
	"math"
	"sort"
	"time"
)

// Operator is recorded on every Cube operation the autoscaler performs; an
// isolation is owned by the autoscaler only when its latest isolate operation
// carries this operator.
const Operator = "cubeops-autoscaler"

// NodeState classifies one pool node for planning.
type NodeState string

const (
	// StateServing: registered, healthy, schedulable, templates ready.
	StateServing NodeState = "serving"
	// StateStarting: in the Kubernetes pool but not yet healthy in Cube.
	StateStarting NodeState = "starting"
	// StateWarming: healthy in Cube but required templates are not READY.
	StateWarming NodeState = "warming"
	// StateDraining: isolated by the autoscaler, placeholder still present.
	StateDraining NodeState = "draining"
	// StateReleased: isolated by the autoscaler, placeholder removed; waiting
	// for the Cluster Autoscaler to delete the machine.
	StateReleased NodeState = "released"
	// StateForeign: isolated by someone else; never touched or retired.
	StateForeign NodeState = "foreign"
)

// PoolNode is the merged Kubernetes + Cube view of one pool node.
type PoolNode struct {
	Name            string    `json:"name"`
	State           NodeState `json:"state"`
	HasPlaceholder  bool      `json:"has_placeholder"`
	PlaceholderName string    `json:"placeholder,omitempty"`
	QuotaMilliCPU   int64     `json:"quota_milli_cpu"`
	QuotaMemoryMB   int64     `json:"quota_memory_mb"`
	UsedMilliCPU    int64     `json:"used_milli_cpu"`
	UsedMemoryMB    int64     `json:"used_memory_mb"`
	MvmNum          int64     `json:"mvm_num"`
	MaxMvmNum       int64     `json:"max_mvm_num,omitempty"`
	InKubernetes    bool      `json:"in_kubernetes"`
	InCube          bool      `json:"in_cube"`
	CubeHealthy     bool      `json:"cube_healthy"`
	CubeIsolated    bool      `json:"cube_isolated"`
	OwnedIsolation  bool      `json:"owned_isolation"`
	IsolatedSince   time.Time `json:"isolated_since,omitempty"`
	// IsolationLost: the autoscaler isolated this node (operation log) but
	// the snapshot CubeMaster reads no longer says so.
	IsolationLost bool `json:"isolation_lost,omitempty"`
	// MissingTemplates are required templates not READY here (warming).
	MissingTemplates []string  `json:"missing_templates,omitempty"`
	WarmingSince     time.Time `json:"warming_since,omitempty"`
	// FillTemplates: an established serving node that lacks templates the
	// other nodes have; it keeps serving while they are rebuilt.
	FillTemplates bool `json:"fill_templates,omitempty"`
	// Blocked is why this node may not be retired right now (local data).
	Blocked string `json:"blocked,omitempty"`
}

// Sizing holds the knobs the plan needs.
type Sizing struct {
	MinNodes          int
	MaxNodes          int
	TargetUtilization float64
	HeadroomSandboxes int
	SandboxMilliCPU   int64
	SandboxMemoryMB   int64
}

// Demand explains the sizing decision.
type Demand struct {
	ServingNodes int     `json:"serving_nodes"`
	FreeSlots    int     `json:"free_slots"`  // sandboxes of the shape that still fit on serving nodes
	Utilization  float64 `json:"utilization"` // max(cpu, mem) quota fill of serving nodes
	DesiredNodes int     `json:"desired_nodes"`
	// ScaleInVictim is the node that would be retired if DesiredNodes is
	// below ServingNodes; ScaleInBlocked explains why scale-in is not possible.
	ScaleInVictim  string `json:"scale_in_victim,omitempty"`
	ScaleInBlocked string `json:"scale_in_blocked,omitempty"`
	Reason         string `json:"reason"`
}

// slots returns how many sandboxes of the shape fit into free quota with the
// scheduler's strict rule (free must exceed the request before each placement).
func slots(free, req int64) int {
	if req <= 0 {
		return math.MaxInt32
	}
	if free <= 0 {
		return 0
	}
	return int((free+req-1)/req) - 1
}

func nodeSlots(n PoolNode, s Sizing) int {
	k := slots(n.QuotaMilliCPU-n.UsedMilliCPU, s.SandboxMilliCPU)
	if m := slots(n.QuotaMemoryMB-n.UsedMemoryMB, s.SandboxMemoryMB); m < k {
		k = m
	}
	if n.MaxMvmNum > 0 {
		if room := int(n.MaxMvmNum - n.MvmNum); room < k {
			k = room
		}
	}
	if k < 0 {
		k = 0
	}
	return k
}

func capacity(nodes []PoolNode, s Sizing) (free int, util float64) {
	var qc, qm, uc, um int64
	for _, n := range nodes {
		free += nodeSlots(n, s)
		qc += n.QuotaMilliCPU
		qm += n.QuotaMemoryMB
		uc += n.UsedMilliCPU
		um += n.UsedMemoryMB
	}
	if qc > 0 {
		util = float64(uc) / float64(qc)
	}
	if qm > 0 && float64(um)/float64(qm) > util {
		util = float64(um) / float64(qm)
	}
	return free, util
}

// ComputeDemand sizes the pool from the serving nodes. Scale-out when the
// serving nodes cannot place HeadroomSandboxes more sandboxes of the shape,
// or are filled beyond TargetUtilization. Scale-in by one node only when the
// remaining serving nodes would still satisfy both.
func ComputeDemand(nodes []PoolNode, s Sizing) Demand {
	var serving []PoolNode
	for _, n := range nodes {
		if n.State == StateServing {
			serving = append(serving, n)
		}
	}
	target := s.TargetUtilization
	if target <= 0 || target > 1 {
		target = 1
	}
	d := Demand{ServingNodes: len(serving)}
	d.FreeSlots, d.Utilization = capacity(serving, s)
	d.DesiredNodes = len(serving)

	switch {
	case len(serving) == 0:
		// Nothing serving yet (fresh pool or every node warming): hold.
		d.Reason = "no serving node"
	case d.FreeSlots < s.HeadroomSandboxes || d.Utilization > target:
		fresh := freshNode(serving)
		add := 1
		if per := nodeSlots(fresh, s); per > 0 && d.FreeSlots < s.HeadroomSandboxes {
			if n := int(math.Ceil(float64(s.HeadroomSandboxes-d.FreeSlots) / float64(per))); n > add {
				add = n
			}
		}
		if n := utilNodes(serving, target) - len(serving); n > add {
			add = n
		}
		d.DesiredNodes = len(serving) + add
		d.Reason = "headroom or utilization exceeded"
	default:
		d.Reason = "within headroom and target"
		victim, ok := pickVictim(serving)
		if !ok {
			d.ScaleInBlocked = "every serving node is blocked"
			break
		}
		rest := without(serving, victim.Name)
		free, util := capacity(rest, s)
		switch {
		case len(rest) == 0:
			d.ScaleInBlocked = "last serving node"
		case free < s.HeadroomSandboxes:
			d.ScaleInBlocked = "remaining nodes would lack headroom"
		case util > target:
			d.ScaleInBlocked = "remaining nodes would exceed target utilization"
		default:
			d.DesiredNodes = len(serving) - 1
			d.ScaleInVictim = victim.Name
			d.Reason = "one node can be retired"
		}
	}
	d.DesiredNodes = clamp(d.DesiredNodes, s.MinNodes, s.MaxNodes)
	if d.DesiredNodes >= len(serving) {
		d.ScaleInVictim = ""
	}
	return d
}

// freshNode is an empty node with the median quota of the serving nodes.
func freshNode(serving []PoolNode) PoolNode {
	cpu := make([]int64, 0, len(serving))
	mem := make([]int64, 0, len(serving))
	mvm := make([]int64, 0, len(serving))
	for _, n := range serving {
		cpu = append(cpu, n.QuotaMilliCPU)
		mem = append(mem, n.QuotaMemoryMB)
		mvm = append(mvm, n.MaxMvmNum)
	}
	return PoolNode{QuotaMilliCPU: median(cpu), QuotaMemoryMB: median(mem), MaxMvmNum: median(mvm)}
}

// utilNodes is how many median-sized nodes keep current usage at target.
func utilNodes(serving []PoolNode, target float64) int {
	f := freshNode(serving)
	var uc, um int64
	for _, n := range serving {
		uc += n.UsedMilliCPU
		um += n.UsedMemoryMB
	}
	need := 0
	if f.QuotaMilliCPU > 0 {
		need = int(math.Ceil(float64(uc) / (float64(f.QuotaMilliCPU) * target)))
	}
	if f.QuotaMemoryMB > 0 {
		if m := int(math.Ceil(float64(um) / (float64(f.QuotaMemoryMB) * target))); m > need {
			need = m
		}
	}
	return need
}

func without(nodes []PoolNode, name string) []PoolNode {
	out := make([]PoolNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Name != name {
			out = append(out, n)
		}
	}
	return out
}

func clamp(v, lo, hi int) int {
	if v < lo {
		v = lo
	}
	if hi > 0 && v > hi {
		v = hi
	}
	return v
}

func median(v []int64) int64 {
	if len(v) == 0 {
		return 0
	}
	c := append([]int64(nil), v...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)/2]
}

// Placeholder is the planner's view of one placeholder Pod.
type Placeholder struct {
	Name      string    `json:"name"`
	NodeName  string    `json:"node,omitempty"` // empty while Pending
	CreatedAt time.Time `json:"created_at"`
}

// Action is one step the reconciler executes. At most one isolation change
// or placeholder release is planned per pass.
type Action struct {
	Kind      ActionKind `json:"kind"`
	Node      string     `json:"node,omitempty"`
	Pod       string     `json:"pod,omitempty"`
	Count     int        `json:"count,omitempty"`
	Templates []string   `json:"templates,omitempty"`
	Reason    string     `json:"reason,omitempty"`
}

// ActionKind enumerates reconciler steps.
type ActionKind string

const (
	ActCreatePlaceholders ActionKind = "create-placeholders"
	ActDeletePending      ActionKind = "delete-pending-placeholder"
	ActWarmTemplates      ActionKind = "warm-templates"
	ActIsolate            ActionKind = "isolate"
	ActReassertIsolation  ActionKind = "reassert-isolation"
	ActCancelDrain        ActionKind = "cancel-drain"
	ActCheckDrained       ActionKind = "check-drained"
	ActDeleteCubeNode     ActionKind = "delete-cube-node"
)

// Input is everything one planning pass needs.
type Input struct {
	Now          time.Time
	Nodes        []PoolNode
	Placeholders []Placeholder
	Sizing       Sizing
	// BelowSince is when the pool first became larger than needed; zero if
	// it is not. The reconciler keeps it across passes.
	BelowSince       time.Time
	ScaleInCooldown  time.Duration
	DrainGrace       time.Duration
	DrainTimeout     time.Duration
	ProvisionTimeout time.Duration
}

// Plan is the decision for one pass.
type Plan struct {
	Demand     Demand    `json:"demand"`
	BelowSince time.Time `json:"below_since,omitempty"`
	Actions    []Action  `json:"actions"`
	Warnings   []string  `json:"warnings,omitempty"`
}

// Decide plans one pass. It never releases a placeholder from a node that has
// not been drained, and never isolates more than one node at a time.
func Decide(in Input) Plan {
	p := Plan{Demand: ComputeDemand(in.Nodes, in.Sizing)}
	desired := p.Demand.DesiredNodes

	var serving, starting, warming, draining, foreign []PoolNode
	for _, n := range in.Nodes {
		switch n.State {
		case StateServing:
			serving = append(serving, n)
		case StateStarting:
			starting = append(starting, n)
		case StateWarming:
			warming = append(warming, n)
		case StateDraining:
			draining = append(draining, n)
		case StateForeign:
			// Isolated by an operator: keep it pinned, never retire it.
			foreign = append(foreign, n)
		case StateReleased:
			if n.IsolationLost && n.InKubernetes {
				// Released but schedulable again: stop new placements.
				p.Actions = append(p.Actions, Action{Kind: ActReassertIsolation, Node: n.Name, Reason: "isolation lost after release"})
			}
			// Machine gone from Kubernetes: drop the Cube metadata so
			// sandbox lookups resolve to "not found". The node was verified
			// empty before its placeholder was released.
			if !n.InKubernetes && n.InCube && !n.CubeHealthy {
				p.Actions = append(p.Actions, Action{Kind: ActDeleteCubeNode, Node: n.Name, Reason: "machine removed after drain"})
			}
		}
	}

	// Every node that should keep running needs a placeholder; extra
	// placeholders ask for new nodes. Only released nodes get none.
	keep := len(serving) + len(starting) + len(warming) + len(draining) + len(foreign)
	pending := 0
	for _, ph := range in.Placeholders {
		if ph.NodeName == "" {
			pending++
		}
	}
	bound := len(in.Placeholders) - pending
	// New capacity must cover what serving nodes lack; nodes on their way
	// (starting, warming) already hold a placeholder and count toward it.
	want := keep
	if w := desired + len(draining) + len(foreign); w > want {
		want = w
	}
	if in.Sizing.MaxNodes > 0 && want > in.Sizing.MaxNodes && keep <= in.Sizing.MaxNodes {
		want = in.Sizing.MaxNodes
	}
	switch have := bound + pending; {
	case have < want:
		p.Actions = append(p.Actions, Action{Kind: ActCreatePlaceholders, Count: want - have,
			Reason: "pin every kept node and request capacity: " + p.Demand.Reason})
	case have > want && pending > 0:
		// Demand fell before the new node arrived: withdraw requests only.
		extra := have - want
		for _, ph := range newestFirst(in.Placeholders) {
			if extra == 0 {
				break
			}
			if ph.NodeName == "" {
				p.Actions = append(p.Actions, Action{Kind: ActDeletePending, Pod: ph.Name, Reason: "request no longer needed"})
				extra--
			}
		}
	}
	for _, ph := range in.Placeholders {
		if ph.NodeName == "" && in.ProvisionTimeout > 0 && in.Now.Sub(ph.CreatedAt) > in.ProvisionTimeout {
			p.Warnings = append(p.Warnings, "placeholder "+ph.Name+" pending beyond provision timeout; check the node pool maximum and Cluster Autoscaler events")
		}
	}
	for _, n := range starting {
		if n.HasPlaceholder && !n.InCube {
			p.Warnings = append(p.Warnings, "node "+n.Name+" has not registered with Cube")
		}
	}
	for _, n := range warming {
		p.Actions = append(p.Actions, Action{Kind: ActWarmTemplates, Node: n.Name, Templates: n.MissingTemplates,
			Reason: "required templates not READY on new node"})
	}
	for _, n := range serving {
		if n.FillTemplates {
			p.Actions = append(p.Actions, Action{Kind: ActWarmTemplates, Node: n.Name, Templates: n.MissingTemplates,
				Reason: "templates READY on every other node are missing here"})
		}
	}

	// Drain bookkeeping: one node at a time.
	if len(draining) > 0 {
		n := draining[0]
		if n.IsolationLost {
			p.Actions = append(p.Actions, Action{Kind: ActReassertIsolation, Node: n.Name, Reason: "isolation lost while draining"})
		}
		switch {
		case len(serving)+len(starting)+len(warming) < desired:
			p.Actions = append(p.Actions, Action{Kind: ActCancelDrain, Node: n.Name, Reason: "demand rose during drain"})
		case in.DrainTimeout > 0 && !n.IsolatedSince.IsZero() && in.Now.Sub(n.IsolatedSince) > in.DrainTimeout:
			p.Actions = append(p.Actions, Action{Kind: ActCancelDrain, Node: n.Name, Reason: "drain timeout; sandboxes are never stopped"})
		case n.IsolationLost || (!n.IsolatedSince.IsZero() && in.Now.Sub(n.IsolatedSince) < in.DrainGrace):
			// Let in-flight creates and resumes land before judging emptiness.
		default:
			p.Actions = append(p.Actions, Action{Kind: ActCheckDrained, Node: n.Name, Pod: n.PlaceholderName})
		}
		return p
	}

	// Scale-in: pool larger than needed for the whole cooldown, nothing on
	// its way, nothing pending.
	if p.Demand.ScaleInVictim != "" && len(serving) > desired && len(starting) == 0 && len(warming) == 0 && pending == 0 {
		p.BelowSince = in.BelowSince
		if p.BelowSince.IsZero() {
			p.BelowSince = in.Now
		}
		if in.Now.Sub(p.BelowSince) >= in.ScaleInCooldown {
			p.Actions = append(p.Actions, Action{Kind: ActIsolate, Node: p.Demand.ScaleInVictim,
				Reason: "pool above need for the scale-in cooldown"})
		}
	}
	return p
}

// pickVictim prefers the node with the fewest MicroVMs, then the least used
// quota, then the name (stable). Blocked nodes are skipped.
func pickVictim(nodes []PoolNode) (PoolNode, bool) {
	c := make([]PoolNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Blocked == "" {
			c = append(c, n)
		}
	}
	if len(c) == 0 {
		return PoolNode{}, false
	}
	sort.Slice(c, func(i, j int) bool {
		if c[i].MvmNum != c[j].MvmNum {
			return c[i].MvmNum < c[j].MvmNum
		}
		if c[i].UsedMilliCPU != c[j].UsedMilliCPU {
			return c[i].UsedMilliCPU < c[j].UsedMilliCPU
		}
		return c[i].Name < c[j].Name
	})
	return c[0], true
}

// newestFirst withdraws the most recent request first; older ones are the
// likeliest to be close to getting their node.
func newestFirst(in []Placeholder) []Placeholder {
	c := append([]Placeholder(nil), in...)
	sort.Slice(c, func(i, j int) bool { return c[i].CreatedAt.After(c[j].CreatedAt) })
	return c
}
