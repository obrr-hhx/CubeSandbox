// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package autoscale

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/config"
	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/logging"
	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/nodemanagement/model"
)

// LockName serialises reconcile passes across CubeOps replicas.
const LockName = "cubeops_node_autoscale_v1"

const (
	templateCacheTTL = time.Minute
	redoRetry        = 10 * time.Minute
	maxRedosPerPass  = 1
	blockedRetry     = 30 * time.Minute
	opsLookback      = 50
)

// NodeService is the subset of the node domain service the reconciler uses.
type NodeService interface {
	ListNodes(ctx context.Context) ([]*model.NodeSnapshot, error)
	GetNode(ctx context.Context, nodeID string) (*model.NodeSnapshot, error)
	SetNodeSchedulingDisabled(ctx context.Context, nodeID string, disabled bool, operator, detail string) (*model.NodeSnapshot, error)
	ListOperations(ctx context.Context, nodeID string, limit int) ([]model.NodeOperation, error)
	DeleteNode(ctx context.Context, nodeID string, force bool) (*model.NodeSnapshot, error)
}

// Locker runs fn only on the replica that holds the named lock.
type Locker func(ctx context.Context, name string, fn func(context.Context) error) error

// Status is the last pass, served by the status endpoint.
type Status struct {
	Enabled            bool              `json:"enabled"`
	DryRun             bool              `json:"dry_run"`
	LastRun            time.Time         `json:"last_run"`
	LastError          string            `json:"last_error,omitempty"`
	Demand             Demand            `json:"demand"`
	Nodes              []PoolNode        `json:"nodes"`
	Pending            int               `json:"pending_placeholders"`
	RequiredTemplates  []string          `json:"required_templates,omitempty"`
	Blocked            map[string]string `json:"blocked,omitempty"`
	IsolationReasserts int               `json:"isolation_reasserts"`
	BelowSince         time.Time         `json:"below_since,omitempty"`
	Actions            []Action          `json:"actions"`
	Warnings           []string          `json:"warnings,omitempty"`
}

type blockEntry struct {
	reason string
	until  time.Time
}

// Reconciler drives the pool toward the planned size.
type Reconciler struct {
	cfg     config.AutoscaleConfig
	nodes   NodeService
	cm      CubeMaster
	cluster Cluster
	lock    Locker
	now     func() time.Time

	mu         sync.Mutex
	belowSince time.Time
	status     Status

	// Soft state: lost on restart, which only delays or repeats harmless work.
	tplCache  []Template
	tplAt     time.Time
	redoAt    map[string]time.Time
	blocked   map[string]blockEntry
	reasserts int
}

// New builds a reconciler. lock may be nil (single replica / tests).
func New(cfg config.AutoscaleConfig, nodes NodeService, cm CubeMaster, cluster Cluster, lock Locker) *Reconciler {
	return &Reconciler{cfg: cfg, nodes: nodes, cm: cm, cluster: cluster, lock: lock, now: time.Now,
		status: Status{Enabled: cfg.Enable, DryRun: cfg.DryRun},
		redoAt: map[string]time.Time{}, blocked: map[string]blockEntry{}}
}

// Run reconciles every Interval until ctx ends.
func (r *Reconciler) Run(ctx context.Context) {
	logging.G(ctx).Infof("autoscale: started: min=%d max=%d target=%.2f headroom=%dx(%dm,%dMB) interval=%s dry_run=%t",
		r.cfg.MinNodes, r.cfg.MaxNodes, r.cfg.TargetUtilization, r.cfg.HeadroomSandboxes,
		r.cfg.SandboxMilliCPU, r.cfg.SandboxMemoryMB, r.cfg.Interval, r.cfg.DryRun)
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce performs one pass under the cluster lock.
func (r *Reconciler) RunOnce(ctx context.Context) {
	pass := func(ctx context.Context) error {
		err := r.reconcile(ctx)
		r.mu.Lock()
		r.status.LastRun = r.now()
		r.status.LastError = ""
		if err != nil {
			r.status.LastError = err.Error()
		}
		r.mu.Unlock()
		if err != nil {
			logging.G(ctx).Warnf("autoscale: pass failed: %v", err)
		}
		return nil
	}
	if r.lock == nil {
		_ = pass(ctx)
		return
	}
	if err := r.lock(ctx, LockName, pass); err != nil {
		logging.G(ctx).Warnf("autoscale: lock: %v", err)
	}
}

// Status returns a copy of the last pass.
func (r *Reconciler) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.status
	s.Nodes = append([]PoolNode(nil), s.Nodes...)
	s.Actions = append([]Action(nil), s.Actions...)
	s.Warnings = append([]string(nil), s.Warnings...)
	s.RequiredTemplates = append([]string(nil), s.RequiredTemplates...)
	s.Blocked = make(map[string]string, len(r.status.Blocked))
	for k, v := range r.status.Blocked {
		s.Blocked[k] = v
	}
	return s
}

func (r *Reconciler) sizing() Sizing {
	return Sizing{
		MinNodes:          r.cfg.MinNodes,
		MaxNodes:          r.cfg.MaxNodes,
		TargetUtilization: r.cfg.TargetUtilization,
		HeadroomSandboxes: r.cfg.HeadroomSandboxes,
		SandboxMilliCPU:   r.cfg.SandboxMilliCPU,
		SandboxMemoryMB:   r.cfg.SandboxMemoryMB,
	}
}

func (r *Reconciler) reconcile(ctx context.Context) error {
	obs, err := r.observe(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	below := r.belowSince
	r.mu.Unlock()

	plan := Decide(Input{
		Now:              r.now(),
		Nodes:            obs.nodes,
		Placeholders:     obs.placeholders,
		Sizing:           r.sizing(),
		BelowSince:       below,
		ScaleInCooldown:  r.cfg.ScaleInCooldown,
		DrainGrace:       r.cfg.DrainGrace,
		DrainTimeout:     r.cfg.DrainTimeout,
		ProvisionTimeout: r.cfg.ProvisionTimeout,
	})
	plan.Warnings = append(obs.warnings, plan.Warnings...)

	pending := 0
	for _, ph := range obs.placeholders {
		if ph.NodeName == "" {
			pending++
		}
	}
	r.mu.Lock()
	r.belowSince = plan.BelowSince
	r.status.Demand = plan.Demand
	r.status.Nodes = obs.nodes
	r.status.Pending = pending
	r.status.RequiredTemplates = obs.required
	r.status.BelowSince = plan.BelowSince
	r.status.Actions = plan.Actions
	r.status.Warnings = plan.Warnings
	r.mu.Unlock()

	for _, w := range plan.Warnings {
		logging.G(ctx).Warnf("autoscale: %s", w)
	}
	var firstErr error
	redos := 0
	for _, a := range plan.Actions {
		if err := r.execute(ctx, a, obs, &redos); err != nil {
			logging.G(ctx).Warnf("autoscale: %s node=%s pod=%s failed: %v", a.Kind, a.Node, a.Pod, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	// Outcomes of this pass's actions.
	blocked := map[string]string{}
	for n, b := range r.blocked {
		if r.now().Before(b.until) {
			blocked[n] = b.reason
		}
	}
	r.mu.Lock()
	r.status.Blocked = blocked
	r.status.IsolationReasserts = r.reasserts
	r.mu.Unlock()
	return firstErr
}

type observation struct {
	nodes        []PoolNode
	placeholders []Placeholder
	avoid        []string // nodes new placeholders must not land on
	required     []string
	present      map[string]bool // healthy, non-isolated pool nodes (replica holders that count)
	warnings     []string
}

// templates returns the cached template inventory, refreshing it when stale
// or when force is set.
func (r *Reconciler) templates(ctx context.Context, force bool) ([]Template, error) {
	if !force && r.tplCache != nil && r.now().Sub(r.tplAt) < templateCacheTTL {
		return r.tplCache, nil
	}
	t, err := r.cm.Templates(ctx)
	if err != nil {
		return nil, err
	}
	r.tplCache, r.tplAt = t, r.now()
	return t, nil
}

// observe merges Kubernetes pool nodes, Cube node metadata, the operation
// log, template replicas and placeholders.
func (r *Reconciler) observe(ctx context.Context) (*observation, error) {
	kubeNodes, err := r.cluster.PoolNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pool nodes: %w", err)
	}
	phs, err := r.cluster.Placeholders(ctx)
	if err != nil {
		return nil, fmt.Errorf("list placeholders: %w", err)
	}
	snaps, err := r.nodes.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list cube nodes: %w", err)
	}
	obs := &observation{placeholders: phs, present: map[string]bool{}}
	tpls, tplErr := r.templates(ctx, false)
	if tplErr != nil {
		obs.warnings = append(obs.warnings, "template inventory unavailable; new nodes count as serving and scale-in is held: "+tplErr.Error())
	}

	inKube := make(map[string]bool, len(kubeNodes))
	created := make(map[string]time.Time, len(kubeNodes))
	for _, n := range kubeNodes {
		inKube[n.Name] = true
		created[n.Name] = n.CreatedAt
	}
	byCube := make(map[string]*model.NodeSnapshot, len(snaps))
	for _, s := range snaps {
		byCube[s.NodeID] = s
	}
	phByNode := make(map[string]string)
	for _, ph := range phs {
		if ph.NodeName != "" {
			phByNode[ph.NodeName] = ph.Name
		}
	}

	names := map[string]bool{}
	for _, n := range kubeNodes {
		names[n.Name] = true
	}
	for id, s := range byCube {
		// Cube nodes outside the pool matter only when the autoscaler
		// isolated them (machine already removed, metadata to clean up).
		if !inKube[id] && s.SchedulingDisabled {
			names[id] = true
		}
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)

	// Pass 1: classify isolation and health.
	var nodes []PoolNode
	for _, name := range sorted {
		s := byCube[name]
		n := PoolNode{Name: name, InKubernetes: inKube[name], InCube: s != nil}
		n.PlaceholderName, n.HasPlaceholder = phByNode[name], phByNode[name] != ""
		if s != nil {
			n.CubeHealthy = s.Healthy
			n.CubeIsolated = s.SchedulingDisabled
			// Same fallback as the scheduler export (ToSchedulerNode).
			n.QuotaMilliCPU, n.QuotaMemoryMB = s.QuotaCPU, s.QuotaMemMB
			if n.QuotaMilliCPU == 0 {
				n.QuotaMilliCPU = s.Allocatable.MilliCPU
			}
			if n.QuotaMemoryMB == 0 {
				n.QuotaMemoryMB = s.Allocatable.MemoryMB
			}
			n.UsedMilliCPU, n.UsedMemoryMB = s.QuotaCpuUsage, s.QuotaMemUsage
			n.MvmNum, n.MaxMvmNum = s.MvmNum, s.MaxMvmNum
		}
		var owned bool
		if n.InCube {
			owned, n.IsolatedSince, err = r.ownedIsolation(ctx, name)
			if err != nil {
				return nil, err
			}
		}
		switch {
		case n.CubeIsolated && !owned:
			n.State = StateForeign
		case owned:
			// Owned by the autoscaler. If the snapshot CubeMaster reads lost
			// the label, keep treating the node as isolated and re-assert.
			n.OwnedIsolation = true
			n.IsolationLost = !n.CubeIsolated
			if n.HasPlaceholder {
				n.State = StateDraining
			} else {
				n.State = StateReleased
			}
		case !n.InKubernetes:
			continue
		case n.InCube && n.CubeHealthy:
			n.State = StateServing
		default:
			n.State = StateStarting
		}
		if !n.InKubernetes && n.State != StateReleased {
			continue
		}
		if n.State == StateReleased || n.State == StateDraining {
			obs.avoid = append(obs.avoid, name)
		}
		if n.State == StateServing && !n.CubeIsolated {
			obs.present[name] = true
		}
		nodes = append(nodes, n)
	}

	// Pass 2: template readiness turns new healthy nodes into warming ones.
	ready := readyByNode(tpls)
	if tplErr == nil {
		explicit := len(r.cfg.RequiredTemplates) > 0
		known := map[string]bool{}
		for _, t := range tpls {
			known[t.ID] = true
		}
		for _, id := range r.cfg.RequiredTemplates {
			if !known[id] {
				obs.warnings = append(obs.warnings, "required template "+id+" is not a READY template")
			}
		}
		reqUnion := map[string]bool{}
		for i := range nodes {
			n := &nodes[i]
			if n.State != StateServing {
				continue
			}
			var required []string
			if explicit {
				for _, id := range r.cfg.RequiredTemplates {
					if known[id] {
						required = append(required, id)
					}
				}
			} else {
				required = requiredFor(n.Name, obs.present, tpls)
			}
			for _, id := range required {
				reqUnion[id] = true
				if !ready[n.Name][id] {
					n.MissingTemplates = append(n.MissingTemplates, id)
				}
			}
			if len(n.MissingTemplates) == 0 {
				continue
			}
			// Only a new machine is held back as warming; an established
			// node keeps serving while its gaps are filled.
			age := r.now().Sub(created[n.Name])
			if created[n.Name].IsZero() || age > r.cfg.WarmTimeout {
				n.FillTemplates = true
				continue
			}
			n.WarmingSince = created[n.Name]
			n.State = StateWarming
		}
		for id := range reqUnion {
			obs.required = append(obs.required, id)
		}
		sort.Strings(obs.required)
		// Replica rows left for nodes that no longer exist (issue #1965).
		stale := map[string]int{}
		for _, t := range tpls {
			for _, rep := range t.Replicas {
				if rep.NodeID != "" && byCube[rep.NodeID] == nil && !inKube[rep.NodeID] {
					stale[rep.NodeID]++
				}
			}
		}
		for node, c := range stale {
			obs.warnings = append(obs.warnings, fmt.Sprintf("%d template replica row(s) still reference removed node %s", c, node))
		}
	}

	for i := range nodes {
		if b, ok := r.blocked[nodes[i].Name]; ok && r.now().Before(b.until) {
			nodes[i].Blocked = b.reason
		}
	}
	if tplErr != nil {
		// Without the inventory nothing can be proven safe to retire.
		for i := range nodes {
			if nodes[i].Blocked == "" {
				nodes[i].Blocked = "template inventory unavailable"
			}
		}
	}
	obs.nodes = nodes
	return obs, nil
}

// readyByNode maps node -> template -> READY.
func readyByNode(tpls []Template) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, t := range tpls {
		for _, rep := range t.Replicas {
			if strings.EqualFold(rep.Status, replicaReady) {
				if out[rep.NodeID] == nil {
					out[rep.NodeID] = map[string]bool{}
				}
				out[rep.NodeID][t.ID] = true
			}
		}
	}
	return out
}

// requiredFor returns the templates a node must hold: those READY on every
// other present (serving, schedulable) node. A node alone requires nothing.
func requiredFor(node string, present map[string]bool, tpls []Template) []string {
	var others []string
	for n := range present {
		if n != node {
			others = append(others, n)
		}
	}
	if len(others) == 0 {
		return nil
	}
	ready := readyByNode(tpls)
	var out []string
	for _, t := range tpls {
		all := true
		for _, o := range others {
			if !ready[o][t.ID] {
				all = false
				break
			}
		}
		if all {
			out = append(out, t.ID)
		}
	}
	sort.Strings(out)
	return out
}

// ownedIsolation reports whether the latest isolate/unisolate of node was an
// isolation made by the autoscaler, and when.
func (r *Reconciler) ownedIsolation(ctx context.Context, node string) (bool, time.Time, error) {
	ops, err := r.nodes.ListOperations(ctx, node, opsLookback)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("list operations node=%s: %w", node, err)
	}
	var latest *model.NodeOperation
	for i := range ops {
		if ops[i].Type != model.OpIsolate && ops[i].Type != model.OpUnisolate {
			continue
		}
		if latest == nil || ops[i].CreatedAt.After(latest.CreatedAt) {
			latest = &ops[i]
		}
	}
	if latest == nil || latest.Type != model.OpIsolate || latest.Operator != Operator {
		return false, time.Time{}, nil
	}
	return true, latest.CreatedAt, nil
}

// blockers lists reasons the node may not be retired. transient is true when
// every reason is work in progress that will finish on its own.
func (r *Reconciler) blockers(ctx context.Context, node string, present map[string]bool) (reasons []string, transient bool, err error) {
	tpls, err := r.templates(ctx, true)
	if err != nil {
		return nil, false, fmt.Errorf("template inventory: %w", err)
	}
	snaps, err := r.cm.Snapshots(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("snapshot inventory: %w", err)
	}
	permanent := false
	for _, s := range snaps {
		if s.OriginNodeID == node && s.LocalOnly() {
			reasons = append(reasons, "snapshot "+s.ID+" exists only on this node")
			if !strings.EqualFold(s.Status, "CREATING") && !strings.EqualFold(s.Status, "DELETING") {
				permanent = true
			}
		}
	}
	for _, t := range tpls {
		here, elsewhere := false, false
		for _, rep := range t.Replicas {
			switch {
			case rep.NodeID == node && strings.EqualFold(rep.Status, replicaReady):
				here = true
			case rep.NodeID == node && inFlightPhases[strings.ToUpper(rep.Phase)]:
				reasons = append(reasons, "template "+t.ID+" replica work in progress")
			case rep.NodeID != node && strings.EqualFold(rep.Status, replicaReady) && present[rep.NodeID]:
				elsewhere = true
			}
		}
		if here && !elsewhere {
			reasons = append(reasons, "only READY replica of template "+t.ID)
			permanent = true
		}
	}
	return reasons, len(reasons) > 0 && !permanent, nil
}

func summarize(reasons []string) string {
	if len(reasons) <= 3 {
		return strings.Join(reasons, "; ")
	}
	return strings.Join(reasons[:3], "; ") + fmt.Sprintf("; and %d more", len(reasons)-3)
}

func (r *Reconciler) block(node string, reasons []string) {
	r.blocked[node] = blockEntry{reason: summarize(reasons), until: r.now().Add(blockedRetry)}
}

func (r *Reconciler) execute(ctx context.Context, a Action, obs *observation, redos *int) error {
	log := logging.G(ctx)
	if r.cfg.DryRun {
		if a.Kind == ActIsolate {
			// Report what the preflight would decide; changes nothing.
			if reasons, _, err := r.blockers(ctx, a.Node, obs.present); err == nil && len(reasons) > 0 {
				r.block(a.Node, reasons)
				log.Infof("autoscale: dry-run isolate node=%s would be blocked: %s", a.Node, summarize(reasons))
				return nil
			}
		}
		log.Infof("autoscale: dry-run %s node=%s pod=%s count=%d templates=%d reason=%q",
			a.Kind, a.Node, a.Pod, a.Count, len(a.Templates), a.Reason)
		return nil
	}
	switch a.Kind {
	case ActCreatePlaceholders:
		for i := 0; i < a.Count; i++ {
			if err := r.cluster.CreatePlaceholder(ctx, obs.avoid); err != nil {
				return err
			}
		}
		log.Infof("autoscale: created %d placeholder(s): %s", a.Count, a.Reason)
	case ActDeletePending:
		if err := r.cluster.DeletePlaceholder(ctx, a.Pod); err != nil {
			return err
		}
		log.Infof("autoscale: withdrew pending placeholder %s: %s", a.Pod, a.Reason)
	case ActWarmTemplates:
		return r.warm(ctx, a, redos)
	case ActIsolate:
		reasons, _, err := r.blockers(ctx, a.Node, obs.present)
		if err != nil {
			return fmt.Errorf("preflight %s: %w", a.Node, err)
		}
		if len(reasons) > 0 {
			r.block(a.Node, reasons)
			log.Infof("autoscale: node=%s not retired: %s", a.Node, summarize(reasons))
			return nil
		}
		if _, err := r.nodes.SetNodeSchedulingDisabled(ctx, a.Node, true, Operator, "autoscale: drain for scale-in"); err != nil {
			return err
		}
		log.Infof("autoscale: isolated node=%s for scale-in", a.Node)
	case ActReassertIsolation:
		r.reasserts++
		if _, err := r.nodes.SetNodeSchedulingDisabled(ctx, a.Node, true, Operator, "autoscale: re-assert isolation"); err != nil {
			return err
		}
		log.Warnf("autoscale: node=%s isolation was not visible to the scheduler; re-asserted (%s)", a.Node, a.Reason)
	case ActCancelDrain:
		if _, err := r.nodes.SetNodeSchedulingDisabled(ctx, a.Node, false, Operator, "autoscale: "+a.Reason); err != nil {
			return err
		}
		log.Infof("autoscale: returned node=%s to service: %s", a.Node, a.Reason)
	case ActCheckDrained:
		return r.releaseIfDrained(ctx, a.Node, a.Pod, obs.present)
	case ActDeleteCubeNode:
		if _, err := r.nodes.DeleteNode(ctx, a.Node, true); err != nil {
			return err
		}
		log.Infof("autoscale: deleted cube metadata for removed node=%s", a.Node)
	default:
		return fmt.Errorf("unknown action %q", a.Kind)
	}
	return nil
}

// warm asks CubeMaster to build missing template replicas on a warming node,
// a few per pass and each at most once per redoRetry.
func (r *Reconciler) warm(ctx context.Context, a Action, redos *int) error {
	// One rebuild at a time per node: wait while replica work is running.
	for _, t := range r.tplCache {
		for _, rep := range t.Replicas {
			if rep.NodeID == a.Node && inFlightPhases[strings.ToUpper(rep.Phase)] && !strings.EqualFold(rep.Status, replicaReady) {
				return nil
			}
		}
	}
	var firstErr error
	for _, id := range a.Templates {
		if *redos >= maxRedosPerPass {
			break
		}
		key := a.Node + "|" + id
		if last, ok := r.redoAt[key]; ok && r.now().Sub(last) < redoRetry {
			continue
		}
		r.redoAt[key] = r.now()
		*redos++
		if err := r.cm.RedoTemplateOnNode(ctx, id, a.Node); err != nil {
			logging.G(ctx).Warnf("autoscale: warm node=%s template=%s: %v", a.Node, id, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		logging.G(ctx).Infof("autoscale: warming node=%s: redo template=%s", a.Node, id)
	}
	return firstErr
}

// releaseIfDrained removes the node's placeholder once it is isolated by the
// autoscaler, holds no running or paused sandbox, and holds no data that
// exists only there. Any doubt keeps waiting.
func (r *Reconciler) releaseIfDrained(ctx context.Context, node, pod string, present map[string]bool) error {
	snap, err := r.nodes.GetNode(ctx, node)
	if err != nil {
		return err
	}
	if !snap.Healthy {
		return fmt.Errorf("node %s unhealthy; cannot verify it is empty", node)
	}
	n, err := r.cm.CountNodeSandboxes(ctx, node)
	if err != nil {
		return fmt.Errorf("count sandboxes: %w", err)
	}
	if n != 0 {
		logging.G(ctx).Infof("autoscale: draining node=%s sandboxes=%d", node, n)
		return nil
	}
	reasons, transient, err := r.blockers(ctx, node, present)
	if err != nil {
		return fmt.Errorf("retire check %s: %w", node, err)
	}
	if len(reasons) > 0 {
		if transient {
			logging.G(ctx).Infof("autoscale: draining node=%s waits: %s", node, summarize(reasons))
			return nil
		}
		// Data that only lives here: return the node to service.
		r.block(node, reasons)
		if _, err := r.nodes.SetNodeSchedulingDisabled(ctx, node, false, Operator, "autoscale: retire blocked"); err != nil {
			return err
		}
		logging.G(ctx).Infof("autoscale: node=%s returned to service; retire blocked: %s", node, summarize(reasons))
		return nil
	}
	// Re-check ownership and isolation right before the irreversible step.
	owned, _, err := r.ownedIsolation(ctx, node)
	if err != nil {
		return err
	}
	snap, err = r.nodes.GetNode(ctx, node)
	if err != nil {
		return err
	}
	if !owned || !snap.SchedulingDisabled {
		return fmt.Errorf("node %s no longer isolated by the autoscaler; not releasing", node)
	}
	if n, err = r.cm.CountNodeSandboxes(ctx, node); err != nil || n != 0 {
		return fmt.Errorf("node %s not empty on re-check (count=%d err=%v); not releasing", node, n, err)
	}
	if pod == "" {
		return fmt.Errorf("node %s has no placeholder to release", node)
	}
	if err := r.cluster.DeletePlaceholder(ctx, pod); err != nil {
		return err
	}
	logging.G(ctx).Infof("autoscale: node=%s drained; released placeholder %s to the cluster autoscaler", node, pod)
	return nil
}
