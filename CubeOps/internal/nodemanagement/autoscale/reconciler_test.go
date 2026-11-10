// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package autoscale

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/config"
	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/nodemanagement/model"
)

type fakeNodes struct {
	snaps   map[string]*model.NodeSnapshot
	ops     map[string][]model.NodeOperation
	deleted []string
	now     *time.Time
}

func (f *fakeNodes) ListNodes(context.Context) ([]*model.NodeSnapshot, error) {
	var out []*model.NodeSnapshot
	for _, s := range f.snaps {
		c := *s
		out = append(out, &c)
	}
	return out, nil
}

func (f *fakeNodes) GetNode(_ context.Context, id string) (*model.NodeSnapshot, error) {
	s, ok := f.snaps[id]
	if !ok {
		return nil, errors.New("not found")
	}
	c := *s
	return &c, nil
}

// SetNodeSchedulingDisabled mirrors the service: the DB label (here: the last
// isolate/unisolate op) changes only when it differs, and records an op; the
// snapshot CubeMaster reads is always rewritten.
func (f *fakeNodes) SetNodeSchedulingDisabled(_ context.Context, id string, disabled bool, operator, detail string) (*model.NodeSnapshot, error) {
	s := f.snaps[id]
	s.SchedulingDisabled = disabled
	ops := f.ops[id]
	dbIsolated := len(ops) > 0 && ops[len(ops)-1].Type == model.OpIsolate
	if dbIsolated != disabled {
		typ := model.OpUnisolate
		if disabled {
			typ = model.OpIsolate
		}
		f.ops[id] = append(f.ops[id], model.NodeOperation{NodeID: id, Type: typ, Operator: operator, Detail: detail, CreatedAt: *f.now})
	}
	return s, nil
}

func (f *fakeNodes) ListOperations(_ context.Context, id string, limit int) ([]model.NodeOperation, error) {
	ops := f.ops[id]
	// newest first, like the store
	out := make([]model.NodeOperation, 0, len(ops))
	for i := len(ops) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, ops[i])
	}
	return out, nil
}

func (f *fakeNodes) DeleteNode(_ context.Context, id string, _ bool) (*model.NodeSnapshot, error) {
	f.deleted = append(f.deleted, id)
	delete(f.snaps, id)
	return nil, nil
}

type fakeCM struct {
	sandboxes map[string]int
	templates []Template
	snapshots []Snapshot
	redos     []string
	tplErr    error
}

func (f *fakeCM) CountNodeSandboxes(_ context.Context, id string) (int, error) {
	return f.sandboxes[id], nil
}
func (f *fakeCM) Templates(context.Context) ([]Template, error) {
	if f.tplErr != nil {
		return nil, f.tplErr
	}
	return f.templates, nil
}
func (f *fakeCM) Snapshots(context.Context) ([]Snapshot, error) { return f.snapshots, nil }
func (f *fakeCM) RedoTemplateOnNode(_ context.Context, tpl, node string) error {
	f.redos = append(f.redos, tpl+"@"+node)
	return nil
}

type fakeCluster struct {
	nodes   []KubeNode
	phs     []Placeholder
	seq     int
	created [][]string
	deleted []string
}

func (f *fakeCluster) PoolNodes(context.Context) ([]KubeNode, error)       { return f.nodes, nil }
func (f *fakeCluster) Placeholders(context.Context) ([]Placeholder, error) { return f.phs, nil }
func (f *fakeCluster) CreatePlaceholder(_ context.Context, avoid []string) error {
	f.seq++
	f.created = append(f.created, avoid)
	f.phs = append(f.phs, Placeholder{Name: fmt.Sprintf("cube-capacity-%d", f.seq)})
	return nil
}
func (f *fakeCluster) DeletePlaceholder(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	out := f.phs[:0]
	for _, p := range f.phs {
		if p.Name != name {
			out = append(out, p)
		}
	}
	f.phs = out
	return nil
}

// bindPending schedules Pending placeholders onto nodes without one.
func (f *fakeCluster) bindPending() {
	used := map[string]bool{}
	for _, p := range f.phs {
		if p.NodeName != "" {
			used[p.NodeName] = true
		}
	}
	for i := range f.phs {
		if f.phs[i].NodeName != "" {
			continue
		}
		for _, n := range f.nodes {
			if !used[n.Name] {
				f.phs[i].NodeName, used[n.Name] = n.Name, true
				break
			}
		}
	}
}

func kube(names ...string) []KubeNode {
	out := make([]KubeNode, 0, len(names))
	for _, n := range names {
		out = append(out, KubeNode{Name: n, CreatedAt: t0.Add(-24 * time.Hour)})
	}
	return out
}

func snap(id string, usedMilli int64) *model.NodeSnapshot {
	return &model.NodeSnapshot{NodeID: id, Healthy: true, QuotaCPU: 16000, QuotaMemMB: 32000,
		QuotaCpuUsage: usedMilli, QuotaMemUsage: usedMilli}
}

func tpl(id string, readyOn ...string) Template {
	t := Template{ID: id, Status: "READY"}
	for _, n := range readyOn {
		t.Replicas = append(t.Replicas, Replica{NodeID: n, Status: "READY", Phase: "READY"})
	}
	return t
}

type harness struct {
	now   time.Time
	nodes *fakeNodes
	cm    *fakeCM
	cl    *fakeCluster
	r     *Reconciler
}

func newHarness(names ...string) *harness {
	h := &harness{now: t0}
	h.nodes = &fakeNodes{snaps: map[string]*model.NodeSnapshot{}, ops: map[string][]model.NodeOperation{}, now: &h.now}
	for _, n := range names {
		h.nodes.snaps[n] = snap(n, 0)
	}
	h.cm = &fakeCM{sandboxes: map[string]int{}}
	h.cl = &fakeCluster{nodes: kube(names...)}
	cfg := config.AutoscaleConfig{Enable: true, MinNodes: 1, MaxNodes: 4, TargetUtilization: 0.75,
		HeadroomSandboxes: 2, SandboxMilliCPU: 4000, SandboxMemoryMB: 8192,
		ScaleInCooldown: 10 * time.Minute, DrainGrace: 2 * time.Minute, DrainTimeout: time.Hour,
		ProvisionTimeout: 15 * time.Minute, WarmTimeout: 30 * time.Minute,
		NodeSelector: map[string]string{"pool": "cube"}}
	h.r = New(cfg, h.nodes, h.cm, h.cl, nil)
	h.r.now = func() time.Time { return h.now }
	return h
}

func (h *harness) pass() { h.r.RunOnce(context.Background()); h.cl.bindPending() }
func (h *harness) advance(d time.Duration) {
	h.now = h.now.Add(d)
}

func TestReconcilerScaleInDrainsReleasesAndCleansUp(t *testing.T) {
	h := newHarness("a", "b")
	h.nodes.snaps["a"].MvmNum = 1
	h.cm.sandboxes = map[string]int{"a": 1, "b": 1}
	h.cm.templates = []Template{tpl("t1", "a", "b")}

	h.pass() // adopt: pin both nodes
	require.Len(t, h.cl.phs, 2)

	h.pass() // cooldown starts
	h.advance(11 * time.Minute)
	h.pass() // "b" (fewest MicroVMs) isolated
	require.True(t, h.nodes.snaps["b"].SchedulingDisabled)
	assert.False(t, h.nodes.snaps["a"].SchedulingDisabled)

	h.advance(3 * time.Minute)
	h.pass() // sandbox still on b
	assert.Empty(t, h.cl.deleted)

	h.cm.sandboxes["b"] = 0
	h.pass() // empty and no local-only data: released
	require.Len(t, h.cl.deleted, 1)
	assert.Empty(t, h.r.Status().LastError)

	h.pass() // not re-pinned while the machine is still there
	assert.Len(t, h.cl.phs, 1)

	// Cluster Autoscaler removes the machine; the Cube heartbeat stops.
	h.cl.nodes = kube("a")
	h.nodes.snaps["b"].Healthy = false
	h.pass()
	assert.Equal(t, []string{"b"}, h.nodes.deleted)
}

func TestReconcilerDoesNotRetireNodeHoldingOnlyCopyOrLocalSnapshot(t *testing.T) {
	h := newHarness("a", "b")
	h.nodes.snaps["a"].MvmNum = 1
	h.cm.templates = []Template{tpl("t1", "a", "b"), tpl("t-commit", "b")}
	h.pass()
	h.pass()
	h.advance(11 * time.Minute)
	h.pass() // victim b holds the only READY replica of t-commit: not isolated
	assert.False(t, h.nodes.snaps["b"].SchedulingDisabled)
	assert.Contains(t, h.r.Status().Blocked["b"], "t-commit")

	// Next candidate a is fine once it gains a copy... but a holds a local
	// snapshot, so it is blocked too.
	h.cm.snapshots = []Snapshot{{ID: "s1", Status: "READY", OriginNodeID: "a", Backend: "xfs"}}
	h.advance(time.Minute)
	h.pass()
	h.advance(11 * time.Minute)
	h.pass()
	assert.False(t, h.nodes.snaps["a"].SchedulingDisabled)
	assert.Empty(t, h.cl.deleted)
}

func TestReconcilerReturnsNodeToServiceWhenLocalDataAppearsDuringDrain(t *testing.T) {
	h := newHarness("a", "b")
	h.nodes.snaps["a"].MvmNum = 1
	h.cm.templates = []Template{tpl("t1", "a", "b")}
	h.cm.sandboxes = map[string]int{"b": 1}
	h.pass()
	h.pass()
	h.advance(11 * time.Minute)
	h.pass()
	require.True(t, h.nodes.snaps["b"].SchedulingDisabled)

	// A snapshot was taken on b while draining; the sandbox then ends.
	h.cm.snapshots = []Snapshot{{ID: "s1", Status: "READY", OriginNodeID: "b", Backend: "xfs"}}
	h.cm.sandboxes["b"] = 0
	h.advance(3 * time.Minute)
	h.pass()
	assert.Empty(t, h.cl.deleted)
	assert.False(t, h.nodes.snaps["b"].SchedulingDisabled, "returned to service")
	assert.Contains(t, h.r.Status().Blocked["b"], "s1")
}

func TestReconcilerReassertsIsolationLostInSnapshot(t *testing.T) {
	h := newHarness("a", "b")
	h.nodes.snaps["a"].MvmNum = 1
	h.cm.templates = []Template{tpl("t1", "a", "b")}
	h.cm.sandboxes = map[string]int{"b": 1}
	h.pass()
	h.pass()
	h.advance(11 * time.Minute)
	h.pass()
	require.True(t, h.nodes.snaps["b"].SchedulingDisabled)

	// A heartbeat overwrote the snapshot with stale labels.
	h.nodes.snaps["b"].SchedulingDisabled = false
	h.cm.sandboxes["b"] = 0
	h.advance(3 * time.Minute)
	h.pass()
	assert.True(t, h.nodes.snaps["b"].SchedulingDisabled, "re-asserted")
	assert.Empty(t, h.cl.deleted, "never released in the same pass as a lost isolation")
	assert.Equal(t, 1, h.r.Status().IsolationReasserts)

	h.pass()
	assert.Len(t, h.cl.deleted, 1)
}

func TestReconcilerWarmsNewNodeBeforeCountingIt(t *testing.T) {
	h := newHarness("a")
	h.cm.templates = []Template{tpl("t1", "a"), tpl("t2", "a")}
	h.pass()
	// A new machine joined a minute ago and registered with Cube.
	h.cl.nodes = append(h.cl.nodes, KubeNode{Name: "n", CreatedAt: h.now.Add(-time.Minute)})
	h.nodes.snaps["n"] = snap("n", 0)
	h.r.tplAt = time.Time{} // refresh inventory
	h.pass()

	st := h.r.Status()
	var n PoolNode
	for _, x := range st.Nodes {
		if x.Name == "n" {
			n = x
		}
	}
	assert.Equal(t, StateWarming, n.State)
	assert.Equal(t, []string{"t1", "t2"}, n.MissingTemplates)
	assert.Equal(t, []string{"t1@n"}, h.cm.redos, "one rebuild per pass")

	h.advance(time.Minute)
	h.pass()
	assert.Equal(t, []string{"t1@n", "t2@n"}, h.cm.redos)

	h.cm.templates = []Template{tpl("t1", "a", "n"), tpl("t2", "a", "n")}
	h.advance(time.Minute)
	h.r.tplAt = time.Time{}
	h.pass()
	for _, x := range h.r.Status().Nodes {
		assert.Equal(t, StateServing, x.State, x.Name)
	}
}

func TestReconcilerNeverReleasesOperatorIsolatedNode(t *testing.T) {
	h := newHarness("a", "b")
	h.cm.templates = []Template{tpl("t1", "a", "b")}
	_, _ = h.nodes.SetNodeSchedulingDisabled(context.Background(), "b", true, "alice", "maintenance")
	for i := 0; i < 5; i++ {
		h.pass()
		h.advance(20 * time.Minute)
	}
	assert.Empty(t, h.cl.deleted)
	assert.Empty(t, h.nodes.deleted)
	assert.Len(t, h.cl.phs, 2, "operator-isolated node stays pinned")
}

func TestReconcilerHoldsScaleInWithoutTemplateInventory(t *testing.T) {
	h := newHarness("a", "b")
	h.cm.tplErr = errors.New("cubemaster down")
	for i := 0; i < 4; i++ {
		h.pass()
		h.advance(20 * time.Minute)
	}
	assert.False(t, h.nodes.snaps["a"].SchedulingDisabled)
	assert.False(t, h.nodes.snaps["b"].SchedulingDisabled)
	assert.NotEmpty(t, h.r.Status().Warnings)
}

func TestReconcilerDryRunChangesNothing(t *testing.T) {
	h := newHarness("a")
	h.r.cfg.DryRun = true
	h.pass()
	assert.Empty(t, h.cl.phs)
	assert.NotEmpty(t, h.r.Status().Actions)
}
