// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package autoscale

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// CubeMaster is what the reconciler needs from CubeMaster.
type CubeMaster interface {
	// CountNodeSandboxes counts running and paused sandboxes on a node.
	CountNodeSandboxes(ctx context.Context, hostID string) (int, error)
	// Templates lists READY templates with their per-node replicas.
	Templates(ctx context.Context) ([]Template, error)
	// Snapshots lists snapshots that are not deleted.
	Snapshots(ctx context.Context) ([]Snapshot, error)
	// RedoTemplateOnNode rebuilds one template's replica on one node.
	RedoTemplateOnNode(ctx context.Context, templateID, nodeID string) error
}

// Template is a READY template and where its replicas are.
type Template struct {
	ID       string    `json:"template_id"`
	Status   string    `json:"status"`
	Replicas []Replica `json:"replicas,omitempty"`
}

// Replica is one template replica on one node.
type Replica struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"`
	Phase  string `json:"phase,omitempty"`
}

// Snapshot is the part of a snapshot that decides whether it lives only on
// its origin node.
type Snapshot struct {
	ID           string `json:"snapshot_id"`
	Status       string `json:"status"`
	OriginNodeID string `json:"origin_node_id"`
	Backend      string `json:"backend"`
	RemoteStatus string `json:"remote_status"`
}

const (
	replicaReady = "READY"
	templateOK   = "READY"
)

// inFlightPhases mark replica work still running on a node.
var inFlightPhases = map[string]bool{"PENDING": true, "DISTRIBUTING": true, "SNAPSHOTTING": true, "CLEANING": true}

// LocalOnly reports whether the snapshot can only be restored on its origin.
// Anything that is not provably remote and ready counts as local; snapshots
// being created or deleted count too (their node is still involved). Failed
// and deleted snapshots hold nothing restorable.
func (s Snapshot) LocalOnly() bool {
	switch strings.ToUpper(s.Status) {
	case "DELETED", "FAILED":
		return false
	case "CREATING", "DELETING":
		return true
	}
	b := strings.ToLower(s.Backend)
	remote := (b == "s3" || b == "jfs") && strings.EqualFold(s.RemoteStatus, "ready")
	return !remote
}

// rawCubeMaster is the subset of the CubeOps CubeMaster client used here.
type rawCubeMaster interface {
	CountNodeSandboxes(ctx context.Context, hostID string) (int, error)
	ListTemplates(ctx context.Context, templateID string, includeRequest bool) (json.RawMessage, error)
	ListSnapshots(ctx context.Context, params map[string]string) (json.RawMessage, error)
	RedoTemplate(ctx context.Context, body interface{}) (json.RawMessage, error)
}

// NewCubeMaster adapts the CubeOps CubeMaster client.
func NewCubeMaster(c rawCubeMaster) CubeMaster { return &cubeMaster{c: c} }

type cubeMaster struct{ c rawCubeMaster }

type cmRet struct {
	Ret *struct {
		Code int    `json:"ret_code"`
		Msg  string `json:"ret_msg"`
	} `json:"ret"`
}

func (r cmRet) err(what string) error {
	if r.Ret == nil {
		return fmt.Errorf("%s: response without ret", what)
	}
	if r.Ret.Code != 200 && r.Ret.Code != 0 {
		return fmt.Errorf("%s: ret_code=%d %s", what, r.Ret.Code, r.Ret.Msg)
	}
	return nil
}

func (m *cubeMaster) CountNodeSandboxes(ctx context.Context, hostID string) (int, error) {
	return m.c.CountNodeSandboxes(ctx, hostID)
}

func (m *cubeMaster) Templates(ctx context.Context) ([]Template, error) {
	raw, err := m.c.ListTemplates(ctx, "", false)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	var list struct {
		cmRet
		Data []Template `json:"data"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("list templates: decode: %w", err)
	}
	if err := list.err("list templates"); err != nil {
		return nil, err
	}
	out := make([]Template, 0, len(list.Data))
	for _, t := range list.Data {
		if !strings.EqualFold(t.Status, templateOK) {
			continue
		}
		raw, err := m.c.ListTemplates(ctx, t.ID, false)
		if err != nil {
			return nil, fmt.Errorf("get template %s: %w", t.ID, err)
		}
		var one struct {
			cmRet
			Template
		}
		if err := json.Unmarshal(raw, &one); err != nil {
			return nil, fmt.Errorf("get template %s: decode: %w", t.ID, err)
		}
		if err := one.err("get template " + t.ID); err != nil {
			return nil, err
		}
		if !strings.EqualFold(one.Status, templateOK) {
			continue
		}
		out = append(out, one.Template)
	}
	return out, nil
}

func (m *cubeMaster) Snapshots(ctx context.Context) ([]Snapshot, error) {
	var out []Snapshot
	token := ""
	for page := 0; page < 100; page++ {
		params := map[string]string{"limit": "200"}
		if token != "" {
			params["next_token"] = token
		}
		raw, err := m.c.ListSnapshots(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("list snapshots: %w", err)
		}
		var resp struct {
			cmRet
			Data      []Snapshot `json:"data"`
			NextToken string     `json:"next_token"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("list snapshots: decode: %w", err)
		}
		if err := resp.err("list snapshots"); err != nil {
			return nil, err
		}
		for _, s := range resp.Data {
			if !strings.EqualFold(s.Status, "DELETED") {
				out = append(out, s)
			}
		}
		if resp.NextToken == "" || resp.NextToken == token {
			return out, nil
		}
		token = resp.NextToken
	}
	return nil, fmt.Errorf("list snapshots: too many pages")
}

func (m *cubeMaster) RedoTemplateOnNode(ctx context.Context, templateID, nodeID string) error {
	// Fields are flat: CubeMaster's redo request embeds Request.
	body := map[string]any{
		"requestID":          uuid.NewString(),
		"template_id":        templateID,
		"distribution_scope": []string{nodeID},
	}
	raw, err := m.c.RedoTemplate(ctx, body)
	if err != nil {
		return fmt.Errorf("redo template %s on %s: %w", templateID, nodeID, err)
	}
	var r cmRet
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("redo template %s on %s: decode: %w", templateID, nodeID, err)
	}
	return r.err("redo template " + templateID + " on " + nodeID)
}
