// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package autoscale

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rawFake struct {
	templates map[string]string // "" = list
	snapPages []string
	pageReqs  []map[string]string
	redo      map[string]any
	redoResp  string
}

func (f *rawFake) CountNodeSandboxes(context.Context, string) (int, error) { return 0, nil }
func (f *rawFake) ListTemplates(_ context.Context, id string, _ bool) (json.RawMessage, error) {
	return json.RawMessage(f.templates[id]), nil
}
func (f *rawFake) ListSnapshots(_ context.Context, p map[string]string) (json.RawMessage, error) {
	f.pageReqs = append(f.pageReqs, p)
	return json.RawMessage(f.snapPages[len(f.pageReqs)-1]), nil
}
func (f *rawFake) RedoTemplate(_ context.Context, body interface{}) (json.RawMessage, error) {
	f.redo = body.(map[string]any)
	return json.RawMessage(f.redoResp), nil
}

func TestCubeMasterAdapterParsesInventory(t *testing.T) {
	f := &rawFake{
		templates: map[string]string{
			"":   `{"ret":{"ret_code":200,"ret_msg":"success"},"data":[{"template_id":"t1","status":"READY"},{"template_id":"t2","status":"FAILED"}],"total":2}`,
			"t1": `{"ret":{"ret_code":200,"ret_msg":"success"},"template_id":"t1","status":"READY","replicas":[{"node_id":"a","status":"READY","phase":"READY"},{"node_id":"b","status":"FAILED","phase":"DISTRIBUTED"}]}`,
		},
		snapPages: []string{
			`{"ret":{"ret_code":200},"data":[{"snapshot_id":"s1","status":"READY","origin_node_id":"a","backend":"xfs"}],"next_token":"p2"}`,
			`{"ret":{"ret_code":200},"data":[{"snapshot_id":"s2","status":"READY","origin_node_id":"b","backend":"s3","remote_status":"ready"},{"snapshot_id":"s3","status":"DELETED","origin_node_id":"a"}]}`,
		},
		redoResp: `{"ret":{"ret_code":200,"ret_msg":"success"},"job_id":"j1"}`,
	}
	cm := NewCubeMaster(f)
	ctx := context.Background()

	tpls, err := cm.Templates(ctx)
	require.NoError(t, err)
	require.Len(t, tpls, 1, "only READY templates")
	assert.Equal(t, "t1", tpls[0].ID)
	assert.Len(t, tpls[0].Replicas, 2)

	snaps, err := cm.Snapshots(ctx)
	require.NoError(t, err)
	require.Len(t, snaps, 2, "deleted snapshots dropped")
	assert.Equal(t, "p2", f.pageReqs[1]["next_token"])
	assert.True(t, snaps[0].LocalOnly(), "xfs is local")
	assert.False(t, snaps[1].LocalOnly(), "s3 + remote ready is not")

	require.NoError(t, cm.RedoTemplateOnNode(ctx, "t1", "b"))
	assert.Equal(t, "t1", f.redo["template_id"])
	assert.Equal(t, []string{"b"}, f.redo["distribution_scope"])
	assert.NotEmpty(t, f.redo["requestID"], "flat requestID, not nested under request")
	_, nested := f.redo["request"]
	assert.False(t, nested)

	f.redoResp = `{"ret":{"ret_code":130500,"ret_msg":"boom"}}`
	assert.Error(t, cm.RedoTemplateOnNode(ctx, "t1", "b"))
}

func TestSnapshotLocalOnly(t *testing.T) {
	assert.True(t, Snapshot{Status: "CREATING", Backend: "s3", RemoteStatus: "ready"}.LocalOnly())
	assert.False(t, Snapshot{Status: "FAILED"}.LocalOnly())
	assert.True(t, Snapshot{Status: "READY", Backend: "jfs", RemoteStatus: "uploading"}.LocalOnly())
	assert.True(t, Snapshot{Status: "SOMETHING_NEW"}.LocalOnly(), "unknown counts as local")
}
