// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"testing"

	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/nodemanagement/model"
	"github.com/tencentcloud/CubeSandbox/CubeOps/internal/nodemanagement/store"
)

// A heartbeat that read a Redis snapshot from before an isolation must not
// write the old labels back: the registration row wins.
func TestApplyRegistrationLabelsRestoresIsolationLostInRedis(t *testing.T) {
	stale := &model.NodeSnapshot{NodeID: "n1", Labels: map[string]string{"zone": "a"}}
	reg := &store.NodeRegistration{NodeID: "n1",
		LabelsJSON: `{"zone":"a","` + model.LabelSchedulingDisabled + `":"` + model.LabelSchedulingDisabledValue + `"}`}

	applyRegistrationLabels(stale, reg)
	stale.SchedulingDisabled = snapSchedulingDisabled(stale)

	if !stale.SchedulingDisabled {
		t.Fatalf("isolation from the registration row was not applied: labels=%v", stale.Labels)
	}
	if stale.Labels["zone"] != "a" {
		t.Fatalf("other labels lost: %v", stale.Labels)
	}
}

func TestApplyRegistrationLabelsUnisolateAndCorrupt(t *testing.T) {
	snap := &model.NodeSnapshot{NodeID: "n1",
		Labels: map[string]string{model.LabelSchedulingDisabled: model.LabelSchedulingDisabledValue}}
	applyRegistrationLabels(snap, &store.NodeRegistration{NodeID: "n1", LabelsJSON: `{}`})
	if snapSchedulingDisabled(snap) {
		t.Fatalf("unisolate in the registration row was not applied")
	}

	applyRegistrationLabels(snap, &store.NodeRegistration{NodeID: "n1", LabelsJSON: `{not json`})
	if !snap.LabelsJSONCorrupt {
		t.Fatalf("corrupt labels must be flagged")
	}
}
