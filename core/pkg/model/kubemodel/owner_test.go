package kubemodel

import (
	"testing"
)

func TestParseOwnerKind(t *testing.T) {
	tests := []struct {
		input    string
		expected OwnerKind
	}{
		// Exact canonical values
		{"deployment", OwnerKindDeployment},
		{"statefulset", OwnerKindStatefulSet},
		{"daemonset", OwnerKindDaemonSet},
		{"job", OwnerKindJob},
		{"cronjob", OwnerKindCronJob},
		{"replicaset", OwnerKindReplicaSet},
		// Case-insensitive
		{"Deployment", OwnerKindDeployment},
		{"StatefulSet", OwnerKindStatefulSet},
		{"DaemonSet", OwnerKindDaemonSet},
		{"Job", OwnerKindJob},
		{"CronJob", OwnerKindCronJob},
		{"ReplicaSet", OwnerKindReplicaSet},
		{"DEPLOYMENT", OwnerKindDeployment},
		// Unknown input: lowercased passthrough (no mapping)
		{"Rollout", OwnerKind("rollout")},
		{"CustomController", OwnerKind("customcontroller")},
		{"", OwnerKind("")},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseOwnerKind(tt.input)
			if got != tt.expected {
				t.Errorf("ParseOwnerKind(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
