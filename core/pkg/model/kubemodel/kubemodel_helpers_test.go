package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requireWindowEqual compares two start/end time pairs using time.Time.Equal,
// which is insensitive to monotonic clock readings and timezone representation.
func requireWindowEqual(t *testing.T, thisStart, thatStart, thisEnd, thatEnd time.Time) {
	t.Helper()
	require.True(t, thisStart.Equal(thatStart), "Start mismatch: %v != %v", thisStart, thatStart)
	require.True(t, thisEnd.Equal(thatEnd), "End mismatch: %v != %v", thisEnd, thatEnd)
}

// DiagnosticEquals asserts that two Diagnostics have identical state.
// Timestamp is excluded from comparison since it is volatile across instances.
func DiagnosticEquals(t *testing.T, this, that Diagnostic) {
	t.Helper()
	require.Equal(t, this.Level, that.Level)
	require.Equal(t, this.Message, that.Message)
	require.Equal(t, this.Details, that.Details)
}

// MetadataEquals asserts that two Metadata structs have identical state.
// Timestamps (CreatedAt, CompletedAt, Diagnostic.Timestamp) are excluded
// from comparison since they are volatile across instances.
func MetadataEquals(t *testing.T, this, that *Metadata) {
	t.Helper()
	require.Equal(t, this.ObjectCount, that.ObjectCount)
	require.Equal(t, this.DiagnosticLevel, that.DiagnosticLevel)
	require.Equal(t, len(this.Diagnostics), len(that.Diagnostics))
	for i := range this.Diagnostics {
		DiagnosticEquals(t, this.Diagnostics[i], that.Diagnostics[i])
	}
}

// KubeModelSetEquals asserts that two KubeModelSets have identical state.
// Metadata timestamps (CreatedAt, CompletedAt, Diagnostic.Timestamp) are
// excluded from comparison since they are volatile across instances.
func KubeModelSetEquals(t *testing.T, this, that *KubeModelSet) {
	t.Helper()
	require.Equal(t, this.Window, that.Window)
	require.Equal(t, this.Cluster, that.Cluster)
	MetadataEquals(t, this.Metadata, that.Metadata)
	require.Equal(t, this.Namespaces, that.Namespaces)
	require.Equal(t, this.ResourceQuotas, that.ResourceQuotas)
	require.Equal(t, this.Containers, that.Containers)
	require.Equal(t, this.Deployments, that.Deployments)
	require.Equal(t, this.StatefulSets, that.StatefulSets)
	require.Equal(t, this.DaemonSets, that.DaemonSets)
	require.Equal(t, this.Jobs, that.Jobs)
	require.Equal(t, this.CronJobs, that.CronJobs)
	require.Equal(t, this.ReplicaSets, that.ReplicaSets)
	require.Equal(t, this.Nodes, that.Nodes)
	require.Equal(t, this.Pods, that.Pods)
	require.Equal(t, this.PersistentVolumeClaims, that.PersistentVolumeClaims)
	require.Equal(t, this.Services, that.Services)
	require.Equal(t, this.PersistentVolumes, that.PersistentVolumes)
	require.Equal(t, this.DCGMDevices, that.DCGMDevices)
}
