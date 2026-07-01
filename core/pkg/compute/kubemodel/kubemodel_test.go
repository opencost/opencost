package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
)

const testClusterUID = "cluster-uid-1"

func newTestWindow() (time.Time, time.Time) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	return start, start.Add(time.Hour)
}

// seedCluster sets the minimum overrides needed for computeCluster to succeed.
func seedCluster(ds *source.MockOpenCostDataSource, start, end time.Time) {
	ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
		{UID: testClusterUID, Cluster: "my-cluster", Provider: "aws"},
	})
	ds.Querier.SetOverride(source.QueryClusterUptime, []*source.UptimeResult{
		{UID: testClusterUID, First: start, Last: end},
	})
}

// ---- NewKubeModel ----

func TestNewKubeModel_NilDataSource(t *testing.T) {
	_, err := NewKubeModel(testClusterUID, false, nil)
	require.Error(t, err)
}

// ---- ComputeKubeModelSet orchestration ----

func TestComputeKubeModelSet(t *testing.T) {
	start, end := newTestWindow()

	tests := []struct {
		name    string
		setup   func(ds *source.MockOpenCostDataSource)
		wantErr bool
		check   func(t *testing.T, kms *kubemodel.KubeModelSet)
	}{
		{
			name: "cluster UID not found returns error",
			setup: func(ds *source.MockOpenCostDataSource) {
				ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
					{UID: "wrong-uid", Cluster: "other"},
				})
			},
			wantErr: true,
		},
		{
			name: "cluster uptime missing: cluster nil but other compute functions still run",
			setup: func(ds *source.MockOpenCostDataSource) {
				// cluster info present but no uptime — cluster won't register, but no error
				ds.Querier.SetOverride(source.QueryClusterInfo, []*source.ClusterInfoResult{
					{UID: testClusterUID, Cluster: "my-cluster"},
				})
				ds.Querier.SetOverride(source.QueryNamespaceInfo, []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "default"},
				})
				ds.Querier.SetOverride(source.QueryNamespaceUptime, []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
				})
			},
			check: func(t *testing.T, kms *kubemodel.KubeModelSet) {
				assert.Nil(t, kms.Cluster, "cluster with no uptime should not be registered")
				assert.NotEmpty(t, kms.Namespaces, "namespace compute should still run after cluster registration fails")
			},
		},
		{
			name: "all compute functions produce results when given data",
			setup: func(ds *source.MockOpenCostDataSource) {
				seedCluster(ds, start, end)
				ds.Querier.SetOverride(source.QueryNodeInfo, []*source.NodeInfoResult{
					{UID: "node-1", Node: "node-a"},
				})
				ds.Querier.SetOverride(source.QueryNodeUptime, []*source.UptimeResult{
					{UID: "node-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryNamespaceInfo, []*source.NamespaceInfoResult{
					{UID: "ns-1", Namespace: "default"},
				})
				ds.Querier.SetOverride(source.QueryNamespaceUptime, []*source.UptimeResult{
					{UID: "ns-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryPodInfo, []*source.PodInfoResult{
					{UID: "pod-1", Pod: "my-pod", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryPodUptime, []*source.UptimeResult{
					{UID: "pod-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryContainerUptime, []*source.ContainerUptimeResult{
					{UptimeResult: source.UptimeResult{UID: "pod-1", First: start, Last: end}, Container: "app"},
				})
				ds.Querier.SetOverride(source.QueryDeploymentInfo, []*source.DeploymentInfoResult{
					{UID: "dep-1", Deployment: "my-dep", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryDeploymentUptime, []*source.UptimeResult{
					{UID: "dep-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryStatefulSetInfo, []*source.StatefulSetInfoResult{
					{UID: "sts-1", StatefulSet: "my-sts", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryStatefulSetUptime, []*source.UptimeResult{
					{UID: "sts-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryDaemonSetInfo, []*source.DaemonSetInfoResult{
					{UID: "ds-1", DaemonSet: "my-ds", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryDaemonSetUptime, []*source.UptimeResult{
					{UID: "ds-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryJobInfo, []*source.JobInfoResult{
					{UID: "job-1", Job: "my-job", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryJobUptime, []*source.UptimeResult{
					{UID: "job-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryCronJobInfo, []*source.CronJobInfoResult{
					{UID: "cj-1", CronJob: "my-cj", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryCronJobUptime, []*source.UptimeResult{
					{UID: "cj-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryReplicaSetInfo, []*source.ReplicaSetInfoResult{
					{UID: "rs-1", ReplicaSet: "my-rs", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryReplicaSetUptime, []*source.UptimeResult{
					{UID: "rs-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryResourceQuotaInfo, []*source.ResourceQuotaInfoResult{
					{UID: "rq-1", ResourceQuota: "default-quota", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryResourceQuotaUptime, []*source.UptimeResult{
					{UID: "rq-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryServiceInfo, []*source.ServiceInfoResult{
					{UID: "svc-1", Service: "my-svc", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryServiceUptime, []*source.UptimeResult{
					{UID: "svc-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryKMPVInfo, []*source.PVInfoResult{
					{UID: "pv-1", PersistentVolume: "my-pv"},
				})
				ds.Querier.SetOverride(source.QueryPVUptime, []*source.UptimeResult{
					{UID: "pv-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryPVBytes, []*source.PVBytesResult{
					{UID: "pv-1", Value: 10 * 1024 * 1024 * 1024},
				})
				ds.Querier.SetOverride(source.QueryKMPVCInfo, []*source.PVCInfoResult{
					{UID: "pvc-1", PersistentVolumeClaim: "data-claim", NamespaceUID: "ns-1"},
				})
				ds.Querier.SetOverride(source.QueryPVCUptime, []*source.UptimeResult{
					{UID: "pvc-1", First: start, Last: end},
				})
				ds.Querier.SetOverride(source.QueryDCGMDeviceInfo, []*source.DCGMDeviceInfoResult{
					{UUID: "GPU-abc123", Device: "nvidia0", ModelName: "A100"},
				})
				ds.Querier.SetOverride(source.QueryDCGMDeviceUptime, []*source.DCGMDeviceUptimeResult{
					{UUID: "GPU-abc123", First: start, Last: end},
				})
			},
			check: func(t *testing.T, kms *kubemodel.KubeModelSet) {
				assert.NotNil(t, kms.Cluster)
				assert.NotEmpty(t, kms.Nodes)
				assert.NotEmpty(t, kms.Namespaces)
				assert.NotEmpty(t, kms.Pods)
				assert.NotEmpty(t, kms.Containers)
				assert.NotEmpty(t, kms.Deployments)
				assert.NotEmpty(t, kms.StatefulSets)
				assert.NotEmpty(t, kms.DaemonSets)
				assert.NotEmpty(t, kms.Jobs)
				assert.NotEmpty(t, kms.CronJobs)
				assert.NotEmpty(t, kms.ReplicaSets)
				assert.NotEmpty(t, kms.ResourceQuotas)
				assert.NotEmpty(t, kms.Services)
				assert.NotEmpty(t, kms.PersistentVolumes)
				assert.NotEmpty(t, kms.PersistentVolumeClaims)
				//assert.NotEmpty(t, kms.DCGMDevices)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := source.NewMockOpenCostDataSource()
			ds.ResolutionValue = 5 * time.Minute
			tt.setup(ds)

			km, err := NewKubeModel(testClusterUID, false, ds)
			require.NoError(t, err)

			kms, err := km.ComputeKubeModelSet(start, end)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, kms)
			}
		})
	}
}
