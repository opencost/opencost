package kubemodel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opencost/opencost/core/pkg/source"
)

// pinnableRecordQuerier records calls made to it directly, and hands out a separate recording querier
// over the seeded data when pinned.
type pinnableRecordQuerier struct {
	*source.RecordMetricsQuerier
	pinned   *source.RecordMetricsQuerier
	pins     int
	releases int
}

func (p *pinnableRecordQuerier) Pin() (source.MetricsQuerier, func()) {
	p.pins++
	return p.pinned, func() { p.releases++ }
}

type pinnableDataSource struct {
	*source.MockOpenCostDataSource
	querier *pinnableRecordQuerier
}

func (d *pinnableDataSource) Metrics() source.MetricsQuerier { return d.querier }

// Every query issued while computing a KubeModelSet must go through one pinned querier, so that all
// resources are computed from one consistent state of the data source.
func TestComputeKubeModelSet_PinsDataSource(t *testing.T) {
	start, end := newTestWindow()

	seeded := source.NewMockOpenCostDataSource()
	seedCluster(seeded, start, end)

	querier := &pinnableRecordQuerier{
		RecordMetricsQuerier: source.NewRecordMetricsQuerier(source.NewMockMetricsQuerier()),
		pinned:               source.NewRecordMetricsQuerier(seeded.Querier),
	}
	ds := &pinnableDataSource{MockOpenCostDataSource: source.NewMockOpenCostDataSource(), querier: querier}

	km, err := NewKubeModel(testClusterUID, false, ds)
	require.NoError(t, err)

	kms, err := km.ComputeKubeModelSet(start, end)
	require.NoError(t, err)
	require.NotNil(t, kms.Cluster, "cluster should be computed from the pinned data")

	assert.Empty(t, querier.Calls, "no query should bypass the pinned querier")
	assert.NotEmpty(t, querier.pinned.Calls)
	assert.Equal(t, 1, querier.pins)
	assert.Equal(t, 1, querier.releases)
}
