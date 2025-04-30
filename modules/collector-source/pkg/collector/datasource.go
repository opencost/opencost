package collector

import (
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/source"
)

type collectorDataSource struct {
	metricsQuerier *CollectorMetricsQuerier
}

func (c collectorDataSource) RegisterEndPoints(router *httprouter.Router) {
	return
}

func (c collectorDataSource) Metrics() source.MetricsQuerier {
	return c.metricsQuerier
}

func (c collectorDataSource) ClusterMap() clusters.ClusterMap {
	//TODO implement me
	panic("implement me")
}

func (c collectorDataSource) ClusterInfo() clusters.ClusterInfoProvider {
	//TODO implement me
	panic("implement me")
}

func (c collectorDataSource) BatchDuration() time.Duration {
	//TODO implement me
	panic("implement me")
}

func (c collectorDataSource) Resolution() time.Duration {
	//TODO implement me
	panic("implement me")
}
