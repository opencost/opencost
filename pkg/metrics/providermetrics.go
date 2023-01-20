package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	AwsSavingsPlanFetchStatus prometheus.Gauge
)

func init() {
	AwsSavingsPlanFetchStatus = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "opencost_aws_savings_plan_fetch_status",
		Help: "opencost_aws_savings_plan_fetch_status is 1 if savings plan data was fetched 0 if there was an error",
	})
	prometheus.MustRegister(AwsSavingsPlanFetchStatus)
}
