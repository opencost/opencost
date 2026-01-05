package prom

import (
	"fmt"

	prometheus "github.com/prometheus/client_golang/api"
)

const UpQuery = "up"

// PrometheusMetadata represents a validation result for prometheus running
// opencost.
type PrometheusMetadata struct {
	Running            bool `json:"running"`
	KubecostDataExists bool `json:"kubecostDataExists"`
}

// Validate tells the model what data prometheus has on it.
func Validate(cli prometheus.Client, config *OpenCostPrometheusConfig) (*PrometheusMetadata, error) {
	return validate(cli, validationQueryFor(config), config)
}

func validationQueryFor(config *OpenCostPrometheusConfig) string {
	if config.Offset != "" {
		return fmt.Sprintf("%s offset %s", UpQuery, config.Offset)
	}

	return UpQuery
}

// validate executes the prometheus query against the provided client.
func validate(cli prometheus.Client, q string, config *OpenCostPrometheusConfig) (*PrometheusMetadata, error) {
	ctx := NewContext(cli, config)

	resUp, _, err := ctx.QuerySync(q)
	if err != nil {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, err
	}

	if len(resUp) == 0 {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, fmt.Errorf("no running jobs on Prometheus at %s", ctx.QueryURL().Path)
	}

	running := false

	for _, result := range resUp {
		job, err := result.GetString("job")
		if err != nil {
			// Stop evaluating result if it does not have a job label (continue to next result)
			// We don't error here because not every result from `up` will necessarily have a job label.
			continue
		}

		// At least one job is running, so we can set running to true
		running = true

		if job == config.JobName {
			return &PrometheusMetadata{
				Running:            true,
				KubecostDataExists: true,
			}, err
		}
	}

	if !running {
		return &PrometheusMetadata{
			Running:            false,
			KubecostDataExists: false,
		}, fmt.Errorf("up query does not have job names")
	}

	return &PrometheusMetadata{
		Running:            true,
		KubecostDataExists: false,
	}, nil
}
