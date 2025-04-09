package collector

import (
	"fmt"
	"time"

	"golang.org/x/exp/maps"
)

// UpdateRecorderCollector is a mock MetricsCollector which records the arguments passed to the update function in an array
type UpdateRecorderCollector struct {
	updateArgs []UpdateArgs
}

func (u *UpdateRecorderCollector) Register(collector *MetricCollector) error {
	panic("this mock does not support this action")
}

func (u *UpdateRecorderCollector) Unregister(collectorID MetricCollectorID) bool {
	panic("this mock does not support this action")
}

func (u *UpdateRecorderCollector) Query(collectorID MetricCollectorID) ([]*MetricResult, error) {
	panic("this mock does not support this action")
}

func (u *UpdateRecorderCollector) Update(metricName string, labels map[string]string, value float64, timestamp *time.Time, additionalInformation map[string]string) {
	u.updateArgs = append(u.updateArgs, UpdateArgs{
		metricName:            metricName,
		labels:                labels,
		value:                 value,
		timestamp:             timestamp,
		additionalInformation: additionalInformation,
	})
}

type UpdateArgs struct {
	metricName            string
	labels                map[string]string
	value                 float64
	timestamp             *time.Time
	additionalInformation map[string]string
}

func (u UpdateArgs) equals(that UpdateArgs) error {
	if u.metricName != that.metricName {
		return fmt.Errorf("expected metric name %s, got %s", u.metricName, that.metricName)
	}

	if !maps.Equal(u.labels, that.labels) {
		return fmt.Errorf("expected labels %s, got %s", u.labels, that.labels)
	}

	if u.value != that.value {
		return fmt.Errorf("expected value %f, got %f", u.value, that.value)
	}

	if that.timestamp != nil {
		if u.timestamp == nil {
			return fmt.Errorf("expected timestamp nil, got %v", that.timestamp)
		}
		if !u.timestamp.Equal(*that.timestamp) {
			return fmt.Errorf("expected timestamp %s, got %s", u.timestamp, that.timestamp)
		}
	} else if u.timestamp != nil {
		return fmt.Errorf("expected timestamp %v, got nil", u.timestamp)
	}

	if !maps.Equal(u.additionalInformation, that.additionalInformation) {
		return fmt.Errorf("expected additionalInformation %v, got %v", u.additionalInformation, that.additionalInformation)
	}

	return nil
}
