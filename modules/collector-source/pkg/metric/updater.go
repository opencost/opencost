package metric

import (
	"fmt"
	"time"

	"golang.org/x/exp/maps"
)

type MetricUpdater interface {
	// Update accepts the name of a metric, the label set and values to update the metric, the updated Value, and a Timestamp.
	// This method does not accept a `MetricCollectorID` because it provides updates across many potential MetricCollector instances
	// which utilize the same metric.
	Update(metricName string, labels map[string]string, value float64, timestamp *time.Time, additionalInformation map[string]string)
}

// ArgRecordUpdater is a mock MetricStore which records the arguments passed to the update function in an array
type ArgRecordUpdater struct {
	UpdateArgs []UpdateArgs
}

func (u *ArgRecordUpdater) Update(metricName string, labels map[string]string, value float64, timestamp *time.Time, additionalInformation map[string]string) {
	u.UpdateArgs = append(u.UpdateArgs, UpdateArgs{
		MetricName:            metricName,
		Labels:                labels,
		Value:                 value,
		Timestamp:             timestamp,
		AdditionalInformation: additionalInformation,
	})
}

type UpdateArgs struct {
	MetricName            string
	Labels                map[string]string
	Value                 float64
	Timestamp             *time.Time
	AdditionalInformation map[string]string
}

func (u UpdateArgs) Equals(that UpdateArgs) error {
	if u.MetricName != that.MetricName {
		return fmt.Errorf("expected metric name %s, got %s", u.MetricName, that.MetricName)
	}

	if !maps.Equal(u.Labels, that.Labels) {
		return fmt.Errorf("expected Labels %s, got %s", u.Labels, that.Labels)
	}

	if u.Value != that.Value {
		return fmt.Errorf("expected Value %f, got %f", u.Value, that.Value)
	}

	if that.Timestamp != nil {
		if u.Timestamp == nil {
			return fmt.Errorf("expected Timestamp nil, got %v", that.Timestamp)
		}
		if !u.Timestamp.Equal(*that.Timestamp) {
			return fmt.Errorf("expected Timestamp %s, got %s", u.Timestamp, that.Timestamp)
		}
	} else if u.Timestamp != nil {
		return fmt.Errorf("expected Timestamp %v, got nil", u.Timestamp)
	}

	if !maps.Equal(u.AdditionalInformation, that.AdditionalInformation) {
		return fmt.Errorf("expected AdditionalInformation %v, got %v", u.AdditionalInformation, that.AdditionalInformation)
	}

	return nil
}
