package exporter

import (
	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/heartbeat"
)

// NewHeartbeatEncoder returns a JSON encoder used to encode Heartbeat events.
func NewHeartbeatEncoder() exporter.Encoder[heartbeat.Heartbeat] {
	return exporter.NewJSONEncoder[heartbeat.Heartbeat]()
}
