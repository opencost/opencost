package heartbeat

import "github.com/opencost/opencost/core/pkg/exporter"

// NewHeartbeatEncoder returns a JSON encoder used to encode Heartbeat events.
func NewHeartbeatEncoder() exporter.Encoder[Heartbeat] {
	return exporter.NewJSONEncoder[Heartbeat]()
}
