package heartbeat

import (
	"time"
)

// HeartbeatEventName is used to represent the name of the heartbeat pipeline event to categorize for storage.
const HeartbeatEventName string = "heartbeat"

// Heartbeat is a payload struct that contains custom information and the timestamp of the heartbeat.
type Heartbeat struct {
	Id          string         `json:"id"`
	Timestamp   time.Time      `json:"timestamp"`
	Uptime      uint64         `json:"uptime"`
	Application string         `json:"application"`
	Version     string         `json:"version"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// NewHeartbeat creates a new Heartbeat instance with the provided parameters.
// The `id` is a unique identifier for the heartbeat, `timestamp` is the time of the heartbeat,
// `uptime` is the uptime in seconds, `version` is the version of the heartbeat, and `metadata`
// is a pointer to a generic type that can hold any additional information. Metadata _can_ be omitted
// by passing `nil`.
func NewHeartbeat(id string, timestamp time.Time, uptime uint64, application string, version string, metadata map[string]any) *Heartbeat {
	return &Heartbeat{
		Id:          id,
		Timestamp:   timestamp,
		Uptime:      uptime,
		Application: application,
		Version:     version,
		Metadata:    metadata,
	}
}
