package source

import "time"

// WALStatus reports the health of a data source's write-ahead log: whether updates are being
// persisted, and how complete the most recent restore was. Durations serialize to JSON as integer
// nanoseconds.
type WALStatus struct {
	// Enabled is false when the data source has no WAL configured; all other fields are zero.
	Enabled bool `json:"enabled"`

	// LastExportSuccess is the time of the most recent successful WAL write.
	LastExportSuccess time.Time `json:"lastExportSuccess"`
	// LastExportError is the most recent WAL write error, with any URL query strings and
	// credentials removed.
	LastExportError string `json:"lastExportError,omitempty"`
	// LastExportErrorAt is the time of the most recent WAL write error.
	LastExportErrorAt time.Time `json:"lastExportErrorAt"`
	// ConsecutiveExportFailures is the number of WAL writes that have failed since the last success.
	ConsecutiveExportFailures int `json:"consecutiveExportFailures"`
	// ExportFailuresTotal is the number of WAL writes that have failed since start.
	ExportFailuresTotal uint64 `json:"exportFailuresTotal"`

	// RestoreCompleted is true once the startup restore has finished, whether or not it had errors.
	RestoreCompleted bool `json:"restoreCompleted"`
	// RestoreStartedAt is the time the startup restore began.
	RestoreStartedAt time.Time `json:"restoreStartedAt"`
	// RestoreListError is set when the WAL objects could not be listed, meaning nothing was restored.
	RestoreListError string `json:"restoreListError,omitempty"`
	// RestoreObjectsSeen is the number of WAL objects inside the retention window.
	RestoreObjectsSeen int `json:"restoreObjectsSeen"`
	// RestoreObjectsApplied is the number of WAL objects successfully read, decoded and applied.
	RestoreObjectsApplied int `json:"restoreObjectsApplied"`
	// RestoreErrors is the number of WAL objects that could not be read or decoded.
	RestoreErrors int `json:"restoreErrors"`
	// RestoreDuration is how long the startup restore took.
	RestoreDuration time.Duration `json:"restoreDuration"`
	// RestoreOldest and RestoreNewest are the timestamps of the oldest and newest applied objects.
	RestoreOldest time.Time `json:"restoreOldest"`
	RestoreNewest time.Time `json:"restoreNewest"`
	// RestoreLargestGap is the largest interval between consecutive applied objects, starting at
	// RestoreLargestGapStart. A gap much larger than the scrape interval means history in that range
	// was never persisted or could not be restored.
	RestoreLargestGap      time.Duration `json:"restoreLargestGap"`
	RestoreLargestGapStart time.Time     `json:"restoreLargestGapStart"`
	// RestoreTailGap is the interval between the newest applied object (or the start of the retention
	// window, if nothing was applied) and the start of the restore: history that was not persisted
	// before the restart, whether because the process was down or because writes were failing.
	RestoreTailGap time.Duration `json:"restoreTailGap"`
}

// WALStatusProvider is optionally implemented by an OpenCostDataSource that persists its state
// through a write-ahead log.
type WALStatusProvider interface {
	WALStatus() WALStatus
}
