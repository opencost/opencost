package pathing

import "time"

// StoragePathFormatter is a contract for an object capable of building storage paths for pipeline files
// provided a window.
type StoragePathFormatter interface {
	// RootDir returns the root directory for the storage path.
	RootDir() string

	// ToFullPath returns the full path to a file name within the storage
	// directory leveraging a prefix and start and end times.
	ToFullPath(prefix string, start, end time.Time) string
}
