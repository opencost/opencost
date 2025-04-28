package pathing

// StoragePathFormatter is an interface used to format storage paths for exporting data types.
type StoragePathFormatter[T any] interface {
	// RootDir returns the root directory for the storage path.
	RootDir() string

	// ToFullPath returns the full path to a file name within the storage
	// directory leveraging a prefix and an incoming T type (generally a daterange or timestamp).
	ToFullPath(prefix string, in T, fileExt string) string
}
