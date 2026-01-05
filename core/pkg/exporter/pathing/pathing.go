package pathing

// StoragePathFormatter is an interface used to format storage paths for exporting data types.
type StoragePathFormatter[T any] interface {
	// Dir returns the directory where files are placed
	Dir() string

	// ToFullPath returns the full path to a file name within the storage
	// directory leveraging a prefix and an incoming T type (generally a daterange or timestamp).
	ToFullPath(prefix string, in T, fileExt string) string
}
