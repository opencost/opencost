package storage

import (
	"os"
	"time"
)

// DoesNotExistError is used as a generic error to return when a target path does not
// exist in storage. Equivalent to os.ErrorNotExist such that it will work with os.IsNotExist(err)
var DoesNotExistError = os.ErrNotExist

// StorageInfo is a data object containing basic information about the path in storage.
type StorageInfo struct {
	Name    string    // base name of the file
	Size    int64     // length in bytes for regular files
	ModTime time.Time // modification time
}
