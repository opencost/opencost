package fileutil

import (
	"io"
	"os"
)

// LockedFileWriter is an io.WriteCloser implementation of a writer that will use flock
// for file locking during the write and unlock on close.
type LockedFileWriter struct {
	f *os.File
}

// Creates a new FLocking file writer that will flock a file on open, and unlock when the writer is
// closed.
func NewLockedFileWriter(path string) (io.WriteCloser, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	if err := LockFile(f); err != nil {
		f.Close()
		return nil, err
	}

	if err := f.Truncate(0); err != nil {
		UnlockFile(f)
		f.Close()
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		UnlockFile(f)
		f.Close()
		return nil, err
	}

	return &LockedFileWriter{
		f: f,
	}, nil
}

func (lf *LockedFileWriter) Write(p []byte) (int, error) {
	return lf.f.Write(p)
}

func (lf *LockedFileWriter) Close() error {
	if err := UnlockFile(lf.f); err != nil {
		lf.f.Close()
		return err
	}
	return lf.f.Close()
}
