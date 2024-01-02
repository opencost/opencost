package fileutil

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"syscall"
)

// File exists has three different return cases that should be handled:
//  1. File exists and is not a directory (true, nil)
//  2. File does not exist (false, nil)
//  3. File may or may not exist. Error occurred during stat (false, error)
//
// The third case represents the scenario where the stat returns an error,
// but the error isn't relevant to the path. This can happen when the current
// user doesn't have permission to access the file.
func FileExists(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return !info.IsDir(), nil
}

// WriteLocked uses the flock() syscall to safely write to a file as long as
// other users of the file are also using flock()-based access.
//
// WriteLocked will block until it gets lock access.
//
// For the reasons outlined best in https://lwn.net/Articles/586904/ this uses
// flock() instead of fcntl(). The ability to lock byte ranges is not necessary
// and flock() has better behavior.
func WriteLocked(filename string, data []byte) (int, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", filename, err)
	}
	defer file.Close()

	// For the reasons outlined best in https://lwn.net/Articles/586904/ we're
	// going to use flock() instead of fcntl() because we want a whole-file lock.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("unexpected error flock()-ing with EX: %w", err)
	}

	n, err := file.Write(data)
	if err != nil {
		return n, fmt.Errorf("writing data: %w", err)
	}

	// While calling file.Close() should remove the lock because it is the only
	// reference to this specific file descriptor, it is safest to manually
	// release the lock.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return n, fmt.Errorf("unexpected error flock()-ing with UN: %w", err)
	}

	return n, nil
}

// ReadLocked uses the flock() syscall to safely read from a file as long as
// other users of the file are also using flock()-based access.
//
// ReadLocked will block until it gets lock access.
//
// For the reasons outlined best in https://lwn.net/Articles/586904/ this uses
// flock() instead of fcntl(). The ability to lock byte ranges is not necessary
// and flock() has better behavior.
func ReadLocked(filename string) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", filename, err)
	}
	defer file.Close()

	// For the reasons outlined best in https://lwn.net/Articles/586904/ we're
	// going to use flock() instead of fcntl() because we want a whole-file lock.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("unexpected error flock()-ing with SH: %w", err)
	}

	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, file); err != nil {
		return nil, fmt.Errorf("copying data out of file: %w", err)
	}

	// While calling file.Close() should remove the lock because it is the only
	// reference to this specific file descriptor, it is safest to manually
	// release the lock.
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		return nil, fmt.Errorf("unexpected error flock()-ing with UN: %w", err)
	}

	return buf.Bytes(), nil
}
