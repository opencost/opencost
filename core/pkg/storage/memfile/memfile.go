package memfile

import (
	"iter"
	"maps"
	"time"
)

// MemoryFile represents a file in memory storage. It's part of the directory tree
// structure used to look up files by path.
type MemoryFile struct {
	Name     string
	Contents []byte
	ModTime  time.Time

	directory *MemoryDirectory
}

// Size returns the size of the file in bytes.
func (mf *MemoryFile) Size() int64 {
	return int64(len(mf.Contents))
}

// NewMemoryFile creates a new MemoryFile instance with the provided name and and byte contents.
func NewMemoryFile(name string, contents []byte) *MemoryFile {
	return &MemoryFile{
		Name:      name,
		Contents:  contents,
		ModTime:   time.Now().UTC(),
		directory: nil,
	}
}

// MemoryDirectory represents a directory in memory storage. It is the root of the file system
// tree structure used to look up files by path.
type MemoryDirectory struct {
	Name    string
	ModTime time.Time

	dirs      map[string]*MemoryDirectory
	files     map[string]*MemoryFile
	directory *MemoryDirectory
}

// NewMemoryDirectory creates a new Directory instance with the provided path name.
func NewMemoryDirectory(name string) *MemoryDirectory {
	return &MemoryDirectory{
		Name:  name,
		dirs:  make(map[string]*MemoryDirectory),
		files: make(map[string]*MemoryFile),
	}
}

// Size returns the size of all subdirectories and files within this directory.
func (d *MemoryDirectory) Size() int64 {
	var size int64
	for _, f := range d.files {
		size += f.Size()
	}
	for _, subdir := range d.dirs {
		size += subdir.Size()
	}
	return size
}

// AddFile adds a file to the directory. Note that files can only exist within a single directory
// at a time.
func (d *MemoryDirectory) AddFile(f *MemoryFile) {
	if f.directory != nil {
		f.directory.RemoveFile(f.Name)
		f.directory = nil
	}

	d.files[f.Name] = f
	d.ModTime = time.Now().UTC()
	f.directory = d
}

// AddDirectory adds a subdirectory to the parent directory. Note that directories can only exist within a single directory.
func (d *MemoryDirectory) AddDirectory(subdir *MemoryDirectory) {
	if subdir.directory != nil {
		subdir.directory.RemoveDirectory(subdir.Name)
		subdir.directory = nil
	}

	d.dirs[subdir.Name] = subdir
	d.ModTime = time.Now().UTC()
	subdir.directory = d
}

// RemoveFile removes a file from the directoory tree.
func (d *MemoryDirectory) RemoveFile(name string) {
	if _, ok := d.files[name]; ok {
		delete(d.files, name)
		d.ModTime = time.Now().UTC()
	}
}

// RemoveDirectory remove a subdirectory from the directory tree.
func (d *MemoryDirectory) RemoveDirectory(name string) {
	if _, ok := d.dirs[name]; ok {
		delete(d.dirs, name)
		d.ModTime = time.Now().UTC()
	}
}

// FileCount returns the total number of files in this directory.
func (d *MemoryDirectory) FileCount() int {
	return len(d.files)
}

// DirCount returns the total number of subdirectories in this directory.
func (d *MemoryDirectory) DirCount() int {
	return len(d.dirs)
}

// Files returns a slice of files located within this directory.
func (d *MemoryDirectory) Files() iter.Seq[*MemoryFile] {
	return maps.Values(d.files)
}

func (d *MemoryDirectory) Directories() iter.Seq[*MemoryDirectory] {
	return maps.Values(d.dirs)
}
