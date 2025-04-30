package target

import (
	"io"
	"os"
)

type FileTarget struct {
	path string
}

func NewFileTarget(path string) *FileTarget {
	return &FileTarget{
		path: path,
	}
}

func (t *FileTarget) Load() (io.Reader, error) {
	return os.Open(t.path)
}
