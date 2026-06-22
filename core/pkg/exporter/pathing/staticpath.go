package pathing

import (
	"fmt"
	"path"
)

// StaticFileStoragePathFormatter is an implementation of the StoragePathFormatter interface
// for static files whose path does not vary with time. The format is:
//
//	<rootDir>/<pipeline>/<prefix>.<in>.<fileExt>
//
// If prefix is empty the leading dot is omitted. If fileExt is empty the trailing dot is omitted.
type StaticFileStoragePathFormatter struct {
	rootDir  string
	pipeline string
}

// NewStaticFileStoragePathFormatter creates a StaticFileStoragePathFormatter with the given
// root directory and pipeline name.
func NewStaticFileStoragePathFormatter(rootDir, pipeline string) (*StaticFileStoragePathFormatter, error) {
	if rootDir == "" {
		return nil, fmt.Errorf("rootDir cannot be empty")
	}
	if pipeline == "" {
		return nil, fmt.Errorf("pipeline cannot be empty")
	}
	return &StaticFileStoragePathFormatter{
		rootDir:  rootDir,
		pipeline: pipeline,
	}, nil
}

// Dir returns the directory where static files are placed.
func (s *StaticFileStoragePathFormatter) Dir() string {
	return path.Join(s.rootDir, s.pipeline)
}

// ToFullPath returns the full path for a static file. The in parameter is used as the
// file name and may include subdirectory segments. prefix and fileExt are optional and
// apply only to the base file name component of in.
func (s *StaticFileStoragePathFormatter) ToFullPath(prefix string, in string, fileExt string) string {
	dir, base := path.Split(in)

	name := base
	if prefix != "" {
		name = fmt.Sprintf("%s.%s", prefix, base)
	}
	if fileExt != "" {
		name = fmt.Sprintf("%s.%s", name, fileExt)
	}
	return path.Join(s.rootDir, s.pipeline, dir, name)
}
