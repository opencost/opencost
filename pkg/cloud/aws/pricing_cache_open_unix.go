//go:build unix

package aws

import (
	"os"
	"syscall"
)

// openPricingCacheFile opens a file from the pricing cache without following a symlink, so
// a link planted at a cache path cannot have the exporter read and parse whatever it
// points at.
func openPricingCacheFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}
