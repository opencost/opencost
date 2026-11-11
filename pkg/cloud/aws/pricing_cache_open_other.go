//go:build !unix

package aws

import (
	"fmt"
	"os"
)

// openPricingCacheFile checks for a symlink before opening where O_NOFOLLOW is not
// available. This races, unlike the O_NOFOLLOW path, but the cache directory is private to
// this process's user, so an attacker able to win the race can already write the body.
func openPricingCacheFile(path string) (*os.File, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%q is not a regular file", path)
	}
	return os.Open(path)
}
