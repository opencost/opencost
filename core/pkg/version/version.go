package version

import (
	"fmt"
	"runtime"
)

var (
	Version      = "dev"
	GitCommit    = "HEAD"
	Architecture = runtime.GOARCH
)

func FriendlyVersion() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}
