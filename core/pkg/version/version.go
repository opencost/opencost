package version

import "fmt"

var (
	Version      = "dev"
	GitCommit    = "HEAD"
	Architecture = "amd64"
)

func FriendlyVersion() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}
