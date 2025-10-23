package version

import "fmt"

var (
	Version    = "dev"
	GitCommit  = "HEAD"
	AppVersion = "unknown"
)

func FriendlyVersion() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}
