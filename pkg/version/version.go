package version

import "fmt"

var (
	Version   = "dev"
	GitCommit = "HEAD"
)

func FriendlyVersion() string {
	// TODO: Make this dynamically pul the git commit version
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}
