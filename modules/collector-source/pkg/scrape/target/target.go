package target

import "io"

// ScrapeTarget is an interface representing an object that is capable of loading/refreshing it's
// target data.
type ScrapeTarget interface {
	Load() (io.Reader, error)
}

type TargetProvider interface {
	GetTargets() []ScrapeTarget
}
