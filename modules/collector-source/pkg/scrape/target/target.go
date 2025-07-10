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

type DefaultTargetProvider struct {
	targets []ScrapeTarget
}

func NewDefaultTargetProvider(targets ...ScrapeTarget) *DefaultTargetProvider {
	return &DefaultTargetProvider{targets: targets}
}

func (m *DefaultTargetProvider) GetTargets() []ScrapeTarget {
	return m.targets
}
