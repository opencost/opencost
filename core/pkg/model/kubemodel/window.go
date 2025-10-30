package kubemodel

import "time"

type Window struct {
	Start      time.Time
	Resolution time.Duration
}
