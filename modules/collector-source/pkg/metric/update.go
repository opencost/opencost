package metric

import (
	"time"
)

type UpdateSet struct {
	Timestamp time.Time `json:"timestamp"`
	Updates   []Update  `json:"updates"`
}

type Update struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
	// AdditionalInfo is stored by reference across every resolution; don't mutate it after Update.
	AdditionalInfo map[string]string `json:"additionalInfo"`
}

type Updater interface {
	Update(*UpdateSet)
}
