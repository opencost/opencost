package metric

import (
	"time"
)

type UpdateSet struct {
	Updates []Update `json:"updates"`
}

type Update struct {
	Name           string            `json:"name"`
	Labels         map[string]string `json:"labels"`
	Value          float64           `json:"value"`
	AdditionalInfo map[string]string `json:"additionalInfo"`
}

type Updater interface {
	Update([]Update, time.Time)
}
