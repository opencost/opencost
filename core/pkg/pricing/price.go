package pricing

import "github.com/opencost/opencost/core/pkg/unit"

type Price struct {
	Price    float64       `json:"price" yaml:"price"`
	Currency unit.Currency `json:"currency" yaml:"currency"`
	Unit     unit.Unit     `json:"unit" yaml:"unit"`
}
