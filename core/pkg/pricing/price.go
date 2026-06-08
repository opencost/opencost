package pricing

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/opencost/opencost/core/pkg/unit"
)

var NotFound = errors.New("Not found")

type Price struct {
	Unit  unit.Unit `json:"unit" yaml:"unit"`
	Price float64   `json:"price" yaml:"price"`
}

type Prices map[Resource]Price

// canonical returns a deterministic string representation of the prices,
// independent of map iteration order. It is used to make pricing checksums
// sensitive to price values (not just properties).
func (p Prices) canonical() string {
	if len(p) == 0 {
		return ""
	}

	resources := make([]string, 0, len(p))
	for r := range p {
		resources = append(resources, string(r))
	}
	sort.Strings(resources)

	var b strings.Builder
	for _, r := range resources {
		price := p[Resource(r)]
		b.WriteString(r)
		b.WriteByte('=')
		b.WriteString(string(price.Unit))
		b.WriteByte('=')
		b.WriteString(strconv.FormatFloat(price.Price, 'f', -1, 64))
		b.WriteByte(';')
	}

	return b.String()
}
