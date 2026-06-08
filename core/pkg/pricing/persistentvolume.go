package pricing

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/model/shared"
)

type PersistentVolumePricing struct {
	Properties PersistentVolumePricingProperties `json:"properties" yaml:"properties"`
	Prices     Prices                            `json:"prices" yaml:"pricing"`
}

func (vp *PersistentVolumePricing) String() string {
	return vp.Properties.String() + "|" + vp.Prices.canonical()
}

type PersistentVolumePricingProperties struct {
	Provider   shared.Provider   `json:"provider,omitempty" yaml:"provider,omitempty"`
	Region     string            `json:"region,omitempty" yaml:"region,omitempty"`
	VolumeType VolumeType        `json:"volumeType,omitempty" yaml:"volumeType,omitempty"`
	Cluster    string            `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	ProviderID string            `json:"providerID,omitempty" yaml:"providerID,omitempty"`
	Labels     map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Start      *time.Time        `json:"start,omitempty" yaml:"start,omitempty"`
	End        *time.Time        `json:"end,omitempty" yaml:"end,omitempty"`
}

// TODO: precompute this somewhere along the way?
func (vp *PersistentVolumePricingProperties) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s",
		vp.Provider,
		vp.Region,
		vp.VolumeType,
		vp.Cluster,
		vp.ProviderID,
		vp.labelsKey(),
		vp.timeKey(),
	)
}

func (vp *PersistentVolumePricingProperties) labelsKey() string {
	if len(vp.Labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(vp.Labels))
	for k := range vp.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(':')
		}

		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(vp.Labels[k])
	}

	return b.String()
}

func (vp *PersistentVolumePricingProperties) timeKey() string {
	s := "nil"
	e := "nil"

	if vp.Start != nil {
		s = vp.Start.UTC().Format(time.RFC3339Nano)
	}

	if vp.End != nil {
		e = vp.End.UTC().Format(time.RFC3339Nano)
	}

	return fmt.Sprintf("%s:%s", s, e)
}
