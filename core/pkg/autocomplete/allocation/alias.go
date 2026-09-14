package allocation

import (
	"github.com/opencost/opencost/core/pkg/opencost"
)

// ResolveAliasLabelKey resolves an alias field (department, environment, owner,
// product, team) to its configured label key.
func ResolveAliasLabelKey(field string, lc *opencost.LabelConfig) (string, bool) {
	if lc == nil {
		lc = opencost.NewLabelConfig()
	}

	labelConfigMap := lc.Map()
	value, ok := labelConfigMap[field+"_label"]
	return lc.Sanitize(value), ok
}
