package kubemodel

import (
	"time"
)

// TODO: should we add a lock so that we can safely modify KubeModelSet in parallel?

// @bingen:generate[stringtable]:KubeModelSet
type KubeModelSet struct {
	Metadata       *Metadata                 `json:"meta"`           // @bingen:field[version=1]
	Window         Window                    `json:"window"`         // @bingen:field[version=1]
	Cluster        *Cluster                  `json:"cluster"`        // @bingen:field[version=1]
	Namespaces     map[string]*Namespace     `json:"namespaces"`     // @bingen:field[version=1]
	ResourceQuotas map[string]*ResourceQuota `json:"resourceQuotas"` // @bingen:field[version=1]
	idx            *index                    // @bingen:field[ignore]
}

func NewKubeModelSet(start, end time.Time) *KubeModelSet {
	index := &index{
		namespaceByName: map[string]*Namespace{},
	}

	return &KubeModelSet{
		Metadata: &Metadata{
			CreatedAt: time.Now().UTC(),
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Namespaces:     map[string]*Namespace{},
		ResourceQuotas: map[string]*ResourceQuota{},
		idx:            index,
	}
}

func (kms *KubeModelSet) RegisterError(str string) {
	kms.Metadata.Errors = append(kms.Metadata.Errors, str)
}

func (kms *KubeModelSet) IsEmpty() bool {
	return kms == nil || kms.Cluster == nil || kms.Metadata.ObjectCount == 0
}

type index struct {
	namespaceByName map[string]*Namespace
}
