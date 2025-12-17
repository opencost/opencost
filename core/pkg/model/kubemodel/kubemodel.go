package kubemodel

import (
	"time"
)

// TODO: should we add a lock so that we can safely modify KubeModelSet in parallel?

// @bingen:generate[stringtable]:KubeModelSet
type KubeModelSet struct {
	Metadata       *Metadata                 `json:"meta"`                     // @bingen:field[version=1]
	Window         Window                    `json:"window"`                   // @bingen:field[version=1]
	Cluster        *Cluster                  `json:"cluster"`                  // @bingen:field[version=1]
	Containers     map[string]*Container     `json:"containers,omitempty"`     // @bingen:field[ignore]
	Namespaces     map[string]*Namespace     `json:"namespaces"`               // @bingen:field[version=1]
	Nodes          map[string]*Node          `json:"nodes,omitempty"`          // @bingen:field[ignore]
	Owners         map[string]*Owner         `json:"owners,omitempty"`         // @bingen:field[ignore]
	Pods           map[string]*Pod           `json:"pods,omitempty"`           // @bingen:field[ignore]
	ResourceQuotas map[string]*ResourceQuota `json:"resourceQuotas,omitempty"` // @bingen:field[version=1]
	Services       map[string]*Service       `json:"services,omitempty"`       // @bingen:field[ignore]
	idx            *index                    // @bingen:field[ignore]
}

func NewKubeModelSet(start, end time.Time) *KubeModelSet {
	index := &index{
		namespaceByName: map[string]*Namespace{},
	}

	return &KubeModelSet{
		Metadata: &Metadata{
			CreatedAt:       time.Now().UTC(),
			DiagnosticLevel: DefaultDiagnosticLevel,
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Containers:     map[string]*Container{},
		Namespaces:     map[string]*Namespace{},
		Nodes:          map[string]*Node{},
		Owners:         map[string]*Owner{},
		Pods:           map[string]*Pod{},
		ResourceQuotas: map[string]*ResourceQuota{},
		Services:       map[string]*Service{},
		idx:            index,
	}
}

func (kms *KubeModelSet) IsEmpty() bool {
	return kms == nil || kms.Cluster == nil || kms.Metadata.ObjectCount == 0
}

type index struct {
	namespaceByName map[string]*Namespace
}
