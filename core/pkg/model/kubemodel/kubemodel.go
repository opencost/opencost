package kubemodel

import (
	"errors"
	"time"
)

type KubeModelSet struct {
	Metadata       *KubeModelSetMetadata
	Window         Window
	Cluster        *Cluster
	Namespaces     map[string]*Namespace
	ResourceQuotas map[string]*ResourceQuota
}

func NewKubeModelSet(start, end time.Time) *KubeModelSet {
	return &KubeModelSet{
		Metadata: &KubeModelSetMetadata{
			CreatedAt: time.Now().UTC(),
		},
		Window: Window{
			Start: start,
			End:   end,
		},
		Namespaces:     map[string]*Namespace{},
		ResourceQuotas: map[string]*ResourceQuota{},
	}
}

// TODO: determine what "IsEmpty()" should mean here
func (kms *KubeModelSet) IsEmpty() bool {
	return kms == nil || kms.Cluster == nil
}

// TODO: generate bingen codec
func (kms *KubeModelSet) MarshalBinary() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type KubeModelSetMetadata struct {
	CreatedAt   time.Time
	ObjectCount int
	Errors      []error
	Warnings    []string
}
