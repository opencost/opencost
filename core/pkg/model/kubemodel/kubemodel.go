package kubemodel

import (
	"errors"
	"time"
)

type KubeModelSet struct {
	Window         Window
	Cluster        *Cluster
	Namespaces     []*Namespace
	ResourceQuotas []*ResourceQuota
	Metadata       *KubeModelSetMetadata
}

// TODO: determine what "IsEmpty()" should mean here
func (kms *KubeModelSet) IsEmpty() bool {
	return kms == nil
}

// TODO: generate bingen codec
func (kms *KubeModelSet) MarshalBinary() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type KubeModelSetMetadata struct {
	CreatedAt  time.Time
	DataSource string
	Warnings   []string
	Errors     []error
}
