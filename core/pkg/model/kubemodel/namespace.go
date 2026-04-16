package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:Namespace
type Namespace struct {
	UID string `json:"uid"` // @bingen:field[version=1]
	// This field was included in the initial bigen codec but was never populated. If you need to add a new string
	// field to this structure rename this field and delete this comment
	bingenPlaceHolder string            // @bingen:field[version=1]
	Name              string            `json:"name"`            // @bingen:field[version=1]
	Labels            map[string]string `json:"labels"`          // @bingen:field[version=1]
	Annotations       map[string]string `json:"annotations"`     // @bingen:field[version=1]
	Start             time.Time         `json:"start,omitempty"` // @bingen:field[version=1]
	End               time.Time         `json:"end,omitempty"`   // @bingen:field[version=1]
}

func (kms *KubeModelSet) RegisterNamespace(namespace *Namespace) error {
	// Check required fields
	if namespace.UID == "" {
		err := fmt.Errorf("UID is missing for Namespace with name '%s'", namespace.Name)
		kms.Error(err)
		return err
	}

	if namespace.Name == "" {
		err := fmt.Errorf("Name is missing for Namespace '%s'", namespace.UID)
		kms.Error(err)
		return err
	}

	if err := checkWindow(kms.Window, namespace.Start, namespace.End); err != nil {
		kms.Error(err)
		return err
	}

	if _, ok := kms.Namespaces[namespace.UID]; !ok {
		if kms.Cluster == nil {
			kms.Warnf("RegisterNamespace: Cluster is nil")
		}

		kms.Namespaces[namespace.UID] = namespace

		kms.idx.namespaceByName[namespace.Name] = namespace

		kms.Metadata.ObjectCount++
	}

	return nil
}
