package filterutil

import (
	"reflect"

	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/mapper"
)

func ConvertFilterQueryParams(qp mapper.PrimitiveMapReader, labelConfig *kubecost.LabelConfig) AllocationFilterV1 {
	filter := AllocationFilterV1{
		Annotations:     qp.GetList(ParamFilterAnnotations, ","),
		Containers:      qp.GetList(ParamFilterContainers, ","),
		Controllers:     qp.GetList(ParamFilterControllers, ","),
		ControllerKinds: qp.GetList(ParamFilterControllerKinds, ","),
		Clusters:        qp.GetList(ParamFilterClusters, ","),
		Labels:          qp.GetList(ParamFilterLabels, ","),
		Namespaces:      qp.GetList(ParamFilterNamespaces, ","),
		Nodes:           qp.GetList(ParamFilterNodes, ","),
		Pods:            qp.GetList(ParamFilterPods, ","),
		Services:        qp.GetList(ParamFilterServices, ","),
	}

	if labelConfig != nil {
		filter.Departments = qp.GetList(ParamFilterDepartments, ",")
		filter.Environments = qp.GetList(ParamFilterEnvironments, ",")
		filter.Owners = qp.GetList(ParamFilterOwners, ",")
		filter.Products = qp.GetList(ParamFilterProducts, ",")
		filter.Teams = qp.GetList(ParamFilterTeams, ",")
	} else {
		log.Debugf("No label config is available. Not creating filters for label-mapped 'fields'.")
	}

	return filter
}

type AllocationFilterV1 struct {
	Annotations     []string `json:"annotations,omitempty"`
	Containers      []string `json:"containers,omitempty"`
	Controllers     []string `json:"controllers,omitempty"`
	ControllerKinds []string `json:"controllerKinds,omitempty"`
	Clusters        []string `json:"clusters,omitempty"`
	Departments     []string `json:"departments,omitempty"`
	Environments    []string `json:"environments,omitempty"`
	Labels          []string `json:"labels,omitempty"`
	Namespaces      []string `json:"namespaces,omitempty"`
	Nodes           []string `json:"nodes,omitempty"`
	Owners          []string `json:"owners,omitempty"`
	Pods            []string `json:"pods,omitempty"`
	Products        []string `json:"products,omitempty"`
	Services        []string `json:"services,omitempty"`
	Teams           []string `json:"teams,omitempty"`
}

func (f AllocationFilterV1) Equals(that AllocationFilterV1) bool {
	return reflect.DeepEqual(f.Annotations, that.Annotations) &&
		reflect.DeepEqual(f.Containers, that.Containers) &&
		reflect.DeepEqual(f.Controllers, that.Controllers) &&
		reflect.DeepEqual(f.ControllerKinds, that.ControllerKinds) &&
		reflect.DeepEqual(f.Clusters, that.Clusters) &&
		reflect.DeepEqual(f.Departments, that.Departments) &&
		reflect.DeepEqual(f.Environments, that.Environments) &&
		reflect.DeepEqual(f.Labels, that.Labels) &&
		reflect.DeepEqual(f.Namespaces, that.Namespaces) &&
		reflect.DeepEqual(f.Nodes, that.Nodes) &&
		reflect.DeepEqual(f.Owners, that.Owners) &&
		reflect.DeepEqual(f.Pods, that.Pods) &&
		reflect.DeepEqual(f.Products, that.Products) &&
		reflect.DeepEqual(f.Services, that.Services) &&
		reflect.DeepEqual(f.Teams, that.Teams)
}
