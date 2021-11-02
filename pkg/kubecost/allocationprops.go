package kubecost

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kubecost/cost-model/pkg/prom"
)

const (
	AllocationNilProp            string = ""
	AllocationClusterProp        string = "cluster"
	AllocationNodeProp           string = "node"
	AllocationContainerProp      string = "container"
	AllocationControllerProp     string = "controller"
	AllocationControllerKindProp string = "controllerKind"
	AllocationNamespaceProp      string = "namespace"
	AllocationPodProp            string = "pod"
	AllocationProviderIDProp     string = "providerID"
	AllocationServiceProp        string = "service"
	AllocationLabelProp          string = "label"
	AllocationAnnotationProp     string = "annotation"
	AllocationDeploymentProp     string = "deployment"
	AllocationStatefulSetProp    string = "statefulset"
	AllocationDaemonSetProp      string = "daemonset"
	AllocationJobProp            string = "job"
	AllocationDepartmentProp     string = "department"
	AllocationEnvironmentProp    string = "environment"
	AllocationOwnerProp          string = "owner"
	AllocationProductProp        string = "product"
	AllocationTeamProp           string = "team"
)

func ParseProperty(text string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(text)) {
	case "cluster":
		return AllocationClusterProp, nil
	case "node":
		return AllocationNodeProp, nil
	case "container":
		return AllocationContainerProp, nil
	case "controller":
		return AllocationControllerProp, nil
	case "controllerkind":
		return AllocationControllerKindProp, nil
	case "namespace":
		return AllocationNamespaceProp, nil
	case "pod":
		return AllocationPodProp, nil
	case "providerid":
		return AllocationProviderIDProp, nil
	case "service":
		return AllocationServiceProp, nil
	case "label":
		return AllocationLabelProp, nil
	case "annotation":
		return AllocationAnnotationProp, nil
	case "deployment":
		return AllocationDeploymentProp, nil
	case "daemonset":
		return AllocationDaemonSetProp, nil
	case "statefulset":
		return AllocationStatefulSetProp, nil
	case "job":
		return AllocationJobProp, nil
	case "department":
		return AllocationDepartmentProp, nil
	case "environment":
		return AllocationEnvironmentProp, nil
	case "owner":
		return AllocationOwnerProp, nil
	case "product":
		return AllocationProductProp, nil
	case "team":
		return AllocationTeamProp, nil
	}

	if strings.HasPrefix(text, "label:") {
		label := prom.SanitizeLabelName(strings.TrimSpace(strings.TrimPrefix(text, "label:")))
		return fmt.Sprintf("label:%s", label), nil
	}

	if strings.HasPrefix(text, "annotation:") {
		annotation := prom.SanitizeLabelName(strings.TrimSpace(strings.TrimPrefix(text, "annotation:")))
		return fmt.Sprintf("annotation:%s", annotation), nil
	}

	return AllocationNilProp, fmt.Errorf("invalid allocation property: %s", text)
}

// AllocationProperties describes a set of Kubernetes objects.
type AllocationProperties struct {
	Cluster        string                `json:"cluster,omitempty"`
	Node           string                `json:"node,omitempty"`
	Container      string                `json:"container,omitempty"`
	Controller     string                `json:"controller,omitempty"`
	ControllerKind string                `json:"controllerKind,omitempty"`
	Namespace      string                `json:"namespace,omitempty"`
	Pod            string                `json:"pod,omitempty"`
	Services       []string              `json:"services,omitempty"`
	ProviderID     string                `json:"providerID,omitempty"`
	Labels         AllocationLabels      `json:"labels,omitempty"`
	Annotations    AllocationAnnotations `json:"annotations,omitempty"`
}

// AllocationLabels is a schema-free mapping of key/value pairs that can be
// attributed to an Allocation
type AllocationLabels map[string]string

// AllocationAnnotations is a schema-free mapping of key/value pairs that can be
// attributed to an Allocation
type AllocationAnnotations map[string]string

func (p *AllocationProperties) Clone() *AllocationProperties {
	if p == nil {
		return nil
	}

	clone := &AllocationProperties{}
	clone.Cluster = p.Cluster
	clone.Node = p.Node
	clone.Container = p.Container
	clone.Controller = p.Controller
	clone.ControllerKind = p.ControllerKind
	clone.Namespace = p.Namespace
	clone.Pod = p.Pod
	clone.ProviderID = p.ProviderID

	var services []string
	for _, s := range p.Services {
		services = append(services, s)
	}
	clone.Services = services

	labels := make(map[string]string, len(p.Labels))
	for k, v := range p.Labels {
		labels[k] = v
	}
	clone.Labels = labels

	annotations := make(map[string]string, len(p.Annotations))
	for k, v := range p.Annotations {
		annotations[k] = v
	}
	clone.Annotations = annotations

	return clone
}

func (p *AllocationProperties) Equal(that *AllocationProperties) bool {
	if p == nil || that == nil {
		return false
	}

	if p.Cluster != that.Cluster {
		return false
	}

	if p.Node != that.Node {
		return false
	}

	if p.Container != that.Container {
		return false
	}

	if p.Controller != that.Controller {
		return false
	}

	if p.ControllerKind != that.ControllerKind {
		return false
	}

	if p.Namespace != that.Namespace {
		return false
	}

	if p.Pod != that.Pod {
		return false
	}

	if p.ProviderID != that.ProviderID {
		return false
	}

	pLabels := p.Labels
	thatLabels := that.Labels
	if len(pLabels) == len(thatLabels) {
		for k, pv := range pLabels {
			tv, ok := thatLabels[k]
			if !ok || tv != pv {
				return false
			}
		}
	} else {
		return false
	}

	pAnnotations := p.Annotations
	thatAnnotations := that.Annotations
	if len(pAnnotations) == len(thatAnnotations) {
		for k, pv := range pAnnotations {
			tv, ok := thatAnnotations[k]
			if !ok || tv != pv {
				return false
			}
		}
	} else {
		return false
	}

	pServices := p.Services
	thatServices := that.Services
	if len(pServices) == len(thatServices) {
		sort.Strings(pServices)
		sort.Strings(thatServices)
		for i, pv := range pServices {
			tv := thatServices[i]
			if tv != pv {
				return false
			}
		}
	} else {
		return false
	}

	return true
}

// Intersection returns an *AllocationProperties which contains all matching fields between the calling and parameter AllocationProperties
// nillable slices and maps are left as nil
func (p *AllocationProperties) Intersection(that *AllocationProperties) *AllocationProperties {
	if p == nil || that == nil {
		return nil
	}
	intersectionProps := &AllocationProperties{}
	if p.Cluster == that.Cluster {
		intersectionProps.Cluster = p.Cluster
	}
	if p.Node == that.Node {
		intersectionProps.Node = p.Node
	}
	if p.Container == that.Container {
		intersectionProps.Container = p.Container
	}
	if p.Controller == that.Controller {
		intersectionProps.Controller = p.Controller
	}
	if p.ControllerKind == that.ControllerKind {
		intersectionProps.ControllerKind = p.ControllerKind
	}
	if p.Namespace == that.Namespace {
		intersectionProps.Namespace = p.Namespace
	}
	if p.Pod == that.Pod {
		intersectionProps.Pod = p.Pod
	}
	if p.ProviderID == that.ProviderID {
		intersectionProps.ProviderID = p.ProviderID
	}
	return intersectionProps
}

func (p *AllocationProperties) String() string {
	if p == nil {
		return "<nil>"
	}

	strs := []string{}

	if p.Cluster != "" {
		strs = append(strs, "Cluster:"+p.Cluster)
	}

	if p.Node != "" {
		strs = append(strs, "Node:"+p.Node)
	}

	if p.Container != "" {
		strs = append(strs, "Container:"+p.Container)
	}

	if p.Controller != "" {
		strs = append(strs, "Controller:"+p.Controller)
	}

	if p.ControllerKind != "" {
		strs = append(strs, "ControllerKind:"+p.ControllerKind)
	}

	if p.Namespace != "" {
		strs = append(strs, "Namespace:"+p.Namespace)
	}

	if p.Pod != "" {
		strs = append(strs, "Pod:"+p.Pod)
	}

	if p.ProviderID != "" {
		strs = append(strs, "ProviderID:"+p.ProviderID)
	}

	if len(p.Services) > 0 {
		strs = append(strs, "Services:"+strings.Join(p.Services, ";"))
	}

	var labelStrs []string
	for k, prop := range p.Labels {
		labelStrs = append(labelStrs, fmt.Sprintf("%s:%s", k, prop))
	}
	strs = append(strs, fmt.Sprintf("Labels:{%s}", strings.Join(strs, ",")))

	var AnnotationStrs []string
	for k, prop := range p.Annotations {
		AnnotationStrs = append(AnnotationStrs, fmt.Sprintf("%s:%s", k, prop))
	}
	strs = append(strs, fmt.Sprintf("Annotations:{%s}", strings.Join(strs, ",")))

	return fmt.Sprintf("{%s}", strings.Join(strs, "; "))
}
