package kubecost

import (
	"fmt"
	"sort"
	"strings"
)

type AllocationProperty string

const (
	AllocationNilProp            AllocationProperty = ""
	AllocationClusterProp        AllocationProperty = "cluster"
	AllocationNodeProp           AllocationProperty = "node"
	AllocationContainerProp      AllocationProperty = "container"
	AllocationControllerProp     AllocationProperty = "controller"
	AllocationControllerKindProp AllocationProperty = "controllerKind"
	AllocationNamespaceProp      AllocationProperty = "namespace"
	AllocationPodProp            AllocationProperty = "pod"
	AllocationProviderIDProp     AllocationProperty = "providerID"
	AllocationServiceProp        AllocationProperty = "service"
	AllocationLabelProp          AllocationProperty = "label"
	AllocationAnnotationProp     AllocationProperty = "annotation"
)

func ParseProperty(text string) (AllocationProperty, error) {
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
	}
	return AllocationNilProp, fmt.Errorf("invalid allocation property: %s", text)
}

func (p AllocationProperty) String() string {
	return string(p)
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
	Labels         AllocationLabels      `json:"allocationLabels,omitempty"`
	Annotations    AllocationAnnotations `json:"allocationAnnotations,omitempty"`
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

	labels := make(map[string]string)
	for k, v := range p.Labels {
		labels[k] = v
	}
	clone.Labels = labels

	annotations := make(map[string]string)
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

// AggregationStrings converts a AllocationProperties object into a slice of strings
// representing a request to aggregate by certain properties.
// NOTE: today, the ordering of the properties *has to match the ordering
// of the allocation function generateKey*
func (p *AllocationProperties) AggregationStrings() []string {
	if p == nil {
		return []string{}
	}

	aggStrs := []string{}
	if p.Cluster != "" {
		aggStrs = append(aggStrs, AllocationClusterProp.String())
	}
	if p.Node != "" {
		aggStrs = append(aggStrs, AllocationNodeProp.String())
	}
	if p.Container != "" {
		aggStrs = append(aggStrs, AllocationContainerProp.String())
	}
	if p.Controller != "" {
		aggStrs = append(aggStrs, AllocationControllerProp.String())
	}
	if p.ControllerKind != "" {
		aggStrs = append(aggStrs, AllocationControllerKindProp.String())
	}
	if p.Namespace != "" {
		aggStrs = append(aggStrs, AllocationNamespaceProp.String())
	}
	if p.Pod != "" {
		aggStrs = append(aggStrs, AllocationPodProp.String())
	}
	if p.ProviderID != "" {
		aggStrs = append(aggStrs, AllocationProviderIDProp.String())
	}
	if len(p.Services) > 0 {
		aggStrs = append(aggStrs, AllocationServiceProp.String())
	}
	
	if len(p.Labels) > 0  {
		// e.g. expect format map[string]string{
		// 	 "env":""
		// 	 "app":"",
		// }
		// for aggregating by "label:app,label:env"
		labels := p.Labels
		labelAggStrs := []string{}
		for labelName := range labels {
			labelAggStrs = append(labelAggStrs, fmt.Sprintf("label:%s", labelName))
		}
		if len(labelAggStrs) > 0 {
			// Enforce alphabetical ordering, then append to aggStrs
			sort.Strings(labelAggStrs)
			for _, labelName := range labelAggStrs {
				aggStrs = append(aggStrs, labelName)
			}
		}
	}


	return aggStrs
}

func (p *AllocationProperties) IsEmpty() bool {
	if p == nil {
		return true
	}
	return p.Equal(&AllocationProperties{})
}
