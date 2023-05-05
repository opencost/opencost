package kubecost

import (
	"fmt"
	"sort"
	"strings"

	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/prom"
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
	// When set to true, maintain the intersection of all labels + annotations
	// in the aggregated AllocationProperties object
	AggregatedMetadata bool `json:"-"` //@bingen:field[ignore]
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
	services = append(services, p.Services...)
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

// GenerateKey generates a string that represents the key by which the
// AllocationProperties should be aggregated, given the properties defined by
// the aggregateBy parameter and the given label configuration.
func (p *AllocationProperties) GenerateKey(aggregateBy []string, labelConfig *LabelConfig) string {
	if p == nil {
		return ""
	}

	if labelConfig == nil {
		labelConfig = NewLabelConfig()
	}

	// Names will ultimately be joined into a single name, which uniquely
	// identifies allocations.
	names := []string{}

	for _, agg := range aggregateBy {
		switch true {
		case agg == AllocationClusterProp:
			names = append(names, p.Cluster)
		case agg == AllocationNodeProp:
			names = append(names, p.Node)
		case agg == AllocationNamespaceProp:
			names = append(names, p.Namespace)
		case agg == AllocationControllerKindProp:
			controllerKind := p.ControllerKind
			if controllerKind == "" {
				// Indicate that allocation has no controller
				controllerKind = UnallocatedSuffix
			}
			names = append(names, controllerKind)
		case agg == AllocationDaemonSetProp || agg == AllocationStatefulSetProp || agg == AllocationDeploymentProp || agg == AllocationJobProp:
			controller := p.Controller
			if agg != p.ControllerKind || controller == "" {
				// The allocation does not have the specified controller kind
				controller = UnallocatedSuffix
			}
			names = append(names, controller)
		case agg == AllocationControllerProp:
			controller := p.Controller
			if controller == "" {
				// Indicate that allocation has no controller
				controller = UnallocatedSuffix
			} else if p.ControllerKind != "" {
				controller = fmt.Sprintf("%s:%s", p.ControllerKind, controller)
			}
			names = append(names, controller)
		case agg == AllocationPodProp:
			names = append(names, p.Pod)
		case agg == AllocationContainerProp:
			names = append(names, p.Container)
		case agg == AllocationServiceProp:
			services := p.Services
			if len(services) == 0 {
				// Indicate that allocation has no services
				names = append(names, UnallocatedSuffix)
			} else {
				// Unmounted load balancers lead to __unmounted__ Allocations whose
				// services field is populated. If we don't have a special case, the
				// __unmounted__ Allocation will be transformed into a regular Allocation,
				// causing issues with AggregateBy and drilldown
				if p.Pod == UnmountedSuffix || p.Namespace == UnmountedSuffix || p.Container == UnmountedSuffix {
					names = append(names, UnmountedSuffix)
				} else {
					// This just uses the first service
					for _, service := range services {
						names = append(names, service)
						break
					}
				}
			}
		case strings.HasPrefix(agg, "label:"):
			labels := p.Labels
			if labels == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				labelName := labelConfig.Sanitize(strings.TrimPrefix(agg, "label:"))
				if labelValue, ok := labels[labelName]; ok {
					names = append(names, fmt.Sprintf("%s", labelValue))
				} else {
					names = append(names, UnallocatedSuffix)
				}
			}
		case strings.HasPrefix(agg, "annotation:"):
			annotations := p.Annotations
			if annotations == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				annotationName := labelConfig.Sanitize(strings.TrimPrefix(agg, "annotation:"))
				if annotationValue, ok := annotations[annotationName]; ok {
					names = append(names, fmt.Sprintf("%s", annotationValue))
				} else {
					names = append(names, UnallocatedSuffix)
				}
			}
		case agg == AllocationDepartmentProp:
			labels := p.Labels
			annotations := p.Annotations
			if labels == nil && annotations == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				labelNames := strings.Split(labelConfig.DepartmentLabel, ",")
				for _, labelName := range labelNames {
					labelName = labelConfig.Sanitize(labelName)
					if labelValue, ok := labels[labelName]; ok {
						names = append(names, labelValue)
					} else if annotationValue, ok := annotations[labelName]; ok {
						names = append(names, annotationValue)
					} else {
						names = append(names, UnallocatedSuffix)
					}
				}
			}
		case agg == AllocationEnvironmentProp:
			labels := p.Labels
			annotations := p.Annotations
			if labels == nil && annotations == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				labelNames := strings.Split(labelConfig.EnvironmentLabel, ",")
				for _, labelName := range labelNames {
					labelName = labelConfig.Sanitize(labelName)
					if labelValue, ok := labels[labelName]; ok {
						names = append(names, labelValue)
					} else if annotationValue, ok := annotations[labelName]; ok {
						names = append(names, annotationValue)
					} else {
						names = append(names, UnallocatedSuffix)
					}
				}
			}
		case agg == AllocationOwnerProp:
			labels := p.Labels
			annotations := p.Annotations
			if labels == nil && annotations == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				labelNames := strings.Split(labelConfig.OwnerLabel, ",")
				for _, labelName := range labelNames {
					labelName = labelConfig.Sanitize(labelName)
					if labelValue, ok := labels[labelName]; ok {
						names = append(names, labelValue)
					} else if annotationValue, ok := annotations[labelName]; ok {
						names = append(names, annotationValue)
					} else {
						names = append(names, UnallocatedSuffix)
					}
				}
			}
		case agg == AllocationProductProp:
			labels := p.Labels
			annotations := p.Annotations
			if labels == nil && annotations == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				labelNames := strings.Split(labelConfig.ProductLabel, ",")
				for _, labelName := range labelNames {
					labelName = labelConfig.Sanitize(labelName)
					if labelValue, ok := labels[labelName]; ok {
						names = append(names, labelValue)
					} else if annotationValue, ok := annotations[labelName]; ok {
						names = append(names, annotationValue)
					} else {
						names = append(names, UnallocatedSuffix)
					}
				}
			}
		case agg == AllocationTeamProp:
			labels := p.Labels
			annotations := p.Annotations
			if labels == nil && annotations == nil {
				names = append(names, UnallocatedSuffix)
			} else {
				labelNames := strings.Split(labelConfig.TeamLabel, ",")
				for _, labelName := range labelNames {
					labelName = labelConfig.Sanitize(labelName)
					if labelValue, ok := labels[labelName]; ok {
						names = append(names, labelValue)
					} else if annotationValue, ok := annotations[labelName]; ok {
						names = append(names, annotationValue)
					} else {
						names = append(names, UnallocatedSuffix)
					}
				}
			}
		default:
			// This case should never be reached, as input up until this point
			// should be checked and rejected if invalid. But if we do get a
			// value we don't recognize, log a warning.
			log.Warnf("generateKey: illegal aggregation parameter: %s", agg)
		}
	}

	return strings.Join(names, "/")
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
		// ignore the incoming labels from unallocated or unmounted special case pods
		if p.AggregatedMetadata || that.AggregatedMetadata {
			intersectionProps.AggregatedMetadata = true

			// When aggregating by metadata, we maintain the intersection of the labels/annotations
			// of the two AllocationProperties objects being intersected here.
			// Special case unallocated/unmounted Allocations never have any labels or annotations.
			// As a result, they have the effect of always clearing out the intersection,
			// regardless if all the other actual allocations/etc have them.
			// This logic is designed to effectively ignore the unmounted/unallocated objects
			// and just copy over the labels from the other object - we only take the intersection
			// of 'legitimate' allocations.
			if p.Container == UnmountedSuffix {
				intersectionProps.Annotations = that.Annotations
				intersectionProps.Labels = that.Labels
			} else if that.Container == UnmountedSuffix {
				intersectionProps.Annotations = p.Annotations
				intersectionProps.Labels = p.Labels
			} else {
				intersectionProps.Annotations = mapIntersection(p.Annotations, that.Annotations)
				intersectionProps.Labels = mapIntersection(p.Labels, that.Labels)
			}
		}
	}
	if p.Pod == that.Pod {
		intersectionProps.Pod = p.Pod
	}
	if p.ProviderID == that.ProviderID {
		intersectionProps.ProviderID = p.ProviderID
	}
	return intersectionProps
}

func mapIntersection(map1, map2 map[string]string) map[string]string {
	result := make(map[string]string)
	for key, value := range map1 {
		if value2, ok := map2[key]; ok {
			if value2 == value {
				result[key] = value
			}
		}

	}

	return result
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
	strs = append(strs, fmt.Sprintf("Labels:{%s}", strings.Join(labelStrs, ",")))

	var annotationStrs []string
	for k, prop := range p.Annotations {
		annotationStrs = append(annotationStrs, fmt.Sprintf("%s:%s", k, prop))
	}
	strs = append(strs, fmt.Sprintf("Annotations:{%s}", strings.Join(annotationStrs, ",")))

	return fmt.Sprintf("{%s}", strings.Join(strs, "; "))
}
