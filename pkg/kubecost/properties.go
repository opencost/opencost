package kubecost

import (
	"fmt"
	"sort"
	"strings"

	util "github.com/kubecost/cost-model/pkg/util"
)

type Property string

const (
	NilProp            Property = ""
	ClusterProp        Property = "cluster"
	NodeProp           Property = "node"
	ContainerProp      Property = "container"
	ControllerProp     Property = "controller"
	ControllerKindProp Property = "controllerKind"
	LabelProp          Property = "label"
	AnnotationProp     Property = "annotation"
	NamespaceProp      Property = "namespace"
	PodProp            Property = "pod"
	ServiceProp        Property = "service"
)

var availableProperties []Property = []Property{
	NilProp,
	ClusterProp,
	NodeProp,
	ContainerProp,
	ControllerProp,
	ControllerKindProp,
	LabelProp,
	AnnotationProp,
	NamespaceProp,
	PodProp,
	ServiceProp,
}

func ParseProperty(prop string) Property {
	for _, property := range availableProperties {
		if strings.ToLower(string(property)) == strings.ToLower(prop) {
			return property
		}
	}
	return NilProp
}

func (p Property) String() string {
	return string(p)
}

type PropertyValue struct {
	Property Property
	Value    interface{}
}

// Properties describes a set of Kubernetes objects.
type Properties map[Property]interface{}

// TODO niko/etl make sure Services deep copy works correctly
func (p *Properties) Clone() Properties {
	if p == nil {
		return nil
	}

	clone := make(Properties, len(*p))
	for k, v := range *p {
		clone[k] = v
	}
	return clone
}

func (p *Properties) Equal(that *Properties) bool {
	if p == nil || that == nil {
		return false
	}

	if p.Length() != that.Length() {
		return false
	}

	pCluster, _ := p.GetCluster()
	thatCluster, _ := that.GetCluster()
	if pCluster != thatCluster {
		return false
	}

	pNode, _ := p.GetNode()
	thatNode, _ := that.GetNode()
	if pNode != thatNode {
		return false
	}

	pContainer, _ := p.GetContainer()
	thatContainer, _ := that.GetContainer()
	if pContainer != thatContainer {
		return false
	}

	pController, _ := p.GetController()
	thatController, _ := that.GetController()
	if pController != thatController {
		return false
	}

	pControllerKind, _ := p.GetControllerKind()
	thatControllerKind, _ := that.GetControllerKind()
	if pControllerKind != thatControllerKind {
		return false
	}

	pNamespace, _ := p.GetNamespace()
	thatNamespace, _ := that.GetNamespace()
	if pNamespace != thatNamespace {
		return false
	}

	pPod, _ := p.GetPod()
	thatPod, _ := that.GetPod()
	if pPod != thatPod {
		return false
	}

	pLabels, _ := p.GetLabels()
	thatLabels, _ := that.GetLabels()
	if len(pLabels) != len(thatLabels) {
		for k, pv := range pLabels {
			tv, ok := thatLabels[k]
			if !ok || tv != pv {
				return false
			}
		}
		return false
	}

	pAnnotations, _ := p.GetAnnotations()
	thatAnnotations, _ := that.GetAnnotations()
	if len(pAnnotations) != len(thatAnnotations) {
		for k, pv := range pAnnotations {
			tv, ok := thatAnnotations[k]
			if !ok || tv != pv {
				return false
			}
		}
		return false
	}

	pServices, _ := p.GetServices()
	thatServices, _ := that.GetServices()
	if len(pServices) != len(thatServices) {
		sort.Strings(pServices)
		sort.Strings(thatServices)
		for i, pv := range pServices {
			tv := thatServices[i]
			if tv != pv {
				return false
			}
		}
		return false
	}

	return true
}

func (p *Properties) Intersection(that Properties) Properties {
	spec := &Properties{}

	sCluster, sErr := p.GetCluster()
	tCluster, tErr := that.GetCluster()
	if sErr == nil && tErr == nil && sCluster == tCluster {
		spec.SetCluster(sCluster)
	}

	sNode, sErr := p.GetNode()
	tNode, tErr := that.GetNode()
	if sErr == nil && tErr == nil && sNode == tNode {
		spec.SetNode(sNode)
	}

	sContainer, sErr := p.GetContainer()
	tContainer, tErr := that.GetContainer()
	if sErr == nil && tErr == nil && sContainer == tContainer {
		spec.SetContainer(sContainer)
	}

	sController, sErr := p.GetController()
	tController, tErr := that.GetController()
	if sErr == nil && tErr == nil && sController == tController {
		spec.SetController(sController)
	}

	sControllerKind, sErr := p.GetControllerKind()
	tControllerKind, tErr := that.GetControllerKind()
	if sErr == nil && tErr == nil && sControllerKind == tControllerKind {
		spec.SetControllerKind(sControllerKind)
	}

	sNamespace, sErr := p.GetNamespace()
	tNamespace, tErr := that.GetNamespace()
	if sErr == nil && tErr == nil && sNamespace == tNamespace {
		spec.SetNamespace(sNamespace)
	}

	sPod, sErr := p.GetPod()
	tPod, tErr := that.GetPod()
	if sErr == nil && tErr == nil && sPod == tPod {
		spec.SetPod(sPod)
	}

	// TODO niko/etl intersection of services and labels and annotations

	return *spec
}

// Length returns the number of Properties
func (p *Properties) Length() int {
	if p == nil {
		return 0
	}
	return len(*p)
}

// TODO: deprecate
func (p *Properties) Matches(that Properties) bool {
	// The only Properties that a nil Properties matches is an empty one
	if p == nil {
		return that.Length() == 0
	}

	// Matching on cluster, namespace, controller, controller kind, pod,
	// and container are simple string equality comparisons. By default,
	// we assume a match. For each Property given to match, we say that the
	// match fails if we don't have that Property, or if we have it but the
	// strings are not equal.

	if thatCluster, thatErr := that.GetCluster(); thatErr == nil {
		if thisCluster, thisErr := p.GetCluster(); thisErr != nil || thisCluster != thatCluster {
			return false
		}
	}

	if thatNode, thatErr := that.GetNode(); thatErr == nil {
		if thisNode, thisErr := p.GetNode(); thisErr != nil || thisNode != thatNode {
			return false
		}
	}

	if thatNamespace, thatErr := that.GetNamespace(); thatErr == nil {
		if thisNamespace, thisErr := p.GetNamespace(); thisErr != nil || thisNamespace != thatNamespace {
			return false
		}
	}

	if thatController, thatErr := that.GetController(); thatErr == nil {
		if thisController, thisErr := p.GetController(); thisErr != nil || thisController != thatController {
			return false
		}
	}

	if thatControllerKind, thatErr := that.GetControllerKind(); thatErr == nil {
		if thisControllerKind, thisErr := p.GetControllerKind(); thisErr != nil || thisControllerKind != thatControllerKind {
			return false
		}
	}

	if thatPod, thatErr := that.GetPod(); thatErr == nil {
		if thisPod, thisErr := p.GetPod(); thisErr != nil || thisPod != thatPod {
			return false
		}
	}

	if thatContainer, thatErr := that.GetContainer(); thatErr == nil {
		if thisContainer, thisErr := p.GetContainer(); thisErr != nil || thisContainer != thatContainer {
			return false
		}
	}

	// Matching on Services only occurs if a non-zero length slice of strings
	// is given. The comparison fails if there exists a string to match that is
	// not present in our slice of services.
	if thatServices, thatErr := that.GetServices(); thatErr == nil && len(thatServices) > 0 {
		thisServices, thisErr := p.GetServices()
		if thisErr != nil {
			return false
		}

		for _, service := range thatServices {
			match := false
			for _, s := range thisServices {
				if s == service {
					match = true
					break
				}
			}
			if !match {
				return false
			}
		}
	}

	// Matching on Labels only occurs if a non-zero length map of strings is
	// given. The comparison fails if there exists a key/value pair to match
	// that is not present in our set of labels.
	if thatServices, thatErr := that.GetServices(); thatErr == nil && len(thatServices) > 0 {
		thisServices, thisErr := p.GetServices()
		if thisErr != nil {
			return false
		}

		for _, service := range thatServices {
			match := false
			for _, s := range thisServices {
				if s == service {
					match = true
					break
				}
			}
			if !match {
				return false
			}
		}
	}

	return true
}

func (p *Properties) String() string {
	if p == nil {
		return "<nil>"
	}

	strs := []string{}
	for key, prop := range *p {
		strs = append(strs, fmt.Sprintf("%s:%s", key, prop))
	}
	return fmt.Sprintf("{%s}", strings.Join(strs, "; "))
}

func (p *Properties) Get(prop Property) (string, error) {
	if raw, ok := (*p)[prop]; ok {
		if result, ok := raw.(string); ok {
			return result, nil
		}
		return "", fmt.Errorf("%s is not a string", prop)
	}
	return "", fmt.Errorf("%s not set", prop)
}

func (p *Properties) Has(prop Property) bool {
	_, ok := (*p)[prop]
	return ok
}

func (p *Properties) Set(prop Property, value string) {
	(*p)[prop] = value
}

func (p *Properties) GetCluster() (string, error) {
	if raw, ok := (*p)[ClusterProp]; ok {
		if cluster, ok := raw.(string); ok {
			return cluster, nil
		}
		return "", fmt.Errorf("ClusterProp is not a string")
	}
	return "", fmt.Errorf("ClusterProp not set")
}

func (p *Properties) HasCluster() bool {
	_, ok := (*p)[ClusterProp]
	return ok
}

func (p *Properties) SetCluster(cluster string) {
	(*p)[ClusterProp] = cluster
}

func (p *Properties) GetNode() (string, error) {
	if raw, ok := (*p)[NodeProp]; ok {
		if node, ok := raw.(string); ok {
			return node, nil
		}
		return "", fmt.Errorf("NodeProp is not a string")
	}
	return "", fmt.Errorf("NodeProp not set")
}

func (p *Properties) HasNode() bool {
	_, ok := (*p)[NodeProp]
	return ok
}

func (p *Properties) SetNode(node string) {
	(*p)[NodeProp] = node
}

func (p *Properties) GetContainer() (string, error) {
	if raw, ok := (*p)[ContainerProp]; ok {
		if container, ok := raw.(string); ok {
			return container, nil
		}
		return "", fmt.Errorf("ContainerProp is not a string")
	}
	return "", fmt.Errorf("ContainerProp not set")
}

func (p *Properties) HasContainer() bool {
	_, ok := (*p)[ContainerProp]
	return ok
}

func (p *Properties) SetContainer(container string) {
	(*p)[ContainerProp] = container
}

func (p *Properties) GetController() (string, error) {
	if raw, ok := (*p)[ControllerProp]; ok {
		if controller, ok := raw.(string); ok {
			return controller, nil
		}
		return "", fmt.Errorf("ControllerProp is not a string")
	}
	return "", fmt.Errorf("ControllerProp not set")
}

func (p *Properties) HasController() bool {
	_, ok := (*p)[ControllerProp]
	return ok
}

func (p *Properties) SetController(controller string) {
	(*p)[ControllerProp] = controller
}

func (p *Properties) GetControllerKind() (string, error) {
	if raw, ok := (*p)[ControllerKindProp]; ok {
		if controllerKind, ok := raw.(string); ok {
			return controllerKind, nil
		}
		return "", fmt.Errorf("ControllerKindProp is not a string")
	}
	return "", fmt.Errorf("ControllerKindProp not set")
}

func (p *Properties) HasControllerKind() bool {
	_, ok := (*p)[ControllerKindProp]
	return ok
}

func (p *Properties) SetControllerKind(controllerKind string) {
	(*p)[ControllerKindProp] = controllerKind
}

func (p *Properties) GetLabels() (map[string]string, error) {
	if raw, ok := (*p)[LabelProp]; ok {
		if labels, ok := raw.(map[string]string); ok {
			return labels, nil
		}
		return map[string]string{}, fmt.Errorf("LabelProp is not a map[string]string")
	}
	return map[string]string{}, fmt.Errorf("LabelProp not set")
}

func (p *Properties) HasLabel() bool {
	_, ok := (*p)[LabelProp]
	return ok
}

func (p *Properties) SetLabels(labels map[string]string) {
	(*p)[LabelProp] = labels
}

func (p *Properties) GetAnnotations() (map[string]string, error) {
	if raw, ok := (*p)[AnnotationProp]; ok {
		if annotations, ok := raw.(map[string]string); ok {
			return annotations, nil
		}
		return map[string]string{}, fmt.Errorf("AnnotationProp is not a map[string]string")
	}
	return map[string]string{}, fmt.Errorf("AnnotationProp not set")
}

func (p *Properties) HasAnnotations() bool {
	_, ok := (*p)[AnnotationProp]
	return ok
}

func (p *Properties) SetAnnotations(annotations map[string]string) {
	(*p)[AnnotationProp] = annotations
}

func (p *Properties) GetNamespace() (string, error) {
	if raw, ok := (*p)[NamespaceProp]; ok {
		if namespace, ok := raw.(string); ok {
			return namespace, nil
		}
		return "", fmt.Errorf("NamespaceProp is not a string")
	}
	return "", fmt.Errorf("NamespaceProp not set")
}

func (p *Properties) HasNamespace() bool {
	_, ok := (*p)[NamespaceProp]
	return ok
}

func (p *Properties) SetNamespace(namespace string) {
	(*p)[NamespaceProp] = namespace
}

func (p *Properties) GetPod() (string, error) {
	if raw, ok := (*p)[PodProp]; ok {
		if pod, ok := raw.(string); ok {
			return pod, nil
		}
		return "", fmt.Errorf("PodProp is not a string")
	}
	return "", fmt.Errorf("PodProp not set")
}

func (p *Properties) HasPod() bool {
	_, ok := (*p)[PodProp]
	return ok
}

func (p *Properties) SetPod(pod string) {
	(*p)[PodProp] = pod
}

func (p *Properties) GetServices() ([]string, error) {
	if raw, ok := (*p)[ServiceProp]; ok {
		if services, ok := raw.([]string); ok {
			return services, nil
		}
		return []string{}, fmt.Errorf("ServiceProp is not a string")
	}
	return []string{}, fmt.Errorf("ServiceProp not set")
}

func (p *Properties) HasService() bool {
	_, ok := (*p)[ServiceProp]
	return ok
}

func (p *Properties) SetServices(services []string) {
	(*p)[ServiceProp] = services
}

func (p *Properties) MarshalBinary() (data []byte, err error) {
	buff := util.NewBuffer()
	buff.WriteUInt8(CodecVersion) // version

	// ClusterProp
	cluster, err := p.GetCluster()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1)) // write non-nil byte
		buff.WriteString(cluster) // write string
	}

	// NodeProp
	node, err := p.GetNode()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1)) // write non-nil byte
		buff.WriteString(node)    // write string
	}

	// ContainerProp
	container, err := p.GetContainer()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))   // write non-nil byte
		buff.WriteString(container) // write string
	}

	// ControllerProp
	controller, err := p.GetController()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))    // write non-nil byte
		buff.WriteString(controller) // write string
	}

	// ControllerKindProp
	controllerKind, err := p.GetControllerKind()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))        // write non-nil byte
		buff.WriteString(controllerKind) // write string
	}

	// NamespaceProp
	namespace, err := p.GetNamespace()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))   // write non-nil byte
		buff.WriteString(namespace) // write string
	}

	// PodProp
	pod, err := p.GetPod()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1)) // write non-nil byte
		buff.WriteString(pod)     // write string
	}

	// LabelProp
	labels, err := p.GetLabels()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))  // write non-nil byte
		buff.WriteInt(len(labels)) // map length
		for k, v := range labels {
			buff.WriteString(k) // write string
			buff.WriteString(v) // write string
		}
	}

	// AnnotationProp
	annotations, err := p.GetAnnotations()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))       // write non-nil byte
		buff.WriteInt(len(annotations)) // map length
		for k, v := range annotations {
			buff.WriteString(k) // write string
			buff.WriteString(v) // write string
		}
	}

	// ServiceProp
	services, err := p.GetServices()
	if err != nil {
		buff.WriteUInt8(uint8(0)) // write nil byte
	} else {
		buff.WriteUInt8(uint8(1))    // write non-nil byte
		buff.WriteInt(len(services)) // slice length
		for _, v := range services {
			buff.WriteString(v) // write string
		}
	}

	return buff.Bytes(), nil
}

func (p *Properties) UnmarshalBinary(data []byte) error {
	buff := util.NewBufferFromBytes(data)
	v := buff.ReadUInt8() // version
	if v != CodecVersion {
		return fmt.Errorf("Invalid Version. Expected %d, got %d", CodecVersion, v)
	}

	*p = Properties{}

	// ClusterProp
	if buff.ReadUInt8() == 1 { // read nil byte
		cluster := buff.ReadString() // read string
		p.SetCluster(cluster)
	}

	// NodeProp
	if buff.ReadUInt8() == 1 { // read nil byte
		node := buff.ReadString() // read string
		p.SetNode(node)
	}

	// ContainerProp
	if buff.ReadUInt8() == 1 { // read nil byte
		container := buff.ReadString() // read string
		p.SetContainer(container)
	}

	// ControllerProp
	if buff.ReadUInt8() == 1 { // read nil byte
		controller := buff.ReadString() // read string
		p.SetController(controller)
	}

	// ControllerKindProp
	if buff.ReadUInt8() == 1 { // read nil byte
		controllerKind := buff.ReadString() // read string
		p.SetControllerKind(controllerKind)
	}

	// NamespaceProp
	if buff.ReadUInt8() == 1 { // read nil byte
		namespace := buff.ReadString() // read string
		p.SetNamespace(namespace)
	}

	// PodProp
	if buff.ReadUInt8() == 1 { // read nil byte
		pod := buff.ReadString() // read string
		p.SetPod(pod)
	}

	// LabelProp
	if buff.ReadUInt8() == 1 { // read nil byte
		length := buff.ReadInt() // read map len
		labels := make(map[string]string, length)
		for idx := 0; idx < length; idx++ {
			key := buff.ReadString()
			val := buff.ReadString()
			labels[key] = val
		}
		p.SetLabels(labels)
	}

	// AnnotationProp
	if buff.ReadUInt8() == 1 { // read nil byte
		length := buff.ReadInt() // read map len
		annotations := make(map[string]string, length)
		for idx := 0; idx < length; idx++ {
			key := buff.ReadString()
			val := buff.ReadString()
			annotations[key] = val
		}
		p.SetAnnotations(annotations)
	}

	// ServiceProp
	if buff.ReadUInt8() == 1 { // read nil byte
		length := buff.ReadInt() // read map len
		services := make([]string, length)
		for idx := 0; idx < length; idx++ {
			val := buff.ReadString()
			services[idx] = val
		}
		p.SetServices(services)
	}

	return nil
}
