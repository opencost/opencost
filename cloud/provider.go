package cloud

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"

	"k8s.io/klog"

	"cloud.google.com/go/compute/metadata"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Node is the interface by which the provider and cost model communicate.
// The provider will best-effort try to fill out this struct.
type Node struct {
	Cost             string `json:"hourlyCost"`
	VCPU             string `json:"CPU"`
	VCPUCost         string `json:"CPUHourlyCost"`
	RAM              string `json:"RAM"`
	RAMBytes         string `json:"RAMBytes"`
	RAMCost          string `json:"RAMGBHourlyCost"`
	Storage          string `json:"storage"`
	StorageCost      string `json:"storageHourlyCost"`
	UsesBaseCPUPrice bool   `json:"usesDefaultPrice"`
	BaseCPUPrice     string `json:"baseCPUPrice"` // Used to compute an implicit RAM GB/Hr price when RAM pricing is not provided.
	UsageType        string `json:"usageType"`
	GPU              string `json:"gpu"`
	GPUName          string `json:"gpuName"`
	GPUCost          string `json:"gpuCost"`
}

// Key represents a way for nodes to match between the k8s API and a pricing API
type Key interface {
	ID() string       // ID represents an exact match
	Features() string // Features are a comma separated string of node metadata that could match pricing
	GPUType() string  // GPUType returns "" if no GPU exists, but the name of the GPU otherwise
}

// OutOfClusterAllocation represents a cloud provider cost not associated with kubernetes
type OutOfClusterAllocation struct {
	Aggregator  string  `json:"aggregator"`
	Environment string  `json:"environment"`
	Service     string  `json:"service"`
	Cost        float64 `json:"cost"`
	Cluster     string  `json:"cluster"`
}

// Provider represents a k8s provider.
type Provider interface {
	ClusterName() ([]byte, error)
	AddServiceKey(url.Values) error
	GetDisks() ([]byte, error)
	NodePricing(Key) (*Node, error)
	AllNodePricing() (interface{}, error)
	DownloadPricingData() error
	GetKey(map[string]string) Key
	UpdateConfig(r io.Reader) (*CustomPricing, error)
	GetConfig() (*CustomPricing, error)

	ExternalAllocations(string, string) ([]*OutOfClusterAllocation, error)
}

// GetDefaultPricingData will search for a json file representing pricing data in /models/ and use it for base pricing info.
func GetDefaultPricingData(fname string) (*CustomPricing, error) {
	jsonFile, err := os.Open("/models/" + fname)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}
	var customPricing = &CustomPricing{}
	err = json.Unmarshal([]byte(byteValue), customPricing)
	if err != nil {
		return nil, err
	}
	return customPricing, nil
}

type CustomPricing struct {
	Provider           string `json:"provider"`
	Description        string `json:"description"`
	CPU                string `json:"CPU"`
	SpotCPU            string `json:"spotCPU"`
	RAM                string `json:"RAM"`
	SpotRAM            string `json:"spotRAM"`
	SpotLabel          string `json:"spotLabel,omitempty"`
	SpotLabelValue     string `json:"spotLabelValue,omitempty"`
	ServiceKeyName     string `json:"awsServiceKeyName,omitempty"`
	ServiceKeySecret   string `json:"awsServiceKeySecret,omitempty"`
	SpotDataRegion     string `json:"awsSpotDataRegion,omitempty"`
	SpotDataBucket     string `json:"awsSpotDataBucket,omitempty"`
	SpotDataPrefix     string `json:"awsSpotDataPrefix,omitempty"`
	ProjectID          string `json:"projectID,omitempty"`
	BillingDataDataset string `json:"billingDataDataset,omitempty"`
}

type NodePrice struct {
	CPU string
	RAM string
}

type CustomProvider struct {
	Clientset      *kubernetes.Clientset
	Pricing        map[string]*NodePrice
	SpotLabel      string
	SpotLabelValue string
}

func (*CustomProvider) GetConfig() (*CustomPricing, error) {
	return nil, nil
}

func (*CustomProvider) UpdateConfig(r io.Reader) (*CustomPricing, error) {
	return nil, nil
}

func (*CustomProvider) ClusterName() ([]byte, error) {
	return nil, nil
}

func (*CustomProvider) AddServiceKey(url.Values) error {
	return nil
}

func (*CustomProvider) GetDisks() ([]byte, error) {
	return nil, nil
}

func (c *CustomProvider) AllNodePricing() (interface{}, error) {
	return c.Pricing, nil
}

func (c *CustomProvider) NodePricing(key Key) (*Node, error) {
	k := key.Features()
	if _, ok := c.Pricing[k]; !ok {
		k = "default"
	}
	return &Node{
		VCPUCost: c.Pricing[k].CPU,
		RAMCost:  c.Pricing[k].RAM,
	}, nil
}

func (c *CustomProvider) DownloadPricingData() error {

	if c.Pricing == nil {
		m := make(map[string]*NodePrice)
		c.Pricing = m
	}
	p, err := GetDefaultPricingData("default.json")
	if err != nil {
		return err
	}
	c.Pricing["default"] = &NodePrice{
		CPU: p.CPU,
		RAM: p.RAM,
	}
	c.Pricing["default,spot"] = &NodePrice{
		CPU: p.SpotCPU,
		RAM: p.SpotRAM,
	}
	return nil
}

type customProviderKey struct {
	SpotLabel      string
	SpotLabelValue string
	Labels         map[string]string
}

func (c *customProviderKey) GPUType() string {
	return ""
}

func (c *customProviderKey) ID() string {
	return ""
}

func (c *customProviderKey) Features() string {
	if c.Labels[c.SpotLabel] != "" && c.Labels[c.SpotLabel] == c.SpotLabelValue {
		return "default,spot"
	}
	return "default" // TODO: multiple custom pricing support.
}

func (c *CustomProvider) GetKey(labels map[string]string) Key {
	return &customProviderKey{
		SpotLabel:      c.SpotLabel,
		SpotLabelValue: c.SpotLabelValue,
		Labels:         labels,
	}
}

func (*CustomProvider) ExternalAllocations(start string, end string) ([]*OutOfClusterAllocation, error) {
	return nil, nil // TODO: transform the QuerySQL lines into the new OutOfClusterAllocation Struct
}

func (*CustomProvider) QuerySQL(query string) ([]byte, error) {
	return nil, nil
}

// NewProvider looks at the nodespec or provider metadata server to decide which provider to instantiate.
func NewProvider(clientset *kubernetes.Clientset, apiKey string) (Provider, error) {
	if metadata.OnGCE() {
		klog.V(3).Info("metadata reports we are in GCE")
		if apiKey == "" {
			return nil, errors.New("Supply a GCP Key to start getting data")
		}
		return &GCP{
			Clientset: clientset,
			APIKey:    apiKey,
		}, nil
	}
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	provider := strings.ToLower(nodes.Items[0].Spec.ProviderID)
	if strings.HasPrefix(provider, "aws") {
		klog.V(2).Info("Found ProviderID starting with \"aws\", using AWS Provider")
		return &AWS{
			Clientset: clientset,
		}, nil
	} else if strings.HasPrefix(provider, "azure") {
		klog.V(2).Info("Found ProviderID starting with \"azure\", using Azure Provider")
		return &Azure{
			CustomProvider: &CustomProvider{
				Clientset: clientset,
			},
		}, nil
	} else {
		klog.V(2).Info("Unsupported provider, falling back to default")
		return &CustomProvider{
			Clientset: clientset,
		}, nil
	}

}
