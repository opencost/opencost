package cloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"

	"k8s.io/klog"

	"cloud.google.com/go/compute/metadata"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Node is the interface by which the provider and cost model communicate Node prices.
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
	BaseRAMPrice     string `json:"baseRAMPrice"` // Used to compute an implicit RAM GB/Hr price when RAM pricing is not provided.
	BaseGPUPrice     string `json:"baseGPUPrice"`
	UsageType        string `json:"usageType"`
	GPU              string `json:"gpu"` // GPU represents the number of GPU on the instance
	GPUName          string `json:"gpuName"`
	GPUCost          string `json:"gpuCost"`
}

// PV is the interface by which the provider and cost model communicate PV prices.
// The provider will best-effort try to fill out this struct.
type PV struct {
	Cost       string            `json:"hourlyCost"`
	CostPerIO  string            `json:"costPerIOOperation"`
	Class      string            `json:"storageClass"`
	Size       string            `json:"size"`
	Region     string            `json:"region"`
	Parameters map[string]string `json:"parameters"`
}

// Key represents a way for nodes to match between the k8s API and a pricing API
type Key interface {
	ID() string       // ID represents an exact match
	Features() string // Features are a comma separated string of node metadata that could match pricing
	GPUType() string  // GPUType returns "" if no GPU exists, but the name of the GPU otherwise
}

type PVKey interface {
	Features() string
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
	PVPricing(PVKey) (*PV, error)
	AllNodePricing() (interface{}, error)
	DownloadPricingData() error
	GetKey(map[string]string) Key
	GetPVKey(*v1.PersistentVolume, map[string]string) PVKey
	UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error)
	GetConfig() (*CustomPricing, error)

	ExternalAllocations(string, string, string) ([]*OutOfClusterAllocation, error)
}

// GetDefaultPricingData will search for a json file representing pricing data in /models/ and use it for base pricing info.
func GetDefaultPricingData(fname string) (*CustomPricing, error) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/models/"
	}
	path += fname
	if _, err := os.Stat(path); err == nil {
		jsonFile, err := os.Open(path)
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
	} else if os.IsNotExist(err) {
		c := &CustomPricing{
			Provider:            fname,
			Description:         "Default prices based on GCP us-central1",
			CPU:                 "0.031611",
			SpotCPU:             "0.006655",
			RAM:                 "0.004237",
			SpotRAM:             "0.000892",
			GPU:                 "0.95",
			CustomPricesEnabled: "false",
		}
		cj, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}

		err = ioutil.WriteFile(path, cj, 0644)
		if err != nil {
			return nil, err
		}
		return c, nil
	} else {
		return nil, err
	}
}

const KeyUpdateType = "athenainfo"

type CustomPricing struct {
	Provider            string `json:"provider"`
	Description         string `json:"description"`
	CPU                 string `json:"CPU"`
	SpotCPU             string `json:"spotCPU"`
	RAM                 string `json:"RAM"`
	SpotRAM             string `json:"spotRAM"`
	GPU                 string `json:"GPU"`
	SpotLabel           string `json:"spotLabel,omitempty"`
	SpotLabelValue      string `json:"spotLabelValue,omitempty"`
	ServiceKeyName      string `json:"awsServiceKeyName,omitempty"`
	ServiceKeySecret    string `json:"awsServiceKeySecret,omitempty"`
	SpotDataRegion      string `json:"awsSpotDataRegion,omitempty"`
	SpotDataBucket      string `json:"awsSpotDataBucket,omitempty"`
	SpotDataPrefix      string `json:"awsSpotDataPrefix,omitempty"`
	ProjectID           string `json:"projectID,omitempty"`
	AthenaBucketName    string `json:"athenaBucketName"`
	AthenaRegion        string `json:"athenaRegion"`
	AthenaDatabase      string `json:"athenaDatabase"`
	AthenaTable         string `json:"athenaTable"`
	BillingDataDataset  string `json:"billingDataDataset,omitempty"`
	CustomPricesEnabled string `json:"customPricesEnabled"`
	AzureSubscriptionID string `json:"azureSubscriptionID"`
	AzureClientID       string `json:"azureClientID"`
	AzureClientSecret   string `json:"azureClientSecret"`
	AzureTenantID       string `json:"azureTenantID"`
	CurrencyCode        string `json:"currencyCode"`
}

func SetCustomPricingField(obj *CustomPricing, name string, value string) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return fmt.Errorf("Provided value type didn't match custom pricing field type")
	}

	structFieldValue.Set(val)
	return nil
}

type NodePrice struct {
	CPU string
	RAM string
}

type CustomProvider struct {
	Clientset               *kubernetes.Clientset
	Pricing                 map[string]*NodePrice
	SpotLabel               string
	SpotLabelValue          string
	DownloadPricingDataLock sync.RWMutex
}

func (*CustomProvider) GetConfig() (*CustomPricing, error) {
	return GetDefaultPricingData("default.json")
}

func (*CustomProvider) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	c, err := GetDefaultPricingData("default.json")
	if err != nil {
		return nil, err
	}
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/models/"
	}
	a := make(map[string]string)
	err = json.NewDecoder(r).Decode(&a)
	if err != nil {
		return nil, err
	}
	for k, v := range a {
		kUpper := strings.Title(k) // Just so we consistently supply / receive the same values, uppercase the first letter.
		err := SetCustomPricingField(c, kUpper, v)
		if err != nil {
			return nil, err
		}
	}

	cj, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	configPath := path + "default.json"
	err = ioutil.WriteFile(configPath, cj, 0644)
	if err != nil {
		return nil, err
	}

	return c, nil

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
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

	return c.Pricing, nil
}

func (c *CustomProvider) NodePricing(key Key) (*Node, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()

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
	c.DownloadPricingDataLock.Lock()
	defer c.DownloadPricingDataLock.Unlock()

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

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (*CustomProvider) ExternalAllocations(start string, end string, aggregator string) ([]*OutOfClusterAllocation, error) {
	return nil, nil // TODO: transform the QuerySQL lines into the new OutOfClusterAllocation Struct
}

func (*CustomProvider) QuerySQL(query string) ([]byte, error) {
	return nil, nil
}

func (*CustomProvider) PVPricing(pvk PVKey) (*PV, error) {
	return nil, nil
}

func (*CustomProvider) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string) PVKey {
	return &awsPVKey{
		Labels:           pv.Labels,
		StorageClassName: pv.Spec.StorageClassName,
	}
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
		return &Azure{}, nil
	} else {
		klog.V(2).Info("Unsupported provider, falling back to default")
		return &CustomProvider{
			Clientset: clientset,
		}, nil
	}
}
