package cloud

import (
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/kubecost/cost-model/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
)

type NodePrice struct {
	CPU string
	RAM string
	GPU string
}

type CustomProvider struct {
	Clientset               clustercache.ClusterCache
	Pricing                 map[string]*NodePrice
	SpotLabel               string
	SpotLabelValue          string
	GPULabel                string
	GPULabelValue           string
	DownloadPricingDataLock sync.RWMutex
	Config                  *ProviderConfig
}

type customProviderKey struct {
	SpotLabel      string
	SpotLabelValue string
	GPULabel       string
	GPULabelValue  string
	Labels         map[string]string
}

func (*CustomProvider) GetLocalStorageQuery(window, offset string, rate bool, used bool) string {
	return ""
}

func (cp *CustomProvider) GetConfig() (*CustomPricing, error) {
	return cp.Config.GetCustomPricingData()
}

func (*CustomProvider) GetManagementPlatform() (string, error) {
	return "", nil
}

func (*CustomProvider) ApplyReservedInstancePricing(nodes map[string]*Node) {

}

func (cp *CustomProvider) UpdateConfigFromConfigMap(a map[string]string) (*CustomPricing, error) {
	return cp.Config.UpdateFromMap(a)
}

func (cp *CustomProvider) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	// Parse config updates from reader
	a := make(map[string]interface{})
	err := json.NewDecoder(r).Decode(&a)
	if err != nil {
		return nil, err
	}

	// Update Config
	c, err := cp.Config.Update(func(c *CustomPricing) error {
		for k, v := range a {
			kUpper := strings.Title(k) // Just so we consistently supply / receive the same values, uppercase the first letter.
			vstr, ok := v.(string)
			if ok {
				err := SetCustomPricingField(c, kUpper, vstr)
				if err != nil {
					return err
				}
			} else {
				sci := v.(map[string]interface{})
				sc := make(map[string]string)
				for k, val := range sci {
					sc[k] = val.(string)
				}
				c.SharedCosts = sc //todo: support reflection/multiple map fields
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	defer cp.DownloadPricingData()
	return c, nil
}

func (cp *CustomProvider) ClusterInfo() (map[string]string, error) {
	conf, err := cp.GetConfig()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	if conf.ClusterName != "" {
		m["name"] = conf.ClusterName
	}
	m["provider"] = "custom"
	return m, nil
}

func (*CustomProvider) AddServiceKey(url.Values) error {
	return nil
}

func (*CustomProvider) GetDisks() ([]byte, error) {
	return nil, nil
}

func (cp *CustomProvider) AllNodePricing() (interface{}, error) {
	cp.DownloadPricingDataLock.RLock()
	defer cp.DownloadPricingDataLock.RUnlock()

	return cp.Pricing, nil
}

func (cp *CustomProvider) NodePricing(key Key) (*Node, error) {
	cp.DownloadPricingDataLock.RLock()
	defer cp.DownloadPricingDataLock.RUnlock()

	k := key.Features()
	var gpuCount string
	if _, ok := cp.Pricing[k]; !ok {
		k = "default"
	}
	if key.GPUType() != "" {
		k += ",gpu"    // TODO: support multiple custom gpu types.
		gpuCount = "1" // TODO: support more than one gpu.
	}

	return &Node{
		VCPUCost: cp.Pricing[k].CPU,
		RAMCost:  cp.Pricing[k].RAM,
		GPUCost:  cp.Pricing[k].GPU,
		GPU:      gpuCount,
	}, nil
}

func (cp *CustomProvider) DownloadPricingData() error {
	cp.DownloadPricingDataLock.Lock()
	defer cp.DownloadPricingDataLock.Unlock()

	if cp.Pricing == nil {
		m := make(map[string]*NodePrice)
		cp.Pricing = m
	}
	p, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return err
	}
	cp.SpotLabel = p.SpotLabel
	cp.SpotLabelValue = p.SpotLabelValue
	cp.GPULabel = p.GpuLabel
	cp.GPULabelValue = p.GpuLabelValue
	cp.Pricing["default"] = &NodePrice{
		CPU: p.CPU,
		RAM: p.RAM,
	}
	cp.Pricing["default,spot"] = &NodePrice{
		CPU: p.SpotCPU,
		RAM: p.SpotRAM,
	}
	cp.Pricing["default,gpu"] = &NodePrice{
		CPU: p.CPU,
		RAM: p.RAM,
		GPU: p.GPU,
	}
	return nil
}

func (cp *CustomProvider) GetKey(labels map[string]string) Key {
	return &customProviderKey{
		SpotLabel:      cp.SpotLabel,
		SpotLabelValue: cp.SpotLabelValue,
		GPULabel:       cp.GPULabel,
		GPULabelValue:  cp.GPULabelValue,
		Labels:         labels,
	}
}

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (*CustomProvider) ExternalAllocations(start string, end string, aggregator []string, filterType string, filterValue string, crossCluster bool) ([]*OutOfClusterAllocation, error) {
	return nil, nil // TODO: transform the QuerySQL lines into the new OutOfClusterAllocation Struct
}

func (*CustomProvider) QuerySQL(query string) ([]byte, error) {
	return nil, nil
}

func (cp *CustomProvider) PVPricing(pvk PVKey) (*PV, error) {
	cpricing, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	return &PV{
		Cost: cpricing.Storage,
	}, nil
}

func (cp *CustomProvider) NetworkPricing() (*Network, error) {
	cpricing, err := cp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	znec, err := strconv.ParseFloat(cpricing.ZoneNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	rnec, err := strconv.ParseFloat(cpricing.RegionNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	inec, err := strconv.ParseFloat(cpricing.InternetNetworkEgress, 64)
	if err != nil {
		return nil, err
	}

	return &Network{
		ZoneNetworkEgressCost:     znec,
		RegionNetworkEgressCost:   rnec,
		InternetNetworkEgressCost: inec,
	}, nil
}

func (*CustomProvider) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string, defaultRegion string) PVKey {
	return &awsPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		DefaultRegion:          defaultRegion,
	}
}

func (cpk *customProviderKey) GPUType() string {
	if t, ok := cpk.Labels[cpk.GPULabel]; ok {
		return t
	}
	return ""
}

func (cpk *customProviderKey) ID() string {
	return ""
}

func (cpk *customProviderKey) Features() string {
	if cpk.Labels[cpk.SpotLabel] != "" && cpk.Labels[cpk.SpotLabel] == cpk.SpotLabelValue {
		return "default,spot"
	}
	return "default" // TODO: multiple custom pricing support.
}
