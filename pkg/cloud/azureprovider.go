package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/kubecost/cost-model/pkg/clustercache"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2017-09-01/skus"
	"github.com/Azure/azure-sdk-for-go/services/containerservice/mgmt/2018-03-31/containerservice"
	"github.com/Azure/azure-sdk-for-go/services/preview/commerce/mgmt/2015-06-01-preview/commerce"
	"github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2016-06-01/subscriptions"
	"github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2018-05-01/resources"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
)

const (
	AzurePremiumStorageClass  = "premium"
	AzureStandardStorageClass = "standard"
)

var (
	regionCodeMappings = map[string]string{
		"ap": "asia",
		"au": "australia",
		"br": "brazil",
		"ca": "canada",
		"eu": "europe",
		"fr": "france",
		"in": "india",
		"ja": "japan",
		"kr": "korea",
		"uk": "uk",
		"us": "us",
		"za": "southafrica",
	}

	//mtBasic, _     = regexp.Compile("^BASIC.A\\d+[_Promo]*$")
	//mtStandardA, _ = regexp.Compile("^A\\d+[_Promo]*$")
	mtStandardB, _ = regexp.Compile(`^Standard_B\d+m?[_v\d]*[_Promo]*$`)
	mtStandardD, _ = regexp.Compile(`^Standard_D\d[_v\d]*[_Promo]*$`)
	mtStandardE, _ = regexp.Compile(`^Standard_E\d+i?[_v\d]*[_Promo]*$`)
	mtStandardF, _ = regexp.Compile(`^Standard_F\d+[_v\d]*[_Promo]*$`)
	mtStandardG, _ = regexp.Compile(`^Standard_G\d+[_v\d]*[_Promo]*$`)
	mtStandardL, _ = regexp.Compile(`^Standard_L\d+[_v\d]*[_Promo]*$`)
	mtStandardM, _ = regexp.Compile(`^Standard_M\d+[m|t|l]*s[_v\d]*[_Promo]*$`)
	mtStandardN, _ = regexp.Compile(`^Standard_N[C|D|V]\d+r?[_v\d]*[_Promo]*$`)
)

type regionParts []string

func (r regionParts) String() string {
	var result string
	for _, p := range r {
		result += p
	}
	return result
}

func getRegions(service string, subscriptionsClient subscriptions.Client, providersClient resources.ProvidersClient, subscriptionID string) (map[string]string, error) {

	allLocations := make(map[string]string)
	supLocations := make(map[string]string)

	// retrieve all locations for the subscription id (some of them may not be supported by the required provider)
	if locations, err := subscriptionsClient.ListLocations(context.TODO(), subscriptionID); err == nil {
		// fill up the map: DisplayName - > Name
		for _, loc := range *locations.Value {
			allLocations[*loc.DisplayName] = *loc.Name
		}
	} else {
		return nil, err
	}

	// identify supported locations for the namespace and resource type
	const (
		providerNamespaceForCompute = "Microsoft.Compute"
		resourceTypeForCompute      = "locations/vmSizes"
		providerNamespaceForAks     = "Microsoft.ContainerService"
		resourceTypeForAks          = "managedClusters"
	)

	switch service {
	case "aks":
		if providers, err := providersClient.Get(context.TODO(), providerNamespaceForAks, ""); err == nil {
			for _, pr := range *providers.ResourceTypes {
				if *pr.ResourceType == resourceTypeForAks {
					for _, displName := range *pr.Locations {
						if loc, ok := allLocations[displName]; ok {
							supLocations[loc] = displName
						} else {
							klog.V(1).Infof("unsupported cloud region %s", loc)
						}
					}
					break
				}
			}
		} else {
			return nil, err
		}
		return supLocations, nil
	default:
		if providers, err := providersClient.Get(context.TODO(), providerNamespaceForCompute, ""); err == nil {
			for _, pr := range *providers.ResourceTypes {
				if *pr.ResourceType == resourceTypeForCompute {
					for _, displName := range *pr.Locations {
						if loc, ok := allLocations[displName]; ok {
							supLocations[loc] = displName
						} else {
							klog.V(1).Infof("unsupported cloud region %s", loc)
						}
					}
					break
				}
			}
		} else {
			return nil, err
		}

		return supLocations, nil
	}
}

func toRegionID(meterRegion string, regions map[string]string) (string, error) {
	var rp regionParts = strings.Split(strings.ToLower(meterRegion), " ")
	regionCode := regionCodeMappings[rp[0]]
	lastPart := rp[len(rp)-1]
	var regionIds []string
	if _, err := strconv.Atoi(lastPart); err == nil {
		regionIds = []string{
			fmt.Sprintf("%s%s%s", regionCode, rp[1:len(rp)-1], lastPart),
			fmt.Sprintf("%s%s%s", rp[1:len(rp)-1], regionCode, lastPart),
		}
	} else {
		regionIds = []string{
			fmt.Sprintf("%s%s", regionCode, rp[1:]),
			fmt.Sprintf("%s%s", rp[1:], regionCode),
		}
	}
	for _, regionID := range regionIds {
		if checkRegionID(regionID, regions) {
			return regionID, nil
		}
	}
	return "", fmt.Errorf("Couldn't find region")
}

func checkRegionID(regionID string, regions map[string]string) bool {
	for region := range regions {
		if regionID == region {
			return true
		}
	}
	return false
}

// AzurePricing either contains a Node or PV
type AzurePricing struct {
	Node *Node
	PV   *PV
}

type Azure struct {
	Pricing                 map[string]*AzurePricing
	DownloadPricingDataLock sync.RWMutex
	Clientset               clustercache.ClusterCache
	Config                  *ProviderConfig
}

type azureKey struct {
	Labels        map[string]string
	GPULabel      string
	GPULabelValue string
}

func (k *azureKey) Features() string {
	region := strings.ToLower(k.Labels[v1.LabelZoneRegion])
	instance := k.Labels[v1.LabelInstanceType]
	usageType := "ondemand"
	return fmt.Sprintf("%s,%s,%s", region, instance, usageType)
}

func (k *azureKey) GPUType() string {
	if t, ok := k.Labels[k.GPULabel]; ok {
		return t
	}
	return ""

}

func (k *azureKey) ID() string {
	return ""
}

func (az *Azure) GetKey(labels map[string]string) Key {
	cfg, err := az.GetConfig()
	if err != nil {
		klog.Infof("Error loading azure custom pricing information")
	}
	// azure defaults, see https://docs.microsoft.com/en-us/azure/aks/gpu-cluster
	gpuLabel := "accelerator"
	gpuLabelValue := "nvidia"
	if cfg.GpuLabel != "" {
		gpuLabel = cfg.GpuLabel
	}
	if cfg.GpuLabelValue != "" {
		gpuLabelValue = cfg.GpuLabelValue
	}
	return &azureKey{
		Labels:        labels,
		GPULabel:      gpuLabel,
		GPULabelValue: gpuLabelValue,
	}
}

// CreateString builds strings effectively
func createString(keys ...string) string {
	var b strings.Builder
	for _, key := range keys {
		b.WriteString(key)
	}
	return b.String()
}

func transformMachineType(subCategory string, mt []string) []string {
	switch {
	case strings.Contains(subCategory, "Basic"):
		return []string{createString("Basic_", mt[0])}
	case len(mt) == 2:
		return []string{createString("Standard_", mt[0]), createString("Standard_", mt[1])}
	default:
		return []string{createString("Standard_", mt[0])}
	}
}

func addSuffix(mt string, suffixes ...string) []string {
	result := make([]string, len(suffixes))
	var suffix string
	parts := strings.Split(mt, "_")
	if len(parts) > 2 {
		for _, p := range parts[2:] {
			suffix = createString(suffix, "_", p)
		}
	}
	for i, s := range suffixes {
		result[i] = createString(parts[0], "_", parts[1], s, suffix)
	}
	return result
}

func getMachineTypeVariants(mt string) []string {
	switch {
	case mtStandardB.MatchString(mt):
		return []string{createString(mt, "s")}
	case mtStandardD.MatchString(mt):
		var result []string
		result = append(result, addSuffix(mt, "s")[0])
		dsType := strings.Replace(mt, "Standard_D", "Standard_DS", -1)
		result = append(result, dsType)
		result = append(result, addSuffix(dsType, "-1", "-2", "-4", "-8")...)
		return result
	case mtStandardE.MatchString(mt):
		return addSuffix(mt, "s", "-2s", "-4s", "-8s", "-16s", "-32s")
	case mtStandardF.MatchString(mt):
		return addSuffix(mt, "s")
	case mtStandardG.MatchString(mt):
		var result []string
		gsType := strings.Replace(mt, "Standard_G", "Standard_GS", -1)
		result = append(result, gsType)
		return append(result, addSuffix(gsType, "-4", "-8", "-16")...)
	case mtStandardL.MatchString(mt):
		return addSuffix(mt, "s")
	case mtStandardM.MatchString(mt) && strings.HasSuffix(mt, "ms"):
		base := strings.TrimSuffix(mt, "ms")
		return addSuffix(base, "-2ms", "-4ms", "-8ms", "-16ms", "-32ms", "-64ms")
	case mtStandardM.MatchString(mt) && (strings.HasSuffix(mt, "ls") || strings.HasSuffix(mt, "ts")):
		return []string{}
	case mtStandardM.MatchString(mt) && strings.HasSuffix(mt, "s"):
		base := strings.TrimSuffix(mt, "s")
		return addSuffix(base, "", "m")
	case mtStandardN.MatchString(mt):
		return addSuffix(mt, "s")
	}
	return []string{}
}

func (az *Azure) GetManagementPlatform() (string, error) {
	nodes := az.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		n := nodes[0]
		providerID := n.Spec.ProviderID
		if strings.Contains(providerID, "aks") {
			return "aks", nil
		}
	}
	return "", nil
}

// DownloadPricingData uses provided azure "best guesses" for pricing
func (az *Azure) DownloadPricingData() error {
	az.DownloadPricingDataLock.Lock()
	defer az.DownloadPricingDataLock.Unlock()

	config, err := az.GetConfig()
	if err != nil {
		return err
	}

	var authorizer autorest.Authorizer

	if config.AzureClientID != "" && config.AzureClientSecret != "" && config.AzureTenantID != "" {
		credentialsConfig := auth.NewClientCredentialsConfig(config.AzureClientID, config.AzureClientSecret, config.AzureTenantID)
		a, err := credentialsConfig.Authorizer()
		if err != nil {
			return err
		}
		authorizer = a
	}

	if authorizer == nil {
		a, err := auth.NewAuthorizerFromEnvironment()
		authorizer = a
		if err != nil { // Failed to create authorizer from environment, try from file
			a, err := auth.NewAuthorizerFromFile(azure.PublicCloud.ResourceManagerEndpoint)
			if err != nil {
				return err
			}
			authorizer = a
		}
	}

	sClient := subscriptions.NewClient()
	sClient.Authorizer = authorizer

	rcClient := commerce.NewRateCardClient(config.AzureSubscriptionID)
	rcClient.Authorizer = authorizer

	skusClient := skus.NewResourceSkusClient(config.AzureSubscriptionID)
	skusClient.Authorizer = authorizer

	providersClient := resources.NewProvidersClient(config.AzureSubscriptionID)
	providersClient.Authorizer = authorizer

	containerServiceClient := containerservice.NewContainerServicesClient(config.AzureSubscriptionID)
	containerServiceClient.Authorizer = authorizer

	rateCardFilter := fmt.Sprintf("OfferDurableId eq 'MS-AZR-0003p' and Currency eq '%s' and Locale eq 'en-US' and RegionInfo eq '%s'", config.CurrencyCode, config.AzureBillingRegion)

	klog.Infof("Using ratecard query %s", rateCardFilter)
	result, err := rcClient.Get(context.TODO(), rateCardFilter)
	if err != nil {
		return err
	}
	allPrices := make(map[string]*AzurePricing)
	regions, err := getRegions("compute", sClient, providersClient, config.AzureSubscriptionID)
	if err != nil {
		return err
	}

	c, err := az.GetConfig()
	if err != nil {
		return err
	}
	baseCPUPrice := c.CPU

	for _, v := range *result.Meters {
		meterName := *v.MeterName
		meterRegion := *v.MeterRegion
		meterCategory := *v.MeterCategory
		meterSubCategory := *v.MeterSubCategory

		region, err := toRegionID(meterRegion, regions)
		if err != nil {
			continue
		}

		if !strings.Contains(meterSubCategory, "Windows") {

			if strings.Contains(meterCategory, "Storage") {
				if strings.Contains(meterSubCategory, "HDD") || strings.Contains(meterSubCategory, "SSD") {
					var storageClass string = ""
					if strings.Contains(meterName, "S4 ") {
						storageClass = AzureStandardStorageClass
					} else if strings.Contains(meterName, "P4 ") {
						storageClass = AzurePremiumStorageClass
					}

					if storageClass != "" {
						var priceInUsd float64

						if len(v.MeterRates) < 1 {
							klog.V(1).Infof("missing rate info %+v", map[string]interface{}{"MeterSubCategory": *v.MeterSubCategory, "region": region})
							continue
						}
						for _, rate := range v.MeterRates {
							priceInUsd += *rate
						}
						priceStr := fmt.Sprintf("%f", priceInUsd)

						key := region + "," + storageClass
						klog.V(4).Infof("Adding PV.Key: %s, Cost: %s", key, priceStr)
						allPrices[key] = &AzurePricing{
							PV: &PV{
								Cost:   priceStr,
								Region: region,
							},
						}
					}
				}
			}

			if strings.Contains(meterCategory, "Virtual Machines") {

				// not available now
				if strings.Contains(meterSubCategory, "Promo") {
					continue
				}

				usageType := ""
				if !strings.Contains(meterName, "Low Priority") {
					usageType = "ondemand"
				} else {
					usageType = "preemptible"
				}

				var instanceTypes []string
				name := strings.TrimSuffix(meterName, " Low Priority")
				instanceType := strings.Split(name, "/")
				for _, it := range instanceType {
					instanceTypes = append(instanceTypes, strings.Replace(it, " ", "_", 1))
				}

				instanceTypes = transformMachineType(meterSubCategory, instanceTypes)
				if strings.Contains(name, "Expired") {
					instanceTypes = []string{}
				}

				var priceInUsd float64

				if len(v.MeterRates) < 1 {
					klog.V(1).Infof("missing rate info %+v", map[string]interface{}{"MeterSubCategory": *v.MeterSubCategory, "region": region})
					continue
				}
				for _, rate := range v.MeterRates {
					priceInUsd += *rate
				}
				priceStr := fmt.Sprintf("%f", priceInUsd)
				for _, instanceType := range instanceTypes {

					key := fmt.Sprintf("%s,%s,%s", region, instanceType, usageType)
					allPrices[key] = &AzurePricing{
						Node: &Node{
							Cost:         priceStr,
							BaseCPUPrice: baseCPUPrice,
						},
					}
				}
			}
		}
	}
	az.Pricing = allPrices
	return nil
}

// AllNodePricing returns the Azure pricing objects stored
func (az *Azure) AllNodePricing() (interface{}, error) {
	az.DownloadPricingDataLock.RLock()
	defer az.DownloadPricingDataLock.RUnlock()
	return az.Pricing, nil
}

// NodePricing returns Azure pricing data for a single node
func (az *Azure) NodePricing(key Key) (*Node, error) {
	az.DownloadPricingDataLock.RLock()
	defer az.DownloadPricingDataLock.RUnlock()
	if n, ok := az.Pricing[key.Features()]; ok {
		klog.V(4).Infof("Returning pricing for node %s: %+v from key %s", key, n, key.Features())
		if key.GPUType() != "" {
			n.Node.GPU = "1" // TODO: support multiple GPUs
		}
		return n.Node, nil
	}
	klog.V(1).Infof("[Warning] no pricing data found for %s: %s", key.Features(), key)
	c, err := az.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("No default pricing data available")
	}
	if key.GPUType() != "" {
		return &Node{
			VCPUCost: c.CPU,
			RAMCost:  c.RAM,
			GPUCost:  c.GPU,
			GPU:      "1", // TODO: support multiple GPUs
		}, nil
	}
	return &Node{
		VCPUCost:         c.CPU,
		RAMCost:          c.RAM,
		UsesBaseCPUPrice: true,
	}, nil
}

// Stubbed NetworkPricing for Azure. Pull directly from azure.json for now
func (az *Azure) NetworkPricing() (*Network, error) {
	cpricing, err := az.Config.GetCustomPricingData()
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

type azurePvKey struct {
	Labels                 map[string]string
	StorageClass           string
	StorageClassParameters map[string]string
	DefaultRegion          string
}

func (az *Azure) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string, defaultRegion string) PVKey {
	return &azurePvKey{
		Labels:                 pv.Labels,
		StorageClass:           pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		DefaultRegion:          defaultRegion,
	}
}

func (key *azurePvKey) GetStorageClass() string {
	return key.StorageClass
}

func (key *azurePvKey) Features() string {
	storageClass := key.StorageClassParameters["storageaccounttype"]
	if strings.EqualFold(storageClass, "Premium_LRS") {
		storageClass = AzurePremiumStorageClass
	} else if strings.EqualFold(storageClass, "Standard_LRS") {
		storageClass = AzureStandardStorageClass
	}
	if region, ok := key.Labels[v1.LabelZoneRegion]; ok {
		return region + "," + storageClass
	}

	return key.DefaultRegion + "," + storageClass
}

func (*Azure) GetDisks() ([]byte, error) {
	return nil, nil
}

func (az *Azure) ClusterInfo() (map[string]string, error) {
	remote := os.Getenv(remoteEnabled)
	remoteEnabled := false
	if os.Getenv(remote) == "true" {
		remoteEnabled = true
	}

	m := make(map[string]string)
	m["name"] = "Azure Cluster #1"
	c, err := az.GetConfig()
	if err != nil {
		return nil, err
	}
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = "azure"
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = os.Getenv(clusterIDKey)
	return m, nil

}

func (az *Azure) AddServiceKey(url url.Values) error {
	return nil
}

func (az *Azure) UpdateConfigFromConfigMap(a map[string]string) (*CustomPricing, error) {
	return az.Config.UpdateFromMap(a)
}

func (az *Azure) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	defer az.DownloadPricingData()

	return az.Config.Update(func(c *CustomPricing) error {
		a := make(map[string]interface{})
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return err
		}
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

		remoteEnabled := os.Getenv(remoteEnabled)
		if remoteEnabled == "true" {
			err := UpdateClusterMeta(os.Getenv(clusterIDKey), c.ClusterName)
			if err != nil {
				return err
			}
		}

		return nil
	})
}
func (az *Azure) GetConfig() (*CustomPricing, error) {
	c, err := az.Config.GetCustomPricingData()
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = "USD"
	}
	if c.AzureBillingRegion == "" {
		c.AzureBillingRegion = "US"
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (az *Azure) ExternalAllocations(string, string, []string, string, string, bool) ([]*OutOfClusterAllocation, error) {
	return nil, nil
}

func (az *Azure) ApplyReservedInstancePricing(nodes map[string]*Node) {

}

func (az *Azure) PVPricing(pvk PVKey) (*PV, error) {
	az.DownloadPricingDataLock.RLock()
	defer az.DownloadPricingDataLock.RUnlock()

	pricing, ok := az.Pricing[pvk.Features()]
	if !ok {
		klog.V(4).Infof("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &PV{}, nil
	}
	return pricing.PV, nil
}

func (az *Azure) GetLocalStorageQuery(window, offset string, rate bool, used bool) string {
	return ""
}
