package cloud

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/kubecost/cost-model/pkg/kubecost"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/util"

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
	AzureFilePremiumStorageClass     = "premium_smb"
	AzureFileStandardStorageClass    = "standard_smb"
	AzureDiskPremiumSSDStorageClass  = "premium_ssd"
	AzureDiskStandardSSDStorageClass = "standard_ssd"
	AzureDiskStandardStorageClass    = "standard_hdd"
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

const AzureLayout = "2006-01-02"

var loadedAzureSecret bool = false
var azureSecret *AzureServiceKey = nil
var loadedAzureStorageConfigSecret bool = false
var azureStorageConfig *AzureStorageConfig= nil

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
	r, _ := util.GetRegion(k.Labels)
	region := strings.ToLower(r)
	instance, _ := util.GetInstanceType(k.Labels)
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

// Represents an azure storage config
type AzureStorageConfig struct {
	AccountName string `json:"azureStorageAccount"`
	AccessKey string `json:"azureStorageAccessKey"`
	ContainerName string `json:"azureStorageContainer"`
}

// Represents an azure app key
type AzureAppKey struct {
	AppID       string `json:"appId"`
	DisplayName string `json:"displayName"`
	Name        string `json:"name"`
	Password    string `json:"password"`
	Tenant      string `json:"tenant"`
}

// Azure service key for a specific subscription
type AzureServiceKey struct {
	SubscriptionID string       `json:"subscriptionId"`
	ServiceKey     *AzureAppKey `json:"serviceKey"`
}


// Validity check on service key
func (ask *AzureServiceKey) IsValid() bool {
	return ask.SubscriptionID != "" &&
		ask.ServiceKey != nil &&
		ask.ServiceKey.AppID != "" &&
		ask.ServiceKey.Password != "" &&
		ask.ServiceKey.Tenant != ""
}

// Loads the azure authentication via configuration or a secret set at install time.
func (az *Azure) getAzureAuth(forceReload bool, cp *CustomPricing) (subscriptionID, clientID, clientSecret, tenantID string) {
	// 1. Check config values first (set from frontend UI)
	if cp.AzureSubscriptionID != "" && cp.AzureClientID != "" && cp.AzureClientSecret != "" && cp.AzureTenantID != "" {
		subscriptionID = cp.AzureSubscriptionID
		clientID = cp.AzureClientID
		clientSecret = cp.AzureClientSecret
		tenantID = cp.AzureTenantID
		return
	}

	// 2. Check for secret
	s, _ := az.loadAzureAuthSecret(forceReload)
	if s != nil && s.IsValid() {
		subscriptionID = s.SubscriptionID
		clientID = s.ServiceKey.AppID
		clientSecret = s.ServiceKey.Password
		tenantID = s.ServiceKey.Tenant
		return
	}

	// 3. Empty values
	return "", "", "", ""
}

func (az *Azure) ConfigureAzureStorage() error {
	accessKey, accountName, containerName := az.getAzureStorageConfig(false)
	if accessKey != "" && accountName != "" && containerName != "" {
		err := env.Set(env.AzureStorageAccessKeyEnvVar, accessKey)
		if err != nil {
			return err
		}
		err = env.Set(env.AzureStorageAccountNameEnvVar, accountName)
		if err != nil {
			return err
		}
		err = env.Set(env.AzureStorageContainerNameEnvVar, containerName)
		if err != nil {
			return err
		}
	}
	return nil
}
func (az *Azure) getAzureStorageConfig(forceReload bool) (accessKey, accountName, containerName string) {

	// 1. Check for secret
	s, _ := az.loadAzureStorageConfig(forceReload)
	if s != nil && s.AccessKey != "" && s.AccountName != ""  && s.ContainerName != ""{
		accessKey = s.AccessKey
		accountName = s.AccountName
		containerName = s.ContainerName
		return
	}

	// 3. Fall back to env vars
	return env.GetAzureStorageAccessKey(), env.GetAzureStorageAccountName(), env.GetAzureStorageContainerName()
}

// Load once and cache the result (even on failure). This is an install time secret, so
// we don't expect the secret to change. If it does, however, we can force reload using
// the input parameter.
func (az *Azure) loadAzureAuthSecret(force bool) (*AzureServiceKey, error) {
	if !force && loadedAzureSecret {
		return azureSecret, nil
	}
	loadedAzureSecret = true

	exists, err := util.FileExists(authSecretPath)
	if !exists || err != nil {
		return nil, fmt.Errorf("Failed to locate service account file: %s", authSecretPath)
	}

	result, err := ioutil.ReadFile(authSecretPath)
	if err != nil {
		return nil, err
	}

	var ask AzureServiceKey
	err = json.Unmarshal(result, &ask)
	if err != nil {
		return nil, err
	}

	azureSecret = &ask
	return azureSecret, nil
}

// Load once and cache the result (even on failure). This is an install time secret, so
// we don't expect the secret to change. If it does, however, we can force reload using
// the input parameter.
func (az *Azure) loadAzureStorageConfig(force bool) (*AzureStorageConfig, error) {
	if !force && loadedAzureStorageConfigSecret {
		return azureStorageConfig, nil
	}
	loadedAzureSecret = true

	exists, err := util.FileExists(storageConfigSecretPath)
	if !exists || err != nil {
		return nil, fmt.Errorf("Failed to locate azure storage config file: %s", storageConfigSecretPath)
	}

	result, err := ioutil.ReadFile(storageConfigSecretPath)
	if err != nil {
		return nil, err
	}

	var ask AzureStorageConfig
	err = json.Unmarshal(result, &ask)
	if err != nil {
		return nil, err
	}

	azureStorageConfig = &ask
	return azureStorageConfig, nil
}

func (az *Azure) GetKey(labels map[string]string, n *v1.Node) Key {
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

	// Load the service provider keys
	subscriptionID, clientID, clientSecret, tenantID := az.getAzureAuth(false, config)
	config.AzureSubscriptionID = subscriptionID
	config.AzureClientID = clientID
	config.AzureClientSecret = clientSecret
	config.AzureTenantID = tenantID

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
				if strings.Contains(meterSubCategory, "HDD") || strings.Contains(meterSubCategory, "SSD") || strings.Contains(meterSubCategory, "Premium Files") {
					var storageClass string = ""
					if strings.Contains(meterName, "P4 ") {
						storageClass = AzureDiskPremiumSSDStorageClass
					} else if strings.Contains(meterName, "E4 ") {
						storageClass = AzureDiskStandardSSDStorageClass
					} else if strings.Contains(meterName, "S4 ") {
						storageClass = AzureDiskStandardStorageClass
					} else if strings.Contains(meterName, "LRS Provisioned") {
						storageClass = AzureFilePremiumStorageClass
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
						// rate is in disk per month, resolve price per hour, then GB per hour
						pricePerHour := priceInUsd / 730.0 / 32.0
						priceStr := fmt.Sprintf("%f", pricePerHour)

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
					if strings.Contains(meterSubCategory, "Promo") {
						it = it + " Promo"
					}
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

	// There is no easy way of supporting Standard Azure-File, because it's billed per used GB
	// this will set the price to "0" as a workaround to not spam with `Persistent Volume pricing not found for` error
	// check https://github.com/kubecost/cost-model/issues/159 for more information (same problem on AWS)
	zeroPrice := "0.0"
	for region := range regions {
		key := region + "," + AzureFileStandardStorageClass
		klog.V(4).Infof("Adding PV.Key: %s, Cost: %s", key, zeroPrice)
		allPrices[key] = &AzurePricing{
			PV: &PV{
				Cost:   zeroPrice,
				Region: region,
			},
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

func (azr *Azure) LoadBalancerPricing() (*LoadBalancer, error) {
	fffrc := 0.025
	afrc := 0.010
	lbidc := 0.008

	numForwardingRules := 1.0
	dataIngressGB := 0.0

	var totalCost float64
	if numForwardingRules < 5 {
		totalCost = fffrc*numForwardingRules + lbidc*dataIngressGB
	} else {
		totalCost = fffrc*5 + afrc*(numForwardingRules-5) + lbidc*dataIngressGB
	}
	return &LoadBalancer{
		Cost: totalCost,
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

func (key *azurePvKey) ID() string {
	return ""
}

func (key *azurePvKey) GetStorageClass() string {
	return key.StorageClass
}

func (key *azurePvKey) Features() string {
	storageClass := key.StorageClassParameters["storageaccounttype"]
	storageSKU := key.StorageClassParameters["skuName"]
	if storageClass != "" {
		if strings.EqualFold(storageClass, "Premium_LRS") {
			storageClass = AzureDiskPremiumSSDStorageClass
		} else if strings.EqualFold(storageClass, "StandardSSD_LRS") {
			storageClass = AzureDiskStandardSSDStorageClass
		} else if strings.EqualFold(storageClass, "Standard_LRS") {
			storageClass = AzureDiskStandardStorageClass
		}
	} else {
		if strings.EqualFold(storageSKU, "Premium_LRS") {
			storageClass = AzureFilePremiumStorageClass
		} else if strings.EqualFold(storageSKU, "Standard_LRS") {
			storageClass = AzureFileStandardStorageClass
		}
	}
	if region, ok := util.GetRegion(key.Labels); ok {
		return region + "," + storageClass
	}

	return key.DefaultRegion + "," + storageClass
}

func (*Azure) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (*Azure) GetDisks() ([]byte, error) {
	return nil, nil
}

func (az *Azure) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

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
	m["id"] = env.GetClusterID()
	return m, nil

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

		if env.IsRemoteEnabled() {
			err := UpdateClusterMeta(env.GetClusterID(), c.ClusterName)
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

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (az *Azure) ExternalAllocations(start string, end string, aggregators []string, filterType string, filterValue string, crossCluster bool) ([]*OutOfClusterAllocation, error) {
	var csvRetriever CSVRetriever = AzureCSVRetriever{}
	err := az.ConfigureAzureStorage() // load Azure Storage config
	if err != nil {
		return nil, err
	}
	return GetExternalAllocations(start, end, aggregators, filterType, filterValue, crossCluster, csvRetriever)
}

func GetExternalAllocations(start string, end string, aggregators []string, filterType string, filterValue string, crossCluster bool, csvRetriever CSVRetriever) ([]*OutOfClusterAllocation, error) {
	dateFormat := "2006-1-2"
	startTime, err := time.Parse(dateFormat, start)
	if err != nil {
		return nil, err
	}
	endTime, err := time.Parse(dateFormat, end)
	if err != nil {
		return nil, err
	}
	readers, err := csvRetriever.GetCSVReaders(startTime, endTime)
	if err != nil {
		return nil, err
	}
	oocAllocs := make(map[string]*OutOfClusterAllocation)
	for _, reader := range readers {
		err = ParseCSV(reader, startTime, endTime, oocAllocs, aggregators, filterType, filterValue, crossCluster)
		if err != nil {
			return nil, err
		}
	}
	var oocAllocsArr []*OutOfClusterAllocation
	for _, alloc := range oocAllocs {
		oocAllocsArr = append(oocAllocsArr, alloc)
	}
	return oocAllocsArr, nil
}

func ParseCSV (reader *csv.Reader, start, end time.Time, oocAllocs map[string]*OutOfClusterAllocation, aggregators []string, filterType string, filterValue string, crossCluster bool) error {
	headers, _ := reader.Read()

	headerMap := map[string]int{}
	for i, header := range headers {
		headerMap[header] = i
	}

	for {
		var record, err = reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		meterCategory := record[headerMap["MeterCategory"]]
		category := selectCategory(meterCategory)
		usageDateTime, err := time.Parse(AzureLayout, record[headerMap["UsageDateTime"]])
		if err != nil {
			klog.Errorf("failed to parse usage date: '%s'", record[headerMap["UsageDateTime"]])
			continue
		}
		// Ignore VM's and Storage Items for now
		if category == kubecost.ComputeCategory || category == kubecost.StorageCategory || !isValidUsageDateTime(start, end, usageDateTime) {
			continue
		}

		itemCost, err := strconv.ParseFloat(record[headerMap["PreTaxCost"]], 64)
		if err != nil {
			klog.Infof("failed to parse cost: '%s'", record[headerMap["PreTaxCost"]])
			continue
		}

		itemTags := make(map[string]string)
		itemTagJson := record[headerMap["Tags"]]
		if itemTagJson != "" {
			err = json.Unmarshal([]byte(itemTagJson), &itemTags)
			if err != nil {
				klog.Infof("Could not parse item tags %v", err)
			}
		}

		if filterType != "kubernetes_" {
			if value, ok := itemTags[filterType];!ok || value != filterValue {
				continue
			}
		}
		environment := ""
		for _, agg := range aggregators {
			if tag, ok := itemTags[agg]; ok {
				environment = tag // just set to the first nonempty match
				break
			}
		}
		key := environment + record[headerMap["ConsumedService"]]
		if alloc, ok := oocAllocs[key]; ok {
			alloc.Cost += itemCost
		} else {
			ooc := &OutOfClusterAllocation{
				Aggregator:  strings.Join(aggregators, ","),
				Environment: environment,
				Service:     record[headerMap["ConsumedService"]],
				Cost:        itemCost,
			}
			oocAllocs[key] = ooc
		}


	}
	return nil
}



// UsageDateTime only contains date information and not time because of this filtering usageDate time is inclusive on start and exclusive on end
func isValidUsageDateTime(start, end, usageDateTime time.Time) bool {
	return (usageDateTime.After(start) || usageDateTime.Equal(start)) && usageDateTime.Before(end)
}

func getStartAndEndTimes(usageDateTime time.Time) (time.Time, time.Time) {
	start := time.Date(usageDateTime.Year(), usageDateTime.Month(), usageDateTime.Day(), 0, 0, 0, 0, usageDateTime.Location())
	end := time.Date(usageDateTime.Year(), usageDateTime.Month(), usageDateTime.Day(), 23, 59, 59, 999999999, usageDateTime.Location())
	return start, end
}

func selectCategory(meterCategory string) string {
	if meterCategory == "Virtual Machines" {
		return kubecost.ComputeCategory
	} else if meterCategory == "Storage" {
		return kubecost.StorageCategory
	} else if meterCategory == "Load Balancer" || meterCategory == "Bandwidth" {
		return kubecost.NetworkCategory
	} else {
		return kubecost.OtherCategory
	}
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

func (az *Azure) ServiceAccountStatus() *ServiceAccountStatus {
	return &ServiceAccountStatus{
		Checks: []*ServiceAccountCheck{},
	}
}

func (az *Azure) PricingSourceStatus() map[string]*PricingSource {
	return make(map[string]*PricingSource)
}

func (*Azure) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (az *Azure) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (az *Azure) ParseID(id string) string {
	return id
}

func (az *Azure) ParsePVID(id string) string {
	return id
}
