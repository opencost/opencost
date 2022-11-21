package cloud

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/signers"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/fileutil"
	"github.com/opencost/opencost/pkg/util/json"
	"github.com/opencost/opencost/pkg/util/stringutil"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

const (
	ALIBABA_ECS_PRODUCT_CODE                   = "ecs"
	ALIBABA_ECS_VERSION                        = "2014-05-26"
	ALIBABA_ECS_DOMAIN                         = "ecs.aliyuncs.com"
	ALIBABA_DESCRIBE_PRICE_API_ACTION          = "DescribePrice"
	ALIBABA_INSTANCE_RESOURCE_TYPE             = "instance"
	ALIBABA_DISK_RESOURCE_TYPE                 = "disk"
	ALIBABA_PAY_AS_YOU_GO_BILLING              = "Pay-As-You-Go"
	ALIBABA_SUBSCRIPTION_BILLING               = "Subscription"
	ALIBABA_PREEMPTIBLE_BILLING                = "Preemptible"
	ALIBABA_OPTIMIZE_KEYWORD                   = "optimize"
	ALIBABA_NON_OPTIMIZE_KEYWORD               = "nonoptimize"
	ALIBABA_HOUR_PRICE_UNIT                    = "Hour"
	ALIBABA_MONTH_PRICE_UNIT                   = "Month"
	ALIBABA_YEAR_PRICE_UNIT                    = "Year"
	ALIBABA_UNKNOWN_INSTANCE_FAMILY_TYPE       = "unknown"
	ALIBABA_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE = "unsupported"
	ALIBABA_ENHANCED_GENERAL_PURPOSE_TYPE      = "g6e"
	ALIBABA_DISK_CLOUD_ESSD_CATEGORY           = "cloud_essd"
	ALIBABA_PV_DISK_CATEGORY                   = "system"
	ALIBABA_LOCAL_DISK_CATEGORY                = "data"
)

// Why predefined and dependency on code? Can be converted to API call - https://www.alibabacloud.com/help/en/elastic-compute-service/latest/regions-describeregions
var alibabaRegions = []string{
	"cn-qingdao",
	"cn-beijing",
	"cn-zhangjiakou",
	"cn-huhehaote",
	"cn-wulanchabu",
	"cn-hangzhou",
	"cn-shanghai",
	"cn-nanjing",
	"cn-fuzhou",
	"cn-shenzhen",
	"cn-guangzhou",
	"cn-chengdu",
	"cn-hongkong",
	"ap-southeast-1",
	"ap-southeast-2",
	"ap-southeast-3",
	"ap-southeast-5",
	"ap-southeast-6",
	"ap-southeast-7",
	"ap-south-1",
	"ap-northeast-1",
	"ap-northeast-2",
	"us-west-1",
	"us-east-1",
	"eu-central-1",
	"me-east-1",
}

// To-Do: Convert to API call - https://www.alibabacloud.com/help/en/elastic-compute-service/latest/describeinstancetypefamilies
// Also first pass only completely tested pricing API for General pupose instances families.
var alibabaInstanceFamilies = []string{
	"g6e",
	"g6",
	"g5",
	"sn2",
	"sn2ne",
}

// AlibabaAccessKey holds Alibaba credentials parsing from the service-key.json file.
type AlibabaAccessKey struct {
	AccessKeyID     string `json:"alibaba_access_key_id"`
	SecretAccessKey string `json:"alibaba_secret_access_key"`
}

// Slim Version of k8s disk assigned to a node or PV, To be used if price adjustment need to happen with local disk information passed to describePrice.
type SlimK8sDisk struct {
	DiskType         string
	RegionID         string
	PriceUnit        string
	SizeInGiB        int
	DiskCategory     string
	PerformanceLevel string
	ProviderID       string
}

func NewSlimK8sDisk(diskType, regionID, priceUnit, diskCategory, performanceLevel, providerID string, sizeInGiB int) *SlimK8sDisk {
	return &SlimK8sDisk{
		DiskType:         diskType,
		RegionID:         regionID,
		PriceUnit:        priceUnit,
		SizeInGiB:        sizeInGiB,
		DiskCategory:     diskCategory,
		PerformanceLevel: performanceLevel,
		ProviderID:       providerID,
	}
}

// Slim version of a k8s v1.node just to pass along the object of this struct instead of constant getting the labels from within v1.Node & unit testing.
type SlimK8sNode struct {
	InstanceType       string
	RegionID           string
	PriceUnit          string
	MemorySizeInKiB    string // TO-DO : Possible to convert to float?
	IsIoOptimized      bool
	OSType             string
	ProviderID         string
	InstanceTypeFamily string // Bug in DescribePrice, doesn't default to enhanced type correct and you get an error in DescribePrice to get around need the family of the InstanceType.
}

func NewSlimK8sNode(instanceType, regionID, priceUnit, memorySizeInKiB, osType, providerID, instanceTypeFamily string, isIOOptimized bool) *SlimK8sNode {
	return &SlimK8sNode{
		InstanceType:       instanceType,
		RegionID:           regionID,
		PriceUnit:          priceUnit,
		MemorySizeInKiB:    memorySizeInKiB,
		IsIoOptimized:      isIOOptimized,
		OSType:             osType,
		ProviderID:         providerID,
		InstanceTypeFamily: instanceTypeFamily,
	}
}

// AlibabaNodeAttributes represents metadata about the product used to map to a node.
// Basic Attributes needed atleast to get the key, Some attributes from k8s Node response
// be populated directly into *Node object.
type AlibabaNodeAttributes struct {
	InstanceType    string `json:"instanceType"`
	MemorySizeInKiB string `json:"memorySizeInKiB"`
	IsIoOptimized   bool   `json:"isIoOptimized"`
	OSType          string `json:"osType"`
}

func NewAlibabaNodeAttributes(node *SlimK8sNode) *AlibabaNodeAttributes {
	return &AlibabaNodeAttributes{
		InstanceType:    node.InstanceType,
		MemorySizeInKiB: node.MemorySizeInKiB,
		IsIoOptimized:   node.IsIoOptimized,
		OSType:          node.OSType,
	}
}

// AlibabaPVAttributes represents metadata about the product used to map to a PV.
// Basic Attributes needed atleast to get the keys, Some attributes from k8s Node response
// be populated directly into *PV object.
// TO_DO: In next PR improve this
type AlibabaPVAttributes struct {
	DiskType         int32  `json:"diskType"`
	DiskCategory     string `json:"diskCategory"`
	PerformanceLevel string `json:"performanceLevel"`
}

// Stage 1 support will be Pay-As-You-Go with HourlyPrice equal to TradePrice with PriceUnit as Hour
// TO-DO: Subscription and Premptible support, need to find how to distinguish node into these categories]
// TO-DO: Open question Subscription would be either Monthly or Yearly, Firstly Data retrieval/population
// TO-DO:  need to be tested from describe price API, but how would you calculate hourly price, is it PRICE_YEARLY/HOURS_IN_THE_YEAR?
type AlibabaPricingDetails struct {
	// Represents hourly price for the given Alibaba cloud Product.
	HourlyPrice float32 `json:"hourlyPrice"`
	// Represents the unit in which Alibaba Product is billed can be Hour, Month or Year based on the billingMethod.
	PriceUnit string `json:"priceUnit"`
	// Original Price paid to acquire the Alibaba Product.
	TradePrice float32 `json:"tradePrice"`
	// Represents the currency unit of the price for billing Alibaba Product.
	CurrencyCode string `json:"currencyCode"`
}

func NewAlibabaPricingDetails(hourlyPrice float32, priceUnit string, tradePrice float32, currencyCode string) *AlibabaPricingDetails {
	return &AlibabaPricingDetails{
		HourlyPrice:  hourlyPrice,
		PriceUnit:    priceUnit,
		TradePrice:   tradePrice,
		CurrencyCode: currencyCode,
	}
}

// AlibabaPricingTerms can have three types of supported billing method Pay-As-You-Go, Subscription and Premptible
type AlibabaPricingTerms struct {
	BillingMethod  string                 `json:"billingMethod"`
	PricingDetails *AlibabaPricingDetails `json:"pricingDetails"`
}

func NewAlibabaPricingTerms(billingMethod string, pricingDetails *AlibabaPricingDetails) *AlibabaPricingTerms {
	return &AlibabaPricingTerms{
		BillingMethod:  billingMethod,
		PricingDetails: pricingDetails,
	}
}

// Alibaba Pricing struct carry the Attributes and pricing information for Node or PV
type AlibabaPricing struct {
	NodeAttributes *AlibabaNodeAttributes
	PVAttributes   *AlibabaPVAttributes
	PricingTerms   *AlibabaPricingTerms
	Node           *Node
	PV             *PV
}

// Alibaba cloud's Provider struct
type Alibaba struct {
	// Data to store Alibaba cloud's pricing struct, key in the map represents exact match to
	// node.features() or pv.features for easy lookup
	Pricing map[string]*AlibabaPricing
	// Lock Needed to provide thread safe
	DownloadPricingDataLock sync.RWMutex
	Clientset               clustercache.ClusterCache
	Config                  *ProviderConfig
	*CustomProvider

	// TO-DO: These needs to be decided if either exported or unexported.
	serviceAccountChecks *ServiceAccountChecks
	clusterAccountId     string
	clusterRegion        string

	// The following fields are unexported because of avoiding any leak of secrets of these keys.
	// Alibaba Access key used specifically in signer interface used to sign API calls
	accessKey *credentials.AccessKeyCredential
	// Map of regionID to sdk.client to call API for that region
	clients map[string]*sdk.Client
}

// GetAlibabaAccessKey return the Access Key used to interact with the Alibaba cloud, if not set it
// set it first by looking at env variables else load it from secret files.
// <IMPORTANT>Ask in PR what is the exact purpose of so many functions to set the key in AWS providers, am i missing something here!!!!!
func (alibaba *Alibaba) GetAlibabaAccessKey() (*credentials.AccessKeyCredential, error) {
	if alibaba.accessKeyisLoaded() {
		return alibaba.accessKey, nil
	}

	config, err := alibaba.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting the default config for Alibaba Cloud provider: %w", err)
	}

	//Look for service key values in env if not present in config via helm chart once changes are done
	if config.AlibabaServiceKeyName == "" {
		config.AlibabaServiceKeyName = env.GetAlibabaAccessKeyID()
	}
	if config.AlibabaServiceKeySecret == "" {
		config.AlibabaServiceKeySecret = env.GetAlibabaAccessKeySecret()
	}

	if config.AlibabaServiceKeyName == "" && config.AlibabaServiceKeySecret == "" {
		log.Debugf("missing service key values for Alibaba cloud integration attempting to use service account integration")
		err := alibaba.loadAlibabaAuthSecretAndSetEnv(true)
		if err != nil {
			return nil, fmt.Errorf("unable to set the Alibaba Cloud key/secret from config file %w", err)
		}
		// set custom pricing keys too
		config.AlibabaServiceKeyName = env.GetAlibabaAccessKeyID()
		config.AlibabaServiceKeySecret = env.GetAlibabaAccessKeySecret()
	}

	if config.AlibabaServiceKeyName == "" && config.AlibabaServiceKeySecret == "" {
		return nil, fmt.Errorf("failed to get the access key for the current alibaba account")
	}

	alibaba.accessKey = &credentials.AccessKeyCredential{AccessKeyId: env.GetAlibabaAccessKeyID(), AccessKeySecret: env.GetAlibabaAccessKeySecret()}

	return alibaba.accessKey, nil
}

func (alibaba *Alibaba) DownloadPricingData() error {
	alibaba.DownloadPricingDataLock.Lock()
	defer alibaba.DownloadPricingDataLock.Unlock()

	var aak *credentials.AccessKeyCredential
	var err error

	if !alibaba.accessKeyisLoaded() {
		aak, err = alibaba.GetAlibabaAccessKey()
		if err != nil {
			return fmt.Errorf("unable to get the access key information: %w", err)
		}
	} else {
		aak = alibaba.accessKey
	}

	c, err := alibaba.Config.GetCustomPricingData()
	if err != nil {
		return fmt.Errorf("error downloading default pricing data: %w", err)
	}

	// Get all the nodes from Alibaba cluster.
	nodeList := alibaba.Clientset.GetAllNodes()

	var client *sdk.Client
	var signer *signers.AccessKeySigner
	var ok bool
	var pricingObj *AlibabaPricing
	var lookupKey string
	alibaba.clients = make(map[string]*sdk.Client)
	alibaba.Pricing = make(map[string]*AlibabaPricing)

	// TO-DO: Add disk price adjustment by parsing the local disk information and putting it as a param in describe Price function.
	for _, node := range nodeList {
		slimK8sNode := generateSlimK8sNodeFromV1Node(node)
		lookupKey, err = determineKeyForPricing(slimK8sNode)
		if _, ok := alibaba.Pricing[lookupKey]; ok {
			log.Debugf("Pricing information for node with same features %s already exists hence skipping", lookupKey)
			continue
		}

		if client, ok = alibaba.clients[slimK8sNode.RegionID]; !ok {
			client, err = sdk.NewClientWithAccessKey(slimK8sNode.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
			if err != nil {
				return fmt.Errorf("unable to initiate alibaba cloud sdk client for region %s : %w", slimK8sNode.RegionID, err)
			}
			alibaba.clients[slimK8sNode.RegionID] = client
		}
		signer = signers.NewAccessKeySigner(aak)
		pricingObj, err = processDescribePriceAndCreateAlibabaPricing(client, slimK8sNode, signer, c)

		if err != nil {
			return fmt.Errorf("failed to create pricing information for node with type %s with error: %w", slimK8sNode.InstanceType, err)
		}
		alibaba.Pricing[lookupKey] = pricingObj
	}

	// TO-DO: PV pricing
	// //get pvList ultimately from Alibaba cloud provider and resemble data from the pvtype to
	// // Hardcodedk8sNodeDiskStruct
	// pvList := alibaba.Clientset.GetAllPersistentVolumes()

	// pvList := []*Hardcodedk8sNodeDiskStruct{}
	// pvList = append(pvList, &Hardcodedk8sNodeDiskStruct{
	// 	DiskType:         "data",
	// 	DiskCategory:     "cloud",
	// 	PerformanceLevel: "",
	// 	RegionID:         "cn-hangzhou",
	// 	PriceUnit:        "Hour",
	// 	SizeInGiB:        60,
	// 	ProviderID:       "Ali-XXX-pv-01",
	// }, &Hardcodedk8sNodeDiskStruct{
	// 	DiskType:         "data",
	// 	DiskCategory:     "cloud",
	// 	PerformanceLevel: "P1",
	// 	RegionID:         "cn-hangzhou",
	// 	PriceUnit:        "Hour",
	// 	SizeInGiB:        40,
	// 	ProviderID:       "Ali-XXX-pv-01",
	// })

	// for _, pv := range pvList {
	// 	slimK8sNode := generateSlimK8sDiskFromV1PV(pv)
	// 	if client, ok = alibaba.clients[pv.RegionID]; !ok {
	// 		client, err = sdk.NewClientWithAccessKey(pv.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
	// 		if err != nil {
	// 			return fmt.Errorf("access key provided does not have access to location %s", pv.RegionID)
	// 		}
	// 		alibaba.clients[pv.RegionID] = client
	// 	}
	// 	signer = signers.NewAccessKeySigner(aak)
	// 	pricingObj, err = processDescribePriceAndCreateAlibabaPricing(client, pv, signer)
	// 	lookupKey, err = determineKeyForPricing(pv)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	alibaba.Pricing[lookupKey] = pricingObj
	// }
	// log.Infof("Length of pricing is %d", len(alibaba.Pricing))
	// log.Infof("random value is %v", alibaba.Pricing[lookupKey])
	return nil
}

// AllNodePricing returns all the billing data for nodes and pvs
func (alibaba *Alibaba) AllNodePricing() (interface{}, error) {
	alibaba.DownloadPricingDataLock.RLock()
	defer alibaba.DownloadPricingDataLock.RUnlock()
	return alibaba.Pricing, nil
}

// NodePricing gives a specific node for the key
func (alibaba *Alibaba) NodePricing(key Key) (*Node, error) {
	alibaba.DownloadPricingDataLock.RLock()
	defer alibaba.DownloadPricingDataLock.RUnlock()

	// Get node features for the key
	keyFeature := key.Features()

	pricing, ok := alibaba.Pricing[keyFeature]
	if !ok {
		log.Warnf("Node pricing information not found for node with feature: %s", keyFeature)
		return &Node{}, nil
	}

	log.Debugf("returning the node price for the node with feature: %s", keyFeature)
	// adjust the price of the node with local disk informatio
	additionalLocalDiskPrice := applyAlibabaLocalDiskAdjustment(map[string]string{})

	returnNode := pricing.Node
	log.Debugf("Current Node price is %s and additionalPrice is: %f", returnNode.Cost, additionalLocalDiskPrice)
	returnNode.adjustCost(additionalLocalDiskPrice)
	return returnNode, nil
}

// PVPricing gives a specific PV price for the PVkey
func (alibaba *Alibaba) PVPricing(pvk PVKey) (*PV, error) {
	alibaba.DownloadPricingDataLock.RLock()
	defer alibaba.DownloadPricingDataLock.RUnlock()

	keyFeature := pvk.Features()

	pricing, ok := alibaba.Pricing[keyFeature]

	if !ok {
		log.Warnf("Persistent Volume pricing not found for PV with feature: %s", keyFeature)
		return &PV{}, nil
	}

	log.Debugf("returning the PV price for the node with feature: %s", keyFeature)
	return pricing.PV, nil
}

// Stubbed NetworkPricing for Alibaba Cloud. Will look at this in Next PR
func (alibaba *Alibaba) NetworkPricing() (*Network, error) {
	return &Network{
		ZoneNetworkEgressCost:     0.0,
		RegionNetworkEgressCost:   0.0,
		InternetNetworkEgressCost: 0.0,
	}, nil
}

// Stubbed LoadBalancerPricing for Alibaba Cloud. Will look at this in Next PR
func (alibaba *Alibaba) LoadBalancerPricing() (*LoadBalancer, error) {
	return &LoadBalancer{
		Cost: 0.0,
	}, nil
}

func (alibaba *Alibaba) GetConfig() (*CustomPricing, error) {
	c, err := alibaba.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "0%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.ShareTenancyCosts == "" {
		c.ShareTenancyCosts = defaultShareTenancyCost
	}

	return c, nil
}

// Load once and cache the result (even on failure). This is an install time secret, so
// we don't expect the secret to change. If it does, however, we can force reload using
// the input parameter.
func (alibaba *Alibaba) loadAlibabaAuthSecretAndSetEnv(force bool) error {
	if !force && alibaba.accessKeyisLoaded() {
		return nil
	}

	exists, err := fileutil.FileExists(authSecretPath)
	if !exists || err != nil {
		return fmt.Errorf("failed to locate service account file: %s with err: %w", authSecretPath, err)
	}

	result, err := ioutil.ReadFile(authSecretPath)
	if err != nil {
		return fmt.Errorf("failed to read service account file: %s with err: %w", authSecretPath, err)
	}

	var ak *AlibabaAccessKey
	err = json.Unmarshal(result, &ak)
	if err != nil {
		return fmt.Errorf("failed to unmarshall access key id and access key secret with err: %w", err)
	}

	err = env.Set(env.AlibabaAccessKeyIDEnvVar, ak.AccessKeyID)
	if err != nil {
		return fmt.Errorf("failed to set environment variable: %s with err: %w", env.AlibabaAccessKeyIDEnvVar, err)
	}
	err = env.Set(env.AlibabaAccessKeySecretEnvVar, ak.SecretAccessKey)
	if err != nil {
		return fmt.Errorf("failed to set environment variable: %s with err: %w", env.AlibabaAccessKeySecretEnvVar, err)
	}

	alibaba.accessKey = &credentials.AccessKeyCredential{
		AccessKeyId:     ak.AccessKeyID,
		AccessKeySecret: ak.SecretAccessKey,
	}
	return nil
}

// Regions returns a current supported list of Alibaba regions
func (alibaba *Alibaba) Regions() []string {
	return alibabaRegions
}

// ClusterInfo returns information about Alibaba Cloud cluster, as provided by metadata. TO-DO: Look at this function closely at next PR iteration
func (alibaba *Alibaba) ClusterInfo() (map[string]string, error) {

	c, err := alibaba.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to getConfig with err: %w", err)
	}

	var clusterName string
	if c.ClusterName != "" {
		clusterName = c.ClusterName
	}

	// Set it to environment clusterID if not set at this point
	if clusterName == "" {
		clusterName = env.GetClusterID()
	}

	m := make(map[string]string)
	m["name"] = clusterName
	m["provider"] = kubecost.AlibabaProvider
	m["project"] = alibaba.clusterAccountId
	m["region"] = alibaba.clusterRegion
	m["id"] = env.GetClusterID()
	return m, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetAddresses() ([]byte, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetDisks() ([]byte, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) UpdateConfigFromConfigMap(cm map[string]string) (*CustomPricing, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetManagementPlatform() (string, error) {
	return "", nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	return ""
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) ApplyReservedInstancePricing(nodes map[string]*Node) {

}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) ServiceAccountStatus() *ServiceAccountStatus {
	return &ServiceAccountStatus{}
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) PricingSourceStatus() map[string]*PricingSource {
	return map[string]*PricingSource{}
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

// Will look at this in Next PR if needed
func (alibaba *Alibaba) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	return 0.0
}

func (alibaba *Alibaba) accessKeyisLoaded() bool {
	return alibaba.accessKey != nil
}

type AlibabaNodeKey struct {
	ProviderID       string
	RegionID         string
	InstanceType     string
	OSType           string
	OptimizedKeyword string //If IsIoOptimized key will have optimize if not unoptimized the key for the node
	LocalNodeDisks   map[string]string
}

func NewAlibabaNodeKey(node *SlimK8sNode, optimizedKeyword string) *AlibabaNodeKey {
	//TO-DO: populate local disk via API
	localNodeDisks := map[string]string{}
	return &AlibabaNodeKey{
		ProviderID:       node.ProviderID,
		RegionID:         node.RegionID,
		InstanceType:     node.InstanceType,
		OSType:           node.OSType,
		OptimizedKeyword: optimizedKeyword,
		LocalNodeDisks:   localNodeDisks,
	}
}

func (alibabaNodeKey *AlibabaNodeKey) ID() string {
	return alibabaNodeKey.ProviderID
}

func (alibabaNodeKey *AlibabaNodeKey) Features() string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{alibabaNodeKey.RegionID, alibabaNodeKey.InstanceType, alibabaNodeKey.OSType, alibabaNodeKey.OptimizedKeyword})
	return strings.Join(keyLookup, "::")
}

func (alibabaNodeKey *AlibabaNodeKey) GPUType() string {
	return ""
}

func (alibabaNodeKey *AlibabaNodeKey) GPUCount() int {
	return 0
}

func (alibabaNodeKey *AlibabaNodeKey) GetLocalDisks() map[string]string {
	return alibabaNodeKey.LocalNodeDisks
}

// Get's the key for the k8s node input
func (alibaba *Alibaba) GetKey(mapValue map[string]string, node *v1.Node) Key {
	//Mostly parse the Node object and get the ProviderID, region, InstanceType, OSType and OptimizedKeyword(In if block)
	// Currently just hardcoding a Node but eventually need to Node object
	slimK8sNode := generateSlimK8sNodeFromV1Node(node)

	optimizedKeyword := ""
	if slimK8sNode.IsIoOptimized {
		optimizedKeyword = ALIBABA_OPTIMIZE_KEYWORD
	} else {
		optimizedKeyword = ALIBABA_NON_OPTIMIZE_KEYWORD
	}
	return NewAlibabaNodeKey(slimK8sNode, optimizedKeyword)
}

type AlibabaPVKey struct {
	ProviderID       string
	RegionID         string
	DiskType         string
	DiskCategory     string
	PerformaceLevel  string
	StorageClassName string
}

func (alibabaPVKey *AlibabaPVKey) Features() string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{alibabaPVKey.RegionID, alibabaPVKey.DiskType, alibabaPVKey.DiskCategory, alibabaPVKey.PerformaceLevel})
	return strings.Join(keyLookup, "::")
}

func (alibabaPVKey *AlibabaPVKey) ID() string {
	return alibabaPVKey.ProviderID
}

// Get storage class information for PV.
func (alibabaPVKey *AlibabaPVKey) GetStorageClass() string {
	return alibabaPVKey.StorageClassName
}

// Helper functions for alibabaprovider.go

// createDescribePriceACSRequest creates the HTTP GET request for the required resources' Price information,
// When supporting subscription and Premptible resources this HTTP call needs to be modified with PriceUnit information
// When supporting different new type of instances like Compute Optimized, Memory Optimized etc make sure you add the instance type
// in unit test and check if it works or not to create the ack request and processDescribePriceAndCreateAlibabaPricing function
// else more paramters need to be pulled from kubernetes node response or gather infromation from elsewhere and function modified.
// TO-DO: Add disk adjustments to the node , Test it out!
func createDescribePriceACSRequest(i interface{}) (*requests.CommonRequest, error) {
	request := requests.NewCommonRequest()
	request.Method = requests.GET
	request.Product = ALIBABA_ECS_PRODUCT_CODE
	request.Domain = ALIBABA_ECS_DOMAIN
	request.Version = ALIBABA_ECS_VERSION
	request.Scheme = requests.HTTPS
	request.ApiName = ALIBABA_DESCRIBE_PRICE_API_ACTION
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		request.QueryParams["RegionId"] = node.RegionID
		request.QueryParams["ResourceType"] = ALIBABA_INSTANCE_RESOURCE_TYPE
		request.QueryParams["InstanceType"] = node.InstanceType
		request.QueryParams["PriceUnit"] = node.PriceUnit
		// For Enhanced General Purpose Type g6e SystemDisk.Category param doesn't default right,
		// need it to be specifically assigned to "cloud_ssd" otherwise there's errors
		if node.InstanceTypeFamily == ALIBABA_ENHANCED_GENERAL_PURPOSE_TYPE {
			request.QueryParams["SystemDisk.Category"] = ALIBABA_DISK_CLOUD_ESSD_CATEGORY
		}
		request.TransToAcsRequest()
		return request, nil
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		request.QueryParams["RegionId"] = disk.RegionID
		request.QueryParams["ResourceType"] = ALIBABA_DISK_RESOURCE_TYPE
		request.QueryParams["DataDisk.1.Category"] = disk.DiskCategory
		request.QueryParams["DataDisk.1.Size"] = fmt.Sprintf("%d", disk.SizeInGiB)
		request.QueryParams["PriceUnit"] = disk.PriceUnit
		request.TransToAcsRequest()
		return request, nil
	default:
		return nil, fmt.Errorf("unsupported ECS type (%T) for DescribePrice at this time", i)
	}
}

// determineKeyForPricing generate a unique key from SlimK8sNode object that is construct from v1.Node object.
func determineKeyForPricing(i interface{}) (string, error) {
	if i == nil {
		return "", fmt.Errorf("nil component passed to determine key")
	}
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		if node.IsIoOptimized {
			keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{node.RegionID, node.InstanceType, node.OSType, ALIBABA_OPTIMIZE_KEYWORD})
			return strings.Join(keyLookup, "::"), nil
		} else {
			keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{node.RegionID, node.InstanceType, node.OSType, ALIBABA_NON_OPTIMIZE_KEYWORD})
			return strings.Join(keyLookup, "::"), nil
		}
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{disk.RegionID, disk.DiskCategory, disk.DiskType, disk.PerformanceLevel})
		return strings.Join(keyLookup, "::"), nil
	default:
		return "", fmt.Errorf("unsupported ECS type (%T) at this time", i)
	}
}

// Below structs are used to unmarshal json response of Alibaba cloud's API DescribePrice
type Price struct {
	OriginalPrice             float32 `json:"OriginalPrice"`
	ReservedInstanceHourPrice float32 `json:"ReservedInstanceHourPrice"`
	DiscountPrice             float32 `json:"DiscountPrice"`
	Currency                  string  `json:"Currency"`
	TradePrice                float32 `json:"TradePrice"`
}

type PriceInfo struct {
	Price Price `json:"Price"`
}
type DescribePriceResponse struct {
	RequestId string    `json:"RequestId"`
	PriceInfo PriceInfo `json:"PriceInfo"`
}

// processDescribePriceAndCreateAlibabaPricing processes the DescribePrice API and generates the pricing information for alibaba node resource.
func processDescribePriceAndCreateAlibabaPricing(client *sdk.Client, i interface{}, signer *signers.AccessKeySigner, custom *CustomPricing) (pricing *AlibabaPricing, err error) {
	pricing = &AlibabaPricing{}
	var response DescribePriceResponse
	if i == nil {
		return nil, fmt.Errorf("nil component passed to process the pricing information")
	}
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		req, err := createDescribePriceACSRequest(node)
		if err != nil {
			return nil, err
		}
		resp, err := client.ProcessCommonRequestWithSigner(req, signer)
		pricing.NodeAttributes = NewAlibabaNodeAttributes(node)
		if err != nil || resp.GetHttpStatus() != 200 {
			// Can be defaulted to some value here?
			return nil, fmt.Errorf("unable to fetch information for node with InstanceType: %v", node.InstanceType)
		} else {
			// This is where population of Pricing happens
			err = json.Unmarshal(resp.GetHttpContentBytes(), &response)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshall json response to custom struct with err: %w", err)
			}
			// TO-DO : Ask in PR How to get the defaults is it equal to AWS/GCP defaults? And what needs to be returned
			pricing.Node = &Node{
				Cost:         fmt.Sprintf("%f", response.PriceInfo.Price.TradePrice),
				BaseCPUPrice: custom.CPU,
				BaseRAMPrice: custom.RAM,
				BaseGPUPrice: custom.GPU,
			}
			// TO-DO : Currently with Pay-As-You-go Offering TradePrice = HourlyPrice , When support happens to other type HourlyPrice Need to be determined.
			pricing.PricingTerms = NewAlibabaPricingTerms(ALIBABA_PAY_AS_YOU_GO_BILLING, NewAlibabaPricingDetails(response.PriceInfo.Price.TradePrice, ALIBABA_HOUR_PRICE_UNIT, response.PriceInfo.Price.TradePrice, response.PriceInfo.Price.Currency))
		}
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		req, err := createDescribePriceACSRequest(disk)
		if err != nil {
			return nil, err
		}
		resp, err := client.ProcessCommonRequestWithSigner(req, signer)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch information for disk with DiskType: %v", disk.DiskType)
		} else {
			// This is where population of Pricing happens
			err = json.Unmarshal(resp.GetHttpContentBytes(), &response)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshall json response to custom struct with err: %w", err)
			}
			pricing.PVAttributes = &AlibabaPVAttributes{}
			pricing.PV = &PV{
				Cost: fmt.Sprintf("%f", response.PriceInfo.Price.TradePrice),
			}

		}
	default:
		return nil, fmt.Errorf("unsupported ECS Pricing component of type (%T) at this time", i)
	}

	return pricing, nil
}

// This function is to get the InstanceFamily from the InstanceType , convention followed in
// instance type is ecs.[FamilyName].[DifferentSize], it gets the familyName , if it is unable to get it
// it lists the instance family name as Unknown.
// TO-DO: might need predefined list of instance types.
func getInstanceFamilyFromType(instanceType string) string {
	splitinstanceType := strings.Split(instanceType, ".")
	if len(splitinstanceType) != 3 {
		log.Warnf("unable to find the family of the instance type %s, returning it's family type unknown", instanceType)
		return ALIBABA_UNKNOWN_INSTANCE_FAMILY_TYPE
	}
	if !slices.Contains(alibabaInstanceFamilies, splitinstanceType[1]) {
		log.Warnf("currently the instance family type %s is not valid or not tested completely for pricing API", instanceType)
		return ALIBABA_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE
	}
	return splitinstanceType[1]
}

// function geenerates SlimK8sNode from v1.Node for better passing slimmed struct between functions
func generateSlimK8sNodeFromV1Node(node *v1.Node) *SlimK8sNode {
	var regionID, osType, instanceType, providerID, priceUnit, instanceFamily string
	var memorySizeInKiB string // TO-DO: try to convert it into float
	var ok, IsIoOptimized bool
	if regionID, ok = node.Labels["topology.kubernetes.io/region"]; !ok {
		// HIGHLY UNLIKELY THAT THIS LABEL WONT BE THERE.
		log.Debugf("No RegionID label for the node: %s", node.Name)
	}
	if osType, ok = node.Labels["beta.kubernetes.io/os"]; !ok {
		// HIGHLY UNLIKELY THAT THIS LABEL WONT BE THERE.
		log.Debugf("OS type undetected for the node: %s", node.Name)
	}
	if instanceType, ok = node.Labels["node.kubernetes.io/instance-type"]; !ok {
		// HIGHLY UNLIKELY THAT THIS LABEL WONT BE THERE.
		log.Debugf("Instance Type undetected for the node: %s", node.Name)
	}

	instanceFamily = getInstanceFamilyFromType(instanceType)
	memorySizeInKiB = fmt.Sprintf("%s", node.Status.Capacity.Memory())
	providerID = node.Spec.ProviderID // Alibaba Cloud provider doesnt follow convention of prefix with cloud provider name

	// Looking at current Instance offering , all of the Instances seem to be I/O optimized - https://www.alibabacloud.com/help/en/elastic-compute-service/latest/instance-family
	// Basic price Json has it as part of the key so defaulting to true.
	IsIoOptimized = true
	priceUnit = ALIBABA_HOUR_PRICE_UNIT

	return NewSlimK8sNode(instanceType, regionID, priceUnit, memorySizeInKiB, osType, providerID, instanceFamily, IsIoOptimized)
}

func generateSlimK8sDiskFromV1PV(pv v1.PersistentVolume) *SlimK8sDisk {

}

// applyAlibabaLocalDiskAdjustment will adjust the return node price with the loal disks that are attached
// to a specific node.
func applyAlibabaLocalDiskAdjustment(listOfDisks map[string]string) float32 {
	return 0.0
}
