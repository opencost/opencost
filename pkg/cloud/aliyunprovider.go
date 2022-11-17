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
	ALIYUN_ECS_PRODUCT_CODE                   = "ecs"
	ALIYUN_ECS_VERSION                        = "2014-05-26"
	ALIYUN_ECS_DOMAIN                         = "ecs.aliyuncs.com"
	ALIYUN_DESCRIBE_PRICE_API_ACTION          = "DescribePrice"
	ALIYUN_INSTANCE_RESOURCE_TYPE             = "instance"
	ALIYUN_DISK_RESOURCE_TYPE                 = "disk"
	ALIYUN_PAY_AS_YOU_GO_BILLING              = "Pay-As-You-Go"
	ALIYUN_SUBSCRIPTION_BILLING               = "Subscription"
	ALIYUN_PREEMPTIBLE_BILLING                = "Preemptible"
	ALIYUN_OPTIMIZE_KEYWORD                   = "optimize"
	ALIYUN_NON_OPTIMIZE_KEYWORD               = "nonoptimize"
	ALIYUN_HOUR_PRICE_UNIT                    = "Hour"
	ALIYUN_MONTH_PRICE_UNIT                   = "Month"
	ALIYUN_YEAR_PRICE_UNIT                    = "Year"
	ALIYUN_UNKNOWN_INSTANCE_FAMILY_TYPE       = "unknown"
	ALIYUN_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE = "unsupported"
	ALIYUN_ENHANCED_GENERAL_PURPOSE_TYPE      = "g6e"
	ALIYUN_SYSTEMDISK_CLOUD_ESSD_CATEGORY     = "cloud_essd"
)

// Why predefined and dependency on code? Can be converted to API call - https://www.alibabacloud.com/help/en/elastic-compute-service/latest/regions-describeregions
var aliyunRegions = []string{
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
var aliyunInstanceFamilies = []string{
	"g6e",
	"g6",
	"g5",
	"sn2",
	"sn2ne",
}

// AliyunAccessKey holds Aliyun credentials parsing from the service-key.json file.
type AliyunAccessKey struct {
	AccessKeyID     string `json:"aliyun_access_key_id"`
	SecretAccessKey string `json:"aliyun_secret_access_key"`
}

// TO-DO: Slim Version of k8s disk assigned to a node, To be used if price adjustment need to happen with local disk information passed to describePrice.
type SlimK8sDisk struct {
	DiskType         string
	RegionID         string
	DiskCategory     string
	PerformanceLevel string
	PriceUnit        string
	SizeInGiB        int32
	ProviderID       string
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

// AliyunNodeAttributes represents metadata about the product used to map to a node.
// Basic Attributes needed atleast to get the key, Some attributes from k8s Node response
// be populated directly into *Node object.
type AliyunNodeAttributes struct {
	InstanceType    string `json:"instanceType"`
	MemorySizeInKiB string `json:"memorySizeInKiB"`
	IsIoOptimized   bool   `json:"isIoOptimized"`
	OSType          string `json:"osType"`
}

func NewAliyunNodeAttributes(node *SlimK8sNode) *AliyunNodeAttributes {
	return &AliyunNodeAttributes{
		InstanceType:    node.InstanceType,
		MemorySizeInKiB: node.MemorySizeInKiB,
		IsIoOptimized:   node.IsIoOptimized,
		OSType:          node.OSType,
	}
}

// AliyunDiskAttributes represents metadata about the product used to map to a PV.
// Basic Attributes needed atleast to get the keys, Some attributes from k8s Node response
// be populated directly into *PV object.
type AliyunPVAttributes struct {
	DiskType         int32  `json:"diskType"`
	DiskCategory     string `json:"diskCategory"`
	PerformanceLevel string `json:"performanceLevel"`
}

// Stage 1 support will be Pay-As-You-Go with HourlyPrice equal to TradePrice with PriceUnit as Hour
// TO-DO: Subscription and Premptible support, need to find how to distinguish node into these categories]
// TO-DO: Open question Subscription would be either Monthly or Yearly, Firstly Data retrieval/population
// TO-DO:  need to be tested from describe price API, but how would you calculate hourly price, is it PRICE_YEARLY/HOURS_IN_THE_YEAR?
type AliyunPricingDetails struct {
	HourlyPrice  float32 `json:"hourlyPrice"`  // Represents hourly price for the given Aliyun Product
	PriceUnit    string  `json:"priceUnit"`    // Represents the unit in which Alibaba Product is billed can be Hour, Month or Year based on the billingMethod
	TradePrice   float32 `json:"tradePrice"`   // Original Price used to acquire the Alibaba Product
	CurrencyCode string  `json:"currencyCode"` // Represents the currency unit of the
}

func NewAliyunPricingDetails(hourlyPrice float32, priceUnit string, tradePrice float32, currencyCode string) *AliyunPricingDetails {
	return &AliyunPricingDetails{
		HourlyPrice:  hourlyPrice,
		PriceUnit:    priceUnit,
		TradePrice:   tradePrice,
		CurrencyCode: currencyCode,
	}
}

// AliyunPricingTerms can have three types of supported billing method Pay-As-You-Go, Subscription and Premptible
type AliyunPricingTerms struct {
	BillingMethod  string                `json:"billingMethod"`
	PricingDetails *AliyunPricingDetails `json:"pricingDetails"`
}

func NewAliyunPricingTerms(billingMethod string, pricingDetails *AliyunPricingDetails) *AliyunPricingTerms {
	return &AliyunPricingTerms{
		BillingMethod:  billingMethod,
		PricingDetails: pricingDetails,
	}
}

// Alibaba Pricing struct carry the Attributes and pricing information for Node or PV
type AliyunPricing struct {
	NodeAttributes *AliyunNodeAttributes
	PVAttributes   *AliyunPVAttributes
	PricingTerms   *AliyunPricingTerms
	Node           *Node
	PV             *PV
}

// Aliyun's Provider struct
type Aliyun struct {
	Pricing                 map[string]*AliyunPricing // Data to store Aliyun(Alibaba cloud) pricing struct
	DownloadPricingDataLock sync.RWMutex              // Lock Needed to provide thread safe
	Clientset               clustercache.ClusterCache
	Config                  *ProviderConfig
	serviceAccountChecks    *ServiceAccountChecks
	clusterAccountId        string
	clusterRegion           string
	loadedAccessKey         bool                             // Check if Aliyun is authenticated
	accessKey               *credentials.AccessKeyCredential // Aliyun Access key used specifically in signer interface used to sign API calls
	clients                 map[string]*sdk.Client           // Map of regionID to sdk.client to call API for that region
	*CustomProvider
}

// GetAliyunAccessKey return the Access Key used to interact with the Alibaba cloud, if not set it
// set it first by looking at env variables else load it from secret files.
// <IMPORTANT>Ask in PR what is the exact purpose of so many functions to set the key in AWS providers, am i missing something here!!!!!
func (aliyun *Aliyun) GetAliyunAccessKey() (*credentials.AccessKeyCredential, error) {
	if aliyun.loadedAccessKey {
		return aliyun.accessKey, nil
	}

	config, err := aliyun.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("Error getting the default config for aliyun provider: %s", err.Error())
	}

	//Look for service key values in env if not present in config via helm chart once changes are done
	if config.AliyunServiceKeyName == "" {
		config.AliyunServiceKeyName = env.GetAliyunAccessKeyID()
	}
	if config.AliyunServiceKeySecret == "" {
		config.AliyunServiceKeySecret = env.GetAliyunAccessKeySecret()
	}

	if config.AliyunServiceKeyName == "" && config.AliyunServiceKeySecret == "" {
		log.Debugf("missing service key values for Aliyun cloud integration attempting to use service account integration")
		err := aliyun.loadAliyunAuthSecretAndSetEnv(true)
		if err != nil {
			return nil, fmt.Errorf("unable to set the aliyun key/secret from config file")
		}
		// set custom pricing keys too
		config.AliyunServiceKeyName = env.GetAliyunAccessKeyID()
		config.AliyunServiceKeySecret = env.GetAliyunAccessKeySecret()
	}

	if config.AliyunServiceKeyName == "" && config.AliyunServiceKeySecret == "" {
		return nil, fmt.Errorf("failed to get the access key for the current alibaba account")
	}

	aliyun.accessKey = &credentials.AccessKeyCredential{AccessKeyId: env.GetAliyunAccessKeyID(), AccessKeySecret: env.GetAliyunAccessKeySecret()}
	aliyun.loadedAccessKey = true

	return aliyun.accessKey, nil
}

func (aliyun *Aliyun) DownloadPricingData() error {
	aliyun.DownloadPricingDataLock.Lock()
	defer aliyun.DownloadPricingDataLock.Unlock()

	var aak *credentials.AccessKeyCredential
	var err error

	if !aliyun.loadedAccessKey {
		aak, err = aliyun.GetAliyunAccessKey()
		if err != nil {
			log.Errorf("Unable to get the access key information")
			return fmt.Errorf("unable to get the access key information")
		}
	} else {
		aak = aliyun.accessKey
	}

	c, err := aliyun.Config.GetCustomPricingData()
	if err != nil {
		log.Errorf("Error downloading default pricing data: %s", err.Error())
		return fmt.Errorf("error downloading default pricing data: %s", err.Error())
	}

	// // Get the nodes in Aliyun provider, Once alibaba cloud is setup
	// // and populate data from the node object to resemble the data in hardcodeNodes
	nodeList := aliyun.Clientset.GetAllNodes()

	var client *sdk.Client
	var signer *signers.AccessKeySigner
	var ok bool
	var pricingObj *AliyunPricing
	var lookupKey string
	aliyun.clients = make(map[string]*sdk.Client)
	aliyun.Pricing = make(map[string]*AliyunPricing)

	// TO-DO: Add disk price adjustment by parsing the local disk information and putting it as a param in describe Price function.
	for _, node := range nodeList {
		slimK8sNode := generateSlimK8sNodeFromV1Node(node)
		lookupKey, err = determineKeyForPricing(slimK8sNode)
		if _, ok := aliyun.Pricing[lookupKey]; ok {
			log.Infof("Pricing information for node with same features %s already exists hence skipping", lookupKey)
			continue
		}
		// TO-DO: Check if Node pricing already available , skip it if available
		if client, ok = aliyun.clients[slimK8sNode.RegionID]; !ok {
			client, err = sdk.NewClientWithAccessKey(slimK8sNode.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
			if err != nil {
				return fmt.Errorf("access key provided does not have access to location %s", slimK8sNode.RegionID)
			}
			aliyun.clients[slimK8sNode.RegionID] = client
		}
		signer = signers.NewAccessKeySigner(aak)
		pricingObj, err = processDescribePriceAndCreateAliyunPricing(client, slimK8sNode, signer, c)

		if err != nil {
			return err
		}
		aliyun.Pricing[lookupKey] = pricingObj
	}

	// TO-DO: PV pricing
	// //get pvList ultimately from aliyun cloud provider and resemble data from the pvtype to
	// // Hardcodedk8sNodeDiskStruct
	// pvList := aliyun.Clientset.GetAllPersistentVolumes()

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
	// 	if client, ok = aliyun.clients[pv.RegionID]; !ok {
	// 		client, err = sdk.NewClientWithAccessKey(pv.RegionID, aak.AccessKeyId, aak.AccessKeySecret)
	// 		if err != nil {
	// 			return fmt.Errorf("access key provided does not have access to location %s", pv.RegionID)
	// 		}
	// 		aliyun.clients[pv.RegionID] = client
	// 	}
	// 	signer = signers.NewAccessKeySigner(aak)
	// 	pricingObj, err = processDescribePriceAndCreateAliyunPricing(client, pv, signer)
	// 	lookupKey, err = determineKeyForPricing(pv)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	aliyun.Pricing[lookupKey] = pricingObj
	// }
	// log.Infof("Length of pricing is %d", len(aliyun.Pricing))
	// log.Infof("random value is %v", aliyun.Pricing[lookupKey])
	return nil
}

// AllNodePricing returns all the billing data for nodes and pvs
func (aliyun *Aliyun) AllNodePricing() (interface{}, error) {
	aliyun.DownloadPricingDataLock.RLock()
	defer aliyun.DownloadPricingDataLock.RUnlock()
	return aliyun.Pricing, nil
}

// NodePricing gives a specific node for the key
func (aliyun *Aliyun) NodePricing(key Key) (*Node, error) {
	aliyun.DownloadPricingDataLock.RLock()
	defer aliyun.DownloadPricingDataLock.RUnlock()

	// Get node features for the key
	keyFeature := key.Features()

	pricing, ok := aliyun.Pricing[keyFeature]
	if !ok {
		log.Infof("Node pricing information not found for node with feature: %s", keyFeature)
		return &Node{}, nil
	}

	log.Infof("returing the node price for the node with feature: %s", keyFeature)
	return pricing.Node, nil
}

// PVPricing gives a specific PV price for the PVkey
func (aliyun *Aliyun) PVPricing(pvk PVKey) (*PV, error) {
	aliyun.DownloadPricingDataLock.RLock()
	defer aliyun.DownloadPricingDataLock.RUnlock()

	keyFeature := pvk.Features()

	pricing, ok := aliyun.Pricing[keyFeature]

	if !ok {
		log.Infof("Persistent Volume pricing not found for PV with feature: %s", keyFeature)
		return &PV{}, nil
	}

	log.Infof("returing the PV price for the node with feature: %s", keyFeature)
	return pricing.PV, nil
}

// Stubbed NetworkPricing for Aliyun. Will look at this in Next PR
func (aliyun *Aliyun) NetworkPricing() (*Network, error) {
	return &Network{
		ZoneNetworkEgressCost:     0.0,
		RegionNetworkEgressCost:   0.0,
		InternetNetworkEgressCost: 0.0,
	}, nil
}

// Stubbed LoadBalancerPricing for Aliyun. Will look at this in Next PR
func (aliyun *Aliyun) LoadBalancerPricing() (*LoadBalancer, error) {
	return &LoadBalancer{
		Cost: 0.0,
	}, nil
}

func (aliyun *Aliyun) GetConfig() (*CustomPricing, error) {
	c, err := aliyun.Config.GetCustomPricingData()
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
func (aliyun *Aliyun) loadAliyunAuthSecretAndSetEnv(force bool) error {
	if !force && aliyun.loadedAccessKey {
		return nil
	}

	exists, err := fileutil.FileExists(authSecretPath)
	if !exists || err != nil {
		return fmt.Errorf("Failed to locate service account file: %s", authSecretPath)
	}

	result, err := ioutil.ReadFile(authSecretPath)
	if err != nil {
		return err
	}

	var ak *AliyunAccessKey
	err = json.Unmarshal(result, &ak)
	if err != nil {
		return err
	}

	err = env.Set(env.AliyunAccessKeyIDEnvVar, ak.AccessKeyID)
	if err != nil {
		return err
	}
	err = env.Set(env.AliyunAccessKeySecretEnvVar, ak.SecretAccessKey)
	if err != nil {
		return err
	}
	aliyun.loadedAccessKey = true
	aliyun.accessKey = &credentials.AccessKeyCredential{
		AccessKeyId:     ak.AccessKeyID,
		AccessKeySecret: ak.SecretAccessKey,
	}
	return nil
}

// Regions returns a current supported list of Alibaba regions
func (aliyun *Aliyun) Regions() []string {
	return aliyunRegions
}

// ClusterInfo returns information about ALiyun cluster, as provided by metadata. TO-DO: Look at this function closely at next PR iteration
func (aliyun *Aliyun) ClusterInfo() (map[string]string, error) {

	c, err := aliyun.GetConfig()
	if err != nil {
		log.Errorf("Error opening config: %s", err.Error())
	}

	var clusterName string
	if c.ClusterName != "" {
		clusterName = c.ClusterName
	}

	// Use a default name if none has been set until this point
	if clusterName == "" {
		clusterName = "Aliyun Cluster #1"
	}

	m := make(map[string]string)
	m["name"] = clusterName
	m["provider"] = kubecost.AliyunProvider
	m["project"] = aliyun.clusterAccountId
	m["region"] = aliyun.clusterRegion
	m["id"] = env.GetClusterID()
	return m, nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) GetAddresses() ([]byte, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) GetDisks() ([]byte, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) UpdateConfigFromConfigMap(cm map[string]string) (*CustomPricing, error) {
	return nil, nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) GetManagementPlatform() (string, error) {
	return "", nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) GetLocalStorageQuery(window, offset time.Duration, rate bool, used bool) string {
	return ""
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) ApplyReservedInstancePricing(nodes map[string]*Node) {

}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) ServiceAccountStatus() *ServiceAccountStatus {
	return &ServiceAccountStatus{}
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) PricingSourceStatus() map[string]*PricingSource {
	return map[string]*PricingSource{}
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

// Will look at this in Next PR if needed
func (aliyun *Aliyun) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	return 0.0
}

type AliyunNodeKey struct {
	ProviderID       string
	RegionID         string
	InstanceType     string
	OSType           string
	OptimizedKeyword string //If IsIoOptimized key will have optimize if not unoptimized the key for the node
}

func NewAliyunNodeKey(node *SlimK8sNode, optimizedKeyword string) *AliyunNodeKey {
	return &AliyunNodeKey{
		ProviderID:       node.ProviderID,
		RegionID:         node.RegionID,
		InstanceType:     node.InstanceType,
		OSType:           node.OSType,
		OptimizedKeyword: optimizedKeyword,
	}
}

func (aliyunNodeKey *AliyunNodeKey) ID() string {
	return aliyunNodeKey.ProviderID
}

func (aliyunNodeKey *AliyunNodeKey) Features() string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{aliyunNodeKey.RegionID, aliyunNodeKey.InstanceType, aliyunNodeKey.OSType, aliyunNodeKey.OptimizedKeyword})
	return strings.Join(keyLookup, "::")
}

func (aliyunNodeKey *AliyunNodeKey) GPUType() string {
	return ""
}

// Get's the key for the k8s node input
func (aliyun *Aliyun) GetKey(mapValue map[string]string, node *v1.Node) Key {
	//Mostly parse the Node object and get the ProviderID, region, InstanceType, OSType and OptimizedKeyword(In if block)
	// Currently just hardcoding a Node but eventually need to Node object
	slimK8sNode := generateSlimK8sNodeFromV1Node(node)

	optimizedKeyword := ""
	if slimK8sNode.IsIoOptimized {
		optimizedKeyword = ALIYUN_OPTIMIZE_KEYWORD
	} else {
		optimizedKeyword = ALIYUN_NON_OPTIMIZE_KEYWORD
	}
	return NewAliyunNodeKey(slimK8sNode, optimizedKeyword)
}

type AliyunPVKey struct {
	ProviderID       string
	RegionID         string
	DiskType         string
	DiskCategory     string
	PerformaceLevel  string
	StorageClassName string
}

func (aliyunPVKey *AliyunPVKey) Features() string {
	keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{aliyunPVKey.RegionID, aliyunPVKey.DiskType, aliyunPVKey.DiskCategory, aliyunPVKey.PerformaceLevel})
	return strings.Join(keyLookup, "::")
}

func (aliyunPVKey *AliyunPVKey) ID() string {
	return aliyunPVKey.ProviderID
}

// Get storage class information for PV.
func (aliyunPVKey *AliyunPVKey) GetStorageClass() string {
	return aliyunPVKey.StorageClassName
}

// Helper functions for aliyunprovider.go

// createDescribePriceACSRequest creates the HTTP GET request for the required resources' Price information,
// When supporting subscription and Premptible resources this HTTP call needs to be modified with PriceUnit information
// When supporting different new type of instances like Compute Optimized, Memory Optimized etc make sure you add the instance type
// in unit test and check if it works or not to create the ack request and processDescribePriceAndCreateAliyunPricing function
// else more paramters need to be pulled from kubernetes node response or gather infromation from elsewhere and function modified.
// TO-DO: Add disk adjustments to the node , Test it out!
func createDescribePriceACSRequest(i interface{}) (*requests.CommonRequest, error) {
	request := requests.NewCommonRequest()
	request.Method = requests.GET
	request.Product = ALIYUN_ECS_PRODUCT_CODE
	request.Domain = ALIYUN_ECS_DOMAIN
	request.Version = ALIYUN_ECS_VERSION
	request.Scheme = requests.HTTPS
	request.ApiName = ALIYUN_DESCRIBE_PRICE_API_ACTION
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		request.QueryParams["RegionId"] = node.RegionID
		request.QueryParams["ResourceType"] = ALIYUN_INSTANCE_RESOURCE_TYPE
		request.QueryParams["InstanceType"] = node.InstanceType
		request.QueryParams["PriceUnit"] = node.PriceUnit
		// For Enhanced General Purpose Type g6e SystemDisk.Category param doesn't default right,
		// need it to be specifically assigned to "cloud_ssd" otherwise there's errors
		if node.InstanceTypeFamily == ALIYUN_ENHANCED_GENERAL_PURPOSE_TYPE {
			request.QueryParams["SystemDisk.Category"] = ALIYUN_SYSTEMDISK_CLOUD_ESSD_CATEGORY
		}
		request.TransToAcsRequest()
		return request, nil
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		request.QueryParams["RegionId"] = disk.RegionID
		request.QueryParams["ResourceType"] = ALIYUN_DISK_RESOURCE_TYPE
		request.QueryParams["DataDisk.1.Category"] = disk.DiskCategory
		request.QueryParams["DataDisk.1.Size"] = fmt.Sprintf("%d", disk.SizeInGiB)
		request.QueryParams["PriceUnit"] = disk.PriceUnit
		request.TransToAcsRequest()
		return request, nil
	default:
		return nil, fmt.Errorf("unsupported ECS type for DescribePrice at this time")
	}
}

// determineKeyForPricing generate a unique key from SlimK8sNode object that is construct from v1.Node object.
// This function is
func determineKeyForPricing(i interface{}) (string, error) {
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		if node.IsIoOptimized {
			keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{node.RegionID, node.InstanceType, node.OSType, ALIYUN_OPTIMIZE_KEYWORD})
			return strings.Join(keyLookup, "::"), nil
		} else {
			keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{node.RegionID, node.InstanceType, node.OSType, ALIYUN_NON_OPTIMIZE_KEYWORD})
			return strings.Join(keyLookup, "::"), nil
		}
	case *SlimK8sDisk:
		disk := i.(*SlimK8sDisk)
		keyLookup := stringutil.DeleteEmptyStringsFromArray([]string{disk.RegionID, disk.DiskCategory, disk.DiskType, disk.PerformanceLevel})
		return strings.Join(keyLookup, "::"), nil
	default:
		return "", fmt.Errorf("unsupported ECS pricing component at this time")
	}
}

// Below structs represents the structs to unmarshal json response of Aliyun's API DescribePrice
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

// processDescribePriceAndCreateAliyunPricing processes the DescribePrice API and generates the pricing information for alibaba node resource.
func processDescribePriceAndCreateAliyunPricing(client *sdk.Client, i interface{}, signer *signers.AccessKeySigner, custom *CustomPricing) (pricing *AliyunPricing, err error) {
	pricing = &AliyunPricing{}
	var response DescribePriceResponse
	log.Infof("type is %v", i)
	switch i.(type) {
	case *SlimK8sNode:
		node := i.(*SlimK8sNode)
		req, err := createDescribePriceACSRequest(node)
		log.Debugf("Request is : %v", req)
		if err != nil {
			return nil, err
		}
		resp, err := client.ProcessCommonRequestWithSigner(req, signer)
		log.Infof("value is %s", resp.GetHttpContentString())
		pricing.NodeAttributes = NewAliyunNodeAttributes(node)
		if err != nil || resp.GetHttpStatus() != 200 {
			// Can be defaulted to some value here?
			return nil, fmt.Errorf("unable to fetch information for node with InstanceType: %v", node.InstanceType)
		} else {
			// This is where population of Pricing happens
			err = json.Unmarshal(resp.GetHttpContentBytes(), &response)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshall json response to custom struct, possible change in json response of DescribePrice")
			}
			// TO-DO : Ask in PR How to get the defaults is it equal to AWS/GCP defaults? And what needs to be returned
			pricing.Node = &Node{
				Cost:         fmt.Sprintf("%f", response.PriceInfo.Price.TradePrice),
				BaseCPUPrice: custom.CPU,
				BaseRAMPrice: custom.RAM,
				BaseGPUPrice: custom.GPU,
			}
			// TO-DO : Currently with Pay-As-You-go Offering TradePrice = HourlyPrice , When support happens to other type HourlyPrice Need to be determined.
			pricing.PricingTerms = NewAliyunPricingTerms(ALIYUN_PAY_AS_YOU_GO_BILLING, NewAliyunPricingDetails(response.PriceInfo.Price.TradePrice, ALIYUN_HOUR_PRICE_UNIT, response.PriceInfo.Price.TradePrice, response.PriceInfo.Price.Currency))
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
				return nil, fmt.Errorf("unable to unmarshall json response to custom struct, possible change in json response of DescribePrice")
			}
			pricing.PVAttributes = &AliyunPVAttributes{}
			pricing.PV = &PV{
				Cost: fmt.Sprintf("%f", response.PriceInfo.Price.TradePrice),
			}

		}
	default:
		return nil, fmt.Errorf("unsupported ECS pricing component at this time")
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
		return ALIYUN_UNKNOWN_INSTANCE_FAMILY_TYPE
	}
	if !slices.Contains(aliyunInstanceFamilies, splitinstanceType[1]) {
		log.Warnf("currently the instance family type %s is not valid or not tested completely for pricing API", instanceType)
		return ALIYUN_NOT_SUPPORTED_INSTANCE_FAMILY_TYPE
	}
	return splitinstanceType[1]
}

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
	memorySizeInKiB = fmt.Sprintf("%v", node.Status.Capacity.Memory())
	providerID = node.Spec.ProviderID // Aliyun provider doesnt follow convention of prefix with cloud provider name

	// Looking at current Instance offering , all of the Instances seem to be I/O optimized - https://www.alibabacloud.com/help/en/elastic-compute-service/latest/instance-family
	// Basic price Json has it as part of the key so defaulting to true.
	IsIoOptimized = true
	priceUnit = ALIYUN_HOUR_PRICE_UNIT

	return NewSlimK8sNode(instanceType, regionID, priceUnit, memorySizeInKiB, osType, providerID, instanceFamily, IsIoOptimized)
}
