package cloud

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/jszwec/csvutil"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const awsAccessKeyIDEnvVar = "AWS_ACCESS_KEY_ID"
const awsAccessKeySecretEnvVar = "AWS_SECRET_ACCESS_KEY"
const supportedSpotFeedVersion = "1"
const SpotInfoUpdateType = "spotinfo"
const AthenaInfoUpdateType = "athenainfo"

// AWS represents an Amazon Provider
type AWS struct {
	Pricing                 map[string]*AWSProductTerms
	SpotPricingByInstanceID map[string]*spotInfo
	ValidPricingKeys        map[string]bool
	Clientset               *kubernetes.Clientset
	BaseCPUPrice            string
	BaseRAMPrice            string
	BaseGPUPrice            string
	BaseSpotCPUPrice        string
	BaseSpotRAMPrice        string
	SpotLabelName           string
	SpotLabelValue          string
	ServiceKeyName          string
	ServiceKeySecret        string
	SpotDataRegion          string
	SpotDataBucket          string
	SpotDataPrefix          string
	ProjectID               string
	DownloadPricingDataLock sync.RWMutex
	*CustomProvider
}

// AWSPricing maps a k8s node to an AWS Pricing "product"
type AWSPricing struct {
	Products map[string]*AWSProduct `json:"products"`
	Terms    AWSPricingTerms        `json:"terms"`
}

// AWSProduct represents a purchased SKU
type AWSProduct struct {
	Sku        string               `json:"sku"`
	Attributes AWSProductAttributes `json:"attributes"`
}

// AWSProductAttributes represents metadata about the product used to map to a node.
type AWSProductAttributes struct {
	Location        string `json:"location"`
	InstanceType    string `json:"instanceType"`
	Memory          string `json:"memory"`
	Storage         string `json:"storage"`
	VCpu            string `json:"vcpu"`
	UsageType       string `json:"usagetype"`
	OperatingSystem string `json:"operatingSystem"`
	PreInstalledSw  string `json:"preInstalledSw"`
	InstanceFamily  string `json:"instanceFamily"`
	GPU             string `json:"gpu"` // GPU represents the number of GPU on the instance
}

// AWSPricingTerms are how you pay for the node: OnDemand, Reserved, or (TODO) Spot
type AWSPricingTerms struct {
	OnDemand map[string]map[string]*AWSOfferTerm `json:"OnDemand"`
	Reserved map[string]map[string]*AWSOfferTerm `json:"Reserved"`
}

// AWSOfferTerm is a sku extension used to pay for the node.
type AWSOfferTerm struct {
	Sku             string                  `json:"sku"`
	PriceDimensions map[string]*AWSRateCode `json:"priceDimensions"`
}

// AWSRateCode encodes data about the price of a product
type AWSRateCode struct {
	Unit         string          `json:"unit"`
	PricePerUnit AWSCurrencyCode `json:"pricePerUnit"`
}

// AWSCurrencyCode is the localized currency. (TODO: support non-USD)
type AWSCurrencyCode struct {
	USD string `json:"USD"`
}

// AWSProductTerms represents the full terms of the product
type AWSProductTerms struct {
	Sku      string        `json:"sku"`
	OnDemand *AWSOfferTerm `json:"OnDemand"`
	Reserved *AWSOfferTerm `json:"Reserved"`
	Memory   string        `json:"memory"`
	Storage  string        `json:"storage"`
	VCpu     string        `json:"vcpu"`
	GPU      string        `json:"gpu"` // GPU represents the number of GPU on the instance
	PV       *PV           `json:"pv"`
}

// ClusterIdEnvVar is the environment variable in which one can manually set the ClusterId
const ClusterIdEnvVar = "AWS_CLUSTER_ID"

// OnDemandRateCode is appended to an node sku
const OnDemandRateCode = ".JRTCKXETXF"

// ReservedRateCode is appended to a node sku
const ReservedRateCode = ".38NPMPTW36"

// HourlyRateCode is appended to a node sku
const HourlyRateCode = ".6YS6EN2CT7"

// volTypes are used to map between AWS UsageTypes and
// EBS volume types, as they would appear in K8s storage class
// name and the EC2 API.
var volTypes = map[string]string{
	"EBS:VolumeUsage.gp2":    "gp2",
	"EBS:VolumeUsage":        "standard",
	"EBS:VolumeUsage.sc1":    "sc1",
	"EBS:VolumeP-IOPS.piops": "io1",
	"EBS:VolumeUsage.st1":    "st1",
	"EBS:VolumeUsage.piops":  "io1",
	"gp2":                    "EBS:VolumeUsage.gp2",
	"standard":               "EBS:VolumeUsage",
	"sc1":                    "EBS:VolumeUsage.sc1",
	"io1":                    "EBS:VolumeUsage.piops",
	"st1":                    "EBS:VolumeUsage.st1",
}

// locationToRegion maps AWS region names (As they come from Billing)
// to actual region identifiers
var locationToRegion = map[string]string{
	"US East (Ohio)":             "us-east-2",
	"US East (N. Virginia)":      "us-east-1",
	"US West (N. California)":    "us-west-1",
	"US West (Oregon)":           "us-west-2",
	"Asia Pacific (Hong Kong)":   "ap-east-1",
	"Asia Pacific (Mumbai)":      "ap-south-1",
	"Asia Pacific (Osaka-Local)": "ap-northeast-3",
	"Asia Pacific (Seoul)":       "ap-northeast-2",
	"Asia Pacific (Singapore)":   "ap-southeast-1",
	"Asia Pacific (Sydney)":      "ap-southeast-2",
	"Asia Pacific (Tokyo)":       "ap-northeast-1",
	"Canada (Central)":           "ca-central-1",
	"China (Beijing)":            "cn-north-1",
	"China (Ningxia)":            "cn-northwest-1",
	"EU (Frankfurt)":             "eu-central-1",
	"EU (Ireland)":               "eu-west-1",
	"EU (London)":                "eu-west-2",
	"EU (Paris)":                 "eu-west-3",
	"EU (Stockholm)":             "eu-north-1",
	"South America (Sao Paulo)":  "sa-east-1",
	"AWS GovCloud (US-East)":     "us-gov-east-1",
	"AWS GovCloud (US)":          "us-gov-west-1",
}

var regionToBillingRegionCode = map[string]string{
	"us-east-2":      "USE2",
	"us-east-1":      "",
	"us-west-1":      "USW1",
	"us-west-2":      "USW2",
	"ap-east-1":      "APE1",
	"ap-south-1":     "APS3",
	"ap-northeast-3": "APN3",
	"ap-northeast-2": "APN2",
	"ap-southeast-1": "APS1",
	"ap-southeast-2": "APS2",
	"ap-northeast-1": "APN1",
	"ca-central-1":   "CAN1",
	"cn-north-1":     "",
	"cn-northwest-1": "",
	"eu-central-1":   "EUC1",
	"eu-west-1":      "EU",
	"eu-west-2":      "EUW2",
	"eu-west-3":      "EUW3",
	"eu-north-1":     "EUN1",
	"sa-east-1":      "SAE1",
	"us-gov-east-1":  "UGE1",
	"us-gov-west-1":  "UGW1",
}

// KubeAttrConversion maps the k8s labels for region to an aws region
func (aws *AWS) KubeAttrConversion(location, instanceType, operatingSystem string) string {
	operatingSystem = strings.ToLower(operatingSystem)

	region := locationToRegion[location]
	return region + "," + instanceType + "," + operatingSystem
}

type AwsSpotFeedInfo struct {
	BucketName       string `json:"bucketName"`
	Prefix           string `json:"prefix"`
	Region           string `json:"region"`
	AccountID        string `json:"projectID"`
	ServiceKeyName   string `json:"serviceKeyName"`
	ServiceKeySecret string `json:"serviceKeySecret"`
	SpotLabel        string `json:"spotLabel"`
	SpotLabelValue   string `json:"spotLabelValue"`
}

type AwsAthenaInfo struct {
	AthenaBucketName string `json:"athenaBucketName"`
	AthenaRegion     string `json:"athenaRegion"`
	AthenaDatabase   string `json:"athenaDatabase"`
	AthenaTable      string `json:"athenaTable"`
	ServiceKeyName   string `json:"serviceKeyName"`
	ServiceKeySecret string `json:"serviceKeySecret"`
	AccountID        string `json:"projectID"`
}

func (aws *AWS) GetConfig() (*CustomPricing, error) {
	c, err := GetDefaultPricingData("aws.json")
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (aws *AWS) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	c, err := GetDefaultPricingData("aws.json")
	if err != nil {
		return nil, err
	}
	if updateType == SpotInfoUpdateType {
		a := AwsSpotFeedInfo{}
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return nil, err
		}

		if err != nil {
			return nil, err
		}
		c.ServiceKeyName = a.ServiceKeyName
		c.ServiceKeySecret = a.ServiceKeySecret
		c.SpotDataPrefix = a.Prefix
		c.SpotDataBucket = a.BucketName
		c.ProjectID = a.AccountID
		c.SpotDataRegion = a.Region
		c.SpotLabel = a.SpotLabel
		c.SpotLabelValue = a.SpotLabelValue

	} else if updateType == AthenaInfoUpdateType {
		a := AwsAthenaInfo{}
		err := json.NewDecoder(r).Decode(&a)
		if err != nil {
			return nil, err
		}
		c.AthenaBucketName = a.AthenaBucketName
		c.AthenaRegion = a.AthenaRegion
		c.AthenaDatabase = a.AthenaDatabase
		c.AthenaTable = a.AthenaTable
		c.ServiceKeyName = a.ServiceKeyName
		c.ServiceKeySecret = a.ServiceKeySecret
		c.ProjectID = a.AccountID
	} else {
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
	}
	cj, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/models/"
	}
	path += "aws.json"
	err = ioutil.WriteFile(path, cj, 0644)
	if err != nil {
		return nil, err
	}
	return c, nil
}

type awsKey struct {
	SpotLabelName  string
	SpotLabelValue string
	Labels         map[string]string
	ProviderID     string
}

func (k *awsKey) GPUType() string {
	return ""
}

func (k *awsKey) ID() string {
	provIdRx := regexp.MustCompile("aws:///([^/]+)/([^/]+)") // It's of the form aws:///us-east-2a/i-0fea4fd46592d050b and we want i-0fea4fd46592d050b, if it exists
	for matchNum, group := range provIdRx.FindStringSubmatch(k.ProviderID) {
		if matchNum == 2 {
			return group
		}
	}
	klog.V(3).Infof("Could not find instance ID in \"%s\"", k.ProviderID)
	return ""
}

func (k *awsKey) Features() string {

	instanceType := k.Labels[v1.LabelInstanceType]
	var operatingSystem string
	operatingSystem, ok := k.Labels[v1.LabelOSStable]
	if !ok {
		operatingSystem = k.Labels["beta.kubernetes.io/os"]
	}
	region := k.Labels[v1.LabelZoneRegion]

	key := region + "," + instanceType + "," + operatingSystem
	usageType := "preemptible"
	spotKey := key + "," + usageType
	if l, ok := k.Labels["lifecycle"]; ok && l == "EC2Spot" {
		return spotKey
	}
	if l, ok := k.Labels[k.SpotLabelName]; ok && l == k.SpotLabelValue {
		return spotKey
	}
	return key
}

func (aws *AWS) PVPricing(pvk PVKey) (*PV, error) {
	pricing, ok := aws.Pricing[pvk.Features()]
	if !ok {
		klog.V(2).Infof("Persistent Volume pricing not found for %s", pvk.Features())
		return &PV{}, nil
	}
	return pricing.PV, nil
}

type awsPVKey struct {
	Labels                 map[string]string
	StorageClassParameters map[string]string
	StorageClassName       string
}

func (aws *AWS) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string) PVKey {
	return &awsPVKey{
		Labels:           pv.Labels,
		StorageClassName: pv.Spec.StorageClassName,
	}
}

func (key *awsPVKey) Features() string {
	storageClass := key.StorageClassName
	if storageClass == "standard" {
		storageClass = "gp2"
	}
	// Storage class names are generally EBS volume types (gp2)
	// Keys in Pricing are based on UsageTypes (EBS:VolumeType.gp2)
	// Converts between the 2
	return key.Labels[v1.LabelZoneRegion] + "," + volTypes[storageClass]
}

// GetKey maps node labels to information needed to retrieve pricing data
func (aws *AWS) GetKey(labels map[string]string) Key {
	return &awsKey{
		SpotLabelName:  aws.SpotLabelName,
		SpotLabelValue: aws.SpotLabelValue,
		Labels:         labels,
		ProviderID:     labels["providerID"],
	}
}

func (aws *AWS) isPreemptible(key string) bool {
	s := strings.Split(key, ",")
	if len(s) == 4 && s[3] == "preemptible" {
		return true
	}
	return false
}

// DownloadPricingData fetches data from the AWS Pricing API
func (aws *AWS) DownloadPricingData() error {
	aws.DownloadPricingDataLock.Lock()
	defer aws.DownloadPricingDataLock.Unlock()
	c, err := GetDefaultPricingData("aws.json")
	if err != nil {
		klog.V(1).Infof("Error downloading default pricing data: %s", err.Error())
	}
	aws.BaseCPUPrice = c.CPU
	aws.BaseRAMPrice = c.RAM
	aws.BaseGPUPrice = c.GPU
	aws.BaseSpotCPUPrice = c.SpotCPU
	aws.BaseSpotRAMPrice = c.SpotRAM
	aws.SpotLabelName = c.SpotLabel
	aws.SpotLabelValue = c.SpotLabelValue
	aws.SpotDataBucket = c.SpotDataBucket
	aws.SpotDataPrefix = c.SpotDataPrefix
	aws.ProjectID = c.ProjectID
	aws.SpotDataRegion = c.SpotDataRegion
	aws.ServiceKeyName = c.ServiceKeyName
	aws.ServiceKeySecret = c.ServiceKeySecret

	if len(aws.SpotDataBucket) != 0 && len(aws.ProjectID) == 0 {
		return fmt.Errorf("using SpotDataBucket \"%s\" without ProjectID will not end well", aws.SpotDataBucket)
	}
	nodeList, err := aws.Clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	inputkeys := make(map[string]bool)
	for _, n := range nodeList.Items {
		labels := n.GetObjectMeta().GetLabels()
		key := aws.GetKey(labels)
		inputkeys[key.Features()] = true
	}

	pvList, err := aws.Clientset.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}

	storageClasses, err := aws.Clientset.StorageV1().StorageClasses().List(metav1.ListOptions{})
	storageClassMap := make(map[string]map[string]string)
	for _, storageClass := range storageClasses.Items {
		params := storageClass.Parameters
		storageClassMap[storageClass.ObjectMeta.Name] = params
	}

	pvkeys := make(map[string]PVKey)
	for _, pv := range pvList.Items {
		params, ok := storageClassMap[pv.Spec.StorageClassName]
		if !ok {
			klog.Infof("Unable to find params for storageClassName %s", pv.Name)
			continue
		}
		key := aws.GetPVKey(&pv, params)
		pvkeys[key.Features()] = key
	}

	aws.Pricing = make(map[string]*AWSProductTerms)
	aws.ValidPricingKeys = make(map[string]bool)
	skusToKeys := make(map[string]string)

	pricingURL := "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"
	klog.V(2).Infof("starting download of \"%s\", which is quite large ...", pricingURL)
	resp, err := http.Get(pricingURL)
	if err != nil {
		klog.V(2).Infof("Bogus fetch of \"%s\": %v", pricingURL, err)
		return err
	}
	klog.V(2).Infof("Finished downloading \"%s\"", pricingURL)

	dec := json.NewDecoder(resp.Body)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			klog.V(2).Infof("done loading \"%s\"\n", pricingURL)
			break
		}
		if t == "products" {
			_, err := dec.Token() // this should parse the opening "{""
			if err != nil {
				return err
			}
			for dec.More() {
				_, err := dec.Token() // the sku token
				if err != nil {
					return err
				}
				product := &AWSProduct{}

				err = dec.Decode(&product)
				if err != nil {
					klog.V(1).Infof("Error parsing response from \"%s\": %v", pricingURL, err.Error())
					break
				}

				if product.Attributes.PreInstalledSw == "NA" &&
					(strings.HasPrefix(product.Attributes.UsageType, "BoxUsage") || strings.Contains(product.Attributes.UsageType, "-BoxUsage")) {
					key := aws.KubeAttrConversion(product.Attributes.Location, product.Attributes.InstanceType, product.Attributes.OperatingSystem)
					spotKey := key + ",preemptible"
					if inputkeys[key] || inputkeys[spotKey] { // Just grab the sku even if spot, and change the price later.
						productTerms := &AWSProductTerms{
							Sku:     product.Sku,
							Memory:  product.Attributes.Memory,
							Storage: product.Attributes.Storage,
							VCpu:    product.Attributes.VCpu,
							GPU:     product.Attributes.GPU,
						}
						aws.Pricing[key] = productTerms
						aws.Pricing[spotKey] = productTerms
						skusToKeys[product.Sku] = key
					}
					aws.ValidPricingKeys[key] = true
					aws.ValidPricingKeys[spotKey] = true
				} else if strings.Contains(product.Attributes.UsageType, "EBS:Volume") {
					// UsageTypes may be prefixed with a region code - we're removing this when using
					// volTypes to keep lookups generic
					usageTypeRegx := regexp.MustCompile(".*(-|^)(EBS.+)")
					usageTypeMatch := usageTypeRegx.FindStringSubmatch(product.Attributes.UsageType)
					usageTypeNoRegion := usageTypeMatch[len(usageTypeMatch)-1]
					key := locationToRegion[product.Attributes.Location] + "," + usageTypeNoRegion
					spotKey := key + ",preemptible"
					klog.V(1).Infof("KEY %s", key)
					pv := &PV{
						Class:  volTypes[usageTypeNoRegion],
						Region: locationToRegion[product.Attributes.Location],
					}
					productTerms := &AWSProductTerms{
						Sku: product.Sku,
						PV:  pv,
					}
					aws.Pricing[key] = productTerms
					aws.Pricing[spotKey] = productTerms
					skusToKeys[product.Sku] = key
					aws.ValidPricingKeys[key] = true
					aws.ValidPricingKeys[spotKey] = true
				}
			}
		}
		if t == "terms" {
			_, err := dec.Token() // this should parse the opening "{""
			if err != nil {
				return err
			}
			termType, err := dec.Token()
			if err != nil {
				return err
			}
			if termType == "OnDemand" {
				_, err := dec.Token()
				if err != nil { // again, should parse an opening "{"
					return err
				}
				for dec.More() {
					sku, err := dec.Token()
					if err != nil {
						return err
					}
					_, err = dec.Token() // another opening "{"
					if err != nil {
						return err
					}
					skuOnDemand, err := dec.Token()
					if err != nil {
						return err
					}
					offerTerm := &AWSOfferTerm{}
					err = dec.Decode(&offerTerm)
					if err != nil {
						klog.V(1).Infof("Error decoding AWS Offer Term: " + err.Error())
					}
					if sku.(string)+OnDemandRateCode == skuOnDemand {
						key, ok := skusToKeys[sku.(string)]
						spotKey := key + ",preemptible"
						if ok {
							aws.Pricing[key].OnDemand = offerTerm
							aws.Pricing[spotKey].OnDemand = offerTerm
							if strings.Contains(key, "EBS:VolumeP-IOPS.piops") {
								// If the specific UsageType is the per IO cost used on io1 volumes
								// we need to add the per IO cost to the io1 PV cost
								cost := offerTerm.PriceDimensions[sku.(string)+OnDemandRateCode+HourlyRateCode].PricePerUnit.USD
								// Add the per IO cost to the PV object for the io1 volume type
								aws.Pricing[key].PV.CostPerIO = cost
							} else if strings.Contains(key, "EBS:Volume") {
								// If volume, we need to get hourly cost and add it to the PV object
								cost := offerTerm.PriceDimensions[sku.(string)+OnDemandRateCode+HourlyRateCode].PricePerUnit.USD
								costFloat, _ := strconv.ParseFloat(cost, 64)
								hourlyPrice := (costFloat * math.Pow10(-9)) / 730

								aws.Pricing[key].PV.Cost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
							}
						}
					}
					_, err = dec.Token()
					if err != nil {
						return err
					}
				}
				_, err = dec.Token()
				if err != nil {
					return err
				}
			}
		}
	}

	sp, err := parseSpotData(aws.SpotDataBucket, aws.SpotDataPrefix, aws.ProjectID, aws.SpotDataRegion, aws.ServiceKeyName, aws.ServiceKeySecret)
	if err != nil {
		klog.V(1).Infof("Error downloading spot data %s", err.Error())
	} else {
		aws.SpotPricingByInstanceID = sp
	}

	return nil
}

// AllNodePricing returns all the billing data fetched.
func (aws *AWS) AllNodePricing() (interface{}, error) {
	aws.DownloadPricingDataLock.RLock()
	defer aws.DownloadPricingDataLock.RUnlock()
	return aws.Pricing, nil
}

func (aws *AWS) createNode(terms *AWSProductTerms, usageType string, k Key) (*Node, error) {
	key := k.Features()
	if aws.isPreemptible(key) {
		if spotInfo, ok := aws.SpotPricingByInstanceID[k.ID()]; ok { // try and match directly to an ID for pricing. We'll still need the features
			var spotcost string
			arr := strings.Split(spotInfo.Charge, " ")
			if len(arr) == 2 {
				spotcost = arr[0]
			} else {
				klog.V(2).Infof("Spot data for node %s is missing", k.ID())
			}
			return &Node{
				Cost:         spotcost,
				VCPU:         terms.VCpu,
				RAM:          terms.Memory,
				GPU:          terms.GPU,
				Storage:      terms.Storage,
				BaseCPUPrice: aws.BaseCPUPrice,
				BaseRAMPrice: aws.BaseRAMPrice,
				BaseGPUPrice: aws.BaseGPUPrice,
				UsageType:    usageType,
			}, nil
		}
		return &Node{
			VCPU:         terms.VCpu,
			VCPUCost:     aws.BaseSpotCPUPrice,
			RAM:          terms.Memory,
			GPU:          terms.GPU,
			RAMCost:      aws.BaseSpotRAMPrice,
			Storage:      terms.Storage,
			BaseCPUPrice: aws.BaseCPUPrice,
			BaseRAMPrice: aws.BaseRAMPrice,
			BaseGPUPrice: aws.BaseGPUPrice,
			UsageType:    usageType,
		}, nil
	}
	c, ok := terms.OnDemand.PriceDimensions[terms.Sku+OnDemandRateCode+HourlyRateCode]
	if !ok {
		return nil, fmt.Errorf("Could not fetch data for \"%s\"", k.ID())
	}
	cost := c.PricePerUnit.USD
	return &Node{
		Cost:         cost,
		VCPU:         terms.VCpu,
		RAM:          terms.Memory,
		GPU:          terms.GPU,
		Storage:      terms.Storage,
		BaseCPUPrice: aws.BaseCPUPrice,
		BaseRAMPrice: aws.BaseRAMPrice,
		BaseGPUPrice: aws.BaseGPUPrice,
		UsageType:    usageType,
	}, nil
}

// NodePricing takes in a key from GetKey and returns a Node object for use in building the cost model.
func (aws *AWS) NodePricing(k Key) (*Node, error) {
	aws.DownloadPricingDataLock.RLock()
	defer aws.DownloadPricingDataLock.RUnlock()

	key := k.Features()
	usageType := "ondemand"
	if aws.isPreemptible(key) {
		usageType = "preemptible"
	}

	terms, ok := aws.Pricing[key]
	if ok {
		return aws.createNode(terms, usageType, k)
	} else if _, ok := aws.ValidPricingKeys[key]; ok {
		aws.DownloadPricingDataLock.RUnlock()
		err := aws.DownloadPricingData()
		aws.DownloadPricingDataLock.RLock()
		if err != nil {
			return nil, err
		}
		terms, termsOk := aws.Pricing[key]
		if !termsOk {
			return nil, fmt.Errorf("Unable to find any Pricing data for \"%s\"", key)
		}
		return aws.createNode(terms, usageType, k)
	} else { // Fall back to base pricing if we can't find the key.
		klog.V(1).Infof("Invalid Pricing Key \"%s\"", key)
		return &Node{
			Cost:             aws.BaseCPUPrice,
			BaseCPUPrice:     aws.BaseCPUPrice,
			BaseRAMPrice:     aws.BaseRAMPrice,
			BaseGPUPrice:     aws.BaseGPUPrice,
			UsageType:        usageType,
			UsesBaseCPUPrice: true,
		}, nil
	}
}

// ClusterName returns an object that represents the cluster. TODO: actually return the name of the cluster. Blocked on cluster federation.
func (awsProvider *AWS) ClusterName() ([]byte, error) {
	defaultClusterName := "AWS Cluster #1"
	makeStructure := func(clusterName string) ([]byte, error) {
		klog.V(2).Infof("Returning \"%s\" as ClusterName", clusterName)
		m := make(map[string]string)
		m["name"] = clusterName
		m["provider"] = "AWS"
		return json.Marshal(m)
	}

	maybeClusterId := os.Getenv(ClusterIdEnvVar)
	if len(maybeClusterId) != 0 {
		return makeStructure(maybeClusterId)
	}
	provIdRx := regexp.MustCompile("aws:///([^/]+)/([^/]+)")
	clusterIdRx := regexp.MustCompile("^kubernetes\\.io/cluster/([^/]+)")
	nodeList, err := awsProvider.Clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, n := range nodeList.Items {
		region := ""
		instanceId := ""
		providerId := n.Spec.ProviderID
		for matchNum, group := range provIdRx.FindStringSubmatch(providerId) {
			if matchNum == 1 {
				region = group
			} else if matchNum == 2 {
				instanceId = group
			}
		}
		if len(instanceId) == 0 {
			klog.V(2).Infof("Unable to decode Node.ProviderID \"%s\", skipping it", providerId)
			continue
		}
		c := &aws.Config{
			Region: aws.String(region),
		}
		s := session.Must(session.NewSession(c))
		ec2Svc := ec2.New(s)
		di, diErr := ec2Svc.DescribeInstances(&ec2.DescribeInstancesInput{
			InstanceIds: []*string{
				aws.String(instanceId),
			},
		})
		if diErr != nil {
			// maybe log this?
			continue
		}
		if len(di.Reservations) != 1 {
			klog.V(2).Infof("Expected 1 Reservation back from DescribeInstances(%s), received %d", instanceId, len(di.Reservations))
			continue
		}
		res := di.Reservations[0]
		if len(res.Instances) != 1 {
			klog.V(2).Infof("Expected 1 Instance back from DescribeInstances(%s), received %d", instanceId, len(res.Instances))
			continue
		}
		inst := res.Instances[0]
		for _, tag := range inst.Tags {
			tagKey := *tag.Key
			for matchNum, group := range clusterIdRx.FindStringSubmatch(tagKey) {
				if matchNum != 1 {
					continue
				}
				return makeStructure(group)
			}
		}
	}
	klog.V(2).Infof("Unable to sniff out cluster ID, perhaps set $%s to force one", ClusterIdEnvVar)
	return makeStructure(defaultClusterName)
}

// AddServiceKey adds an AWS service key, useful for pulling down out-of-cluster costs. Optional-- the container this runs in can be directly authorized.
func (*AWS) AddServiceKey(formValues url.Values) error {
	keyID := formValues.Get("access_key_ID")
	key := formValues.Get("secret_access_key")
	m := make(map[string]string)
	m["access_key_ID"] = keyID
	m["secret_access_key"] = key
	result, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return ioutil.WriteFile("/var/configs/key.json", result, 0644)
}

// GetDisks returns the AWS disks backing PVs. Useful because sometimes k8s will not clean up PVs correctly. Requires a json config in /var/configs with key region.
func (*AWS) GetDisks() ([]byte, error) {
	jsonFile, err := os.Open("/var/configs/key.json")
	if err == nil {
		byteValue, _ := ioutil.ReadAll(jsonFile)
		var result map[string]string
		err := json.Unmarshal([]byte(byteValue), &result)
		if err != nil {
			return nil, err
		}
		err = os.Setenv(awsAccessKeyIDEnvVar, result["access_key_ID"])
		if err != nil {
			return nil, err
		}
		err = os.Setenv(awsAccessKeySecretEnvVar, result["secret_access_key"])
		if err != nil {
			return nil, err
		}
	} else if os.IsNotExist(err) {
		klog.V(2).Infof("Using Default Credentials")
	} else {
		return nil, err
	}
	defer jsonFile.Close()
	clusterConfig, err := os.Open("/var/configs/cluster.json")
	if err != nil {
		return nil, err
	}
	defer clusterConfig.Close()
	b, err := ioutil.ReadAll(clusterConfig)
	if err != nil {
		return nil, err
	}
	var clusterConf map[string]string
	err = json.Unmarshal([]byte(b), &clusterConf)
	if err != nil {
		return nil, err
	}
	region := aws.String(clusterConf["region"])
	c := &aws.Config{
		Region: region,
	}
	s := session.Must(session.NewSession(c))

	ec2Svc := ec2.New(s)
	input := &ec2.DescribeVolumesInput{}
	volumeResult, err := ec2Svc.DescribeVolumes(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return nil, aerr
			}
		} else {
			return nil, err
		}
	}
	return json.Marshal(volumeResult)
}

// ConvertToGlueColumnFormat takes a string and runs through various regex
// and string replacement statements to convert it to a format compatible
// with AWS Glue and Athena column names.
// Following guidance from AWS provided here ('Column Names' section):
// https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/run-athena-sql.html
// It returns a string containing the column name in proper column name format and length.
func ConvertToGlueColumnFormat(column_name string) string {
	klog.V(5).Infof("Converting string \"%s\" to proper AWS Glue column name.", column_name)

	// An underscore is added in front of uppercase letters
	capital_underscore := regexp.MustCompile(`[A-Z]`)
	final := capital_underscore.ReplaceAllString(column_name, `_$0`)

	// Any non-alphanumeric characters are replaced with an underscore
	no_space_punc := regexp.MustCompile(`[\s]{1,}|[^A-Za-z0-9]`)
	final = no_space_punc.ReplaceAllString(final, "_")

	// Duplicate underscores are removed
	no_dup_underscore := regexp.MustCompile(`_{2,}`)
	final = no_dup_underscore.ReplaceAllString(final, "_")

	// Any leading and trailing underscores are removed
	no_front_end_underscore := regexp.MustCompile(`(^\_|\_$)`)
	final = no_front_end_underscore.ReplaceAllString(final, "")

	// Uppercase to lowercase
	final = strings.ToLower(final)

	// Longer column name than expected - remove _ left to right
	allowed_col_len := 128
	undersc_to_remove := len(final) - allowed_col_len
	if undersc_to_remove > 0 {
		final = strings.Replace(final, "_", "", undersc_to_remove)
	}

	// If removing all of the underscores still didn't
	// make the column name < 128 characters, trim it!
	if len(final) > allowed_col_len {
		final = final[:allowed_col_len]
	}

	klog.V(5).Infof("Column name being returned: \"%s\". Length: \"%d\".", final, len(final))

	return final
}

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (a *AWS) ExternalAllocations(start string, end string, aggregator string) ([]*OutOfClusterAllocation, error) {
	customPricing, err := a.GetConfig()
	if err != nil {
		return nil, err
	}
	aggregator_column_name := "resource_tags_user_kubernetes_" + aggregator
	aggregator_column_name = ConvertToGlueColumnFormat(aggregator_column_name)
	query := fmt.Sprintf(`SELECT   
		CAST(line_item_usage_start_date AS DATE) as start_date,
		%s,
		line_item_product_code,
		SUM(line_item_blended_cost) as blended_cost
	FROM %s as cost_data
	WHERE line_item_usage_start_date BETWEEN date '%s' AND date '%s'
	GROUP BY 1,2,3`, aggregator_column_name, customPricing.AthenaTable, start, end)

	if customPricing.ServiceKeyName != "" {
		err = os.Setenv(awsAccessKeyIDEnvVar, customPricing.ServiceKeyName)
		if err != nil {
			return nil, err
		}
		err = os.Setenv(awsAccessKeySecretEnvVar, customPricing.ServiceKeySecret)
		if err != nil {
			return nil, err
		}
	}
	region := aws.String(customPricing.AthenaRegion)
	resultsBucket := customPricing.AthenaBucketName
	database := customPricing.AthenaDatabase

	c := &aws.Config{
		Region: region,
	}
	s := session.Must(session.NewSession(c))
	svc := athena.New(s)

	var e athena.StartQueryExecutionInput

	var r athena.ResultConfiguration
	r.SetOutputLocation(resultsBucket)
	e.SetResultConfiguration(&r)

	e.SetQueryString(query)
	var q athena.QueryExecutionContext
	q.SetDatabase(database)
	e.SetQueryExecutionContext(&q)

	res, err := svc.StartQueryExecution(&e)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("StartQueryExecution result:")
	klog.V(2).Infof(res.GoString())

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*res.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		qrop, err = svc.GetQueryExecution(&qri)
		if err != nil {
			return nil, err
		}
		if *qrop.QueryExecution.Status.State != "RUNNING" {
			break
		}
		time.Sleep(duration)
	}
	var oocAllocs []*OutOfClusterAllocation
	if *qrop.QueryExecution.Status.State == "SUCCEEDED" {

		var ip athena.GetQueryResultsInput
		ip.SetQueryExecutionId(*res.QueryExecutionId)

		op, err := svc.GetQueryResults(&ip)
		if err != nil {
			return nil, err
		}

		for _, r := range op.ResultSet.Rows[1:(len(op.ResultSet.Rows) - 1)] {

			cost, err := strconv.ParseFloat(*r.Data[3].VarCharValue, 64)
			if err != nil {
				return nil, err
			}
			ooc := &OutOfClusterAllocation{
				Aggregator:  aggregator,
				Environment: *r.Data[1].VarCharValue,
				Service:     *r.Data[2].VarCharValue,
				Cost:        cost,
			}
			oocAllocs = append(oocAllocs, ooc)
		}
	}

	return oocAllocs, nil // TODO: transform the QuerySQL lines into the new OutOfClusterAllocation Struct
}

// QuerySQL can query a properly configured Athena database.
// Used to fetch billing data.
// Requires a json config in /var/configs with key region, output, and database.
func (a *AWS) QuerySQL(query string) ([]byte, error) {
	customPricing, err := a.GetConfig()
	if err != nil {
		return nil, err
	}
	if customPricing.ServiceKeyName != "" {
		err = os.Setenv(awsAccessKeyIDEnvVar, customPricing.ServiceKeyName)
		if err != nil {
			return nil, err
		}
		err = os.Setenv(awsAccessKeySecretEnvVar, customPricing.ServiceKeySecret)
		if err != nil {
			return nil, err
		}
	}
	athenaConfigs, err := os.Open("/var/configs/athena.json")
	if err != nil {
		return nil, err
	}
	defer athenaConfigs.Close()
	b, err := ioutil.ReadAll(athenaConfigs)
	if err != nil {
		return nil, err
	}
	var athenaConf map[string]string
	json.Unmarshal([]byte(b), &athenaConf)
	region := aws.String(customPricing.AthenaRegion)
	resultsBucket := customPricing.AthenaBucketName
	database := customPricing.AthenaDatabase

	c := &aws.Config{
		Region: region,
	}
	s := session.Must(session.NewSession(c))
	svc := athena.New(s)

	var e athena.StartQueryExecutionInput

	var r athena.ResultConfiguration
	r.SetOutputLocation(resultsBucket)
	e.SetResultConfiguration(&r)

	e.SetQueryString(query)
	var q athena.QueryExecutionContext
	q.SetDatabase(database)
	e.SetQueryExecutionContext(&q)

	res, err := svc.StartQueryExecution(&e)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("StartQueryExecution result:")
	klog.V(2).Infof(res.GoString())

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*res.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		qrop, err = svc.GetQueryExecution(&qri)
		if err != nil {
			return nil, err
		}
		if *qrop.QueryExecution.Status.State != "RUNNING" {
			break
		}
		time.Sleep(duration)
	}
	if *qrop.QueryExecution.Status.State == "SUCCEEDED" {

		var ip athena.GetQueryResultsInput
		ip.SetQueryExecutionId(*res.QueryExecutionId)

		op, err := svc.GetQueryResults(&ip)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(op.ResultSet)
		if err != nil {
			return nil, err
		}

		return b, nil
	}
	return nil, fmt.Errorf("Error getting query results : %s", *qrop.QueryExecution.Status.State)
}

type spotInfo struct {
	Timestamp   string `csv:"Timestamp"`
	UsageType   string `csv:"UsageType"`
	Operation   string `csv:"Operation"`
	InstanceID  string `csv:"InstanceID"`
	MyBidID     string `csv:"MyBidID"`
	MyMaxPrice  string `csv:"MyMaxPrice"`
	MarketPrice string `csv:"MarketPrice"`
	Charge      string `csv:"Charge"`
	Version     string `csv:"Version"`
}

type fnames []*string

func (f fnames) Len() int {
	return len(f)
}

func (f fnames) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f fnames) Less(i, j int) bool {
	key1 := strings.Split(*f[i], ".")
	key2 := strings.Split(*f[j], ".")

	t1, err := time.Parse("2006-01-02-15", key1[1])
	if err != nil {
		klog.V(1).Info("Unable to parse timestamp" + key1[1])
		return false
	}
	t2, err := time.Parse("2006-01-02-15", key2[1])
	if err != nil {
		klog.V(1).Info("Unable to parse timestamp" + key2[1])
		return false
	}
	return t1.Before(t2)
}

func parseSpotData(bucket string, prefix string, projectID string, region string, accessKeyID string, accessKeySecret string) (map[string]*spotInfo, error) {

	if accessKeyID != "" && accessKeySecret != "" { // credentials may exist on the actual AWS node-- if so, use those. If not, override with the service key
		err := os.Setenv(awsAccessKeyIDEnvVar, accessKeyID)
		if err != nil {
			return nil, err
		}
		err = os.Setenv(awsAccessKeySecretEnvVar, accessKeySecret)
		if err != nil {
			return nil, err
		}
	}
	s3Prefix := projectID
	if len(prefix) != 0 {
		s3Prefix = prefix + "/" + s3Prefix
	}

	c := aws.NewConfig().WithRegion(region)

	s := session.Must(session.NewSession(c))
	s3Svc := s3.New(s)
	downloader := s3manager.NewDownloaderWithClient(s3Svc)

	tNow := time.Now()
	tOneDayAgo := tNow.Add(time.Duration(-24) * time.Hour) // Also get files from one day ago to avoid boundary conditions
	ls := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Prefix + "." + tOneDayAgo.Format("2006-01-02")),
	}
	ls2 := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(s3Prefix + "." + tNow.Format("2006-01-02")),
	}
	lso, err := s3Svc.ListObjects(ls)
	if err != nil {
		return nil, err
	}
	lsoLen := len(lso.Contents)
	klog.V(2).Infof("Found %d spot data files from yesterday", lsoLen)
	if lsoLen == 0 {
		klog.V(5).Infof("ListObjects \"s3://%s/%s\" produced no keys", *ls.Bucket, *ls.Prefix)
	}
	lso2, err := s3Svc.ListObjects(ls2)
	if err != nil {
		return nil, err
	}
	lso2Len := len(lso2.Contents)
	klog.V(2).Infof("Found %d spot data files from today", lso2Len)
	if lso2Len == 0 {
		klog.V(5).Infof("ListObjects \"s3://%s/%s\" produced no keys", *ls2.Bucket, *ls2.Prefix)
	}

	var keys []*string
	for _, obj := range lso.Contents {
		keys = append(keys, obj.Key)
	}
	for _, obj := range lso2.Contents {
		keys = append(keys, obj.Key)
	}

	versionRx := regexp.MustCompile("^#Version: (\\d+)\\.\\d+$")
	header, err := csvutil.Header(spotInfo{}, "csv")
	if err != nil {
		return nil, err
	}
	fieldsPerRecord := len(header)

	spots := make(map[string]*spotInfo)
	for _, key := range keys {
		getObj := &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    key,
		}

		buf := aws.NewWriteAtBuffer([]byte{})
		_, err := downloader.Download(buf, getObj)
		if err != nil {
			return nil, err
		}

		r := bytes.NewReader(buf.Bytes())

		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}

		csvReader := csv.NewReader(gr)
		csvReader.Comma = '\t'
		csvReader.FieldsPerRecord = fieldsPerRecord

		dec, err := csvutil.NewDecoder(csvReader, header...)
		if err != nil {
			return nil, err
		}

		var foundVersion string
		for {
			spot := spotInfo{}
			err := dec.Decode(&spot)
			csvParseErr, isCsvParseErr := err.(*csv.ParseError)
			if err == io.EOF {
				break
			} else if err == csvutil.ErrFieldCount || (isCsvParseErr && csvParseErr.Err == csv.ErrFieldCount) {
				rec := dec.Record()
				// the first two "Record()" will be the comment lines
				// and they show up as len() == 1
				// the first of which is "#Version"
				// the second of which is "#Fields: "
				if len(rec) != 1 {
					klog.V(2).Infof("Expected %d spot info fields but received %d: %s", fieldsPerRecord, len(rec), rec)
					continue
				}
				if len(foundVersion) == 0 {
					spotFeedVersion := rec[0]
					klog.V(3).Infof("Spot feed version is \"%s\"", spotFeedVersion)
					matches := versionRx.FindStringSubmatch(spotFeedVersion)
					if matches != nil {
						foundVersion = matches[1]
						if foundVersion != supportedSpotFeedVersion {
							klog.V(2).Infof("Unsupported spot info feed version: wanted \"%s\" got \"%s\"", supportedSpotFeedVersion, foundVersion)
							break
						}
					}
					continue
				} else if strings.Index(rec[0], "#") == 0 {
					continue
				} else {
					klog.V(3).Infof("skipping non-TSV line: %s", rec)
					continue
				}
			} else if err != nil {
				klog.V(2).Infof("Error during spot info decode: %+v", err)
				continue
			}

			klog.V(3).Infof("Found spot info %+v", spot)
			spots[spot.InstanceID] = &spot
		}
		gr.Close()
	}
	return spots, nil
}
