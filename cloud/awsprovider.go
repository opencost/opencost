package cloud

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/ec2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// AWS represents an Amazon Provider
type AWS struct {
	Pricing          map[string]*AWSProductTerms
	ValidPricingKeys map[string]bool
	Clientset        *kubernetes.Clientset
	BaseCPUPrice     string
	BaseSpotCPUPrice string
	BaseSpotRAMPrice string
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
}

// OnDemandRateCode is appended to an node sku
const OnDemandRateCode = ".JRTCKXETXF"

// ReservedRateCode is appended to a node sku
const ReservedRateCode = ".38NPMPTW36"

// HourlyRateCode is appended to a node sku
const HourlyRateCode = ".6YS6EN2CT7"

// KubeAttrConversion maps the k8s labels for region to an aws region
func (aws *AWS) KubeAttrConversion(location, instanceType, operatingSystem string) string {
	locationToRegion := map[string]string{
		"US East (Ohio)":             "us-east-2",
		"US East (N. Virginia)":      "us-east-1",
		"US West (N. California)":    "us-west-1",
		"US West (Oregon)":           "us-west-2",
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
		"South America (São Paulo)":  "sa-east-1",
		"AWS GovCloud (US-East)":     "us-gov-east-1",
		"AWS GovCloud (US)":          "us-gov-west-1",
	}

	operatingSystem = strings.ToLower(operatingSystem)

	region := locationToRegion[location]
	return region + "," + instanceType + "," + operatingSystem
}

// GetKey maps node labels to information needed to retrieve pricing data
func (aws *AWS) GetKey(labels map[string]string) string {
	instanceType := labels["beta.kubernetes.io/instance-type"]
	operatingSystem := labels["beta.kubernetes.io/os"]
	region := labels["failure-domain.beta.kubernetes.io/region"]
	if l, ok := labels["lifecycle"]; ok && l == "EC2Spot" {
		usageType := "preemptible"
		return region + "," + instanceType + "," + operatingSystem + "," + usageType
	}
	return region + "," + instanceType + "," + operatingSystem
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

	nodeList, err := aws.Clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	inputkeys := make(map[string]bool)
	for _, n := range nodeList.Items {
		labels := n.GetObjectMeta().GetLabels()
		key := aws.GetKey(labels)
		inputkeys[key] = true
	}

	aws.Pricing = make(map[string]*AWSProductTerms)
	aws.ValidPricingKeys = make(map[string]bool)
	skusToKeys := make(map[string]string)

	pricingURL := "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"
	log.Printf("starting download of \"%s\", which is quite large ...", pricingURL)
	resp, err := http.Get(pricingURL)
	if err != nil {
		log.Printf("Bogus fetch of \"%s\": %v", pricingURL, err)
		return err
	}

	dec := json.NewDecoder(resp.Body)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			log.Printf("done loading \"%s\"\n", pricingURL)
			break
		}
		if t == "products" {
			dec.Token() //{
			for dec.More() {
				dec.Token() // the sku token
				product := &AWSProduct{}
				err := dec.Decode(&product)

				if err != nil {
					log.Printf("Error parsing response from \"%s\": %v", pricingURL, err.Error())
					break
				}
				if product.Attributes.PreInstalledSw == "NA" &&
					(strings.HasPrefix(product.Attributes.UsageType, "BoxUsage") || strings.Contains(product.Attributes.UsageType, "-BoxUsage")) {
					key := aws.KubeAttrConversion(product.Attributes.Location, product.Attributes.InstanceType, product.Attributes.OperatingSystem)
					spotKey := key + ",preemptible"
					if inputkeys[key] || inputkeys[spotKey] { // Just grab the sku even if spot, and change the price later.
						aws.Pricing[key] = &AWSProductTerms{
							Sku:     product.Sku,
							Memory:  product.Attributes.Memory,
							Storage: product.Attributes.Storage,
							VCpu:    product.Attributes.VCpu,
						}
						skusToKeys[product.Sku] = key
					}
					aws.ValidPricingKeys[key] = true
					aws.ValidPricingKeys[spotKey] = true
				}
			}
		}
		if t == "terms" {
			dec.Token()
			termType, _ := dec.Token()
			if termType == "OnDemand" {
				dec.Token()
				for dec.More() {
					sku, _ := dec.Token()
					dec.Token()
					skuOnDemand, _ := dec.Token()
					offerTerm := &AWSOfferTerm{}
					err := dec.Decode(&offerTerm)
					if err != nil {
						log.Printf("Error decoding AWS Offer Term: " + err.Error())
					}
					if sku.(string)+OnDemandRateCode == skuOnDemand {
						key, ok := skusToKeys[sku.(string)]
						if ok {
							aws.Pricing[key].OnDemand = offerTerm
						}
					}
					dec.Token()
				}
				dec.Token()
			}
		}
	}

	if err != nil {
		return err
	}
	c, err := GetDefaultPricingData("default.json")
	if err != nil {
		log.Printf("Error downloading default pricing data: %s", err.Error())
	}
	aws.BaseCPUPrice = c.CPU
	aws.BaseSpotCPUPrice = c.SpotCPU
	aws.BaseSpotRAMPrice = c.SpotRAM
	return nil
}

// AllNodePricing returns all the billing data fetched.
func (aws *AWS) AllNodePricing() (interface{}, error) {
	return aws.Pricing, nil
}

// NodePricing takes in a key from GetKey and returns a Node object for use in building the cost model.
func (aws *AWS) NodePricing(key string) (*Node, error) {
	//return json.Marshal(aws.Pricing[key])
	usageType := "ondemand"
	if aws.isPreemptible(key) {
		usageType = "preemptible"
	}
	terms, ok := aws.Pricing[key]
	if ok {
		if aws.isPreemptible(key) {
			return &Node{
				VCPU:         terms.VCpu,
				VCPUCost:     aws.BaseSpotCPUPrice,
				RAM:          terms.Memory,
				RAMCost:      aws.BaseSpotRAMPrice,
				Storage:      terms.Storage,
				BaseCPUPrice: aws.BaseCPUPrice,
				UsageType:    usageType,
			}, nil
		}
		cost := terms.OnDemand.PriceDimensions[terms.Sku+OnDemandRateCode+HourlyRateCode].PricePerUnit.USD
		return &Node{
			Cost:         cost,
			VCPU:         terms.VCpu,
			RAM:          terms.Memory,
			Storage:      terms.Storage,
			BaseCPUPrice: aws.BaseCPUPrice,
			UsageType:    usageType,
		}, nil
	} else if _, ok := aws.ValidPricingKeys[key]; ok {
		err := aws.DownloadPricingData()
		if err != nil {
			return nil, err
		}
		terms := aws.Pricing[key]
		if aws.isPreemptible(key) {
			return &Node{
				VCPU:         terms.VCpu,
				VCPUCost:     aws.BaseSpotCPUPrice,
				RAM:          terms.Memory,
				RAMCost:      aws.BaseSpotRAMPrice,
				Storage:      terms.Storage,
				BaseCPUPrice: aws.BaseCPUPrice,
				UsageType:    usageType,
			}, nil
		}
		cost := terms.OnDemand.PriceDimensions[terms.Sku+OnDemandRateCode+HourlyRateCode].PricePerUnit.USD
		return &Node{
			Cost:         cost,
			VCPU:         terms.VCpu,
			RAM:          terms.Memory,
			Storage:      terms.Storage,
			BaseCPUPrice: aws.BaseCPUPrice,
			UsageType:    usageType,
		}, nil
	} else {
		return nil, fmt.Errorf("Invalid Pricing Key \"%s\"", key)
	}
}

// ClusterName returns an object that represents the cluster. TODO: actually return the name of the cluster. Blocked on cluster federation.
func (*AWS) ClusterName() ([]byte, error) {

	attribute := "AWS Cluster #1"

	m := make(map[string]string)
	m["name"] = attribute
	m["provider"] = "AWS"
	return json.Marshal(m)
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
		json.Unmarshal([]byte(byteValue), &result)
		os.Setenv("AWS_ACCESS_KEY_ID", result["access_key_ID"])
		os.Setenv("AWS_SECRET_ACCESS_KEY", result["secret_access_key"])
	} else if os.IsNotExist(err) {
		log.Print("Using Default Credentials")
	} else {
		return nil, err
	}
	defer jsonFile.Close()
	clusterConfig, err := os.Open("/var/configs/cluster.json")
	if err != nil {
		return nil, err
	}
	defer clusterConfig.Close()
	bytes, _ := ioutil.ReadAll(clusterConfig)
	var clusterConf map[string]string
	json.Unmarshal([]byte(bytes), &clusterConf)
	region := aws.String(clusterConf["region"])
	c := &aws.Config{
		Region:      region,
		Credentials: credentials.NewEnvCredentials(),
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

// QuerySQL can query a properly configured Athena database.
// Used to fetch billing data.
// Requires a json config in /var/configs with key region, output, and database.
func (*AWS) QuerySQL(query string) ([]byte, error) {
	jsonFile, err := os.Open("/var/configs/key.json")
	if err == nil {
		byteValue, _ := ioutil.ReadAll(jsonFile)
		var result map[string]string
		json.Unmarshal([]byte(byteValue), &result)
		os.Setenv("AWS_ACCESS_KEY_ID", result["access_key_ID"])
		os.Setenv("AWS_SECRET_ACCESS_KEY", result["secret_access_key"])
	} else if os.IsNotExist(err) {
		log.Print("Using Default Credentials")
	} else {
		return nil, err
	}
	defer jsonFile.Close()
	athenaConfigs, err := os.Open("/var/configs/athena.json")
	if err != nil {
		return nil, err
	}
	defer athenaConfigs.Close()
	bytes, _ := ioutil.ReadAll(athenaConfigs)
	var athenaConf map[string]string
	json.Unmarshal([]byte(bytes), &athenaConf)
	region := aws.String(athenaConf["region"])
	resultsBucket := athenaConf["output"]
	database := athenaConf["database"]

	c := &aws.Config{
		Region:      region,
		Credentials: credentials.NewEnvCredentials(),
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

	log.Println("StartQueryExecution result:")
	log.Println(res.GoString())

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
		bytes, err := json.Marshal(op.ResultSet)
		if err != nil {
			return nil, err
		}

		return bytes, nil
	}
	return nil, fmt.Errorf("Error getting query results : %s", *qrop.QueryExecution.Status.State)

}
