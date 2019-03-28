package cloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/ec2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Provider interface {
	ClusterName() ([]byte, error)
	AddServiceKey(url.Values) error
	GetDisks() ([]byte, error)
	NodePricing(string) (*Node, error)
	AllNodePricing() (interface{}, error)
	DownloadPricingData() error
	GetKey(map[string]string) string

	QuerySQL(string) ([]byte, error)
}

type Node struct {
	Cost        string
	VCPU        string
	VCPUCost    string
	RAM         string
	RAMCost     string
	Storage     string
	StorageCost string
}

func NewProvider(clientset *kubernetes.Clientset, apiKey string) (Provider, error) {
	if metadata.OnGCE() {
		log.Printf("ON GCP AND KEY IS: %s", apiKey)
		return &GCP{
			Clientset: clientset,
			ApiKey:    apiKey,
		}, nil
	} else {
		return &AWS{
			Clientset: clientset,
		}, nil
	}
}

type userAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

type GCP struct {
	Pricing   map[string]*GCPPricing
	Clientset *kubernetes.Clientset
	ApiKey    string
}

func (*GCP) QuerySQL(query string) ([]byte, error) {
	return nil, nil
}

func (*GCP) ClusterName() ([]byte, error) {
	metadataClient := metadata.NewClient(&http.Client{Transport: userAgentTransport{
		userAgent: "kubecost",
		base:      http.DefaultTransport,
	}})

	attribute, err := metadataClient.InstanceAttributeValue("cluster-name")
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	m["name"] = attribute
	m["provider"] = "GCP"
	return json.Marshal(m)
}

func (*GCP) AddServiceKey(formValues url.Values) error {
	key := formValues.Get("key")
	k := []byte(key)
	return ioutil.WriteFile("/var/configs/key.json", k, 0644)
}

func (*GCP) GetDisks() ([]byte, error) {
	// metadata API setup
	metadataClient := metadata.NewClient(&http.Client{Transport: userAgentTransport{
		userAgent: "kubecost",
		base:      http.DefaultTransport,
	}})
	projID, err := metadataClient.ProjectID()
	if err != nil {
		return nil, err
	}

	client, err := google.DefaultClient(oauth2.NoContext,
		"https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		return nil, err
	}
	svc, err := compute.New(client)
	if err != nil {
		return nil, err
	}
	res, err := svc.Disks.AggregatedList(projID).Do()

	if err != nil {
		return nil, err
	}
	return json.Marshal(res)

}

type GCPPricing struct {
	Name                string           `json:"name"`
	SKUID               string           `json:"skuId"`
	Description         string           `json:"description"`
	Category            *GCPResourceInfo `json:"category"`
	ServiceRegions      []string         `json:"serviceRegions"`
	PricingInfo         []*PricingInfo   `json:"pricingInfo"`
	ServiceProviderName string           `json:"serviceProviderName"`
	Node                *Node            `json:"node"`
}

type PricingInfo struct {
	Summary                string             `json:"summary"`
	PricingExpression      *PricingExpression `json:"pricingExpression"`
	CurrencyConversionRate int                `json:"currencyConversionRate"`
	EffectiveTime          string             `json:""`
}

type PricingExpression struct {
	UsageUnit                string         `json:"usageUnit"`
	UsageUnitDescription     string         `json:"usageUnitDescription"`
	BaseUnit                 string         `json:"baseUnit"`
	BaseUnitConversionFactor int64          `json:"-"`
	DisplayQuantity          int            `json:"displayQuantity"`
	TieredRates              []*TieredRates `json:"tieredRates"`
}

type TieredRates struct {
	StartUsageAmount int            `json:"startUsageAmount"`
	UnitPrice        *UnitPriceInfo `json:"unitPrice"`
}

type UnitPriceInfo struct {
	CurrencyCode string  `json:"currencyCode"`
	Units        string  `json:"units"`
	Nanos        float64 `json:"nanos"`
}

type GCPResourceInfo struct {
	ServiceDisplayName string `json:"serviceDisplayName"`
	ResourceFamily     string `json:"resourceFamily"`
	ResourceGroup      string `json:"resourceGroup"`
	UsageType          string `json:"usageType"`
}

func (gcp *GCP) parsePage(r io.Reader, inputKeys map[string]bool) (map[string]*GCPPricing, string) {
	gcpPricingList := make(map[string]*GCPPricing)
	var nextPageToken string
	dec := json.NewDecoder(r)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		//fmt.Printf("%v  \n", t)
		if t == "skus" {
			dec.Token() // [
			for dec.More() {

				product := &GCPPricing{}
				err := dec.Decode(&product)
				if err != nil {
					fmt.Printf("Error: " + err.Error())
					break
				}
				usageType := strings.ToLower(product.Category.UsageType)
				instanceType := strings.ToLower(product.Category.ResourceGroup)

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "CUSTOM") {
					instanceType = "custom"
				}

				for _, sr := range product.ServiceRegions {
					region := sr

					candidateKey := region + "," + instanceType + "," + usageType
					if _, ok := inputKeys[candidateKey]; ok {
						lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
						var nanos float64
						if len(product.PricingInfo) > 0 {
							nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
						} else {
							continue
						}

						hourlyPrice := nanos * math.Pow10(-9)
						if hourlyPrice == 0 {
							continue
						} else if strings.Contains(strings.ToUpper(product.Description), "RAM") {
							if _, ok := gcpPricingList[candidateKey]; ok {
								gcpPricingList[candidateKey].Node.RAMCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
							} else {
								product.Node = &Node{
									RAMCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
								}
								log.Printf("NODE: %v", product.Node)
								gcpPricingList[candidateKey] = product
							}
							break
						} else {
							if _, ok := gcpPricingList[candidateKey]; ok {
								gcpPricingList[candidateKey].Node.VCPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
							} else {
								product.Node = &Node{
									VCPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
								}
								log.Printf("NODE: %v", product.Node)
								gcpPricingList[candidateKey] = product
							}
							break
						}
					}
				}
			}
		}
		if t == "nextPageToken" {
			pageToken, err := dec.Token()
			if err != nil {
				log.Printf("Error parsing nextpage token: " + err.Error())
				break
			}
			if pageToken.(string) != "" {
				nextPageToken = pageToken.(string)
			} else {
				nextPageToken = "done"
			}
		}
	}
	return gcpPricingList, nextPageToken
}

func (gcp *GCP) parsePages(inputKeys map[string]bool) (map[string]*GCPPricing, error) {
	var pages []map[string]*GCPPricing
	url := "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus?key=" + gcp.ApiKey //AIzaSyDXQPG_MHUEy9neR7stolq6l0ujXmjJlvk
	log.Printf("URL: %s", url)
	var parsePagesHelper func(string) error
	parsePagesHelper = func(pageToken string) error {
		if pageToken == "done" {
			return nil
		} else if pageToken != "" {
			url = url + "&pageToken=" + pageToken
		}
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		page, token := gcp.parsePage(resp.Body, inputKeys)
		pages = append(pages, page)
		return parsePagesHelper(token)
	}
	err := parsePagesHelper("")
	returnPages := make(map[string]*GCPPricing)
	for _, page := range pages {
		for k, v := range page {
			returnPages[k] = v
		}
	}
	return returnPages, err
}

func (gcp *GCP) DownloadPricingData() error {

	nodeList, err := gcp.Clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	inputkeys := make(map[string]bool)

	for _, n := range nodeList.Items {
		labels := n.GetObjectMeta().GetLabels()
		key := gcp.GetKey(labels)
		inputkeys[key] = true
	}

	pages, err := gcp.parsePages(inputkeys)

	if err != nil {
		return err
	}
	gcp.Pricing = pages
	return nil
}

func (gcp *GCP) GetKey(labels map[string]string) string {

	instanceType := strings.ToLower(strings.Join(strings.Split(labels["beta.kubernetes.io/instance-type"], "-")[:2], ""))
	if instanceType == "n1highmem" || instanceType == "n1highcpu" {
		instanceType = "n1standard" // These are priced the same. TODO: support n1ultrahighmem
	} else if strings.HasPrefix(instanceType, "custom") {
		instanceType = "custom" // The suffix of custom does not matter
	}
	region := strings.ToLower(labels["failure-domain.beta.kubernetes.io/region"])
	var usageType string
	if t, ok := labels["cloud.google.com/gke-preemptible"]; ok && t == "true" {
		usageType = "preemptible"
	} else {
		usageType = "ondemand"
	}
	return region + "," + instanceType + "," + usageType
}

func (gcp *GCP) AllNodePricing() (interface{}, error) {
	return gcp.Pricing, nil
}

func (gcp *GCP) NodePricing(key string) (*Node, error) {
	if n, ok := gcp.Pricing[key]; ok {
		return n.Node, nil
	} else {
		log.Printf("Warning: no pricing data found for %s", key)
		return nil, fmt.Errorf("Warning: no pricing data found for %s", key)
	}
}

type AWS struct {
	Pricing          map[string]*AWSProductTerms
	ValidPricingKeys map[string]bool
	Clientset        *kubernetes.Clientset
}

type AWSPricing struct {
	Products map[string]*AWSProduct `json:"products"`
	Terms    AWSPricingTerms        `json:"terms"`
}

type AWSProduct struct {
	Sku        string               `json:"sku"`
	Attributes AWSProductAttributes `json:"attributes"`
}

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

type AWSPricingTerms struct {
	OnDemand map[string]map[string]*AWSOfferTerm `json:"OnDemand"`
	Reserved map[string]map[string]*AWSOfferTerm `json:"Reserved"`
}

type AWSOfferTerm struct {
	Sku             string                  `json:"sku"`
	PriceDimensions map[string]*AWSRateCode `json:"priceDimensions"`
}

type AWSRateCode struct {
	Unit         string          `json:"unit"`
	PricePerUnit AWSCurrencyCode `json:"pricePerUnit"`
}

type AWSCurrencyCode struct {
	USD string `json:"USD"`
}

type AWSProductTerms struct {
	Sku      string        `json:"sku"`
	OnDemand *AWSOfferTerm `json:"OnDemand"`
	Reserved *AWSOfferTerm `json:"Reserved"`
	Memory   string        `json:"memory"`
	Storage  string        `json:"storage"`
	VCpu     string        `json:"vcpu"`
}

const OnDemandRateCode = ".JRTCKXETXF"
const ReservedRateCode = ".38NPMPTW36"
const HourlyRateCode = ".6YS6EN2CT7"

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

func (aws *AWS) GetKey(labels map[string]string) string {
	instanceType := labels["beta.kubernetes.io/instance-type"]
	operatingSystem := labels["beta.kubernetes.io/os"]
	region := labels["failure-domain.beta.kubernetes.io/region"]
	return region + "," + instanceType + "," + operatingSystem
}

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

	resp, err := http.Get("https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json")
	if err != nil {
		return err
	}

	dec := json.NewDecoder(resp.Body)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			fmt.Printf("done \n")
			break
		}
		if t == "products" {
			dec.Token() //{
			for dec.More() {
				dec.Token() // the sku token
				product := &AWSProduct{}
				err := dec.Decode(&product)

				if err != nil {
					fmt.Printf("Error: " + err.Error())
					break
				}
				if product.Attributes.PreInstalledSw == "NA" &&
					(strings.HasPrefix(product.Attributes.UsageType, "BoxUsage") || strings.Contains(product.Attributes.UsageType, "-BoxUsage")) {
					key := aws.KubeAttrConversion(product.Attributes.Location, product.Attributes.InstanceType, product.Attributes.OperatingSystem)
					if inputkeys[key] {
						aws.Pricing[key] = &AWSProductTerms{
							Sku:     product.Sku,
							Memory:  product.Attributes.Memory,
							Storage: product.Attributes.Storage,
							VCpu:    product.Attributes.VCpu,
						}
						skusToKeys[product.Sku] = key
					}
					aws.ValidPricingKeys[key] = true
				}
			}
		}
		if t == "terms" {
			dec.Token()
			termType, _ := dec.Token()
			if termType == "OnDemand" {
				dec.Token() // {
				for dec.More() {
					sku, _ := dec.Token()
					dec.Token()
					skuOnDemand, _ := dec.Token()
					offerTerm := &AWSOfferTerm{}
					err := dec.Decode(&offerTerm)
					if err != nil {
						fmt.Printf("Error: " + err.Error())
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
	log.Printf("Body Parsed")
	return nil
}

func (aws *AWS) AllNodePricing() (interface{}, error) {
	return aws.Pricing, nil
}

func (aws *AWS) NodePricing(key string) (*Node, error) {
	//return json.Marshal(aws.Pricing[key])
	terms, ok := aws.Pricing[key]
	if ok {
		cost := terms.OnDemand.PriceDimensions[terms.Sku+OnDemandRateCode+HourlyRateCode].PricePerUnit.USD
		return &Node{
			Cost:    cost,
			VCPU:    terms.VCpu,
			RAM:     terms.Memory,
			Storage: terms.Storage,
		}, nil
	} else if _, ok := aws.ValidPricingKeys[key]; ok {
		err := aws.DownloadPricingData()
		if err != nil {
			return nil, err
		}
		terms := aws.Pricing[key]
		cost := terms.OnDemand.PriceDimensions[terms.Sku+OnDemandRateCode+HourlyRateCode].PricePerUnit.USD
		return &Node{
			Cost:    cost,
			VCPU:    terms.VCpu,
			RAM:     terms.Memory,
			Storage: terms.Storage,
		}, nil
	} else {
		return nil, errors.New("Invalid Pricing Key: " + key + "\n")
	}
}

func (*AWS) ClusterName() ([]byte, error) {

	attribute := "AWS Cluster #1"

	m := make(map[string]string)
	m["name"] = attribute
	m["provider"] = "AWS"
	return json.Marshal(m)
}

func (*AWS) AddServiceKey(formValues url.Values) error {
	keyID := formValues.Get("access_key_ID")
	key := formValues.Get("secret_access_key")
	m := make(map[string]string)
	m["access_key_ID"] = keyID
	m["secret_access_key"] = key
	json, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return ioutil.WriteFile("/var/configs/key.json", json, 0644)
}

func (*AWS) GetDisks() ([]byte, error) {
	jsonFile, err := os.Open("/var/configs/key.json")
	if err == nil {
		byteValue, _ := ioutil.ReadAll(jsonFile)
		var result map[string]string
		json.Unmarshal([]byte(byteValue), &result)
		os.Setenv("AWS_ACCESS_KEY_ID", result["access_key_ID"])
		os.Setenv("AWS_SECRET_ACCESS_KEY", result["secret_access_key"])
	} else if os.IsNotExist(err) {
		log.Printf("Using Default Credentials")
	} else {
		return nil, err
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var result map[string]string
	json.Unmarshal([]byte(byteValue), &result)
	os.Setenv("AWS_ACCESS_KEY_ID", result["access_key_ID"])
	os.Setenv("AWS_SECRET_ACCESS_KEY", result["secret_access_key"])
	c := &aws.Config{
		Region:      aws.String("us-east-1"),
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

func (*AWS) QuerySQL(query string) ([]byte, error) {
	jsonFile, err := os.Open("/var/configs/key.json")
	if err == nil {
		byteValue, _ := ioutil.ReadAll(jsonFile)
		var result map[string]string
		json.Unmarshal([]byte(byteValue), &result)
		os.Setenv("AWS_ACCESS_KEY_ID", result["access_key_ID"])
		os.Setenv("AWS_SECRET_ACCESS_KEY", result["secret_access_key"])
	} else if os.IsNotExist(err) {
		log.Printf("Using Default Credentials")
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

	fmt.Println("StartQueryExecution result:")
	fmt.Println(res.GoString())

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
	} else {
		return nil, fmt.Errorf("Error getting query results : %s", *qrop.QueryExecution.Status.State)
	}
}
