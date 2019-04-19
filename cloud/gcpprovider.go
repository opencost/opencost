package cloud

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/klog"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type userAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

// GCP implements a provider interface for GCP
type GCP struct {
	Pricing      map[string]*GCPPricing
	Clientset    *kubernetes.Clientset
	APIKey       string
	BaseCPUPrice string
}

// QuerySQL should query BigQuery for billing data for out of cluster costs. TODO: Implement.
func (*GCP) QuerySQL(query string) ([]byte, error) {
	return nil, nil
}

// ClusterName returns the name of a GKE cluster, as provided by metadata.
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

// AddServiceKey adds the service key as required for GetDisks
func (*GCP) AddServiceKey(formValues url.Values) error {
	key := formValues.Get("key")
	k := []byte(key)
	return ioutil.WriteFile("/var/configs/key.json", k, 0644)
}

// GetDisks returns the GCP disks backing PVs. Useful because sometimes k8s will not clean up PVs correctly. Requires a json config in /var/configs with key region.
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

// GCPPricing represents GCP pricing data for a SKU
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

// PricingInfo contains metadata about a cost.
type PricingInfo struct {
	Summary                string             `json:"summary"`
	PricingExpression      *PricingExpression `json:"pricingExpression"`
	CurrencyConversionRate int                `json:"currencyConversionRate"`
	EffectiveTime          string             `json:""`
}

// PricingExpression contains metadata about a cost.
type PricingExpression struct {
	UsageUnit                string         `json:"usageUnit"`
	UsageUnitDescription     string         `json:"usageUnitDescription"`
	BaseUnit                 string         `json:"baseUnit"`
	BaseUnitConversionFactor int64          `json:"-"`
	DisplayQuantity          int            `json:"displayQuantity"`
	TieredRates              []*TieredRates `json:"tieredRates"`
}

// TieredRates contain data about variable pricing.
type TieredRates struct {
	StartUsageAmount int            `json:"startUsageAmount"`
	UnitPrice        *UnitPriceInfo `json:"unitPrice"`
}

// UnitPriceInfo contains data about the actual price being charged.
type UnitPriceInfo struct {
	CurrencyCode string  `json:"currencyCode"`
	Units        string  `json:"units"`
	Nanos        float64 `json:"nanos"`
}

// GCPResourceInfo contains metadata about the node.
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
				// instance.toLowerCase() === “f1micro”
				var partialCPU float64
				if strings.ToLower(instanceType) == "f1micro" {
					partialCPU = 0.2
				} else if strings.ToLower(instanceType) == "g1small" {
					partialCPU = 0.5
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
							if instanceType == "custom" {
								klog.V(2).Infof("RAM custom sku is: " + product.Name)
							}
							if _, ok := gcpPricingList[candidateKey]; ok {
								gcpPricingList[candidateKey].Node.RAMCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
							} else {
								product.Node = &Node{
									RAMCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
								}
								if partialCPU != 0 {
									product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
								}
								product.Node.UsageType = usageType
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
								if partialCPU != 0 {
									product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
								}
								product.Node.UsageType = usageType
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
				klog.V(2).Infof("Error parsing nextpage token: " + err.Error())
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
	url := "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus?key=" + gcp.APIKey //AIzaSyDXQPG_MHUEy9neR7stolq6l0ujXmjJlvk
	klog.V(2).Infof("URL: %s", url)
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
			if val, ok := returnPages[k]; ok { //keys may need to be merged
				if val.Node.RAMCost != "" && val.Node.VCPUCost == "" {
					val.Node.VCPUCost = v.Node.VCPUCost
				} else if val.Node.VCPUCost != "" && val.Node.RAMCost == "" {
					val.Node.RAMCost = v.Node.RAMCost
				} else {
					returnPages[k] = v
				}
			} else {
				returnPages[k] = v
			}
		}
	}
	return returnPages, err
}

// DownloadPricingData fetches data from the GCP Pricing API. Requires a key-- a kubecost key is provided for quickstart, but should be replaced by a users.
func (gcp *GCP) DownloadPricingData() error {

	nodeList, err := gcp.Clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	inputkeys := make(map[string]bool)

	for _, n := range nodeList.Items {
		labels := n.GetObjectMeta().GetLabels()
		key := gcp.GetKey(labels)
		inputkeys[key.Features()] = true
	}

	pages, err := gcp.parsePages(inputkeys)

	if err != nil {
		return err
	}
	gcp.Pricing = pages
	c, err := GetDefaultPricingData("default.json")
	if err != nil {
		klog.V(2).Infof("Error downloading default pricing data: %s", err.Error())
	}
	gcp.BaseCPUPrice = c.CPU

	return nil
}

type gcpKey struct {
	Labels map[string]string
}

func (gcp *GCP) GetKey(labels map[string]string) Key {
	return &gcpKey{
		Labels: labels,
	}
}

func (gcp *gcpKey) ID() string {
	return ""
}

// GetKey maps node labels to information needed to retrieve pricing data
func (gcp *gcpKey) Features() string {

	instanceType := strings.ToLower(strings.Join(strings.Split(gcp.Labels["beta.kubernetes.io/instance-type"], "-")[:2], ""))
	if instanceType == "n1highmem" || instanceType == "n1highcpu" {
		instanceType = "n1standard" // These are priced the same. TODO: support n1ultrahighmem
	} else if strings.HasPrefix(instanceType, "custom") {
		instanceType = "custom" // The suffix of custom does not matter
	}
	region := strings.ToLower(gcp.Labels["failure-domain.beta.kubernetes.io/region"])
	var usageType string
	if t, ok := gcp.Labels["cloud.google.com/gke-preemptible"]; ok && t == "true" {
		usageType = "preemptible"
	} else {
		usageType = "ondemand"
	}
	return region + "," + instanceType + "," + usageType
}

// AllNodePricing returns the GCP pricing objects stored
func (gcp *GCP) AllNodePricing() (interface{}, error) {
	return gcp.Pricing, nil
}

// NodePricing returns GCP pricing data for a single node
func (gcp *GCP) NodePricing(key Key) (*Node, error) {
	if n, ok := gcp.Pricing[key.Features()]; ok {
		klog.V(2).Infof("Returning pricing for node %s: %+v from SKU %s", key, n.Node, n.Name)
		n.Node.BaseCPUPrice = gcp.BaseCPUPrice
		return n.Node, nil
	}
	klog.V(1).Infof("Warning: no pricing data found for %s", key)
	return nil, fmt.Errorf("Warning: no pricing data found for %s", key)
}
