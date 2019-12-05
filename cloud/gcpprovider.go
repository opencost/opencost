package cloud

import (
	"context"
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

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"
	"github.com/kubecost/cost-model/clustercache"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/iterator"
	v1 "k8s.io/api/core/v1"
)

const GKE_GPU_TAG = "cloud.google.com/gke-accelerator"
const BigqueryUpdateType = "bigqueryupdate"

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
	Pricing                 map[string]*GCPPricing
	Clientset               clustercache.ClusterCache
	APIKey                  string
	BaseCPUPrice            string
	ProjectID               string
	BillingDataDataset      string
	DownloadPricingDataLock sync.RWMutex
	ReservedInstances       []*GCPReservedInstance
	*CustomProvider
}

type gcpAllocation struct {
	Aggregator  bigquery.NullString
	Environment bigquery.NullString
	Service     string
	Cost        float64
}

func gcpAllocationToOutOfClusterAllocation(gcpAlloc gcpAllocation) *OutOfClusterAllocation {
	var aggregator string
	if gcpAlloc.Aggregator.Valid {
		aggregator = gcpAlloc.Aggregator.StringVal
	}

	var environment string
	if gcpAlloc.Environment.Valid {
		environment = gcpAlloc.Environment.StringVal
	}

	return &OutOfClusterAllocation{
		Aggregator:  aggregator,
		Environment: environment,
		Service:     gcpAlloc.Service,
		Cost:        gcpAlloc.Cost,
	}
}

func (gcp *GCP) GetLocalStorageQuery(offset string) (string, error) {
	localStorageCost := 0.04 // TODO: Set to the price for the appropriate storage class. It's not trivial to determine the local storage disk type
	return fmt.Sprintf(`sum(sum(container_fs_limit_bytes{device!="tmpfs", id="/"} %s) by (instance, cluster_id)) by (cluster_id) / 1024 / 1024 / 1024 * %f`, offset, localStorageCost), nil
}

func (gcp *GCP) GetConfig() (*CustomPricing, error) {
	c, err := GetDefaultPricingData("gcp.json")
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "30%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	return c, nil
}

type BigQueryConfig struct {
	ProjectID          string            `json:"projectID"`
	BillingDataDataset string            `json:"billingDataDataset"`
	Key                map[string]string `json:"key"`
}

func (gcp *GCP) GetManagementPlatform() (string, error) {
	nodes := gcp.Clientset.GetAllNodes()

	if len(nodes) > 0 {
		n := nodes[0]
		version := n.Status.NodeInfo.KubeletVersion
		if strings.Contains(version, "gke") {
			return "gke", nil
		}
	}
	return "", nil
}

func (gcp *GCP) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	c, err := GetDefaultPricingData("gcp.json")
	if err != nil {
		return nil, err
	}
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/models/"
	}
	if updateType == BigqueryUpdateType {
		a := BigQueryConfig{}
		err = json.NewDecoder(r).Decode(&a)
		if err != nil {
			return nil, err
		}

		c.ProjectID = a.ProjectID
		c.BillingDataDataset = a.BillingDataDataset

		j, err := json.Marshal(a.Key)
		if err != nil {
			return nil, err
		}

		keyPath := path + "key.json"
		err = ioutil.WriteFile(keyPath, j, 0644)
		if err != nil {
			return nil, err
		}
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
	remoteEnabled := os.Getenv(remoteEnabled)
	if remoteEnabled == "true" {
		err = UpdateClusterMeta(os.Getenv(clusterIDKey), c.ClusterName)
		if err != nil {
			return nil, err
		}
	}

	configPath := path + "gcp.json"
	err = ioutil.WriteFile(configPath, cj, 0644)
	if err != nil {
		return nil, err
	}

	return c, nil

}

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (gcp *GCP) ExternalAllocations(start string, end string, aggregator string) ([]*OutOfClusterAllocation, error) {
	c, err := GetDefaultPricingData("gcp.json")
	if err != nil {
		return nil, err
	}
	// start, end formatted like: "2019-04-20 00:00:00"
	queryString := fmt.Sprintf(`SELECT
					service,
					labels.key as aggregator,
					labels.value as environment,
					SUM(cost) as cost
					FROM  (SELECT 
							service.description as service,
							labels,
							cost 
						FROM %s
						WHERE usage_start_time >= "%s" AND usage_start_time < "%s")
						LEFT JOIN UNNEST(labels) as labels
						ON labels.key = "%s"
				GROUP BY aggregator, environment, service;`, c.BillingDataDataset, start, end, aggregator) // For example, "billing_data.gcp_billing_export_v1_01AC9F_74CF1D_5565A2"
	klog.V(4).Infof("Querying \"%s\" with : %s", c.ProjectID, queryString)
	return gcp.QuerySQL(queryString)
}

// QuerySQL should query BigQuery for billing data for out of cluster costs.
func (gcp *GCP) QuerySQL(query string) ([]*OutOfClusterAllocation, error) {
	c, err := GetDefaultPricingData("gcp.json")
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, c.ProjectID) // For example, "guestbook-227502"
	if err != nil {
		return nil, err
	}

	q := client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var allocations []*OutOfClusterAllocation
	for {
		var a gcpAllocation
		err := it.Next(&a)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		allocations = append(allocations, gcpAllocationToOutOfClusterAllocation(a))
	}
	return allocations, nil
}

// ClusterName returns the name of a GKE cluster, as provided by metadata.
func (gcp *GCP) ClusterInfo() (map[string]string, error) {
	remote := os.Getenv(remoteEnabled)
	remoteEnabled := false
	if os.Getenv(remote) == "true" {
		remoteEnabled = true
	}
	metadataClient := metadata.NewClient(&http.Client{Transport: userAgentTransport{
		userAgent: "kubecost",
		base:      http.DefaultTransport,
	}})

	attribute, err := metadataClient.InstanceAttributeValue("cluster-name")
	if err != nil {
		return nil, err
	}

	c, err := gcp.GetConfig()
	if err != nil {
		klog.V(1).Infof("Error opening config: %s", err.Error())
	}
	if c.ClusterName != "" {
		attribute = c.ClusterName
	}

	m := make(map[string]string)
	m["name"] = attribute
	m["provider"] = "GCP"
	m["id"] = os.Getenv(clusterIDKey)
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	return m, nil
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
	PV                  *PV              `json:"pv"`
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

func (gcp *GCP) parsePage(r io.Reader, inputKeys map[string]Key, pvKeys map[string]PVKey) (map[string]*GCPPricing, string, error) {
	gcpPricingList := make(map[string]*GCPPricing)
	var nextPageToken string
	dec := json.NewDecoder(r)
	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		if t == "skus" {
			_, err := dec.Token() // consumes [
			if err != nil {
				return nil, "", err
			}
			for dec.More() {

				product := &GCPPricing{}
				err := dec.Decode(&product)
				if err != nil {
					return nil, "", err
				}
				usageType := strings.ToLower(product.Category.UsageType)
				instanceType := strings.ToLower(product.Category.ResourceGroup)

				if instanceType == "ssd" && !strings.Contains(product.Description, "Regional") { // TODO: support regional
					lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
					var nanos float64
					if len(product.PricingInfo) > 0 {
						nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
					} else {
						continue
					}
					hourlyPrice := (nanos * math.Pow10(-9)) / 730

					for _, sr := range product.ServiceRegions {
						region := sr
						candidateKey := region + "," + "ssd"
						if _, ok := pvKeys[candidateKey]; ok {
							product.PV = &PV{
								Cost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
							}
							gcpPricingList[candidateKey] = product
							continue
						}
					}
					continue
				} else if instanceType == "pdstandard" && !strings.Contains(product.Description, "Regional") { // TODO: support regional
					lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
					var nanos float64
					if len(product.PricingInfo) > 0 {
						nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
					} else {
						continue
					}
					hourlyPrice := (nanos * math.Pow10(-9)) / 730
					for _, sr := range product.ServiceRegions {
						region := sr
						candidateKey := region + "," + "pdstandard"
						if _, ok := pvKeys[candidateKey]; ok {
							product.PV = &PV{
								Cost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
							}
							gcpPricingList[candidateKey] = product
							continue
						}
					}
					continue
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "CUSTOM") {
					instanceType = "custom"
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "N2") {
					instanceType = "n2standard"
				}

				/*
					var partialCPU float64
					if strings.ToLower(instanceType) == "f1micro" {
						partialCPU = 0.2
					} else if strings.ToLower(instanceType) == "g1small" {
						partialCPU = 0.5
					}
				*/
				var gpuType string
				provIdRx := regexp.MustCompile("(Nvidia Tesla [^ ]+) ")
				for matchnum, group := range provIdRx.FindStringSubmatch(product.Description) {
					if matchnum == 1 {
						gpuType = strings.ToLower(strings.Join(strings.Split(group, " "), "-"))
						klog.V(4).Info("GPU type found: " + gpuType)
					}
				}

				for _, sr := range product.ServiceRegions {
					region := sr
					candidateKey := region + "," + instanceType + "," + usageType
					candidateKeyGPU := candidateKey + ",gpu"

					if gpuType != "" {
						lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
						var nanos float64
						if len(product.PricingInfo) > 0 {
							nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
						} else {
							continue
						}
						hourlyPrice := nanos * math.Pow10(-9)

						for k, key := range inputKeys {
							if key.GPUType() == gpuType+","+usageType {
								if region == strings.Split(k, ",")[0] {
									klog.V(3).Infof("Matched GPU to node in region \"%s\"", region)
									matchedKey := key.Features()
									if pl, ok := gcpPricingList[matchedKey]; ok {
										pl.Node.GPUName = gpuType
										pl.Node.GPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
										pl.Node.GPU = "1"
									} else {
										product.Node = &Node{
											GPUName: gpuType,
											GPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
											GPU:     "1",
										}
										gcpPricingList[matchedKey] = product
									}
									klog.V(3).Infof("Added data for " + matchedKey)
								}
							}
						}
					} else {
						_, ok := inputKeys[candidateKey]
						_, ok2 := inputKeys[candidateKeyGPU]
						if ok || ok2 {
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
									klog.V(4).Infof("RAM custom sku is: " + product.Name)
								}
								if _, ok := gcpPricingList[candidateKey]; ok {
									gcpPricingList[candidateKey].Node.RAMCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									product = &GCPPricing{}
									product.Node = &Node{
										RAMCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									/*
										if partialCPU != 0 {
											product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
										}
									*/
									product.Node.UsageType = usageType
									gcpPricingList[candidateKey] = product
								}
								if _, ok := gcpPricingList[candidateKeyGPU]; ok {
									klog.V(1).Infof("Adding RAM %f for %s", hourlyPrice, candidateKeyGPU)
									gcpPricingList[candidateKeyGPU].Node.RAMCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									klog.V(1).Infof("Adding RAM %f for %s", hourlyPrice, candidateKeyGPU)
									product = &GCPPricing{}
									product.Node = &Node{
										RAMCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									/*
										if partialCPU != 0 {
											product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
										}
									*/
									product.Node.UsageType = usageType
									gcpPricingList[candidateKeyGPU] = product
								}
								break
							} else {
								if _, ok := gcpPricingList[candidateKey]; ok {
									gcpPricingList[candidateKey].Node.VCPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									product = &GCPPricing{}
									product.Node = &Node{
										VCPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									/*
										if partialCPU != 0 {
											product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
										}
									*/
									product.Node.UsageType = usageType
									gcpPricingList[candidateKey] = product
								}
								if _, ok := gcpPricingList[candidateKeyGPU]; ok {
									gcpPricingList[candidateKeyGPU].Node.VCPUCost = strconv.FormatFloat(hourlyPrice, 'f', -1, 64)
								} else {
									product = &GCPPricing{}
									product.Node = &Node{
										VCPUCost: strconv.FormatFloat(hourlyPrice, 'f', -1, 64),
									}
									/*
										if partialCPU != 0 {
											product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
										}
									*/
									product.Node.UsageType = usageType
									gcpPricingList[candidateKeyGPU] = product
								}
								break
							}
						}
					}
				}
			}
		}
		if t == "nextPageToken" {
			pageToken, err := dec.Token()
			if err != nil {
				klog.V(2).Infof("Error parsing nextpage token: " + err.Error())
				return nil, "", err
			}
			if pageToken.(string) != "" {
				nextPageToken = pageToken.(string)
			} else {
				nextPageToken = "done"
			}
		}
	}
	return gcpPricingList, nextPageToken, nil
}

func (gcp *GCP) parsePages(inputKeys map[string]Key, pvKeys map[string]PVKey) (map[string]*GCPPricing, error) {
	var pages []map[string]*GCPPricing
	url := "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus?key=" + gcp.APIKey
	klog.V(2).Infof("Fetch GCP Billing Data from URL: %s", url)
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
		page, token, err := gcp.parsePage(resp.Body, inputKeys, pvKeys)
		if err != nil {
			return err
		}
		pages = append(pages, page)
		return parsePagesHelper(token)
	}
	err := parsePagesHelper("")
	if err != nil {
		return nil, err
	}
	returnPages := make(map[string]*GCPPricing)
	for _, page := range pages {
		for k, v := range page {
			if val, ok := returnPages[k]; ok { //keys may need to be merged
				if val.Node != nil {
					if val.Node.VCPUCost == "" {
						val.Node.VCPUCost = v.Node.VCPUCost
					}
					if val.Node.RAMCost == "" {
						val.Node.RAMCost = v.Node.RAMCost
					}
					if val.Node.GPUCost == "" {
						val.Node.GPUCost = v.Node.GPUCost
					}
				}
				if val.PV != nil {
					if val.PV.Cost == "" {
						val.PV.Cost = v.PV.Cost
					}
				}
			} else {
				returnPages[k] = v
			}
		}
	}
	klog.V(1).Infof("ALL PAGES: %+v", returnPages)
	for k, v := range returnPages {
		klog.V(1).Infof("Returned Page: %s : %+v", k, v.Node)
	}
	return returnPages, err
}

// DownloadPricingData fetches data from the GCP Pricing API. Requires a key-- a kubecost key is provided for quickstart, but should be replaced by a users.
func (gcp *GCP) DownloadPricingData() error {
	gcp.DownloadPricingDataLock.Lock()
	defer gcp.DownloadPricingDataLock.Unlock()
	c, err := GetDefaultPricingData("gcp.json")
	if err != nil {
		klog.V(2).Infof("Error downloading default pricing data: %s", err.Error())
		return err
	}
	gcp.BaseCPUPrice = c.CPU
	gcp.ProjectID = c.ProjectID
	gcp.BillingDataDataset = c.BillingDataDataset

	nodeList := gcp.Clientset.GetAllNodes()
	inputkeys := make(map[string]Key)

	for _, n := range nodeList {
		labels := n.GetObjectMeta().GetLabels()
		key := gcp.GetKey(labels)
		inputkeys[key.Features()] = key
	}

	pvList := gcp.Clientset.GetAllPersistentVolumes()
	storageClasses := gcp.Clientset.GetAllStorageClasses()
	storageClassMap := make(map[string]map[string]string)
	for _, storageClass := range storageClasses {
		params := storageClass.Parameters
		storageClassMap[storageClass.ObjectMeta.Name] = params
		if storageClass.GetAnnotations()["storageclass.kubernetes.io/is-default-class"] == "true" || storageClass.GetAnnotations()["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			storageClassMap["default"] = params
			storageClassMap[""] = params
		}
	}

	pvkeys := make(map[string]PVKey)
	for _, pv := range pvList {
		params, ok := storageClassMap[pv.Spec.StorageClassName]
		if !ok {
			klog.Infof("Unable to find params for storageClassName %s", pv.Name)
			continue
		}
		key := gcp.GetPVKey(pv, params)
		pvkeys[key.Features()] = key
	}

	reserved, err := gcp.getReservedInstances()
	if err != nil {
		klog.V(1).Infof("Failed to lookup reserved instance data: %s", err.Error())
	} else {
		klog.V(1).Infof("Found %d reserved instances", len(reserved))
		gcp.ReservedInstances = reserved
		for _, r := range reserved {
			klog.V(1).Infof("Reserved: CPU: %d, RAM: %d, Region: %s, Start: %s, End: %s", r.ReservedCPU, r.ReservedRAM, r.Region, r.StartDate.String(), r.EndDate.String())
		}
	}

	pages, err := gcp.parsePages(inputkeys, pvkeys)

	if err != nil {
		return err
	}
	gcp.Pricing = pages
	return nil
}

func (gcp *GCP) PVPricing(pvk PVKey) (*PV, error) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	pricing, ok := gcp.Pricing[pvk.Features()]
	if !ok {
		klog.V(4).Infof("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &PV{}, nil
	}
	return pricing.PV, nil
}

// Stubbed NetworkPricing for GCP. Pull directly from gcp.json for now
func (c *GCP) NetworkPricing() (*Network, error) {
	cpricing, err := GetDefaultPricingData("gcp.json")
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

const (
	GCPReservedInstanceResourceTypeRAM string = "MEMORY"
	GCPReservedInstanceResourceTypeCPU string = "VCPU"
	GCPReservedInstanceStatusActive    string = "ACTIVE"
	GCPReservedInstancePlanOneYear     string = "TWELVE_MONTH"
	GCPReservedInstancePlanThreeYear   string = "THIRTY_SIX_MONTH"
)

type GCPReservedInstancePlan struct {
	Name    string
	CPUCost float64
	RAMCost float64
}

type GCPReservedInstance struct {
	ReservedRAM int64
	ReservedCPU int64
	Plan        *GCPReservedInstancePlan
	StartDate   time.Time
	EndDate     time.Time
	Region      string
}

type ReservedCounter struct {
	RemainingCPU int64
	RemainingRAM int64
	Instance     *GCPReservedInstance
}

func newReservedCounter(instance *GCPReservedInstance) *ReservedCounter {
	return &ReservedCounter{
		RemainingCPU: instance.ReservedCPU,
		RemainingRAM: instance.ReservedRAM,
		Instance:     instance,
	}
}

// Two available Reservation plans for GCP, 1-year and 3-year
var gcpReservedInstancePlans map[string]*GCPReservedInstancePlan = map[string]*GCPReservedInstancePlan{
	GCPReservedInstancePlanOneYear: &GCPReservedInstancePlan{
		Name:    GCPReservedInstancePlanOneYear,
		CPUCost: 0.019915,
		RAMCost: 0.002669,
	},
	GCPReservedInstancePlanThreeYear: &GCPReservedInstancePlan{
		Name:    GCPReservedInstancePlanThreeYear,
		CPUCost: 0.014225,
		RAMCost: 0.001907,
	},
}

func (gcp *GCP) ApplyReservedInstancePricing(nodes map[string]*Node) {
	numReserved := len(gcp.ReservedInstances)

	// Early return if no reserved instance data loaded
	if numReserved == 0 {
		klog.V(1).Infof("[Reserved] No Reserved Instances")
		return
	}

	now := time.Now()

	counters := make(map[string][]*ReservedCounter)
	for _, r := range gcp.ReservedInstances {
		if now.Before(r.StartDate) || now.After(r.EndDate) {
			klog.V(1).Infof("[Reserved] Skipped Reserved Instance due to dates")
			continue
		}

		_, ok := counters[r.Region]
		counter := newReservedCounter(r)
		if !ok {
			counters[r.Region] = []*ReservedCounter{counter}
		} else {
			counters[r.Region] = append(counters[r.Region], counter)
		}
	}

	gcpNodes := make(map[string]*v1.Node)
	currentNodes := gcp.Clientset.GetAllNodes()

	// Create a node name -> node map
	for _, gcpNode := range currentNodes {
		gcpNodes[gcpNode.GetName()] = gcpNode
	}

	// go through all provider nodes using k8s nodes for region
	for nodeName, node := range nodes {
		// Reset reserved allocation to prevent double allocation
		node.Reserved = nil

		kNode, ok := gcpNodes[nodeName]
		if !ok {
			klog.V(1).Infof("[Reserved] Could not find K8s Node with name: %s", nodeName)
			continue
		}

		nodeRegion, ok := kNode.Labels[v1.LabelZoneRegion]
		if !ok {
			klog.V(1).Infof("[Reserved] Could not find node region")
			continue
		}

		reservedCounters, ok := counters[nodeRegion]
		if !ok {
			klog.V(1).Infof("[Reserved] Could not find counters for region: %s", nodeRegion)
			continue
		}

		node.Reserved = &ReservedInstanceData{
			ReservedCPU: 0,
			ReservedRAM: 0,
		}

		for _, reservedCounter := range reservedCounters {
			if reservedCounter.RemainingCPU != 0 {
				nodeCPU, _ := strconv.ParseInt(node.VCPU, 10, 64)
				nodeCPU -= node.Reserved.ReservedCPU
				node.Reserved.CPUCost = reservedCounter.Instance.Plan.CPUCost

				if reservedCounter.RemainingCPU >= nodeCPU {
					reservedCounter.RemainingCPU -= nodeCPU
					node.Reserved.ReservedCPU += nodeCPU
				} else {
					node.Reserved.ReservedCPU += reservedCounter.RemainingCPU
					reservedCounter.RemainingCPU = 0
				}
			}

			if reservedCounter.RemainingRAM != 0 {
				nodeRAMF, _ := strconv.ParseFloat(node.RAMBytes, 64)
				nodeRAM := int64(nodeRAMF)
				nodeRAM -= node.Reserved.ReservedRAM
				node.Reserved.RAMCost = reservedCounter.Instance.Plan.RAMCost

				if reservedCounter.RemainingRAM >= nodeRAM {
					reservedCounter.RemainingRAM -= nodeRAM
					node.Reserved.ReservedRAM += nodeRAM
				} else {
					node.Reserved.ReservedRAM += reservedCounter.RemainingRAM
					reservedCounter.RemainingRAM = 0
				}
			}
		}
	}
}

func (gcp *GCP) getReservedInstances() ([]*GCPReservedInstance, error) {
	var results []*GCPReservedInstance

	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	if err != nil {
		return nil, err
	}

	commitments, err := computeService.RegionCommitments.AggregatedList(gcp.ProjectID).Do()
	if err != nil {
		return nil, err
	}

	for regionKey, commitList := range commitments.Items {
		for _, commit := range commitList.Commitments {
			if commit.Status != GCPReservedInstanceStatusActive {
				continue
			}

			var vcpu int64 = 0
			var ram int64 = 0
			for _, resource := range commit.Resources {
				switch resource.Type {
				case GCPReservedInstanceResourceTypeRAM:
					ram = resource.Amount * 1024 * 1024
				case GCPReservedInstanceResourceTypeCPU:
					vcpu = resource.Amount
				default:
					klog.V(4).Infof("Failed to handle resource type: %s", resource.Type)
				}
			}

			var region string
			regionStr := strings.Split(regionKey, "/")
			if len(regionStr) == 2 {
				region = regionStr[1]
			}

			timeLayout := "2006-01-02T15:04:05Z07:00"
			startTime, err := time.Parse(timeLayout, commit.StartTimestamp)
			if err != nil {
				klog.V(1).Infof("Failed to parse start date: %s", commit.StartTimestamp)
				continue
			}

			endTime, err := time.Parse(timeLayout, commit.EndTimestamp)
			if err != nil {
				klog.V(1).Infof("Failed to parse end date: %s", commit.EndTimestamp)
				continue
			}

			// Look for a plan based on the name. Default to One Year if it fails
			plan, ok := gcpReservedInstancePlans[commit.Plan]
			if !ok {
				plan = gcpReservedInstancePlans[GCPReservedInstancePlanOneYear]
			}

			results = append(results, &GCPReservedInstance{
				Region:      region,
				ReservedRAM: ram,
				ReservedCPU: vcpu,
				Plan:        plan,
				StartDate:   startTime,
				EndDate:     endTime,
			})
		}
	}

	return results, nil
}

type pvKey struct {
	Labels                 map[string]string
	StorageClass           string
	StorageClassParameters map[string]string
}

func (key *pvKey) GetStorageClass() string {
	return key.StorageClass
}

func (gcp *GCP) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string) PVKey {
	return &pvKey{
		Labels:                 pv.Labels,
		StorageClass:           pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
	}
}

func (key *pvKey) Features() string {
	// TODO: regional cluster pricing.
	storageClass := key.StorageClassParameters["type"]
	if storageClass == "pd-ssd" {
		storageClass = "ssd"
	} else if storageClass == "pd-standard" {
		storageClass = "pdstandard"
	}
	return key.Labels[v1.LabelZoneRegion] + "," + storageClass
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

func (gcp *gcpKey) GPUType() string {
	if t, ok := gcp.Labels[GKE_GPU_TAG]; ok {
		var usageType string
		if t, ok := gcp.Labels["cloud.google.com/gke-preemptible"]; ok && t == "true" {
			usageType = "preemptible"
		} else {
			usageType = "ondemand"
		}
		klog.V(4).Infof("GPU of type: \"%s\" found", t)
		return t + "," + usageType
	}
	return ""
}

// GetKey maps node labels to information needed to retrieve pricing data
func (gcp *gcpKey) Features() string {
	instanceType := strings.ToLower(strings.Join(strings.Split(gcp.Labels[v1.LabelInstanceType], "-")[:2], ""))
	if instanceType == "n1highmem" || instanceType == "n1highcpu" {
		instanceType = "n1standard" // These are priced the same. TODO: support n1ultrahighmem
	} else if strings.HasPrefix(instanceType, "custom") {
		instanceType = "custom" // The suffix of custom does not matter
	}
	region := strings.ToLower(gcp.Labels[v1.LabelZoneRegion])
	var usageType string

	if t, ok := gcp.Labels["cloud.google.com/gke-preemptible"]; ok && t == "true" {
		usageType = "preemptible"
	} else {
		usageType = "ondemand"
	}

	if _, ok := gcp.Labels[GKE_GPU_TAG]; ok {
		return region + "," + instanceType + "," + usageType + "," + "gpu"
	}

	return region + "," + instanceType + "," + usageType
}

// AllNodePricing returns the GCP pricing objects stored
func (gcp *GCP) AllNodePricing() (interface{}, error) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	return gcp.Pricing, nil
}

// NodePricing returns GCP pricing data for a single node
func (gcp *GCP) NodePricing(key Key) (*Node, error) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	if n, ok := gcp.Pricing[key.Features()]; ok {
		klog.V(4).Infof("Returning pricing for node %s: %+v from SKU %s", key, n.Node, n.Name)
		n.Node.BaseCPUPrice = gcp.BaseCPUPrice
		return n.Node, nil
	}
	klog.V(1).Infof("Warning: no pricing data found for %s: %s", key.Features(), key)
	return nil, fmt.Errorf("Warning: no pricing data found for %s", key)
}
