package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/env"
	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/util"

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
	Config                  *ProviderConfig
	serviceKeyProvided      bool
	ValidPricingKeys        map[string]bool
	clusterManagementPrice  float64
	clusterProvisioner      string
	*CustomProvider
}

type gcpAllocation struct {
	Aggregator  bigquery.NullString
	Environment bigquery.NullString
	Service     string
	Cost        float64
}

type multiKeyGCPAllocation struct {
	Keys    bigquery.NullString
	Service string
	Cost    float64
}

func multiKeyGCPAllocationToOutOfClusterAllocation(gcpAlloc multiKeyGCPAllocation, aggregatorNames []string) *OutOfClusterAllocation {
	var keys []map[string]string
	var environment string
	var usedAggregatorName string
	if gcpAlloc.Keys.Valid {
		err := json.Unmarshal([]byte(gcpAlloc.Keys.StringVal), &keys)
		if err != nil {
			klog.Infof("Invalid unmarshaling response from BigQuery filtered query: %s", err.Error())
		}
	keyloop:
		for _, label := range keys {
			for _, aggregatorName := range aggregatorNames {
				if label["key"] == aggregatorName {
					environment = label["value"]
					usedAggregatorName = label["key"]
					break keyloop
				}
			}
		}
	}
	return &OutOfClusterAllocation{
		Aggregator:  usedAggregatorName,
		Environment: environment,
		Service:     gcpAlloc.Service,
		Cost:        gcpAlloc.Cost,
	}
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

// GetLocalStorageQuery returns the cost of local storage for the given window. Setting rate=true
// returns hourly spend. Setting used=true only tracks used storage, not total.
func (gcp *GCP) GetLocalStorageQuery(window, offset string, rate bool, used bool) string {
	// TODO Set to the price for the appropriate storage class. It's not trivial to determine the local storage disk type
	// See https://cloud.google.com/compute/disks-image-pricing#persistentdisk
	localStorageCost := 0.04

	baseMetric := "container_fs_limit_bytes"
	if used {
		baseMetric = "container_fs_usage_bytes"
	}

	fmtOffset := ""
	if offset != "" {
		fmtOffset = fmt.Sprintf("offset %s", offset)
	}

	fmtCumulativeQuery := `sum(
		sum_over_time(%s{device!="tmpfs", id="/"}[%s:1m]%s)
	) by (cluster_id) / 60 / 730 / 1024 / 1024 / 1024 * %f`

	fmtMonthlyQuery := `sum(
		avg_over_time(%s{device!="tmpfs", id="/"}[%s:1m]%s)
	) by (cluster_id) / 1024 / 1024 / 1024 * %f`

	fmtQuery := fmtCumulativeQuery
	if rate {
		fmtQuery = fmtMonthlyQuery
	}

	return fmt.Sprintf(fmtQuery, baseMetric, window, fmtOffset, localStorageCost)
}

func (gcp *GCP) GetConfig() (*CustomPricing, error) {
	c, err := gcp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.Discount == "" {
		c.Discount = "30%"
	}
	if c.NegotiatedDiscount == "" {
		c.NegotiatedDiscount = "0%"
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = "USD"
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

// Attempts to load a GCP auth secret and copy the contents to the key file.
func (*GCP) loadGCPAuthSecret() {
	path := env.GetConfigPathWithDefault("/models/")

	keyPath := path + "key.json"
	keyExists, _ := util.FileExists(keyPath)
	if keyExists {
		klog.V(1).Infof("GCP Auth Key already exists, no need to load from secret")
		return
	}

	exists, err := util.FileExists(authSecretPath)
	if !exists || err != nil {
		errMessage := "Secret does not exist"
		if err != nil {
			errMessage = err.Error()
		}

		klog.V(4).Infof("[Warning] Failed to load auth secret, or was not mounted: %s", errMessage)
		return
	}

	result, err := ioutil.ReadFile(authSecretPath)
	if err != nil {
		klog.V(4).Infof("[Warning] Failed to load auth secret, or was not mounted: %s", err.Error())
		return
	}

	err = ioutil.WriteFile(keyPath, result, 0644)
	if err != nil {
		klog.V(4).Infof("[Warning] Failed to copy auth secret to %s: %s", keyPath, err.Error())
	}
}

func (gcp *GCP) UpdateConfigFromConfigMap(a map[string]string) (*CustomPricing, error) {
	return gcp.Config.UpdateFromMap(a)
}

func (gcp *GCP) UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error) {
	return gcp.Config.Update(func(c *CustomPricing) error {
		if updateType == BigqueryUpdateType {
			a := BigQueryConfig{}
			err := json.NewDecoder(r).Decode(&a)
			if err != nil {
				return err
			}

			c.ProjectID = a.ProjectID
			c.BillingDataDataset = a.BillingDataDataset

			if len(a.Key) > 0 {
				j, err := json.Marshal(a.Key)
				if err != nil {
					return err
				}

				path := env.GetConfigPathWithDefault("/models/")

				keyPath := path + "key.json"
				err = ioutil.WriteFile(keyPath, j, 0644)
				if err != nil {
					return err
				}
				gcp.serviceKeyProvided = true
			}
		} else if updateType == AthenaInfoUpdateType {
			a := AwsAthenaInfo{}
			err := json.NewDecoder(r).Decode(&a)
			if err != nil {
				return err
			}
			c.AthenaBucketName = a.AthenaBucketName
			c.AthenaRegion = a.AthenaRegion
			c.AthenaDatabase = a.AthenaDatabase
			c.AthenaTable = a.AthenaTable
			c.ServiceKeyName = a.ServiceKeyName
			c.ServiceKeySecret = a.ServiceKeySecret
			c.AthenaProjectID = a.AccountID
		} else {
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

// ExternalAllocations represents tagged assets outside the scope of kubernetes.
// "start" and "end" are dates of the format YYYY-MM-DD
// "aggregator" is the tag used to determine how to allocate those assets, ie namespace, pod, etc.
func (gcp *GCP) ExternalAllocations(start string, end string, aggregators []string, filterType string, filterValue string, crossCluster bool) ([]*OutOfClusterAllocation, error) {
	if env.LegacyExternalCostsAPIDisabled() {
		return nil, fmt.Errorf("Legacy External Allocations API disabled.")
	}

	c, err := gcp.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}

	var s []*OutOfClusterAllocation
	if c.ServiceKeyName != "" && c.ServiceKeySecret != "" && !crossCluster {
		aws, err := NewCrossClusterProvider("aws", "gcp.json", gcp.Clientset)
		if err != nil {
			klog.Infof("Could not instantiate cross-cluster provider %s", err.Error())
		}
		awsOOC, err := aws.ExternalAllocations(start, end, aggregators, filterType, filterValue, true)
		if err != nil {
			klog.Infof("Could not fetch cross-cluster costs %s", err.Error())
		}
		s = append(s, awsOOC...)
	}

	formattedAggregators := []string{}
	for _, a := range aggregators {
		formattedAggregators = append(formattedAggregators, strconv.Quote(a))
	}

	aggregator := strings.Join(formattedAggregators, ",")

	var qerr error
	if filterType == "kubernetes_" {
		// start, end formatted like: "2019-04-20 00:00:00"
		/* OLD METHOD: supported getting all data, including unaggregated.
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
		klog.V(3).Infof("Querying \"%s\" with : %s", c.ProjectID, queryString)
		gcpOOC, err := gcp.QuerySQL(queryString)
		s = append(s, gcpOOC...)
		qerr = err
		*/
		queryString := fmt.Sprintf(`(
			SELECT
				service.description as service,
				TO_JSON_STRING(labels) as keys,
				SUM(cost) as cost
			FROM  %s
			WHERE EXISTS (SELECT * FROM UNNEST(labels) AS l2 WHERE l2.key IN (%s))
			AND usage_start_time >= "%s" AND usage_start_time < "%s"
			GROUP BY service, keys
		)`, c.BillingDataDataset, aggregator, start, end)
		klog.V(3).Infof("Querying \"%s\" with : %s", c.ProjectID, queryString)
		gcpOOC, err := gcp.multiLabelQuery(queryString, aggregators)
		s = append(s, gcpOOC...)
		qerr = err
	} else {
		if filterType == "kubernetes_labels" {
			fvs := strings.Split(filterValue, "=")
			if len(fvs) == 2 {
				// if we are given "app=myapp" then look for label "kubernetes_label_app=myapp"
				filterType = fmt.Sprintf("kubernetes_label_%s", fvs[0])
				filterValue = fvs[1]
			} else {
				klog.V(2).Infof("[Warning] illegal kubernetes_labels filterValue: %s", filterValue)
			}
		}

		queryString := fmt.Sprintf(`(
			SELECT
				service.description as service,
				TO_JSON_STRING(labels) as keys,
				SUM(cost) as cost
		  	FROM  %s
		 	WHERE EXISTS (SELECT * FROM UNNEST(labels) AS l2 WHERE l2.key IN (%s))
			AND EXISTS (SELECT * FROM UNNEST(labels) AS l WHERE l.key = "%s" AND l.value = "%s")
			AND usage_start_time >= "%s" AND usage_start_time < "%s"
			GROUP BY service, keys
		)`, c.BillingDataDataset, aggregator, filterType, filterValue, start, end)
		klog.V(4).Infof("Querying \"%s\" with : %s", c.ProjectID, queryString)
		gcpOOC, err := gcp.multiLabelQuery(queryString, aggregators)
		s = append(s, gcpOOC...)
		qerr = err
	}
	if qerr != nil && gcp.serviceKeyProvided {
		klog.Infof("Error querying gcp: %s", qerr)
	}
	return s, qerr
}

func (gcp *GCP) multiLabelQuery(query string, aggregators []string) ([]*OutOfClusterAllocation, error) {
	c, err := gcp.Config.GetCustomPricingData()
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
		var a multiKeyGCPAllocation
		err := it.Next(&a)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		allocations = append(allocations, multiKeyGCPAllocationToOutOfClusterAllocation(a, aggregators))
	}
	return allocations, nil
}

// QuerySQL should query BigQuery for billing data for out of cluster costs.
func (gcp *GCP) QuerySQL(query string) ([]*OutOfClusterAllocation, error) {
	c, err := gcp.Config.GetCustomPricingData()
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
	remoteEnabled := env.IsRemoteEnabled()

	metadataClient := metadata.NewClient(&http.Client{Transport: userAgentTransport{
		userAgent: "kubecost",
		base:      http.DefaultTransport,
	}})

	attribute, err := metadataClient.InstanceAttributeValue("cluster-name")
	if err != nil {
		klog.Infof("Error loading metadata cluster-name: %s", err.Error())
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
	m["provisioner"] = gcp.clusterProvisioner
	m["id"] = env.GetClusterID()
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	return m, nil
}

func (gcp *GCP) ClusterManagementPricing() (string, float64, error) {
	return gcp.clusterProvisioner, gcp.clusterManagementPrice, nil
}

func (*GCP) GetAddresses() ([]byte, error) {
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
	res, err := svc.Addresses.AggregatedList(projID).Do()

	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
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
	CurrencyConversionRate float64            `json:"currencyConversionRate"`
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
					if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
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
					if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
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

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "N2") && !strings.Contains(strings.ToUpper(product.Description), "PREMIUM") {
					if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "N2D AMD") {
						instanceType = "n2dstandard"
					} else {
						instanceType = "n2standard"
					}
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "COMPUTE OPTIMIZED") {
					instanceType = "c2standard"
				}

				if (instanceType == "ram" || instanceType == "cpu") && strings.Contains(strings.ToUpper(product.Description), "E2 INSTANCE") {
					instanceType = "e2"
				}
				partialCPUMap := make(map[string]float64)
				partialCPUMap["e2micro"] = 0.25
				partialCPUMap["e2small"] = 0.5
				partialCPUMap["e2medium"] = 1
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

				candidateKeys := []string{}
				if gcp.ValidPricingKeys == nil {
					gcp.ValidPricingKeys = make(map[string]bool)
				}

				for _, region := range product.ServiceRegions {
					if instanceType == "e2" { // this needs to be done to handle a partial cpu mapping
						candidateKeys = append(candidateKeys, region+","+"e2micro"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2small"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2medium"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2standard"+","+usageType)
						candidateKeys = append(candidateKeys, region+","+"e2custom"+","+usageType)
					} else {
						candidateKey := region + "," + instanceType + "," + usageType
						candidateKeys = append(candidateKeys, candidateKey)
					}
				}

				for _, candidateKey := range candidateKeys {
					instanceType = strings.Split(candidateKey, ",")[1] // we may have overriden this while generating candidate keys
					region := strings.Split(candidateKey, ",")[0]
					candidateKeyGPU := candidateKey + ",gpu"
					gcp.ValidPricingKeys[candidateKey] = true
					gcp.ValidPricingKeys[candidateKeyGPU] = true
					if gpuType != "" {
						lastRateIndex := len(product.PricingInfo[0].PricingExpression.TieredRates) - 1
						var nanos float64
						if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
							nanos = product.PricingInfo[0].PricingExpression.TieredRates[lastRateIndex].UnitPrice.Nanos
						} else {
							continue
						}
						hourlyPrice := nanos * math.Pow10(-9)

						for k, key := range inputKeys {
							if key.GPUType() == gpuType+","+usageType {
								if region == strings.Split(k, ",")[0] {
									klog.V(3).Infof("Matched GPU to node in region \"%s\"", region)
									klog.V(4).Infof("PRODUCT DESCRIPTION: %s", product.Description)
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
							if lastRateIndex > -1 && len(product.PricingInfo) > 0 {
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
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
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
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
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
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
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
									partialCPU, pcok := partialCPUMap[instanceType]
									if pcok {
										product.Node.VCPU = fmt.Sprintf("%f", partialCPU)
									}
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
	c, err := gcp.GetConfig()
	if err != nil {
		return nil, err
	}
	url := "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus?key=" + gcp.APIKey + "&currencyCode=" + c.CurrencyCode
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
	err = parsePagesHelper("")
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
						val.Node.GPU = v.Node.GPU
						val.Node.GPUName = v.Node.GPUName
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
	c, err := gcp.Config.GetCustomPricingData()
	if err != nil {
		klog.V(2).Infof("Error downloading default pricing data: %s", err.Error())
		return err
	}
	gcp.loadGCPAuthSecret()

	gcp.BaseCPUPrice = c.CPU
	gcp.ProjectID = c.ProjectID
	gcp.BillingDataDataset = c.BillingDataDataset

	nodeList := gcp.Clientset.GetAllNodes()
	inputkeys := make(map[string]Key)

	for _, n := range nodeList {
		labels := n.GetObjectMeta().GetLabels()
		if _, ok := labels["cloud.google.com/gke-nodepool"]; ok { // The node is part of a GKE nodepool, so you're paying a cluster management cost
			gcp.clusterManagementPrice = 0.10
			gcp.clusterProvisioner = "GKE"
		}

		key := gcp.GetKey(labels, n)
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
			log.DedupedWarningf(5, "Unable to find params for storageClassName %s", pv.Name)
			continue
		}
		key := gcp.GetPVKey(pv, params, "")
		pvkeys[key.Features()] = key
	}

	reserved, err := gcp.getReservedInstances()
	if err != nil {
		klog.V(1).Infof("Failed to lookup reserved instance data: %s", err.Error())
	} else {
		klog.V(1).Infof("Found %d reserved instances", len(reserved))
		gcp.ReservedInstances = reserved
		for _, r := range reserved {
			klog.V(1).Infof("%s", r)
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
func (gcp *GCP) NetworkPricing() (*Network, error) {
	cpricing, err := gcp.Config.GetCustomPricingData()
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

func (gcp *GCP) LoadBalancerPricing() (*LoadBalancer, error) {
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

func (r *GCPReservedInstance) String() string {
	return fmt.Sprintf("[CPU: %d, RAM: %d, Region: %s, Start: %s, End: %s]", r.ReservedCPU, r.ReservedRAM, r.Region, r.StartDate.String(), r.EndDate.String())
}

type GCPReservedCounter struct {
	RemainingCPU int64
	RemainingRAM int64
	Instance     *GCPReservedInstance
}

func newReservedCounter(instance *GCPReservedInstance) *GCPReservedCounter {
	return &GCPReservedCounter{
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
		klog.V(4).Infof("[Reserved] No Reserved Instances")
		return
	}

	now := time.Now()

	counters := make(map[string][]*GCPReservedCounter)
	for _, r := range gcp.ReservedInstances {
		if now.Before(r.StartDate) || now.After(r.EndDate) {
			klog.V(1).Infof("[Reserved] Skipped Reserved Instance due to dates")
			continue
		}

		_, ok := counters[r.Region]
		counter := newReservedCounter(r)
		if !ok {
			counters[r.Region] = []*GCPReservedCounter{counter}
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
			klog.V(4).Infof("[Reserved] Could not find K8s Node with name: %s", nodeName)
			continue
		}

		nodeRegion, ok := util.GetRegion(kNode.Labels)
		if !ok {
			klog.V(4).Infof("[Reserved] Could not find node region")
			continue
		}

		reservedCounters, ok := counters[nodeRegion]
		if !ok {
			klog.V(4).Infof("[Reserved] Could not find counters for region: %s", nodeRegion)
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
	DefaultRegion          string
}

func (key *pvKey) ID() string {
	return ""
}

func (key *pvKey) GetStorageClass() string {
	return key.StorageClass
}

func (gcp *GCP) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string, defaultRegion string) PVKey {
	return &pvKey{
		Labels:                 pv.Labels,
		StorageClass:           pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		DefaultRegion:          defaultRegion,
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
	region, _ := util.GetRegion(key.Labels)
	return region + "," + storageClass
}

type gcpKey struct {
	Labels map[string]string
}

func (gcp *GCP) GetKey(labels map[string]string, n *v1.Node) Key {
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
	it, _ := util.GetInstanceType(gcp.Labels)
	instanceType := strings.ToLower(strings.Join(strings.Split(it, "-")[:2], ""))
	if instanceType == "n1highmem" || instanceType == "n1highcpu" {
		instanceType = "n1standard" // These are priced the same. TODO: support n1ultrahighmem
	} else if instanceType == "n2highmem" || instanceType == "n2highcpu" {
		instanceType = "n2standard"
	} else if instanceType == "e2highmem" || instanceType == "e2highcpu" {
		instanceType = "e2standard"
	} else if strings.HasPrefix(instanceType, "custom") {
		instanceType = "custom" // The suffix of custom does not matter
	}
	r, _ := util.GetRegion(gcp.Labels)
	region := strings.ToLower(r)
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

func (gcp *GCP) getPricing(key Key) (*GCPPricing, bool) {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	n, ok := gcp.Pricing[key.Features()]
	return n, ok
}
func (gcp *GCP) isValidPricingKey(key Key) bool {
	gcp.DownloadPricingDataLock.RLock()
	defer gcp.DownloadPricingDataLock.RUnlock()
	_, ok := gcp.ValidPricingKeys[key.Features()]
	return ok
}

// NodePricing returns GCP pricing data for a single node
func (gcp *GCP) NodePricing(key Key) (*Node, error) {
	if n, ok := gcp.getPricing(key); ok {
		klog.V(4).Infof("Returning pricing for node %s: %+v from SKU %s", key, n.Node, n.Name)
		n.Node.BaseCPUPrice = gcp.BaseCPUPrice
		return n.Node, nil
	} else if ok := gcp.isValidPricingKey(key); ok {
		err := gcp.DownloadPricingData()
		if err != nil {
			return nil, fmt.Errorf("Download pricing data failed: %s", err.Error())
		}
		if n, ok := gcp.getPricing(key); ok {
			klog.V(4).Infof("Returning pricing for node %s: %+v from SKU %s", key, n.Node, n.Name)
			n.Node.BaseCPUPrice = gcp.BaseCPUPrice
			return n.Node, nil
		}
		klog.V(1).Infof("[Warning] no pricing data found for %s: %s", key.Features(), key)
		return nil, fmt.Errorf("Warning: no pricing data found for %s", key)
	}
	return nil, fmt.Errorf("Warning: no pricing data found for %s", key)
}

func (gcp *GCP) ServiceAccountStatus() *ServiceAccountStatus {
	return &ServiceAccountStatus{
		Checks: []*ServiceAccountCheck{},
	}
}

func (gcp *GCP) PricingSourceStatus() map[string]*PricingSource {
	return make(map[string]*PricingSource)
}

func (gcp *GCP) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	class := strings.Split(instanceType, "-")[0]
	return 1.0 - ((1.0 - sustainedUseDiscount(class, defaultDiscount, isPreemptible)) * (1.0 - negotiatedDiscount))
}

func sustainedUseDiscount(class string, defaultDiscount float64, isPreemptible bool) float64 {
	if isPreemptible {
		return 0.0
	}
	discount := defaultDiscount
	switch class {
	case "e2", "f1", "g1":
		discount = 0.0
	case "n2", "n2d":
		discount = 0.2
	}
	return discount
}

func (gcp *GCP) ParseID(id string) string {
	// gce://guestbook-227502/us-central1-a/gke-niko-n1-standard-2-wljla-8df8e58a-hfy7
	//  => gke-niko-n1-standard-2-wljla-8df8e58a-hfy7
	rx := regexp.MustCompile("gce://[^/]*/[^/]*/([^/]+)")
	match := rx.FindStringSubmatch(id)
	if len(match) < 2 {
		if id != "" {
			log.Infof("gcpprovider.ParseID: failed to parse %s", id)
		}
		return id
	}

	return match[1]
}

func (gcp *GCP) ParsePVID(id string) string {
	return id
}

func (gcp *GCP) ParseLBID(id string) string {
	return id
}
