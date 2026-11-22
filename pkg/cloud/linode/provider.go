package linode

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"
	coreJSON "github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/utils"
	"github.com/opencost/opencost/pkg/env"
)

const (
	LinodeAPIPricing = "Linode API Pricing"

	// RegionLabel is set on LKE nodes by the Linode cloud controller manager.
	RegionLabel = "topology.linode.com/region"
	// PoolIDLabel is set on every LKE node.
	PoolIDLabel = "lke.linode.com/pool-id"

	defaultAPIURL  = "https://api.linode.com/v4"
	apiPageSize    = 500
	apiTimeout     = 30 * time.Second
	pricingSource  = "linode-api"
	lkeStandardID  = "lke-sa"
	lkeHAID        = "lke-ha"
	volumeID       = "volume"
	nodeBalancerID = "nodebalancer"
)

// LKE node names look like lke<cluster_id>-<pool_id>-<suffix>.
var lkeNodeNameRegex = regexp.MustCompile(`^lke(\d+)-`)

// ParseLKEClusterID returns the LKE cluster ID encoded in an LKE node name, or
// "" when the name does not follow the LKE convention.
func ParseLKEClusterID(nodeName string) string {
	match := lkeNodeNameRegex.FindStringSubmatch(nodeName)
	if match == nil {
		return ""
	}
	return match[1]
}

// Price is an hourly price as returned by the Linode API.
type Price struct {
	Hourly float64 `json:"hourly"`
}

// RegionPrice overrides the base price in one region.
type RegionPrice struct {
	ID     string  `json:"id"`
	Hourly float64 `json:"hourly"`
}

// LinodeType is an entry of the Linode /linode/types, /lke/types,
// /volumes/types or /nodebalancers/types endpoints.
type LinodeType struct {
	ID           string        `json:"id"`
	Label        string        `json:"label"`
	Class        string        `json:"class,omitempty"`
	Price        Price         `json:"price"`
	RegionPrices []RegionPrice `json:"region_prices"`
	Memory       int           `json:"memory,omitempty"` // MB
	Disk         int           `json:"disk,omitempty"`   // MB
	VCPUs        int           `json:"vcpus,omitempty"`
	GPUs         int           `json:"gpus,omitempty"`
}

// HourlyPrice returns the hourly price of the type in region, falling back to
// the base price when the region has no specific price.
func (t *LinodeType) HourlyPrice(region string) float64 {
	for _, rp := range t.RegionPrices {
		if rp.ID == region {
			return rp.Hourly
		}
	}
	return t.Price.Hourly
}

type listResponse[T any] struct {
	Data  []T `json:"data"`
	Page  int `json:"page"`
	Pages int `json:"pages"`
}

type region struct {
	ID string `json:"id"`
}

type lkeCluster struct {
	ControlPlane struct {
		HighAvailability bool `json:"high_availability"`
	} `json:"control_plane"`
}

// Linode implements the models.Provider interface for Linode Kubernetes Engine.
// Prices come from the public Linode API; an API token is only used to look up
// whether the cluster has a high availability control plane.
type Linode struct {
	Clientset        clustercache.ClusterCache
	Config           models.ProviderConfig
	ClusterRegion    string
	ClusterAccountID string
	LKEClusterID     string

	DownloadLock   sync.RWMutex
	InstanceTypes  map[string]*LinodeType
	ProductTypes   map[string]*LinodeType
	RegionList     []string
	HAControlPlane bool
	apiURL         string
	apiToken       string
	httpClient     *http.Client
}

func (l *Linode) baseURL() string {
	if l.apiURL != "" {
		return strings.TrimRight(l.apiURL, "/")
	}
	return defaultAPIURL
}

func (l *Linode) client() *http.Client {
	if l.httpClient == nil {
		l.httpClient = &http.Client{Timeout: apiTimeout}
	}
	return l.httpClient
}

func (l *Linode) get(path string, authenticated bool, out any) error {
	req, err := http.NewRequest(http.MethodGet, l.baseURL()+path, nil)
	if err != nil {
		return fmt.Errorf("building Linode API request for %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+l.apiToken)
	}

	resp, err := l.client().Do(req)
	if err != nil {
		return fmt.Errorf("calling Linode API %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("linode API %s returned status %d", path, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading Linode API response for %s: %w", path, err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decoding Linode API response for %s: %w", path, err)
	}
	return nil
}

func listAll[T any](l *Linode, path string) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		q := url.Values{}
		q.Set("page", strconv.Itoa(page))
		q.Set("page_size", strconv.Itoa(apiPageSize))

		var resp listResponse[T]
		if err := l.get(path+"?"+q.Encode(), false, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Data...)
		if resp.Pages <= page {
			return all, nil
		}
	}
}

func typesByID(types []LinodeType) map[string]*LinodeType {
	m := make(map[string]*LinodeType, len(types))
	for i := range types {
		m[types[i].ID] = &types[i]
	}
	return m
}

// DownloadPricingData fetches instance, LKE, volume and NodeBalancer prices
// from the Linode API. Only the instance types are required; anything else
// that fails falls back to the custom pricing config.
func (l *Linode) DownloadPricingData() error {
	l.DownloadLock.Lock()
	defer l.DownloadLock.Unlock()

	instanceTypes, err := listAll[LinodeType](l, "/linode/types")
	if err != nil {
		return fmt.Errorf("fetching Linode instance types: %w", err)
	}
	l.InstanceTypes = typesByID(instanceTypes)

	products := make(map[string]*LinodeType)
	for _, path := range []string{"/lke/types", "/volumes/types", "/nodebalancers/types"} {
		types, err := listAll[LinodeType](l, path)
		if err != nil {
			log.Warnf("Could not fetch Linode prices from %s, using custom pricing instead: %v", path, err)
			continue
		}
		for id, t := range typesByID(types) {
			products[id] = t
		}
	}
	l.ProductTypes = products

	regions, err := listAll[region](l, "/regions")
	if err != nil {
		log.Warnf("Could not fetch Linode regions: %v", err)
	} else {
		l.RegionList = make([]string, 0, len(regions))
		for _, r := range regions {
			l.RegionList = append(l.RegionList, r.ID)
		}
	}

	if l.apiToken == "" {
		l.apiToken = env.GetLinodeToken()
	}
	l.HAControlPlane = false
	if l.LKEClusterID != "" && l.apiToken != "" {
		var cluster lkeCluster
		if err := l.get("/lke/clusters/"+url.PathEscape(l.LKEClusterID), true, &cluster); err != nil {
			log.Warnf("Could not look up LKE cluster %s, assuming a standard control plane: %v", l.LKEClusterID, err)
		} else {
			l.HAControlPlane = cluster.ControlPlane.HighAvailability
		}
	}

	log.Infof("Loaded Linode pricing: %d instance types, %d product types", len(l.InstanceTypes), len(l.ProductTypes))
	return nil
}

// productPrice returns the hourly price of a product type in the cluster region.
func (l *Linode) productPrice(id string) (float64, bool) {
	l.DownloadLock.RLock()
	defer l.DownloadLock.RUnlock()

	t, ok := l.ProductTypes[id]
	if !ok {
		return 0, false
	}
	return t.HourlyPrice(l.ClusterRegion), true
}

// NodePricing returns the hourly price of the Linode plan the node runs on.
func (l *Linode) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	l.DownloadLock.RLock()
	defer l.DownloadLock.RUnlock()

	meta := models.PricingMetadata{Currency: "USD", Source: pricingSource}

	features := strings.Split(key.Features(), ",")
	if len(features) < 2 || features[1] == "" {
		return nil, meta, fmt.Errorf("invalid Linode key features: %s", key.Features())
	}
	region, instanceType := features[0], features[1]

	t, ok := l.InstanceTypes[instanceType]
	if !ok {
		return nil, meta, fmt.Errorf("linode type not found in pricing: %s", instanceType)
	}

	node := &models.Node{
		Cost:         strconv.FormatFloat(t.HourlyPrice(region), 'f', -1, 64),
		VCPU:         strconv.Itoa(t.VCPUs),
		RAM:          fmt.Sprintf("%dMi", t.Memory),
		RAMBytes:     strconv.FormatInt(int64(t.Memory)*1024*1024, 10),
		Storage:      fmt.Sprintf("%dMi", t.Disk),
		InstanceType: instanceType,
		Region:       region,
		PricingType:  models.DefaultPrices,
	}
	if t.GPUs > 0 {
		node.GPU = strconv.Itoa(t.GPUs)
	}
	return node, meta, nil
}

// PVPricing returns the hourly price per GiB of a Linode Block Storage volume.
func (l *Linode) PVPricing(pvk models.PVKey) (*models.PV, error) {
	region := pvk.Features()

	l.DownloadLock.RLock()
	t, ok := l.ProductTypes[volumeID]
	l.DownloadLock.RUnlock()

	if ok {
		return &models.PV{
			Cost:   strconv.FormatFloat(t.HourlyPrice(region), 'f', -1, 64),
			Class:  pvk.GetStorageClass(),
			Region: region,
		}, nil
	}

	cp, err := l.GetConfig()
	if err != nil {
		return nil, err
	}
	return &models.PV{
		Cost:   cp.Storage,
		Class:  pvk.GetStorageClass(),
		Region: region,
	}, nil
}

// LoadBalancerPricing returns the hourly price of a NodeBalancer.
func (l *Linode) LoadBalancerPricing() (*models.LoadBalancer, error) {
	if price, ok := l.productPrice(nodeBalancerID); ok {
		return &models.LoadBalancer{Cost: price}, nil
	}

	cp, err := l.GetConfig()
	if err != nil {
		return nil, err
	}
	price, err := strconv.ParseFloat(cp.DefaultLBPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing Linode defaultLBPrice %q: %w", cp.DefaultLBPrice, err)
	}
	return &models.LoadBalancer{Cost: price}, nil
}

// NetworkPricing returns network egress prices from the custom pricing config.
func (l *Linode) NetworkPricing() (*models.Network, error) {
	cp, err := l.GetConfig()
	if err != nil {
		return nil, err
	}

	parse := func(name, value string) (float64, error) {
		if value == "" {
			return 0, nil
		}
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing Linode %s %q: %w", name, value, err)
		}
		return f, nil
	}

	zone, err := parse("zoneNetworkEgress", cp.ZoneNetworkEgress)
	if err != nil {
		return nil, err
	}
	region, err := parse("regionNetworkEgress", cp.RegionNetworkEgress)
	if err != nil {
		return nil, err
	}
	internet, err := parse("internetNetworkEgress", cp.InternetNetworkEgress)
	if err != nil {
		return nil, err
	}

	return &models.Network{
		ZoneNetworkEgressCost:     zone,
		RegionNetworkEgressCost:   region,
		InternetNetworkEgressCost: internet,
	}, nil
}

// ClusterManagementPricing returns the hourly price of the LKE control plane:
// free for a standard control plane, billed for a high availability one.
func (l *Linode) ClusterManagementPricing() (string, float64, error) {
	l.DownloadLock.RLock()
	ha := l.HAControlPlane
	l.DownloadLock.RUnlock()

	id := lkeStandardID
	if ha {
		id = lkeHAID
	}
	price, _ := l.productPrice(id)
	return "LKE", price, nil
}

func (l *Linode) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (l *Linode) ClusterInfo() (map[string]string, error) {
	conf, err := l.GetConfig()
	if err != nil {
		return nil, err
	}

	m := map[string]string{
		"name":              "Linode Cluster #1",
		"provider":          opencost.LinodeProvider,
		"provisioner":       "LKE",
		"region":            l.ClusterRegion,
		"account":           l.ClusterAccountID,
		"remoteReadEnabled": strconv.FormatBool(env.IsRemoteEnabled()),
		"id":                coreenv.GetClusterID(),
	}
	if conf.ClusterName != "" {
		m["name"] = conf.ClusterName
	}
	return m, nil
}

func (l *Linode) GetManagementPlatform() (string, error) {
	nodes := l.Clientset.GetAllNodes()
	if len(nodes) > 0 {
		if _, ok := nodes[0].Labels[PoolIDLabel]; ok {
			return "lke", nil
		}
	}
	return "", nil
}

type linodeKey struct {
	Labels map[string]string
}

func (k *linodeKey) Features() string {
	region, ok := util.GetRegion(k.Labels)
	if !ok || region == "" {
		region = k.Labels[RegionLabel]
	}
	instanceType, _ := util.GetInstanceType(k.Labels)
	return region + "," + instanceType
}

func (k *linodeKey) GPUType() string {
	return ""
}

func (k *linodeKey) GPUCount() int {
	return 0
}

func (k *linodeKey) ID() string {
	return ""
}

func (l *Linode) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &linodeKey{Labels: labels}
}

type linodePVKey struct {
	StorageClassName string
	ProviderID       string
	Region           string
}

func (k *linodePVKey) Features() string {
	return k.Region
}

func (k *linodePVKey) GetStorageClass() string {
	return k.StorageClassName
}

func (k *linodePVKey) ID() string {
	return k.ProviderID
}

// GetPVKey keys a volume by the region in its node affinity, falling back to
// the cluster region.
func (l *Linode) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	region := l.ClusterRegion
	if region == "" {
		region = defaultRegion
	}
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if (expr.Key == RegionLabel || expr.Key == "topology.kubernetes.io/region") && len(expr.Values) > 0 {
					region = expr.Values[0]
				}
			}
		}
	}

	var providerID string
	if pv.Spec.CSI != nil {
		providerID = pv.Spec.CSI.VolumeHandle
	}

	return &linodePVKey{
		StorageClassName: pv.Spec.StorageClassName,
		ProviderID:       providerID,
		Region:           region,
	}
}

func (l *Linode) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (l *Linode) GetDisks() ([]byte, error) {
	return nil, nil
}

func (l *Linode) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (l *Linode) AllNodePricing() (interface{}, error) {
	l.DownloadLock.RLock()
	defer l.DownloadLock.RUnlock()
	return l.InstanceTypes, nil
}

func (l *Linode) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return l.Config.UpdateFromMap(a)
}

func (l *Linode) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	return l.Config.Update(func(cp *models.CustomPricing) error {
		a := make(map[string]interface{})
		if err := coreJSON.NewDecoder(r).Decode(&a); err != nil {
			return err
		}
		for k, v := range a {
			kUpper := utils.ToTitle.String(k)
			vstr, ok := v.(string)
			if !ok {
				return fmt.Errorf("type error while updating config for %s", kUpper)
			}
			if err := models.SetCustomPricingField(cp, kUpper, vstr); err != nil {
				return fmt.Errorf("error setting custom pricing field: %w", err)
			}
		}

		if env.IsRemoteEnabled() {
			if err := utils.UpdateClusterMeta(coreenv.GetClusterID(), cp.ClusterName); err != nil {
				return err
			}
		}
		return nil
	})
}

func (l *Linode) GetConfig() (*models.CustomPricing, error) {
	cp, err := l.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if cp.Discount == "" {
		cp.Discount = "0%"
	}
	if cp.NegotiatedDiscount == "" {
		cp.NegotiatedDiscount = "0%"
	}
	if cp.CurrencyCode == "" {
		cp.CurrencyCode = "USD"
	}
	return cp, nil
}

func (l *Linode) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (l *Linode) Regions() []string {
	if overrides := env.GetRegionOverrideList(); len(overrides) > 0 {
		log.Debugf("Overriding Linode regions with configured region list: %+v", overrides)
		return overrides
	}

	l.DownloadLock.RLock()
	defer l.DownloadLock.RUnlock()
	return l.RegionList
}

func (l *Linode) ApplyReservedInstancePricing(nodes map[string]*models.Node) {}

func (l *Linode) ServiceAccountStatus() *models.ServiceAccountStatus {
	return &models.ServiceAccountStatus{Checks: []*models.ServiceAccountCheck{}}
}

func (l *Linode) PricingSourceStatus() map[string]*models.PricingSource {
	l.DownloadLock.RLock()
	defer l.DownloadLock.RUnlock()

	return map[string]*models.PricingSource{
		LinodeAPIPricing: {
			Name:      LinodeAPIPricing,
			Enabled:   true,
			Available: len(l.InstanceTypes) > 0,
		},
	}
}

func (l *Linode) PricingSourceSummary() interface{} {
	l.DownloadLock.RLock()
	defer l.DownloadLock.RUnlock()
	return l.InstanceTypes
}

// RefreshCustomPricing is a no-op: node prices come from the Linode API, and
// the custom pricing fallbacks are read from config on every call.
func (l *Linode) RefreshCustomPricing() error {
	return nil
}
