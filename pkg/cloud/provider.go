package cloud

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/kubecost"

	"github.com/opencost/opencost/pkg/util"

	"cloud.google.com/go/compute/metadata"

	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/util/watcher"

	v1 "k8s.io/api/core/v1"
)

const authSecretPath = "/var/secrets/service-key.json"
const storageConfigSecretPath = "/var/azure-storage-config/azure-storage-config.json"
const defaultShareTenancyCost = "true"

var createTableStatements = []string{
	`CREATE TABLE IF NOT EXISTS names (
		cluster_id VARCHAR(255) NOT NULL,
		cluster_name VARCHAR(255) NULL,
		PRIMARY KEY (cluster_id)
	);`,
}

// ReservedInstanceData keeps record of resources on a node should be
// priced at reserved rates
type ReservedInstanceData struct {
	ReservedCPU int64   `json:"reservedCPU"`
	ReservedRAM int64   `json:"reservedRAM"`
	CPUCost     float64 `json:"CPUHourlyCost"`
	RAMCost     float64 `json:"RAMHourlyCost"`
}

// Node is the interface by which the provider and cost model communicate Node prices.
// The provider will best-effort try to fill out this struct.
type Node struct {
	Cost             string                `json:"hourlyCost"`
	VCPU             string                `json:"CPU"`
	VCPUCost         string                `json:"CPUHourlyCost"`
	RAM              string                `json:"RAM"`
	RAMBytes         string                `json:"RAMBytes"`
	RAMCost          string                `json:"RAMGBHourlyCost"`
	Storage          string                `json:"storage"`
	StorageCost      string                `json:"storageHourlyCost"`
	UsesBaseCPUPrice bool                  `json:"usesDefaultPrice"`
	BaseCPUPrice     string                `json:"baseCPUPrice"` // Used to compute an implicit RAM GB/Hr price when RAM pricing is not provided.
	BaseRAMPrice     string                `json:"baseRAMPrice"` // Used to compute an implicit RAM GB/Hr price when RAM pricing is not provided.
	BaseGPUPrice     string                `json:"baseGPUPrice"`
	UsageType        string                `json:"usageType"`
	GPU              string                `json:"gpu"` // GPU represents the number of GPU on the instance
	GPUName          string                `json:"gpuName"`
	GPUCost          string                `json:"gpuCost"`
	InstanceType     string                `json:"instanceType,omitempty"`
	Region           string                `json:"region,omitempty"`
	Reserved         *ReservedInstanceData `json:"reserved,omitempty"`
	ProviderID       string                `json:"providerID,omitempty"`
	PricingType      PricingType           `json:"pricingType,omitempty"`
}

// IsSpot determines whether or not a Node uses spot by usage type
func (n *Node) IsSpot() bool {
	if n != nil {
		return strings.Contains(n.UsageType, "spot") || strings.Contains(n.UsageType, "emptible")
	} else {
		return false
	}
}

// LoadBalancer is the interface by which the provider and cost model communicate LoadBalancer prices.
// The provider will best-effort try to fill out this struct.
type LoadBalancer struct {
	IngressIPAddresses []string `json:"IngressIPAddresses"`
	Cost               float64  `json:"hourlyCost"`
}

// TODO: used for dynamic cloud provider price fetching.
// determine what identifies a load balancer in the json returned from the cloud provider pricing API call
// type LBKey interface {
// }

// Network is the interface by which the provider and cost model communicate network egress prices.
// The provider will best-effort try to fill out this struct.
type Network struct {
	ZoneNetworkEgressCost     float64
	RegionNetworkEgressCost   float64
	InternetNetworkEgressCost float64
}

type OrphanedResource struct {
	Kind        string            `json:"resourceKind"`
	Region      string            `json:"region"`
	Description map[string]string `json:"description"`
	Size        *int64            `json:"diskSizeInGB,omitempty"`
	DiskName    string            `json:"diskName,omitempty"`
	Url         string            `json:"url"`
	Address     string            `json:"ipAddress,omitempty"`
	MonthlyCost *float64          `json:"monthlyCost"`
}

// PV is the interface by which the provider and cost model communicate PV prices.
// The provider will best-effort try to fill out this struct.
type PV struct {
	Cost       string            `json:"hourlyCost"`
	CostPerIO  string            `json:"costPerIOOperation"`
	Class      string            `json:"storageClass"`
	Size       string            `json:"size"`
	Region     string            `json:"region"`
	ProviderID string            `json:"providerID,omitempty"`
	Parameters map[string]string `json:"parameters"`
}

// Key represents a way for nodes to match between the k8s API and a pricing API
type Key interface {
	ID() string       // ID represents an exact match
	Features() string // Features are a comma separated string of node metadata that could match pricing
	GPUType() string  // GPUType returns "" if no GPU exists or GPUs, but the name of the GPU otherwise
	GPUCount() int    // GPUCount returns 0 if no GPU exists or GPUs, but the number of attached GPUs otherwise
}

type PVKey interface {
	Features() string
	GetStorageClass() string
	ID() string
}

// OutOfClusterAllocation represents a cloud provider cost not associated with kubernetes
type OutOfClusterAllocation struct {
	Aggregator  string  `json:"aggregator"`
	Environment string  `json:"environment"`
	Service     string  `json:"service"`
	Cost        float64 `json:"cost"`
	Cluster     string  `json:"cluster"`
}

type CustomPricing struct {
	Provider                     string `json:"provider"`
	Description                  string `json:"description"`
	CPU                          string `json:"CPU"`
	SpotCPU                      string `json:"spotCPU"`
	RAM                          string `json:"RAM"`
	SpotRAM                      string `json:"spotRAM"`
	GPU                          string `json:"GPU"`
	SpotGPU                      string `json:"spotGPU"`
	Storage                      string `json:"storage"`
	ZoneNetworkEgress            string `json:"zoneNetworkEgress"`
	RegionNetworkEgress          string `json:"regionNetworkEgress"`
	InternetNetworkEgress        string `json:"internetNetworkEgress"`
	FirstFiveForwardingRulesCost string `json:"firstFiveForwardingRulesCost"`
	AdditionalForwardingRuleCost string `json:"additionalForwardingRuleCost"`
	LBIngressDataCost            string `json:"LBIngressDataCost"`
	SpotLabel                    string `json:"spotLabel,omitempty"`
	SpotLabelValue               string `json:"spotLabelValue,omitempty"`
	GpuLabel                     string `json:"gpuLabel,omitempty"`
	GpuLabelValue                string `json:"gpuLabelValue,omitempty"`
	ServiceKeyName               string `json:"awsServiceKeyName,omitempty"`
	ServiceKeySecret             string `json:"awsServiceKeySecret,omitempty"`
	AlibabaServiceKeyName        string `json:"alibabaServiceKeyName,omitempty"`
	AlibabaServiceKeySecret      string `json:"alibabaServiceKeySecret,omitempty"`
	AlibabaClusterRegion         string `json:"alibabaClusterRegion,omitempty"`
	SpotDataRegion               string `json:"awsSpotDataRegion,omitempty"`
	SpotDataBucket               string `json:"awsSpotDataBucket,omitempty"`
	SpotDataPrefix               string `json:"awsSpotDataPrefix,omitempty"`
	ProjectID                    string `json:"projectID,omitempty"`
	AthenaProjectID              string `json:"athenaProjectID,omitempty"`
	AthenaBucketName             string `json:"athenaBucketName"`
	AthenaRegion                 string `json:"athenaRegion"`
	AthenaDatabase               string `json:"athenaDatabase"`
	AthenaTable                  string `json:"athenaTable"`
	AthenaWorkgroup              string `json:"athenaWorkgroup"`
	MasterPayerARN               string `json:"masterPayerARN"`
	BillingDataDataset           string `json:"billingDataDataset,omitempty"`
	CustomPricesEnabled          string `json:"customPricesEnabled"`
	DefaultIdle                  string `json:"defaultIdle"`
	AzureSubscriptionID          string `json:"azureSubscriptionID"`
	AzureClientID                string `json:"azureClientID"`
	AzureClientSecret            string `json:"azureClientSecret"`
	AzureTenantID                string `json:"azureTenantID"`
	AzureBillingRegion           string `json:"azureBillingRegion"`
	AzureOfferDurableID          string `json:"azureOfferDurableID"`
	AzureStorageSubscriptionID   string `json:"azureStorageSubscriptionID"`
	AzureStorageAccount          string `json:"azureStorageAccount"`
	AzureStorageAccessKey        string `json:"azureStorageAccessKey"`
	AzureStorageContainer        string `json:"azureStorageContainer"`
	AzureContainerPath           string `json:"azureContainerPath"`
	AzureCloud                   string `json:"azureCloud"`
	CurrencyCode                 string `json:"currencyCode"`
	Discount                     string `json:"discount"`
	NegotiatedDiscount           string `json:"negotiatedDiscount"`
	SharedOverhead               string `json:"sharedOverhead"`
	ClusterName                  string `json:"clusterName"`
	SharedNamespaces             string `json:"sharedNamespaces"`
	SharedLabelNames             string `json:"sharedLabelNames"`
	SharedLabelValues            string `json:"sharedLabelValues"`
	ShareTenancyCosts            string `json:"shareTenancyCosts"` // TODO clean up configuration so we can use a type other that string (this should be a bool, but the app panics if it's not a string)
	ReadOnly                     string `json:"readOnly"`
	EditorAccess                 string `json:"editorAccess"`
	KubecostToken                string `json:"kubecostToken"`
	GoogleAnalyticsTag           string `json:"googleAnalyticsTag"`
	ExcludeProviderID            string `json:"excludeProviderID"`
}

// GetSharedOverheadCostPerMonth parses and returns a float64 representation
// of the configured monthly shared overhead cost. If the string version cannot
// be parsed into a float, an error is logged and 0.0 is returned.
func (cp *CustomPricing) GetSharedOverheadCostPerMonth() float64 {
	// Empty string should be interpreted as "no cost", i.e. 0.0
	if cp.SharedOverhead == "" {
		return 0.0
	}

	// Attempt to parse, but log and return 0.0 if that fails.
	sharedCostPerMonth, err := strconv.ParseFloat(cp.SharedOverhead, 64)
	if err != nil {
		log.Errorf("SharedOverhead: failed to parse shared overhead \"%s\": %s", cp.SharedOverhead, err)
		return 0.0
	}

	return sharedCostPerMonth
}

type ServiceAccountStatus struct {
	Checks []*ServiceAccountCheck `json:"checks"`
}

// ServiceAccountChecks is a thread safe map for holding ServiceAccountCheck objects
type ServiceAccountChecks struct {
	sync.RWMutex
	serviceAccountChecks map[string]*ServiceAccountCheck
}

// NewServiceAccountChecks initialize ServiceAccountChecks
func NewServiceAccountChecks() *ServiceAccountChecks {
	return &ServiceAccountChecks{
		serviceAccountChecks: make(map[string]*ServiceAccountCheck),
	}
}

func (sac *ServiceAccountChecks) set(key string, check *ServiceAccountCheck) {
	sac.Lock()
	defer sac.Unlock()
	sac.serviceAccountChecks[key] = check
}

// getStatus extracts ServiceAccountCheck objects into a slice and returns them in a ServiceAccountStatus
func (sac *ServiceAccountChecks) getStatus() *ServiceAccountStatus {
	sac.Lock()
	defer sac.Unlock()
	checks := []*ServiceAccountCheck{}
	for _, v := range sac.serviceAccountChecks {
		checks = append(checks, v)
	}
	return &ServiceAccountStatus{
		Checks: checks,
	}
}

type ServiceAccountCheck struct {
	Message        string `json:"message"`
	Status         bool   `json:"status"`
	AdditionalInfo string `json:"additionalInfo"`
}

type PricingSources struct {
	PricingSources map[string]*PricingSource
}

type PricingSource struct {
	Name      string `json:"name"`
	Enabled   bool   `json:"enabled"`
	Available bool   `json:"available"`
	Error     string `json:"error"`
}

type PricingType string

const (
	Api           PricingType = "api"
	Spot          PricingType = "spot"
	Reserved      PricingType = "reserved"
	SavingsPlan   PricingType = "savingsPlan"
	CsvExact      PricingType = "csvExact"
	CsvClass      PricingType = "csvClass"
	DefaultPrices PricingType = "defaultPrices"
)

type PricingMatchMetadata struct {
	TotalNodes        int                 `json:"TotalNodes"`
	PricingTypeCounts map[PricingType]int `json:"PricingType"`
}

// Provider represents a k8s provider.
type Provider interface {
	ClusterInfo() (map[string]string, error)
	GetAddresses() ([]byte, error)
	GetDisks() ([]byte, error)
	GetOrphanedResources() ([]OrphanedResource, error)
	NodePricing(Key) (*Node, error)
	PVPricing(PVKey) (*PV, error)
	NetworkPricing() (*Network, error)           // TODO: add key interface arg for dynamic price fetching
	LoadBalancerPricing() (*LoadBalancer, error) // TODO: add key interface arg for dynamic price fetching
	AllNodePricing() (interface{}, error)
	DownloadPricingData() error
	GetKey(map[string]string, *v1.Node) Key
	GetPVKey(*v1.PersistentVolume, map[string]string, string) PVKey
	UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error)
	UpdateConfigFromConfigMap(map[string]string) (*CustomPricing, error)
	GetConfig() (*CustomPricing, error)
	GetManagementPlatform() (string, error)
	GetLocalStorageQuery(time.Duration, time.Duration, bool, bool) string
	ApplyReservedInstancePricing(map[string]*Node)
	ServiceAccountStatus() *ServiceAccountStatus
	PricingSourceStatus() map[string]*PricingSource
	ClusterManagementPricing() (string, float64, error)
	CombinedDiscountForNode(string, bool, float64, float64) float64
	Regions() []string
}

// ClusterName returns the name defined in cluster info, defaulting to the
// CLUSTER_ID environment variable
func ClusterName(p Provider) string {
	info, err := p.ClusterInfo()
	if err != nil {
		return env.GetClusterID()
	}

	name, ok := info["name"]
	if !ok {
		return env.GetClusterID()
	}

	return name
}

// CustomPricesEnabled returns the boolean equivalent of the cloup provider's custom prices flag,
// indicating whether or not the cluster is using custom pricing.
func CustomPricesEnabled(p Provider) bool {
	config, err := p.GetConfig()
	if err != nil {
		return false
	}
	// TODO:CLEANUP what is going on with this?
	if config.NegotiatedDiscount == "" {
		config.NegotiatedDiscount = "0%"
	}

	return config.CustomPricesEnabled == "true"
}

// ConfigWatcherFor returns a new ConfigWatcher instance which watches changes to the "pricing-configs"
// configmap
func ConfigWatcherFor(p Provider) *watcher.ConfigMapWatcher {
	return &watcher.ConfigMapWatcher{
		ConfigMapName: env.GetPricingConfigmapName(),
		WatchFunc: func(name string, data map[string]string) error {
			_, err := p.UpdateConfigFromConfigMap(data)
			return err
		},
	}
}

// AllocateIdleByDefault returns true if the application settings specify to allocate idle by default
func AllocateIdleByDefault(p Provider) bool {
	config, err := p.GetConfig()
	if err != nil {
		return false
	}

	return config.DefaultIdle == "true"
}

// SharedNamespace returns a list of names of shared namespaces, as defined in the application settings
func SharedNamespaces(p Provider) []string {
	namespaces := []string{}

	config, err := p.GetConfig()
	if err != nil {
		return namespaces
	}
	if config.SharedNamespaces == "" {
		return namespaces
	}
	// trim spaces so that "kube-system, kubecost" is equivalent to "kube-system,kubecost"
	for _, ns := range strings.Split(config.SharedNamespaces, ",") {
		namespaces = append(namespaces, strings.Trim(ns, " "))
	}

	return namespaces
}

// SharedLabel returns the configured set of shared labels as a parallel tuple of keys to values; e.g.
// for app:kubecost,type:staging this returns (["app", "type"], ["kubecost", "staging"]) in order to
// match the signature of the NewSharedResourceInfo
func SharedLabels(p Provider) ([]string, []string) {
	names := []string{}
	values := []string{}

	config, err := p.GetConfig()
	if err != nil {
		return names, values
	}

	if config.SharedLabelNames == "" || config.SharedLabelValues == "" {
		return names, values
	}

	ks := strings.Split(config.SharedLabelNames, ",")
	vs := strings.Split(config.SharedLabelValues, ",")
	if len(ks) != len(vs) {
		log.Warnf("Shared labels have mis-matched lengths: %d names, %d values", len(ks), len(vs))
		return names, values
	}

	for i := range ks {
		names = append(names, strings.Trim(ks[i], " "))
		values = append(values, strings.Trim(vs[i], " "))
	}

	return names, values
}

// ShareTenancyCosts returns true if the application settings specify to share
// tenancy costs by default.
func ShareTenancyCosts(p Provider) bool {
	config, err := p.GetConfig()
	if err != nil {
		return false
	}

	return config.ShareTenancyCosts == "true"
}

// NewProvider looks at the nodespec or provider metadata server to decide which provider to instantiate.
func NewProvider(cache clustercache.ClusterCache, apiKey string, config *config.ConfigFileManager) (Provider, error) {
	nodes := cache.GetAllNodes()
	if len(nodes) == 0 {
		log.Infof("Could not locate any nodes for cluster.") // valid in ETL readonly mode
		return &CustomProvider{
			Clientset: cache,
			Config:    NewProviderConfig(config, "default.json"),
		}, nil
	}

	cp := getClusterProperties(nodes[0])

	switch cp.provider {
	case kubecost.CSVProvider:
		log.Infof("Using CSV Provider with CSV at %s", env.GetCSVPath())
		return &CSVProvider{
			CSVLocation: env.GetCSVPath(),
			CustomProvider: &CustomProvider{
				Clientset: cache,
				Config:    NewProviderConfig(config, cp.configFileName),
			},
		}, nil
	case kubecost.GCPProvider:
		log.Info("metadata reports we are in GCE")
		if apiKey == "" {
			return nil, errors.New("Supply a GCP Key to start getting data")
		}
		return &GCP{
			Clientset:        cache,
			APIKey:           apiKey,
			Config:           NewProviderConfig(config, cp.configFileName),
			clusterRegion:    cp.region,
			clusterProjectId: cp.projectID,
			metadataClient: metadata.NewClient(&http.Client{
				Transport: httputil.NewUserAgentTransport("kubecost", http.DefaultTransport),
			}),
		}, nil
	case kubecost.AWSProvider:
		log.Info("Found ProviderID starting with \"aws\", using AWS Provider")
		return &AWS{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			clusterRegion:        cp.region,
			clusterAccountId:     cp.accountID,
			serviceAccountChecks: NewServiceAccountChecks(),
		}, nil
	case kubecost.AzureProvider:
		log.Info("Found ProviderID starting with \"azure\", using Azure Provider")
		return &Azure{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			clusterRegion:        cp.region,
			clusterAccountId:     cp.accountID,
			serviceAccountChecks: NewServiceAccountChecks(),
		}, nil
	case kubecost.AlibabaProvider:
		log.Info("Found ProviderID starting with \"alibaba\", using Alibaba Cloud Provider")
		return &Alibaba{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			clusterRegion:        cp.region,
			clusterAccountId:     cp.accountID,
			serviceAccountChecks: NewServiceAccountChecks(),
		}, nil
	case kubecost.ScalewayProvider:
		log.Info("Found ProviderID starting with \"scaleway\", using Scaleway Provider")
		return &Scaleway{
			Clientset: cache,
			Config:    NewProviderConfig(config, cp.configFileName),
		}, nil

	default:
		log.Info("Unsupported provider, falling back to default")
		return &CustomProvider{
			Clientset: cache,
			Config:    NewProviderConfig(config, cp.configFileName),
		}, nil
	}
}

type clusterProperties struct {
	provider       string
	configFileName string
	region         string
	accountID      string
	projectID      string
}

func getClusterProperties(node *v1.Node) clusterProperties {
	providerID := strings.ToLower(node.Spec.ProviderID)
	region, _ := util.GetRegion(node.Labels)
	cp := clusterProperties{
		provider:       "DEFAULT",
		configFileName: "default.json",
		region:         region,
		accountID:      "",
		projectID:      "",
	}
	if metadata.OnGCE() {
		cp.provider = kubecost.GCPProvider
		cp.configFileName = "gcp.json"
		cp.projectID = parseGCPProjectID(providerID)
	} else if strings.HasPrefix(providerID, "aws") {
		cp.provider = kubecost.AWSProvider
		cp.configFileName = "aws.json"
	} else if strings.HasPrefix(providerID, "azure") {
		cp.provider = kubecost.AzureProvider
		cp.configFileName = "azure.json"
		cp.accountID = parseAzureSubscriptionID(providerID)
	} else if strings.HasPrefix(providerID, "scaleway") { // the scaleway provider ID looks like scaleway://instance/<instance_id>
		cp.provider = kubecost.ScalewayProvider
		cp.configFileName = "scaleway.json"
	} else if strings.Contains(node.Status.NodeInfo.KubeletVersion, "aliyun") { // provider ID is not prefix with any distinct keyword like other providers
		cp.provider = kubecost.AlibabaProvider
		cp.configFileName = "alibaba.json"
	}
	if env.IsUseCSVProvider() {
		cp.provider = kubecost.CSVProvider
	}

	return cp
}

func UpdateClusterMeta(cluster_id, cluster_name string) error {
	pw := env.GetRemotePW()
	address := env.GetSQLAddress()
	connStr := fmt.Sprintf("postgres://postgres:%s@%s:5432?sslmode=disable", pw, address)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()
	updateStmt := `UPDATE names SET cluster_name = $1 WHERE cluster_id = $2;`
	_, err = db.Exec(updateStmt, cluster_name, cluster_id)
	if err != nil {
		return err
	}
	return nil
}

func CreateClusterMeta(cluster_id, cluster_name string) error {
	pw := env.GetRemotePW()
	address := env.GetSQLAddress()
	connStr := fmt.Sprintf("postgres://postgres:%s@%s:5432?sslmode=disable", pw, address)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, stmt := range createTableStatements {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}
	insertStmt := `INSERT INTO names (cluster_id, cluster_name) VALUES ($1, $2);`
	_, err = db.Exec(insertStmt, cluster_id, cluster_name)
	if err != nil {
		return err
	}
	return nil
}

func GetClusterMeta(cluster_id string) (string, string, error) {
	pw := env.GetRemotePW()
	address := env.GetSQLAddress()
	connStr := fmt.Sprintf("postgres://postgres:%s@%s:5432?sslmode=disable", pw, address)
	db, err := sql.Open("postgres", connStr)
	defer db.Close()
	query := `SELECT cluster_id, cluster_name
	FROM names
	WHERE cluster_id = ?`

	rows, err := db.Query(query, cluster_id)
	if err != nil {
		return "", "", err
	}
	defer rows.Close()
	var (
		sql_cluster_id string
		cluster_name   string
	)
	for rows.Next() {
		if err := rows.Scan(&sql_cluster_id, &cluster_name); err != nil {
			return "", "", err
		}
	}

	return sql_cluster_id, cluster_name, nil
}

func GetOrCreateClusterMeta(cluster_id, cluster_name string) (string, string, error) {
	id, name, err := GetClusterMeta(cluster_id)
	if err != nil {
		err := CreateClusterMeta(cluster_id, cluster_name)
		if err != nil {
			return "", "", err
		}
	}
	if id == "" {
		err := CreateClusterMeta(cluster_id, cluster_name)
		if err != nil {
			return "", "", err
		}
	}

	return id, name, nil
}

var (
	// It's of the form aws:///us-east-2a/i-0fea4fd46592d050b and we want i-0fea4fd46592d050b, if it exists
	providerAWSRegex = regexp.MustCompile("aws://[^/]*/[^/]*/([^/]+)")
	// gce://guestbook-227502/us-central1-a/gke-niko-n1-standard-2-wljla-8df8e58a-hfy7
	//  => gke-niko-n1-standard-2-wljla-8df8e58a-hfy7
	providerGCERegex = regexp.MustCompile("gce://[^/]*/[^/]*/([^/]+)")
	// Capture "vol-0fc54c5e83b8d2b76" from "aws://us-east-2a/vol-0fc54c5e83b8d2b76"
	persistentVolumeAWSRegex = regexp.MustCompile("aws:/[^/]*/[^/]*/([^/]+)")
	// Capture "ad9d88195b52a47c89b5055120f28c58" from "ad9d88195b52a47c89b5055120f28c58-1037804914.us-east-2.elb.amazonaws.com"
	loadBalancerAWSRegex = regexp.MustCompile("^([^-]+)-.+amazonaws\\.com$")
)

// ParseID attempts to parse a ProviderId from a string based on formats from the various providers and
// returns the string as is if it cannot find a match
func ParseID(id string) string {
	match := providerAWSRegex.FindStringSubmatch(id)
	if len(match) >= 2 {
		return match[1]
	}

	match = providerGCERegex.FindStringSubmatch(id)
	if len(match) >= 2 {
		return match[1]
	}

	// Return id for Azure Provider, CSV Provider and Custom Provider
	return id
}

// ParsePVID attempts to parse a PV ProviderId from a string based on formats from the various providers and
// returns the string as is if it cannot find a match
func ParsePVID(id string) string {
	match := persistentVolumeAWSRegex.FindStringSubmatch(id)
	if len(match) >= 2 {
		return match[1]
	}

	// Return id for GCP Provider, Azure Provider, CSV Provider and Custom Provider
	return id
}

// ParseLBID attempts to parse a LB ProviderId from a string based on formats from the various providers and
// returns the string as is if it cannot find a match
func ParseLBID(id string) string {
	match := loadBalancerAWSRegex.FindStringSubmatch(id)
	if len(match) >= 2 {
		return match[1]
	}

	// Return id for GCP Provider, Azure Provider, CSV Provider and Custom Provider
	return id
}
