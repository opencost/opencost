package cloud

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"k8s.io/klog"

	"cloud.google.com/go/compute/metadata"

	"github.com/kubecost/cost-model/pkg/clustercache"
	"github.com/kubecost/cost-model/pkg/env"

	v1 "k8s.io/api/core/v1"
)

const authSecretPath = "/var/secrets/service-key.json"
const storageConfigSecretPath = "/var/azure-storage-config/azure-storage-config.json"


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
	GPUType() string  // GPUType returns "" if no GPU exists, but the name of the GPU otherwise
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
	Provider                     string            `json:"provider"`
	Description                  string            `json:"description"`
	CPU                          string            `json:"CPU"`
	SpotCPU                      string            `json:"spotCPU"`
	RAM                          string            `json:"RAM"`
	SpotRAM                      string            `json:"spotRAM"`
	GPU                          string            `json:"GPU"`
	SpotGPU                      string            `json:"spotGPU"`
	Storage                      string            `json:"storage"`
	ZoneNetworkEgress            string            `json:"zoneNetworkEgress"`
	RegionNetworkEgress          string            `json:"regionNetworkEgress"`
	InternetNetworkEgress        string            `json:"internetNetworkEgress"`
	FirstFiveForwardingRulesCost string            `json:"firstFiveForwardingRulesCost"`
	AdditionalForwardingRuleCost string            `json:"additionalForwardingRuleCost"`
	LBIngressDataCost            string            `json:"LBIngressDataCost"`
	SpotLabel                    string            `json:"spotLabel,omitempty"`
	SpotLabelValue               string            `json:"spotLabelValue,omitempty"`
	GpuLabel                     string            `json:"gpuLabel,omitempty"`
	GpuLabelValue                string            `json:"gpuLabelValue,omitempty"`
	ServiceKeyName               string            `json:"awsServiceKeyName,omitempty"`
	ServiceKeySecret             string            `json:"awsServiceKeySecret,omitempty"`
	SpotDataRegion               string            `json:"awsSpotDataRegion,omitempty"`
	SpotDataBucket               string            `json:"awsSpotDataBucket,omitempty"`
	SpotDataPrefix               string            `json:"awsSpotDataPrefix,omitempty"`
	ProjectID                    string            `json:"projectID,omitempty"`
	AthenaProjectID              string            `json:"athenaProjectID,omitempty"`
	AthenaBucketName             string            `json:"athenaBucketName"`
	AthenaRegion                 string            `json:"athenaRegion"`
	AthenaDatabase               string            `json:"athenaDatabase"`
	AthenaTable                  string            `json:"athenaTable"`
	MasterPayerARN               string            `json:"masterPayerARN"`
	BillingDataDataset           string            `json:"billingDataDataset,omitempty"`
	CustomPricesEnabled          string            `json:"customPricesEnabled"`
	DefaultIdle                  string            `json:"defaultIdle"`
	AzureSubscriptionID          string            `json:"azureSubscriptionID"`
	AzureClientID                string            `json:"azureClientID"`
	AzureClientSecret            string            `json:"azureClientSecret"`
	AzureTenantID                string            `json:"azureTenantID"`
	AzureBillingRegion           string            `json:"azureBillingRegion"`
	CurrencyCode                 string            `json:"currencyCode"`
	Discount                     string            `json:"discount"`
	NegotiatedDiscount           string            `json:"negotiatedDiscount"`
	SharedCosts                  map[string]string `json:"sharedCost"`
	ClusterName                  string            `json:"clusterName"`
	SharedNamespaces             string            `json:"sharedNamespaces"`
	SharedLabelNames             string            `json:"sharedLabelNames"`
	SharedLabelValues            string            `json:"sharedLabelValues"`
	ReadOnly                     string            `json:"readOnly"`
}

type ServiceAccountStatus struct {
	Checks []*ServiceAccountCheck `json:"checks"`
}

type ServiceAccountCheck struct {
	Message        string `json:"message"`
	Status         bool   `json:"status"`
	AdditionalInfo string `json:additionalInfo`
}

type PricingSources struct {
	PricingSources map[string]*PricingSource
}

type PricingSource struct {
	Name      string `json:"name"`
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
	GetLocalStorageQuery(string, string, bool, bool) string
	ExternalAllocations(string, string, []string, string, string, bool) ([]*OutOfClusterAllocation, error)
	ApplyReservedInstancePricing(map[string]*Node)
	ServiceAccountStatus() *ServiceAccountStatus
	PricingSourceStatus() map[string]*PricingSource
	ClusterManagementPricing() (string, float64, error)
	CombinedDiscountForNode(string, bool, float64, float64) float64
	ParseID(string) string
	ParsePVID(string) string
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
	if config.NegotiatedDiscount == "" {
		config.NegotiatedDiscount = "0%"
	}

	return config.CustomPricesEnabled == "true"
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
		klog.V(2).Infof("[Warning] shared labels have mis-matched lengths: %d names, %d values", len(ks), len(vs))
		return names, values
	}

	for i := range ks {
		names = append(names, strings.Trim(ks[i], " "))
		values = append(values, strings.Trim(vs[i], " "))
	}

	return names, values
}

func NewCrossClusterProvider(ctype string, overrideConfigPath string, cache clustercache.ClusterCache) (Provider, error) {
	if ctype == "aws" {
		return &AWS{
			Clientset: cache,
			Config:    NewProviderConfig(overrideConfigPath),
		}, nil
	} else if ctype == "gcp" {
		return &GCP{
			Clientset: cache,
			Config:    NewProviderConfig(overrideConfigPath),
		}, nil
	}
	return &CustomProvider{
		Clientset: cache,
		Config:    NewProviderConfig(overrideConfigPath),
	}, nil
}

// NewProvider looks at the nodespec or provider metadata server to decide which provider to instantiate.
func NewProvider(cache clustercache.ClusterCache, apiKey string) (Provider, error) {
	nodes := cache.GetAllNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("Could not locate any nodes for cluster.")
	}

	provider := strings.ToLower(nodes[0].Spec.ProviderID)

	if env.IsUseCSVProvider() {
		klog.Infof("Using CSV Provider with CSV at %s", env.GetCSVPath())
		configFileName := ""
		if metadata.OnGCE() {
			configFileName = "gcp.json"
		} else if strings.HasPrefix(provider, "aws") {
			configFileName = "aws.json"
		} else if strings.HasPrefix(provider, "azure") {
			configFileName = "azure.json"

		} else {
			configFileName = "default.json"
		}
		return &CSVProvider{
			CSVLocation: env.GetCSVPath(),
			CustomProvider: &CustomProvider{
				Clientset: cache,
				Config:    NewProviderConfig(configFileName),
			},
		}, nil
	}
	if metadata.OnGCE() {
		klog.V(3).Info("metadata reports we are in GCE")
		if apiKey == "" {
			return nil, errors.New("Supply a GCP Key to start getting data")
		}
		return &GCP{
			Clientset: cache,
			APIKey:    apiKey,
			Config:    NewProviderConfig("gcp.json"),
		}, nil
	}

	if strings.HasPrefix(provider, "aws") {
		klog.V(2).Info("Found ProviderID starting with \"aws\", using AWS Provider")
		return &AWS{
			Clientset: cache,
			Config:    NewProviderConfig("aws.json"),
		}, nil
	} else if strings.HasPrefix(provider, "azure") {
		klog.V(2).Info("Found ProviderID starting with \"azure\", using Azure Provider")
		return &Azure{
			Clientset: cache,
			Config:    NewProviderConfig("azure.json"),
		}, nil
	} else {
		klog.V(2).Info("Unsupported provider, falling back to default")
		return &CustomProvider{
			Clientset: cache,
			Config:    NewProviderConfig("default.json"),
		}, nil
	}
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
