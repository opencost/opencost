package cloud

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"

	"k8s.io/klog"

	"cloud.google.com/go/compute/metadata"
	"github.com/kubecost/cost-model/clustercache"

	v1 "k8s.io/api/core/v1"
)

const clusterIDKey = "CLUSTER_ID"
const remoteEnabled = "REMOTE_WRITE_ENABLED"
const remotePW = "REMOTE_WRITE_PASSWORD"
const sqlAddress = "SQL_ADDRESS"

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
	Reserved         *ReservedInstanceData `json:"reserved,omitempty"`
}

// IsSpot determines whether or not a Node uses spot by usage type
func (n *Node) IsSpot() bool {
	return strings.Contains(n.UsageType, "spot") || strings.Contains(n.UsageType, "emptible")
}

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
	Provider              string            `json:"provider"`
	Description           string            `json:"description"`
	CPU                   string            `json:"CPU"`
	SpotCPU               string            `json:"spotCPU"`
	RAM                   string            `json:"RAM"`
	SpotRAM               string            `json:"spotRAM"`
	GPU                   string            `json:"GPU"`
	SpotGPU               string            `json:"spotGPU"`
	Storage               string            `json:"storage"`
	ZoneNetworkEgress     string            `json:"zoneNetworkEgress"`
	RegionNetworkEgress   string            `json:"regionNetworkEgress"`
	InternetNetworkEgress string            `json:"internetNetworkEgress"`
	SpotLabel             string            `json:"spotLabel,omitempty"`
	SpotLabelValue        string            `json:"spotLabelValue,omitempty"`
	GpuLabel              string            `json:"gpuLabel,omitempty"`
	GpuLabelValue         string            `json:"gpuLabelValue,omitempty"`
	ServiceKeyName        string            `json:"awsServiceKeyName,omitempty"`
	ServiceKeySecret      string            `json:"awsServiceKeySecret,omitempty"`
	SpotDataRegion        string            `json:"awsSpotDataRegion,omitempty"`
	SpotDataBucket        string            `json:"awsSpotDataBucket,omitempty"`
	SpotDataPrefix        string            `json:"awsSpotDataPrefix,omitempty"`
	ProjectID             string            `json:"projectID,omitempty"`
	AthenaBucketName      string            `json:"athenaBucketName"`
	AthenaRegion          string            `json:"athenaRegion"`
	AthenaDatabase        string            `json:"athenaDatabase"`
	AthenaTable           string            `json:"athenaTable"`
	BillingDataDataset    string            `json:"billingDataDataset,omitempty"`
	CustomPricesEnabled   string            `json:"customPricesEnabled"`
	DefaultIdle           string            `json:"defaultIdle"`
	AzureSubscriptionID   string            `json:"azureSubscriptionID"`
	AzureClientID         string            `json:"azureClientID"`
	AzureClientSecret     string            `json:"azureClientSecret"`
	AzureTenantID         string            `json:"azureTenantID"`
	AzureBillingRegion    string            `json:"azureBillingRegion"`
	CurrencyCode          string            `json:"currencyCode"`
	Discount              string            `json:"discount"`
	NegotiatedDiscount    string            `json:"negotiatedDiscount"`
	SharedCosts           map[string]string `json:"sharedCost"`
	ClusterName           string            `json:"clusterName"`
	SharedNamespaces      string            `json:"sharedNamespaces"`
}

// Provider represents a k8s provider.
type Provider interface {
	ClusterInfo() (map[string]string, error)
	AddServiceKey(url.Values) error
	GetDisks() ([]byte, error)
	NodePricing(Key) (*Node, error)
	PVPricing(PVKey) (*PV, error)
	NetworkPricing() (*Network, error)
	AllNodePricing() (interface{}, error)
	DownloadPricingData() error
	GetKey(map[string]string) Key
	GetPVKey(*v1.PersistentVolume, map[string]string) PVKey
	UpdateConfig(r io.Reader, updateType string) (*CustomPricing, error)
	GetConfig() (*CustomPricing, error)
	GetManagementPlatform() (string, error)
	GetLocalStorageQuery(offset string) (string, error)
	ExternalAllocations(string, string, string, string, string) ([]*OutOfClusterAllocation, error)
	ApplyReservedInstancePricing(map[string]*Node)
}

// ClusterName returns the name defined in cluster info, defaulting to the
// CLUSTER_ID environment variable
func ClusterName(p Provider) string {
	info, err := p.ClusterInfo()
	if err != nil {
		return os.Getenv(clusterIDKey)
	}

	name, ok := info["name"]
	if !ok {
		return os.Getenv(clusterIDKey)
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

// GetDefaultPricingData will search for a json file representing pricing data in /models/ and use it for base pricing info.
func GetDefaultPricingData(fname string) (*CustomPricing, error) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/models/"
	}
	path += fname
	if _, err := os.Stat(path); err == nil {
		jsonFile, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer jsonFile.Close()
		byteValue, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			return nil, err
		}
		var customPricing = &CustomPricing{}
		err = json.Unmarshal([]byte(byteValue), customPricing)
		if err != nil {
			return nil, err
		}
		return customPricing, nil
	} else if os.IsNotExist(err) {
		c := &CustomPricing{
			Provider:              fname,
			Description:           "Default prices based on GCP us-central1",
			CPU:                   "0.031611",
			SpotCPU:               "0.006655",
			RAM:                   "0.004237",
			SpotRAM:               "0.000892",
			GPU:                   "0.95",
			Storage:               "0.00005479452",
			ZoneNetworkEgress:     "0.01",
			RegionNetworkEgress:   "0.01",
			InternetNetworkEgress: "0.12",
			CustomPricesEnabled:   "false",
		}
		cj, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}

		err = ioutil.WriteFile(path, cj, 0644)
		if err != nil {
			return nil, err
		}
		return c, nil
	} else {
		return nil, err
	}
}

func SetCustomPricingField(obj *CustomPricing, name string, value string) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return fmt.Errorf("Provided value type didn't match custom pricing field type")
	}

	structFieldValue.Set(val)
	return nil
}

// NewProvider looks at the nodespec or provider metadata server to decide which provider to instantiate.
func NewProvider(cache clustercache.ClusterCache, apiKey string) (Provider, error) {
	if metadata.OnGCE() {
		klog.V(3).Info("metadata reports we are in GCE")
		if apiKey == "" {
			return nil, errors.New("Supply a GCP Key to start getting data")
		}
		return &GCP{
			Clientset: cache,
			APIKey:    apiKey,
		}, nil
	}

	nodes := cache.GetAllNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("Could not locate any nodes for cluster.")
	}

	provider := strings.ToLower(nodes[0].Spec.ProviderID)
	if strings.HasPrefix(provider, "aws") {
		klog.V(2).Info("Found ProviderID starting with \"aws\", using AWS Provider")
		return &AWS{
			Clientset: cache,
		}, nil
	} else if strings.HasPrefix(provider, "azure") {
		klog.V(2).Info("Found ProviderID starting with \"azure\", using Azure Provider")
		return &Azure{
			Clientset: cache,
		}, nil
	} else {
		klog.V(2).Info("Unsupported provider, falling back to default")
		return &CustomProvider{
			Clientset: cache,
		}, nil
	}
}

func UpdateClusterMeta(cluster_id, cluster_name string) error {
	pw := os.Getenv(remotePW)
	address := os.Getenv(sqlAddress)
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
	pw := os.Getenv(remotePW)
	address := os.Getenv(sqlAddress)
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
	pw := os.Getenv(remotePW)
	address := os.Getenv(sqlAddress)
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
