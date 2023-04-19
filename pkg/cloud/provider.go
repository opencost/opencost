package cloud

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/opencost/opencost/pkg/cloud/types"
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

const KarpenterCapacityTypeLabel = "karpenter.sh/capacity-type"
const KarpenterCapacitySpotTypeValue = "spot"

var toTitle = cases.Title(language.Und, cases.NoLower)

var createTableStatements = []string{
	`CREATE TABLE IF NOT EXISTS names (
		cluster_id VARCHAR(255) NOT NULL,
		cluster_name VARCHAR(255) NULL,
		PRIMARY KEY (cluster_id)
	);`,
}

// ClusterName returns the name defined in cluster info, defaulting to the
// CLUSTER_ID environment variable
func ClusterName(p types.Provider) string {
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
func CustomPricesEnabled(p types.Provider) bool {
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
func ConfigWatcherFor(p types.Provider) *watcher.ConfigMapWatcher {
	return &watcher.ConfigMapWatcher{
		ConfigMapName: env.GetPricingConfigmapName(),
		WatchFunc: func(name string, data map[string]string) error {
			_, err := p.UpdateConfigFromConfigMap(data)
			return err
		},
	}
}

// AllocateIdleByDefault returns true if the application settings specify to allocate idle by default
func AllocateIdleByDefault(p types.Provider) bool {
	config, err := p.GetConfig()
	if err != nil {
		return false
	}

	return config.DefaultIdle == "true"
}

// SharedNamespace returns a list of names of shared namespaces, as defined in the application settings
func SharedNamespaces(p types.Provider) []string {
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
func SharedLabels(p types.Provider) ([]string, []string) {
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
func ShareTenancyCosts(p types.Provider) bool {
	config, err := p.GetConfig()
	if err != nil {
		return false
	}

	return config.ShareTenancyCosts == "true"
}

// NewProvider looks at the nodespec or provider metadata server to decide which provider to instantiate.
func NewProvider(cache clustercache.ClusterCache, apiKey string, config *config.ConfigFileManager) (types.Provider, error) {
	nodes := cache.GetAllNodes()
	if len(nodes) == 0 {
		log.Infof("Could not locate any nodes for cluster.") // valid in ETL readonly mode
		return &CustomProvider{
			Clientset: cache,
			Config:    NewProviderConfig(config, "default.json"),
		}, nil
	}

	cp := getClusterProperties(nodes[0])
	providerConfig := NewProviderConfig(config, cp.configFileName)
	// If ClusterAccount is set apply it to the cluster properties
	if providerConfig.customPricing != nil && providerConfig.customPricing.ClusterAccountID != "" {
		cp.accountID = providerConfig.customPricing.ClusterAccountID
	}

	switch cp.provider {
	case kubecost.CSVProvider:
		log.Infof("Using CSV Provider with CSV at %s", env.GetCSVPath())
		return &CSVProvider{
			CSVLocation: env.GetCSVPath(),
			CustomProvider: &CustomProvider{
				Clientset:        cache,
				clusterRegion:    cp.region,
				clusterAccountID: cp.accountID,
				Config:           NewProviderConfig(config, cp.configFileName),
			},
		}, nil
	case kubecost.GCPProvider:
		log.Info("Found ProviderID starting with \"gce\", using GCP Provider")
		if apiKey == "" {
			return nil, errors.New("Supply a GCP Key to start getting data")
		}
		return &GCP{
			Clientset:        cache,
			APIKey:           apiKey,
			Config:           NewProviderConfig(config, cp.configFileName),
			clusterRegion:    cp.region,
			clusterAccountID: cp.accountID,
			clusterProjectID: cp.projectID,
			metadataClient: metadata.NewClient(
				&http.Client{
					Transport: httputil.NewUserAgentTransport("kubecost", &http.Transport{
						Dial: (&net.Dialer{
							Timeout:   2 * time.Second,
							KeepAlive: 30 * time.Second,
						}).Dial,
					}),
					Timeout: 5 * time.Second,
				}),
		}, nil
	case kubecost.AWSProvider:
		log.Info("Found ProviderID starting with \"aws\", using AWS Provider")
		return &AWS{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			clusterRegion:        cp.region,
			clusterAccountID:     cp.accountID,
			serviceAccountChecks: types.NewServiceAccountChecks(),
		}, nil
	case kubecost.AzureProvider:
		log.Info("Found ProviderID starting with \"azure\", using Azure Provider")
		return &Azure{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			clusterRegion:        cp.region,
			clusterAccountID:     cp.accountID,
			serviceAccountChecks: types.NewServiceAccountChecks(),
		}, nil
	case kubecost.AlibabaProvider:
		log.Info("Found ProviderID starting with \"alibaba\", using Alibaba Cloud Provider")
		return &Alibaba{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			clusterRegion:        cp.region,
			clusterAccountId:     cp.accountID,
			serviceAccountChecks: types.NewServiceAccountChecks(),
		}, nil
	case kubecost.ScalewayProvider:
		log.Info("Found ProviderID starting with \"scaleway\", using Scaleway Provider")
		return &Scaleway{
			Clientset:        cache,
			clusterRegion:    cp.region,
			clusterAccountID: cp.accountID,
			Config:           NewProviderConfig(config, cp.configFileName),
		}, nil

	default:
		log.Info("Unsupported provider, falling back to default")
		return &CustomProvider{
			Clientset:        cache,
			clusterRegion:    cp.region,
			clusterAccountID: cp.accountID,
			Config:           NewProviderConfig(config, cp.configFileName),
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
	// The second conditional is mainly if you're running opencost outside of GCE, say in a local environment.
	if metadata.OnGCE() || strings.HasPrefix(providerID, "gce") {
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
	if err != nil {
		return "", "", err
	}
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
