package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/util/retry"
	"github.com/opencost/opencost/pkg/cloud/alibaba"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/digitalocean"
	"github.com/opencost/opencost/pkg/cloud/gcp"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/cloud/oracle"
	"github.com/opencost/opencost/pkg/cloud/otc"
	"github.com/opencost/opencost/pkg/cloud/scaleway"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util"

	"cloud.google.com/go/compute/metadata"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/pkg/config"
	"github.com/opencost/opencost/pkg/env"
	"github.com/opencost/opencost/pkg/util/watcher"
)

// normalizeProviderName normalizes provider name variations to standard constants
// Handles cases like "CSVProvider" -> "CSV", case-insensitive matching, etc.
func normalizeProviderName(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "csvprovider", "csv":
		return opencost.CSVProvider
	case "custom", "customprovider":
		return opencost.CustomProvider
	case "aws", "awsprovider":
		return opencost.AWSProvider
	case "gcp", "gcpprovider", "google":
		return opencost.GCPProvider
	case "azure", "azureprovider":
		return opencost.AzureProvider
	case "alibaba", "alibabaprovider":
		return opencost.AlibabaProvider
	case "oracle", "oracleprovider":
		return opencost.OracleProvider
	case "scaleway", "scalewayprovider":
		return opencost.ScalewayProvider
	case "otc", "otcprovider":
		return opencost.OTCProvider
	default:
		// Return original if no match, let caller handle unknown providers
		return provider
	}
}

// CustomPricesEnabled returns the boolean equivalent of the cloup provider's custom prices flag,
// indicating whether or not the cluster is using custom pricing.
func CustomPricesEnabled(p models.Provider) bool {
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
func ConfigWatcherFor(p models.Provider) *watcher.ConfigMapWatcher {
	return &watcher.ConfigMapWatcher{
		ConfigMapName: env.GetPricingConfigmapName(),
		WatchFunc: func(name string, data map[string]string) error {
			_, err := p.UpdateConfigFromConfigMap(data)
			return err
		},
	}
}

// NewProvider looks at the nodespec or provider metadata server to decide which provider to instantiate.
func NewProvider(cache clustercache.ClusterCache, apiKey string, config *config.ConfigFileManager) (models.Provider, error) {
	getAllNodesFunc := func() ([]*clustercache.Node, error) {
		nodes := cache.GetAllNodes()
		if len(nodes) == 0 {
			return nil, fmt.Errorf("no nodes found in cluster cache")
		}
		return nodes, nil
	}

	var nodes []*clustercache.Node

	if env.HasKubernetesResourceAccess() {
		// the error can be ignored because getAllNodesFunc only errors if nodes is empty, a case which we explicitly
		// handle by checking the length of nodes below
		nodes, _ = retry.Retry(context.Background(), getAllNodesFunc, 10, time.Second)
	} else {
		nodes, _ = getAllNodesFunc()
	}

	if len(nodes) == 0 {
		log.Infof("Could not locate any nodes for cluster.")
		return &CustomProvider{
			Clientset: cache,
			Config:    NewProviderConfig(config, "default.json"),
		}, nil
	}

	cp := getClusterProperties(nodes[0])

	// If provider is DEFAULT, check for explicitly set cloud provider from environment variable
	envProvider := env.GetCloudProvider()
	if cp.provider == "DEFAULT" && envProvider != "" {
		log.Infof("Using cloud provider from environment variable: %s", envProvider)
		// Normalize provider name to handle variations (e.g., "CSVProvider" -> "CSV")
		normalizedProvider := normalizeProviderName(envProvider)
		log.Debugf("Normalized provider name from %s to %s", envProvider, normalizedProvider)
		cp.provider = normalizedProvider
		switch normalizedProvider {
		case opencost.AWSProvider:
			cp.configFileName = "aws.json"
		case opencost.AzureProvider:
			cp.configFileName = "azure.json"
		case opencost.GCPProvider:
			cp.configFileName = "gcp.json"
		case opencost.AlibabaProvider:
			cp.configFileName = "alibaba.json"
		case opencost.OracleProvider:
			cp.configFileName = "oracle.json"
		case opencost.ScalewayProvider:
			cp.configFileName = "scaleway.json"
		case opencost.OTCProvider:
			cp.configFileName = "otc.json"
		case opencost.CSVProvider:
			cp.configFileName = "default.json"
		case opencost.CustomProvider:
			cp.configFileName = "default.json"
		default:
			log.Warnf("Unrecognized provider from environment variable: %s (normalized: %s), falling back to default", envProvider, normalizedProvider)
		}
	}

	providerConfig := NewProviderConfig(config, cp.configFileName)
	// If ClusterAccount is set apply it to the cluster properties
	if providerConfig.customPricing != nil && providerConfig.customPricing.ClusterAccountID != "" {
		cp.accountID = providerConfig.customPricing.ClusterAccountID
	}

	// Check if custom pricing configuration specifies a provider override
	// This handles cases where Helm configuration sets customPricing.provider: CSVProvider
	if providerConfig.customPricing != nil && providerConfig.customPricing.Provider != "" && cp.provider == "DEFAULT" {
		customProvider := providerConfig.customPricing.Provider
		log.Infof("Using cloud provider from custom pricing configuration: %s", customProvider)
		// Normalize provider name to handle variations (e.g., "CSVProvider" -> "CSV")
		normalizedCustomProvider := normalizeProviderName(customProvider)
		log.Debugf("Normalized custom provider name from %s to %s", customProvider, normalizedCustomProvider)
		cp.provider = normalizedCustomProvider
		switch normalizedCustomProvider {
		case opencost.AWSProvider:
			cp.configFileName = "aws.json"
		case opencost.AzureProvider:
			cp.configFileName = "azure.json"
		case opencost.GCPProvider:
			cp.configFileName = "gcp.json"
		case opencost.AlibabaProvider:
			cp.configFileName = "alibaba.json"
		case opencost.OracleProvider:
			cp.configFileName = "oracle.json"
		case opencost.ScalewayProvider:
			cp.configFileName = "scaleway.json"
		case opencost.OTCProvider:
			cp.configFileName = "otc.json"
		case opencost.CSVProvider:
			cp.configFileName = "default.json"
		case opencost.CustomProvider:
			cp.configFileName = "default.json"
		default:
			log.Warnf("Unrecognized provider from custom pricing configuration: %s (normalized: %s), falling back to default", customProvider, normalizedCustomProvider)
		}
		// Reload provider config with potentially updated config file name
		providerConfig = NewProviderConfig(config, cp.configFileName)
		if providerConfig.customPricing != nil && providerConfig.customPricing.ClusterAccountID != "" {
			cp.accountID = providerConfig.customPricing.ClusterAccountID
		}
	}

	providerConfig.Update(func(cp *models.CustomPricing) error {
		if cp.ServiceKeyName == "AKIXXX" {
			cp.ServiceKeyName = ""
		}
		return nil
	})

	switch cp.provider {
	case opencost.CSVProvider:
		csvPath := env.GetCSVPath()
		log.Infof("Using CSV Provider with CSV at %s", csvPath)
		log.Debugf("CSV Provider configuration: region=%s, accountID=%s, configFile=%s", cp.region, cp.accountID, cp.configFileName)
		
		// Create CSV provider and trigger initial data download
		csvProvider := &CSVProvider{
			CSVLocation: csvPath,
			CustomProvider: &CustomProvider{
				Clientset:        cache,
				ClusterRegion:    cp.region,
				ClusterAccountID: cp.accountID,
				Config:           NewProviderConfig(config, cp.configFileName),
			},
		}
		
		// Download pricing data immediately to ensure CSV is loaded
		if err := csvProvider.DownloadPricingData(); err != nil {
			log.Warnf("Failed to download CSV pricing data on initialization: %v", err)
		} else {
			log.Infof("Successfully loaded CSV pricing data with %d node entries, %d GPU entries, %d GPU label entries", 
				len(csvProvider.Pricing), len(csvProvider.GPUClassPricing), len(csvProvider.GPULabelPricing))
		}
		
		return csvProvider, nil
	case opencost.GCPProvider:
		log.Info("Found ProviderID starting with \"gce\", using GCP Provider")
		if apiKey == "" {
			return nil, errors.New("Supply a GCP Key to start getting data")
		}
		return &gcp.GCP{
			Clientset:        cache,
			APIKey:           apiKey,
			Config:           NewProviderConfig(config, cp.configFileName),
			ClusterRegion:    cp.region,
			ClusterAccountID: cp.accountID,
			ClusterProjectID: cp.projectID,
			MetadataClient: metadata.NewClient(
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
	case opencost.AWSProvider:
		log.Info("Found ProviderID starting with \"aws\", using AWS Provider")
		return &aws.AWS{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			ClusterRegion:        cp.region,
			ClusterAccountID:     cp.accountID,
			ServiceAccountChecks: models.NewServiceAccountChecks(),
		}, nil
	case opencost.AzureProvider:
		log.Info("Found ProviderID starting with \"azure\", using Azure Provider")
		return &azure.Azure{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			ClusterRegion:        cp.region,
			ClusterAccountID:     cp.accountID,
			ServiceAccountChecks: models.NewServiceAccountChecks(),
		}, nil
	case opencost.AlibabaProvider:
		log.Info("Found ProviderID starting with \"alibaba\", using Alibaba Cloud Provider")
		return &alibaba.Alibaba{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			ClusterRegion:        cp.region,
			ClusterAccountId:     cp.accountID,
			ServiceAccountChecks: models.NewServiceAccountChecks(),
		}, nil
	case opencost.ScalewayProvider:
		log.Info("Found ProviderID starting with \"scaleway\", using Scaleway Provider")
		return &scaleway.Scaleway{
			Clientset:        cache,
			ClusterRegion:    cp.region,
			ClusterAccountID: cp.accountID,
			Config:           NewProviderConfig(config, cp.configFileName),
		}, nil
	case opencost.OracleProvider:
		log.Info("Found ProviderID starting with \"oracle\", using Oracle Provider")
		return &oracle.Oracle{
			Clientset:            cache,
			Config:               NewProviderConfig(config, cp.configFileName),
			ClusterRegion:        cp.region,
			ClusterAccountID:     cp.accountID,
			ServiceAccountChecks: models.NewServiceAccountChecks(),
		}, nil
	case opencost.OTCProvider:
		log.Info("Found node label \"cce.cloud.com/cce-nodepool\", using OTC Provider")
		return &otc.OTC{
			Clientset:     cache,
			Config:        NewProviderConfig(config, cp.configFileName),
			ClusterRegion: cp.region,
		}, nil
	case opencost.DigitalOceanProvider:
		log.Info("Detected DigitalOcean, using DOKS")
		return &digitalocean.DOKS{
			Config:                NewProviderConfig(config, cp.configFileName),
			Cache:                 digitalocean.NewPricingCache(),
			Products:              make(map[string][]digitalocean.DOProduct),
			Clientset:             cache,
			ClusterManagementCost: 0.0,
		}, nil
	default:
		log.Infof("Unsupported provider '%s', falling back to default CustomProvider", cp.provider)
		log.Debugf("Provider detection summary - envProvider: %s, customPricingProvider: %s, final: %s", 
			env.GetCloudProvider(), 
			func() string {
				if providerConfig.customPricing != nil {
					return providerConfig.customPricing.Provider
				}
				return "none"
			}(), 
			cp.provider)
		
		customProvider := &CustomProvider{
			Clientset:        cache,
			ClusterRegion:    cp.region,
			ClusterAccountID: cp.accountID,
			Config:           NewProviderConfig(config, cp.configFileName),
		}
		
		// Download pricing data for custom provider to ensure configuration is loaded
		if err := customProvider.DownloadPricingData(); err != nil {
			log.Warnf("Failed to download custom pricing data on initialization: %v", err)
		} else {
			log.Debugf("Successfully loaded custom pricing configuration")
		}
		
		return customProvider, nil
	}
}

type clusterProperties struct {
	provider       string
	configFileName string
	region         string
	accountID      string
	projectID      string
}

func getClusterProperties(node *clustercache.Node) clusterProperties {
	providerID := strings.ToLower(node.SpecProviderID)
	region, _ := util.GetRegion(node.Labels)
	cp := clusterProperties{
		provider:       "DEFAULT",
		configFileName: "default.json",
		region:         region,
		accountID:      "",
		projectID:      "",
	}

	// Check for custom provider settings
	if env.IsUseCustomProvider() {
		// Use CSV provider if set
		if env.IsUseCSVProvider() {
			log.Debug("using custom CSV provider")
			cp.provider = opencost.CSVProvider
		}
		return cp
	}

	// The second conditional is mainly if you're running opencost outside of GCE, say in a local environment.
	if metadata.OnGCE() || strings.HasPrefix(providerID, "gce") {
		log.Debug("using GCP provider")
		cp.provider = opencost.GCPProvider
		cp.configFileName = "gcp.json"
		cp.projectID = gcp.ParseGCPProjectID(providerID)
	} else if strings.HasPrefix(providerID, "aws") {
		log.Debug("using AWS provider")
		cp.provider = opencost.AWSProvider
		cp.configFileName = "aws.json"
	} else if strings.Contains(node.Status.NodeInfo.KubeletVersion, "eks") { // Additional check for EKS, via kubelet check
		log.Debug("using AWS provider from EKS")
		cp.provider = opencost.AWSProvider
		cp.configFileName = "aws.json"
	} else if strings.HasPrefix(providerID, "azure") {
		log.Debug("using Azure provider")
		cp.provider = opencost.AzureProvider
		cp.configFileName = "azure.json"
		cp.accountID = azure.ParseAzureSubscriptionID(providerID)
	} else if strings.HasPrefix(providerID, "scaleway") { // the scaleway provider ID looks like scaleway://instance/<instance_id>
		log.Debug("using Scaleway provider")
		cp.provider = opencost.ScalewayProvider
		cp.configFileName = "scaleway.json"
	} else if strings.Contains(node.Status.NodeInfo.KubeletVersion, "aliyun") { // provider ID is not prefix with any distinct keyword like other providers
		log.Debug("using Alibaba provider")
		cp.provider = opencost.AlibabaProvider
		cp.configFileName = "alibaba.json"
	} else if strings.HasPrefix(providerID, "ocid") {
		log.Debug("using Oracle provider")
		cp.provider = opencost.OracleProvider
		cp.configFileName = "oracle.json"
	} else if _, ok := node.Labels["cce.cloud.com/cce-nodepool"]; ok { // The node label "cce.cloud.com/cce-nodepool" exists
		log.Debug("using OTC provider")
		cp.provider = opencost.OTCProvider
		cp.configFileName = "otc.json"
	} else if strings.HasPrefix(providerID, "digitalocean") {
		log.Debug("using DigitalOcean provider")
		cp.provider = opencost.DigitalOceanProvider
		cp.configFileName = "digitalocean.json"
	}
	// Override provider to CSV if CSVProvider is used and custom provider is not set
	if env.IsUseCSVProvider() {
		log.Debug("using CSV provider")
		cp.provider = opencost.CSVProvider
	}

	return cp
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
	loadBalancerAWSRegex = regexp.MustCompile(`^([^-]+)-.+amazonaws\.com$`)
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

// ParseLocalDiskID attempts to parse a ProviderID from the ProviderID of the node that the local disk is running on
func ParseLocalDiskID(id string) string {
	// Parse like node
	id = ParseID(id)

	if strings.HasPrefix(id, "azure://") {

		// handle vmss ProviderID of type azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourceGroups/mc_test_eastus2/providers/Microsoft.Compute/virtualMachineScaleSets/aks-userpool-12345678-vmss/virtualMachines/11
		if strings.Contains(id, "virtualMachineScaleSets") {
			split := strings.Split(id, "/virtualMachineScaleSets/")
			// combine vmss name and number into a single string ending in a 6 character base 32 number
			vmSplit := strings.Split(split[1], "/")
			if len(vmSplit) != 3 {
				return id
			}
			vmNum, err := strconv.ParseInt(vmSplit[2], 10, 64)
			if err != nil {
				return id
			}

			id = fmt.Sprintf("%s/disks/%s%06s", split[0], vmSplit[0], strconv.FormatInt(vmNum, 32))
		}
		id = strings.Replace(id, "/virtualMachines/", "/disks/", -1)
		id = strings.ToLower(id)
		return fmt.Sprintf("%s_osdisk", id)
	}
	return id
}
