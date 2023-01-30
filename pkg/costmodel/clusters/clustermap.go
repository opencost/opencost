package clusters

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/env"

	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/prom"
	"github.com/opencost/opencost/pkg/thanos"
	"github.com/opencost/opencost/pkg/util/retry"

	prometheus "github.com/prometheus/client_golang/api"
)

const (
	LoadRetries    int           = 6
	LoadRetryDelay time.Duration = 10 * time.Second
)

// The following constants are used as keys into the cluster info map data structure
const (
	ClusterInfoIdKey               = "id"
	ClusterInfoNameKey             = "name"
	ClusterInfoProviderKey         = "provider"
	ClusterInfoProjectKey          = "project"
	ClusterInfoAccountKey          = "account"
	ClusterInfoRegionKey           = "region"
	ClusterInfoProvisionerKey      = "provisioner"
	ClusterInfoProfileKey          = "clusterProfile"
	ClusterInfoLogCollectionKey    = "logCollection"
	ClusterInfoProductAnalyticsKey = "productAnalytics"
	ClusterInfoErrorReportingKey   = "errorReporting"
	ClusterInfoValuesReportingKey  = "valuesReporting"
	ClusterInfoThanosEnabledKey    = "thanosEnabled"
	ClusterInfoThanosOffsetKey     = "thanosOffset"
	ClusterInfoVersionKey          = "version"
)

// prometheus query offset to apply to each non-range query
// package scope to prevent calling duration parse each use
var promQueryOffset = env.GetPrometheusQueryOffset()

// ClusterInfo holds attributes of Cluster from metrics pulled from Prometheus
type ClusterInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Profile     string `json:"profile"`
	Provider    string `json:"provider"`
	Account     string `json:"account"`
	Project     string `json:"project"`
	Region      string `json:"region"`
	Provisioner string `json:"provisioner"`
}

// Clone creates a copy of ClusterInfo and returns it
func (ci *ClusterInfo) Clone() *ClusterInfo {
	if ci == nil {
		return nil
	}

	return &ClusterInfo{
		ID:          ci.ID,
		Name:        ci.Name,
		Profile:     ci.Profile,
		Provider:    ci.Provider,
		Account:     ci.Account,
		Project:     ci.Project,
		Region:      ci.Region,
		Provisioner: ci.Provisioner,
	}
}

type ClusterMap interface {
	// GetClusterIDs returns a slice containing all of the cluster identifiers.
	GetClusterIDs() []string

	// AsMap returns the cluster map as a standard go map
	AsMap() map[string]*ClusterInfo

	// InfoFor returns the ClusterInfo entry for the provided clusterID or nil if it
	// doesn't exist
	InfoFor(clusterID string) *ClusterInfo

	// NameFor returns the name of the cluster provided the clusterID.
	NameFor(clusterID string) string

	// NameIDFor returns an identifier in the format "<clusterName>/<clusterID>" if the cluster has an
	// assigned name. Otherwise, just the clusterID is returned.
	NameIDFor(clusterID string) string
}

// ClusterInfoProvider is a contract which is capable of performing cluster info lookups.
type ClusterInfoProvider interface {
	// GetClusterInfo returns a string map containing the local/remote connected cluster info
	GetClusterInfo() map[string]string
}

// ClusterMap keeps records of all known cost-model clusters.
type PrometheusClusterMap struct {
	lock        sync.RWMutex
	client      prometheus.Client
	clusters    map[string]*ClusterInfo
	clusterInfo ClusterInfoProvider
	stop        chan struct{}
}

// NewClusterMap creates a new ClusterMap implementation using a prometheus or thanos client
func NewClusterMap(client prometheus.Client, cip ClusterInfoProvider, refresh time.Duration) ClusterMap {
	stop := make(chan struct{})

	cm := &PrometheusClusterMap{
		client:      client,
		clusters:    make(map[string]*ClusterInfo),
		clusterInfo: cip,
		stop:        stop,
	}

	// Run an updater to ensure cluster data stays relevant over time
	go func() {
		// Immediately Attempt to refresh the clusters
		cm.refreshClusters()

		// Tick on interval and refresh clusters
		ticker := time.NewTicker(refresh)
		for {
			select {
			case <-ticker.C:
				cm.refreshClusters()
			case <-cm.stop:
				log.Infof("ClusterMap refresh stopped.")
				return
			}
		}
	}()

	return cm
}

// clusterInfoQuery returns the query string to load cluster info
func clusterInfoQuery(offset string) string {
	return fmt.Sprintf("kubecost_cluster_info%s", offset)
}

// loadClusters loads all the cluster info to map
func (pcm *PrometheusClusterMap) loadClusters() (map[string]*ClusterInfo, error) {
	var offset string = ""
	if prom.IsThanos(pcm.client) {
		offset = thanos.QueryOffset()
	}

	// Execute Query
	tryQuery := func() (interface{}, error) {
		ctx := prom.NewNamedContext(pcm.client, prom.ClusterMapContextName)
		resCh := ctx.QueryAtTime(clusterInfoQuery(offset), time.Now().Add(-promQueryOffset))
		r, e := resCh.Await()
		return r, e
	}

	// Retry on failure
	result, err := retry.Retry(context.Background(), tryQuery, uint(LoadRetries), LoadRetryDelay)

	qr, ok := result.([]*prom.QueryResult)
	if !ok || err != nil {
		return nil, err
	}

	clusters := make(map[string]*ClusterInfo)

	// Load the query results. Critical fields are id and name.
	for _, result := range qr {
		id, err := result.GetString("id")
		if err != nil {
			log.Warnf("Failed to load 'id' field for ClusterInfo")
			continue
		}

		name, err := result.GetString("name")
		if err != nil {
			log.Warnf("Failed to load 'name' field for ClusterInfo")
			continue
		}

		profile, err := result.GetString("clusterprofile")
		if err != nil {
			profile = ""
		}

		provider, err := result.GetString("provider")
		if err != nil {
			provider = ""
		}

		account, err := result.GetString("account")
		if err != nil {
			account = ""
		}

		project, err := result.GetString("project")
		if err != nil {
			project = ""
		}

		region, err := result.GetString("region")
		if err != nil {
			region = ""
		}

		provisioner, err := result.GetString("provisioner")
		if err != nil {
			provisioner = ""
		}

		clusters[id] = &ClusterInfo{
			ID:          id,
			Name:        name,
			Profile:     profile,
			Provider:    provider,
			Account:     account,
			Project:     project,
			Region:      region,
			Provisioner: provisioner,
		}
	}

	// populate the local cluster if it doesn't exist
	localInfo, err := pcm.getLocalClusterInfo()
	if err != nil {
		return clusters, nil
	}

	// Check to see if the local cluster's id is part of our loaded clusters, and include if not
	if _, ok := clusters[localInfo.ID]; !ok {
		clusters[localInfo.ID] = localInfo
	}

	return clusters, nil
}

// getLocalClusterInfo returns the local cluster info in the event there does not exist a metric available.
func (pcm *PrometheusClusterMap) getLocalClusterInfo() (*ClusterInfo, error) {
	info := pcm.clusterInfo.GetClusterInfo()

	clusterInfo, err := MapToClusterInfo(info)
	if err != nil {
		return nil, fmt.Errorf("Parsing Local Cluster Info Failed: %s", err)
	}

	return clusterInfo, nil
}

// refreshClusters loads the clusters and updates the internal map
func (pcm *PrometheusClusterMap) refreshClusters() {
	updated, err := pcm.loadClusters()
	if err != nil {
		log.Errorf("Failed to load cluster info via query after %d retries", LoadRetries)
		return
	}

	pcm.lock.Lock()
	pcm.clusters = updated
	pcm.lock.Unlock()
}

// GetClusterIDs returns a slice containing all of the cluster identifiers.
func (pcm *PrometheusClusterMap) GetClusterIDs() []string {
	pcm.lock.RLock()
	defer pcm.lock.RUnlock()

	var clusterIDs []string
	for id := range pcm.clusters {
		clusterIDs = append(clusterIDs, id)
	}

	return clusterIDs
}

// AsMap returns the cluster map as a standard go map
func (pcm *PrometheusClusterMap) AsMap() map[string]*ClusterInfo {
	pcm.lock.RLock()
	defer pcm.lock.RUnlock()

	m := make(map[string]*ClusterInfo)
	for k, v := range pcm.clusters {
		m[k] = v.Clone()
	}

	return m
}

// InfoFor returns the ClusterInfo entry for the provided clusterID or nil if it
// doesn't exist
func (pcm *PrometheusClusterMap) InfoFor(clusterID string) *ClusterInfo {
	pcm.lock.RLock()
	defer pcm.lock.RUnlock()

	if info, ok := pcm.clusters[clusterID]; ok {
		return info.Clone()
	}

	return nil
}

// NameFor returns the name of the cluster provided the clusterID.
func (pcm *PrometheusClusterMap) NameFor(clusterID string) string {
	pcm.lock.RLock()
	defer pcm.lock.RUnlock()

	if info, ok := pcm.clusters[clusterID]; ok {
		return info.Name
	}

	return ""
}

// NameIDFor returns an identifier in the format "<clusterName>/<clusterID>" if the cluster has an
// assigned name. Otherwise, just the clusterID is returned.
func (pcm *PrometheusClusterMap) NameIDFor(clusterID string) string {
	pcm.lock.RLock()
	defer pcm.lock.RUnlock()

	if info, ok := pcm.clusters[clusterID]; ok {
		if info.Name == "" {
			return clusterID
		}

		return fmt.Sprintf("%s/%s", info.Name, clusterID)
	}

	return clusterID
}

// SplitNameID is a helper method that removes the common split format and returns
func SplitNameID(nameID string) (id string, name string) {
	if !strings.Contains(nameID, "/") {
		id = nameID
		name = ""
		return
	}

	split := strings.Split(nameID, "/")
	name = split[0]
	id = split[1]
	return
}

// StopRefresh stops the automatic internal map refresh
func (pcm *PrometheusClusterMap) StopRefresh() {
	if pcm.stop != nil {
		close(pcm.stop)
		pcm.stop = nil
	}
}

// MapToClusterInfo returns a ClusterInfo using parsed data from a string map. If
// parsing the map fails for id and/or name, an error is returned.
func MapToClusterInfo(info map[string]string) (*ClusterInfo, error) {
	var id string
	var name string

	if i, ok := info[ClusterInfoIdKey]; ok {
		id = i
	} else {
		return nil, fmt.Errorf("Cluster Info Missing ID")
	}
	if n, ok := info[ClusterInfoNameKey]; ok {
		name = n
	} else {
		name = id
	}

	var clusterProfile string
	var provider string
	var account string
	var project string
	var region string
	var provisioner string

	if cp, ok := info[ClusterInfoProfileKey]; ok {
		clusterProfile = cp
	}

	if pvdr, ok := info[ClusterInfoProviderKey]; ok {
		provider = pvdr
	}

	if acct, ok := info[ClusterInfoAccountKey]; ok {
		account = acct
	}

	if proj, ok := info[ClusterInfoProjectKey]; ok {
		project = proj
	}

	if reg, ok := info[ClusterInfoRegionKey]; ok {
		region = reg
	}

	if pvsr, ok := info[ClusterInfoProvisionerKey]; ok {
		provisioner = pvsr
	}

	return &ClusterInfo{
		ID:          id,
		Name:        name,
		Profile:     clusterProfile,
		Provider:    provider,
		Account:     account,
		Project:     project,
		Region:      region,
		Provisioner: provisioner,
	}, nil
}
