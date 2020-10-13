package clusters

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/kubecost/cost-model/pkg/log"
	"github.com/kubecost/cost-model/pkg/prom"
	"github.com/kubecost/cost-model/pkg/thanos"

	prometheus "github.com/prometheus/client_golang/api"
)

const (
	LoadRetries    int           = 6
	LoadRetryDelay time.Duration = 10 * time.Second
)

type ClusterInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Profile     string `json:"profile"`
	Provider    string `json:"provider"`
	Provisioner string `json:"provisioner"`
}

type ClusterMap interface {
	// GetClusterIDs returns a slice containing all of the cluster identifiers.
	GetClusterIDs() []string

	// InfoFor returns the ClusterInfo entry for the provided clusterID or nil if it
	// doesn't exist
	InfoFor(clusterID string) *ClusterInfo

	// NameFor returns the name of the cluster provided the clusterID.
	NameFor(clusterID string) string

	// NameIDFor returns an identifier in the format "<clusterName>/<clusterID>" if the cluster has an
	// assigned name. Otherwise, just the clusterID is returned.
	NameIDFor(clusterID string) string

	// SplitNameID splits the nameID back into a separate id and name field
	SplitNameID(nameID string) (id string, name string)

	// StopRefresh stops the automatic internal map refresh
	StopRefresh()
}

// ClusterMap keeps records of all known cost-model clusters.
type PrometheusClusterMap struct {
	lock     *sync.RWMutex
	client   prometheus.Client
	clusters map[string]*ClusterInfo
	stop     chan struct{}
}

// NewClusterMap creates a new ClusterMap implementation using a prometheus or thanos client
func NewClusterMap(client prometheus.Client, refresh time.Duration) ClusterMap {
	stop := make(chan struct{})

	cm := &PrometheusClusterMap{
		lock:     new(sync.RWMutex),
		client:   client,
		clusters: make(map[string]*ClusterInfo),
		stop:     stop,
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
	tryQuery := func() ([]*prom.QueryResult, error) {
		ctx := prom.NewContext(pcm.client)
		return ctx.QuerySync(clusterInfoQuery(offset))
	}

	var qr []*prom.QueryResult
	var err error

	// Retry on failure
	delay := LoadRetryDelay
	for r := LoadRetries; r > 0; r-- {
		qr, err = tryQuery()

		// non-error breaks out of loop
		if err == nil {
			break
		}

		// wait the delay
		time.Sleep(delay)

		// add some random backoff
		jitter := time.Duration(rand.Int63n(int64(delay)))
		delay = delay + jitter/2
	}

	if err != nil {
		return nil, err
	}

	clusters := make(map[string]*ClusterInfo)

	// Load the query results. Critical fields are id and name.
	for _, result := range qr {
		id, err := result.GetString("id")
		if err != nil {
			log.Warningf("Failed to load 'id' field for ClusterInfo")
			continue
		}

		name, err := result.GetString("name")
		if err != nil {
			log.Warningf("Failed to load 'name' field for ClusterInfo")
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

		provisioner, err := result.GetString("provisioner")
		if err != nil {
			provisioner = ""
		}

		clusters[id] = &ClusterInfo{
			ID:          id,
			Name:        name,
			Profile:     profile,
			Provider:    provider,
			Provisioner: provisioner,
		}
	}

	return clusters, nil
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

// InfoFor returns the ClusterInfo entry for the provided clusterID or nil if it
// doesn't exist
func (pcm *PrometheusClusterMap) InfoFor(clusterID string) *ClusterInfo {
	pcm.lock.RLock()
	defer pcm.lock.RUnlock()

	if info, ok := pcm.clusters[clusterID]; ok {
		return info
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

func (pcm *PrometheusClusterMap) SplitNameID(nameID string) (id string, name string) {
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
