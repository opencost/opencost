package nodestats

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/worker"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type StatSummaryClient interface {
	GetNodeData() ([]*stats.Summary, error)
}

// NodeStatsSummaryClient is a client used to retrieve node and container stats summaries from a Kubernetes cluster,
// via communicating with the kubelet API on each node.
type NodeStatsSummaryClient struct {
	config          *NodeClientConfig
	directClient    *NodeHttpClient
	proxyClient     *NodeHttpClient
	cache           clustercache.ClusterCache
	endpoint        string
	clusterHostUrl  string
	bearerTokenFile string
}

// NewNodeStatsSummaryClient creates a new NodeStatsSummaryClient with the provided configuration and in-cluster config.
func NewNodeStatsSummaryClient(cache clustercache.ClusterCache, config *NodeClientConfig, inClusterConfig *rest.Config) *NodeStatsSummaryClient {
	return &NodeStatsSummaryClient{
		config:          config,
		directClient:    NewNodeHttpClient(&http.Client{Transport: config.Transport}),
		proxyClient:     NewNodeHttpClient(&http.Client{Transport: config.Transport}),
		cache:           cache,
		endpoint:        "stats/summary",
		clusterHostUrl:  inClusterConfig.Host,
		bearerTokenFile: inClusterConfig.BearerTokenFile,
	}
}

// GetNodeData creates a number of goroutines that attempt to access a specified endpoint and return the
// corresponding stats data in slice of interfaces which can be converted into a stricter format.
func (nssc *NodeStatsSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	var bearerToken string
	if !nssc.config.ProxyConfig.IsLocalProxy() {
		token, err := nssc.loadBearerToken()
		if err != nil {
			return nil, err
		}
		bearerToken = token
	}

	size := nssc.config.ConcurrentPollers
	nodes := getReadyNodes(nssc.cache)

	var errLock sync.Mutex
	var errs []error

	work := func(n *clustercache.Node) *stats.Summary {
		if n.SpecProviderID == "" {
			log.Warnf("node ProviderID not set, skipping for %s", n.Name)
			return nil
		}

		connections := nssc.connectionOptions(n)

		resp, err := requestNodeData(connections, nssc.endpoint, bearerToken)
		if err != nil {
			errLock.Lock()
			errs = append(errs, err)
			errLock.Unlock()

			log.Warnf("error retrieving node data: %s", err)
			return nil
		}

		data, err := nodeResponseToStatSummary(resp)
		if err != nil {
			errLock.Lock()
			errs = append(errs, err)
			errLock.Unlock()

			log.Warnf("error converting node data: %s", err)
			return nil
		}

		return data
	}

	results := worker.ConcurrentCollectWith(size, work, nodes)

	// no need to lock, as the concurrent collect blocks until all complete
	var err error = nil
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}
	return results, err
}

// connectionOptions returns the connection methods that are allowed for this node based on config
// settings and cluster composition
func (nssc *NodeStatsSummaryClient) connectionOptions(n *clustercache.Node) []*NodeHttpConnection {
	var connections []*NodeHttpConnection

	clusterHostURL := nssc.clusterHostUrl
	if nssc.config.ProxyConfig.IsLocalProxy() {
		clusterHostURL = nssc.config.ProxyConfig.LocalProxy
	}

	proxyFormatter := NewNodeProxyFormatter(clusterHostURL, n.Name)
	connections = append(connections, NewNodeHttpConnection(nssc.proxyClient, proxyFormatter))

	// Do not allow direct connection to fargate nodes
	if !nssc.config.ProxyConfig.ForceKubeProxy && !isFargateNode(n) {
		directFormatter, err := NewDirectNodeFormatterFrom(n)
		if err != nil {
			log.Warnf("error reaching direct node api %s", err)
		} else {
			connections = append(connections, NewNodeHttpConnection(nssc.directClient, directFormatter))
		}
	}

	return connections
}

// Note: These functions are client-independent and can be reused within another function
// for a different datasource using the same config
type nodeFetchData struct {
	nodeName       string
	ClusterHostURL string
}

// requestNodeData fetches summary and container data for the node
func requestNodeData(connections []*NodeHttpConnection, endpoint string, bearerToken string) (*http.Response, error) {
	var errs []error

	// Fail after trying all connections the alloted number of retries
	for _, connection := range connections {
		data, err := connection.AttemptEndPoint(http.MethodGet, endpoint, bearerToken)
		if err == nil {
			return data, err
		}

		// otherwise, append the error to the list
		errs = append(errs, fmt.Errorf("error retrieving node data from %s: %w", connection.formatter.FormatEndpoint(endpoint), err))
	}

	return nil, fmt.Errorf("problem getting node address: %v\n%w", endpoint, errors.Join(errs...))
}

// isFargateNode detects if it is a fargate node, disallowing direct connections
func isFargateNode(n *clustercache.Node) bool {
	v := n.Labels["eks.amazonaws.com/compute-type"]
	if v == "fargate" {
		log.Warnf("Fargate node found: %s", n.Name)
		return true
	}
	return false
}

// getReadyNodes returns all nodes from a cache that have the ready status
func getReadyNodes(cache clustercache.ClusterCache) []*clustercache.Node {
	nodes := cache.GetAllNodes()

	var readyNodes []*clustercache.Node
	for _, n := range nodes {
		nc := getNodeCondition(&n.Status, v1.NodeReady)
		if nc != nil && nc.Type == v1.NodeReady {
			readyNodes = append(readyNodes, n)
		}
	}

	if len(readyNodes) == 0 {
		log.Warnf("no ready nodes were found")
		return nil
	}

	numReadyNodes := len(readyNodes)
	numTotalNodes := len(nodes)
	if numReadyNodes != numTotalNodes {
		log.Warnf("%v out of %v were in a not ready state when retrieving nodes", numTotalNodes-numReadyNodes, numTotalNodes)
	}

	return readyNodes
}

// getNodeCondition extracts the provided condition from the given status and returns that, nil if not present.
func getNodeCondition(status *v1.NodeStatus, conditionType v1.NodeConditionType) *v1.NodeCondition {
	if status == nil {
		return nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return &status.Conditions[i]
		}
	}
	return nil
}

// NodeAddress returns the internal IP address and kubelet port of a given node
func NodeAddress(node *clustercache.Node) (string, int32, error) {
	// adapted from k8s.io/kubernetes/pkg/util/node
	for _, addr := range node.Status.Addresses {
		if addr.Type == v1.NodeInternalIP {
			return addr.Address, node.Status.DaemonEndpoints.KubeletEndpoint.Port, nil
		}
	}
	return "", 0, fmt.Errorf("could not find internal IP address for node %s ", node.Name)
}

func nodeResponseToStatSummary(resp *http.Response) (*stats.Summary, error) {
	if resp == nil || resp.Body == nil {
		return nil, fmt.Errorf("response or response body is nil")
	}

	defer resp.Body.Close()

	data := &stats.Summary{}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %w", err)
	}

	err = json.Unmarshal(bytes, data)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal response body: %w", err)
	}

	return data, nil
}

// loadBearerToken reads the service account token
func (nssc *NodeStatsSummaryClient) loadBearerToken() (string, error) {
	token, err := os.ReadFile(nssc.bearerTokenFile)
	if err != nil {
		return "", fmt.Errorf("could not read bearer token from file")
	}
	return string(token), nil
}
