package nodestats

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/clustercache"
)

// NodeEndpointFormatter is an interface that defines a method to format node endpoints.
type NodeEndpointFormatter interface {
	FormatEndpoint(s string) string
}

// DirectNodeFormatter is an implementation of a NodeEndpointFormatter that formats endpoints for direct node access.
type DirectNodeFormatter struct {
	ip   string
	port int64
}

// NewDirectNodeFormatterFrom creates a new DirectNodeFormatter from a Node instance.
func NewDirectNodeFormatterFrom(n *clustercache.Node) (*DirectNodeFormatter, error) {
	if n == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}

	ip, port, err := NodeAddress(n)
	if err != nil {
		return nil, fmt.Errorf("problem getting node address: %s", err)
	}

	return &DirectNodeFormatter{
		ip:   ip,
		port: int64(port),
	}, nil
}

// FormatEndpoint formats the endpoint URL for direct node access.
func (dnf *DirectNodeFormatter) FormatEndpoint(s string) string {
	return fmt.Sprintf("https://%s:%v/%s", dnf.ip, dnf.port, s)
}

// NodeProxyFormatter is an implementation of a NodeEndpointFormatter that formats endpoints for a node proxy request.
type NodeProxyFormatter struct {
	clusterHostUrl string
	nodeName       string
}

// NewNodeProxyFormatter creates a new NodeProxyFormatter with the given cluster host URL and node name.
func NewNodeProxyFormatter(clusterHostUrl, nodeName string) *NodeProxyFormatter {
	return &NodeProxyFormatter{
		clusterHostUrl: clusterHostUrl,
		nodeName:       nodeName,
	}
}

// FormatEndpoint formats the endpoint URL for a node proxy request.
func (npf *NodeProxyFormatter) FormatEndpoint(s string) string {
	return fmt.Sprintf("%s/api/v1/nodes/%s/proxy/%s", npf.clusterHostUrl, npf.nodeName, s)
}
