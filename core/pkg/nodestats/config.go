package nodestats

import (
	"net/http"
)

type NodeClientProxyConfig struct {
	ForceKubeProxy bool
	LocalProxy     string
}

func (nac NodeClientProxyConfig) IsLocalProxy() bool {
	return nac.LocalProxy != ""
}

type NodeClientConfig struct {
	ClusterId         string
	ConcurrentPollers int
	Transport         *http.Transport
	CertFile          string
	KeyFile           string
	ProxyConfig       NodeClientProxyConfig
}

func NewNodeClientConfig(
	clusterId string,
	concurrentPollers int,
	transport *http.Transport,
	certFile string,
	keyFile string,
	proxyConfig NodeClientProxyConfig,
) *NodeClientConfig {
	return &NodeClientConfig{
		ClusterId:         clusterId,
		ConcurrentPollers: concurrentPollers,
		Transport:         transport,
		CertFile:          certFile,
		KeyFile:           keyFile,
		ProxyConfig:       proxyConfig,
	}
}
