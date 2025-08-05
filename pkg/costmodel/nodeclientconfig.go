package costmodel

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	nodes "github.com/opencost/opencost/core/pkg/nodestats"
	"github.com/opencost/opencost/pkg/env"
)

const (
	defaultConcurrentPollers = 10
	serviceAccountCaCert     = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

func NewNodeClientConfigFromEnv() (*nodes.NodeClientConfig, error) {
	clusterId := coreenv.GetClusterID()
	concurrentPollers := defaultConcurrentPollers
	insecure := env.IsNodeStatsInsecure()
	certFile := env.GetNodeStatsCertFile()
	keyFile := env.GetNodeStatsKeyFile()
	forceKubeProxy := env.IsNodeStatsForceKubeProxy()
	localProxy := env.GetNodeStatsLocalProxy()

	if strings.TrimSpace(clusterId) == "" {
		return nil, fmt.Errorf("cluster id is required and cannot be exclusively whitespace.")
	}

	var transport *http.Transport
	if insecure {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	} else {
		pemData, err := os.ReadFile(serviceAccountCaCert)
		if err != nil {
			log.Fatalf("Could not load CA certificate: %v", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(pemData)

		var tlsConfig *tls.Config

		if certFile != "" && keyFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)

			if err != nil {
				log.Fatalf("Unable to load cert: %s key: %s error: %v", certFile, keyFile, err)
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      caCertPool,
			}

			transport = &http.Transport{TLSClientConfig: tlsConfig}
		} else {
			tlsConfig := &tls.Config{
				RootCAs: caCertPool,
			}
			transport = &http.Transport{TLSClientConfig: tlsConfig}
		}
	}

	return &nodes.NodeClientConfig{
		ClusterId:         clusterId,
		ConcurrentPollers: concurrentPollers,
		Transport:         transport,
		ProxyConfig: nodes.NodeClientProxyConfig{
			ForceKubeProxy: forceKubeProxy,
			LocalProxy:     localProxy,
		},
	}, nil
}
