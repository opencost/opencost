package prom

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewOpenCostPrometheusConfigFromEnv_KubeRbacProxyOutsideCluster(t *testing.T) {
	t.Setenv("PROMETHEUS_SERVER_ENDPOINT", "nowhere")
	t.Setenv("KUBE_RBAC_PROXY_ENABLED", "true")
	// Without these, rest.InClusterConfig returns ErrNotInCluster.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err == nil {
		t.Fatalf("expected an error outside a cluster, got config %+v", config)
	}
	if !strings.Contains(err.Error(), "KUBE_RBAC_PROXY_ENABLED") {
		t.Errorf("error %q does not mention KUBE_RBAC_PROXY_ENABLED", err)
	}
}

// writeTestCertificate writes a self-signed certificate and its key as PEM
// files in dir and returns their paths.
func writeTestCertificate(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "opencost-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestNewOpenCostPrometheusConfigFromEnv_MtlsMisconfigured(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertificate(t, dir)
	notPEM := filepath.Join(dir, "not-pem.crt")
	if err := os.WriteFile(notPEM, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.crt")

	for _, tc := range []struct {
		name, ca, crt, key string
		wantInError        string
	}{
		{name: "missing CA file", ca: missing, crt: certFile, key: keyFile, wantInError: "PROM_MTLS_AUTH_CA_FILE"},
		{name: "CA file without certificates", ca: notPEM, crt: certFile, key: keyFile, wantInError: "no valid PEM certificates"},
		{name: "missing client certificate", ca: certFile, crt: missing, key: keyFile, wantInError: "PROM_MTLS_AUTH_CRT_FILE"},
		{name: "client key does not parse", ca: certFile, crt: certFile, key: notPEM, wantInError: "PROM_MTLS_AUTH_KEY_FILE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PROMETHEUS_SERVER_ENDPOINT", "nowhere")
			t.Setenv("KUBE_RBAC_PROXY_ENABLED", "false")
			t.Setenv("PROM_MTLS_AUTH_CA_FILE", tc.ca)
			t.Setenv("PROM_MTLS_AUTH_CRT_FILE", tc.crt)
			t.Setenv("PROM_MTLS_AUTH_KEY_FILE", tc.key)

			config, err := NewOpenCostPrometheusConfigFromEnv()
			if err == nil {
				t.Fatalf("expected an error for a broken mTLS setup, got config %+v", config)
			}
			if !strings.Contains(err.Error(), tc.wantInError) {
				t.Errorf("error %q does not mention %q", err, tc.wantInError)
			}
		})
	}
}

func TestNewOpenCostPrometheusConfigFromEnv_Mtls(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t, t.TempDir())
	t.Setenv("PROMETHEUS_SERVER_ENDPOINT", "nowhere")
	t.Setenv("KUBE_RBAC_PROXY_ENABLED", "false")
	t.Setenv("PROM_MTLS_AUTH_CA_FILE", certFile)
	t.Setenv("PROM_MTLS_AUTH_CRT_FILE", certFile)
	t.Setenv("PROM_MTLS_AUTH_KEY_FILE", keyFile)

	config, err := NewOpenCostPrometheusConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(config.ClientConfig.ClientCertificates); got != 1 {
		t.Errorf("expected 1 client certificate, got %d", got)
	}
	if config.ClientConfig.RootCAs == nil {
		t.Error("expected the configured CA in RootCAs")
	}
}
