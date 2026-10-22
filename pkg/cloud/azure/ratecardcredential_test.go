package azure

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/unicode"

	"github.com/opencost/opencost/pkg/cloud/models"
)

const testTenantID = "00000000-0000-0000-0000-000000000001"

// fakeTransport answers every request in-process, so credential and client
// tests can see which hosts they would have called.
type fakeTransport struct {
	handle   func(r *http.Request, body string) (int, string)
	requests []recordedRequest
}

type recordedRequest struct {
	method string
	url    *url.URL
	header http.Header
	body   string
}

func (f *fakeTransport) Do(r *http.Request) (*http.Response, error) {
	var body string
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		body = string(b)
	}
	f.requests = append(f.requests, recordedRequest{method: r.Method, url: r.URL, header: r.Header.Clone(), body: body})
	status, respBody := f.handle(r, body)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(respBody)),
		Request:    r,
	}, nil
}

func (f *fakeTransport) hosts() []string {
	var hosts []string
	for _, r := range f.requests {
		hosts = append(hosts, r.url.Host)
	}
	return hosts
}

func (f *fakeTransport) tokenRequests() []recordedRequest {
	var reqs []recordedRequest
	for _, r := range f.requests {
		if r.method == http.MethodPost && strings.HasSuffix(r.url.Path, "/oauth2/v2.0/token") {
			reqs = append(reqs, r)
		}
	}
	return reqs
}

// newFakeEntra serves the endpoints MSAL calls to get an app token: instance
// discovery, tenant metadata and the token endpoint, on whatever host it is
// asked for.
func newFakeEntra() *fakeTransport {
	return &fakeTransport{handle: func(r *http.Request, _ string) (int, string) {
		host := r.URL.Host
		switch {
		case strings.HasSuffix(r.URL.Path, "/discovery/instance"):
			return http.StatusOK, fmt.Sprintf(`{"tenant_discovery_endpoint":"https://%[1]s/%[2]s/v2.0/.well-known/openid-configuration","api-version":"1.1","metadata":[{"preferred_network":"%[1]s","preferred_cache":"%[1]s","aliases":["%[1]s"]}]}`, host, testTenantID)
		case strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration"):
			return http.StatusOK, fmt.Sprintf(`{"token_endpoint":"https://%[1]s/%[2]s/oauth2/v2.0/token","authorization_endpoint":"https://%[1]s/%[2]s/oauth2/v2.0/authorize","issuer":"https://%[1]s/%[2]s/v2.0"}`, host, testTenantID)
		case strings.HasSuffix(r.URL.Path, "/oauth2/v2.0/token"):
			return http.StatusOK, `{"token_type":"Bearer","expires_in":3600,"access_token":"fake-access-token"}`
		}
		return http.StatusNotFound, `{}`
	}}
}

// isolateAzureEnv clears every variable azidentity or the rate card auth reads,
// so the developer's own Azure login can't leak into a test.
func isolateAzureEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET",
		"AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_CLIENT_CERTIFICATE_PASSWORD",
		"AZURE_USERNAME", "AZURE_PASSWORD", "AZURE_FEDERATED_TOKEN_FILE",
		"AZURE_AUTHORITY_HOST", "AZURE_TOKEN_CREDENTIALS", azureAuthLocationEnvVar,
		legacyCertificatePathEnvVar, legacyCertificatePasswordEnvVar,
	} {
		t.Setenv(v, "")
		require.NoError(t, os.Unsetenv(v))
	}
}

func clientOptionsFor(c cloud.Configuration, transport policy.Transporter) *arm.ClientOptions {
	return &arm.ClientOptions{ClientOptions: azcore.ClientOptions{Cloud: c, Transport: transport}}
}

func getToken(t *testing.T, cred azcore.TokenCredential, c cloud.Configuration) {
	t.Helper()
	tok, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{
		Scopes: []string{c.Services[cloud.ResourceManager].Audience + "/.default"},
	})
	require.NoError(t, err)
	require.Equal(t, "fake-access-token", tok.Token)
}

func requireAllHosts(t *testing.T, f *fakeTransport, host string) {
	t.Helper()
	require.NotEmpty(t, f.requests)
	for _, h := range f.hosts() {
		require.Equalf(t, host, h, "every login request must go to %s, got hosts %v", host, f.hosts())
	}
}

func TestDetermineCloudByRegion(t *testing.T) {
	cases := map[string]cloud.Configuration{
		"northeurope":   cloud.AzurePublic,
		"chinanorth3":   cloud.AzureChina,
		"usgovvirginia": cloud.AzureGovernment,
		"usdodeast":     cloud.AzureGovernment,
		"":              cloud.AzurePublic,
	}
	for region, expected := range cases {
		require.Equal(t, expected.ActiveDirectoryAuthorityHost, determineCloudByRegion(region).ActiveDirectoryAuthorityHost, region)
	}
}

func TestNewRateCardCredential_ServicePrincipal(t *testing.T) {
	isolateAzureEnv(t)
	// An explicit service principal wins over an auth file, even a broken one.
	t.Setenv(azureAuthLocationEnvVar, filepath.Join(t.TempDir(), "missing.json"))

	entra := newFakeEntra()
	cred, err := newRateCardCredential("sp-client-id", "sp-secret", testTenantID, clientOptionsFor(cloud.AzureChina, entra))
	require.NoError(t, err)
	require.IsType(t, &azidentity.ClientSecretCredential{}, cred)

	getToken(t, cred, cloud.AzureChina)
	requireAllHosts(t, entra, "login.chinacloudapi.cn")
	tokenReqs := entra.tokenRequests()
	require.Len(t, tokenReqs, 1)
	form, err := url.ParseQuery(tokenReqs[0].body)
	require.NoError(t, err)
	require.Equal(t, "sp-client-id", form.Get("client_id"))
	require.Equal(t, "sp-secret", form.Get("client_secret"))
}

func TestNewRateCardCredential_WorkloadIdentity(t *testing.T) {
	isolateAzureEnv(t)
	tokenFile := filepath.Join(t.TempDir(), "azure-identity-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-service-account-token"), 0o600))
	// What the AKS workload identity webhook injects into the pod.
	t.Setenv("AZURE_CLIENT_ID", "wi-client-id")
	t.Setenv("AZURE_TENANT_ID", testTenantID)
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", tokenFile)

	entra := newFakeEntra()
	cred, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzureChina, entra))
	require.NoError(t, err)
	require.IsType(t, &azidentity.DefaultAzureCredential{}, cred)

	getToken(t, cred, cloud.AzureChina)
	requireAllHosts(t, entra, "login.chinacloudapi.cn")
	tokenReqs := entra.tokenRequests()
	require.Len(t, tokenReqs, 1)
	form, err := url.ParseQuery(tokenReqs[0].body)
	require.NoError(t, err)
	require.Equal(t, "wi-client-id", form.Get("client_id"))
	require.Equal(t, "projected-service-account-token", form.Get("client_assertion"))
	require.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", form.Get("client_assertion_type"))
}

func TestNewRateCardCredential_EnvironmentSecret(t *testing.T) {
	isolateAzureEnv(t)
	// Environment credentials win over the auth file and a legacy certificate
	// path, as they did with go-autorest.
	dir := t.TempDir()
	t.Setenv(azureAuthLocationEnvVar, filepath.Join(dir, "missing.json"))
	t.Setenv(legacyCertificatePathEnvVar, filepath.Join(dir, "missing.pem"))
	t.Setenv("AZURE_TENANT_ID", testTenantID)
	t.Setenv("AZURE_CLIENT_ID", "env-client-id")
	t.Setenv("AZURE_CLIENT_SECRET", "env-secret")

	entra := newFakeEntra()
	cred, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzureChina, entra))
	require.NoError(t, err)
	require.IsType(t, &azidentity.EnvironmentCredential{}, cred)

	getToken(t, cred, cloud.AzureChina)
	requireAllHosts(t, entra, "login.chinacloudapi.cn")
	form, err := url.ParseQuery(entra.tokenRequests()[0].body)
	require.NoError(t, err)
	require.Equal(t, "env-client-id", form.Get("client_id"))
	require.Equal(t, "env-secret", form.Get("client_secret"))
}

func TestNewRateCardCredential_EnvironmentCertificateError(t *testing.T) {
	isolateAzureEnv(t)
	// A broken certificate setting is reported instead of falling through to
	// another identity.
	t.Setenv("AZURE_TENANT_ID", testTenantID)
	t.Setenv("AZURE_CLIENT_ID", "env-client-id")
	t.Setenv("AZURE_CLIENT_CERTIFICATE_PATH", filepath.Join(t.TempDir(), "missing.pem"))

	_, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzurePublic, newFakeEntra()))
	require.ErrorContains(t, err, "missing.pem")
}

func TestNewRateCardCredential_EnvironmentUsername(t *testing.T) {
	isolateAzureEnv(t)
	t.Setenv(azureAuthLocationEnvVar, filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("AZURE_TENANT_ID", testTenantID)
	t.Setenv("AZURE_CLIENT_ID", "env-client-id")
	t.Setenv("AZURE_USERNAME", "user@example.com")
	t.Setenv("AZURE_PASSWORD", "user-password")

	cred, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzurePublic, newFakeEntra()))
	require.NoError(t, err)
	require.IsType(t, &azidentity.EnvironmentCredential{}, cred)
}

func TestNewRateCardCredential_LegacyCertificateEnv(t *testing.T) {
	isolateAzureEnv(t)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "sp.pem")
	require.NoError(t, os.WriteFile(certPath, selfSignedPEM(t), 0o600))
	t.Setenv(azureAuthLocationEnvVar, filepath.Join(dir, "missing.json"))
	t.Setenv("AZURE_TENANT_ID", testTenantID)
	t.Setenv("AZURE_CLIENT_ID", "legacy-client-id")
	t.Setenv(legacyCertificatePathEnvVar, certPath)

	entra := newFakeEntra()
	cred, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzureChina, entra))
	require.NoError(t, err)
	require.IsType(t, &azidentity.ClientCertificateCredential{}, cred)

	getToken(t, cred, cloud.AzureChina)
	requireAllHosts(t, entra, "login.chinacloudapi.cn")
	form, err := url.ParseQuery(entra.tokenRequests()[0].body)
	require.NoError(t, err)
	require.Equal(t, "legacy-client-id", form.Get("client_id"))
	require.NotEmpty(t, form.Get("client_assertion"), "certificate auth must send a signed client assertion")

	// With a password the file is read as PKCS12, so a PEM file failing to
	// parse shows the password was passed through.
	t.Setenv(legacyCertificatePasswordEnvVar, "pfx-password")
	_, err = newRateCardCredential("", "", "", clientOptionsFor(cloud.AzureChina, newFakeEntra()))
	require.ErrorContains(t, err, "parsing client certificate from AZURE_CERTIFICATE_PATH")

	t.Setenv(legacyCertificatePasswordEnvVar, "")
	t.Setenv("AZURE_TENANT_ID", "")
	_, err = newRateCardCredential("", "", "", clientOptionsFor(cloud.AzureChina, newFakeEntra()))
	require.ErrorContains(t, err, "AZURE_TENANT_ID or AZURE_CLIENT_ID is missing")
}

func TestNewRateCardCredential_AuthFileSecret(t *testing.T) {
	isolateAzureEnv(t)
	path := filepath.Join(t.TempDir(), "auth.json")
	// The endpoint in the file overrides the cluster's cloud.
	contents := fmt.Sprintf(`{
		"clientId": "file-client-id",
		"clientSecret": "file-secret",
		"subscriptionId": "sub",
		"tenantId": %q,
		"activeDirectoryEndpointUrl": "https://login.chinacloudapi.cn/"
	}`, testTenantID)
	utf16le, err := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewEncoder().String(contents)
	require.NoError(t, err)

	encodings := map[string]string{
		"utf-8":        contents,
		"utf-8 bom":    "\xef\xbb\xbf" + contents,
		"utf-16le bom": utf16le,
	}
	for name, encoded := range encodings {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(encoded), 0o600))
			t.Setenv(azureAuthLocationEnvVar, path)

			entra := newFakeEntra()
			cred, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzurePublic, entra))
			require.NoError(t, err)
			require.IsType(t, &azidentity.ClientSecretCredential{}, cred)

			getToken(t, cred, cloud.AzureChina)
			requireAllHosts(t, entra, "login.chinacloudapi.cn")
			form, err := url.ParseQuery(entra.tokenRequests()[0].body)
			require.NoError(t, err)
			require.Equal(t, "file-client-id", form.Get("client_id"))
			require.Equal(t, "file-secret", form.Get("client_secret"))
		})
	}
}

func TestNewRateCardCredential_AuthFileCertificate(t *testing.T) {
	isolateAzureEnv(t)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "sp.pem")
	require.NoError(t, os.WriteFile(certPath, selfSignedPEM(t), 0o600))
	authPath := filepath.Join(dir, "auth.json")
	require.NoError(t, os.WriteFile(authPath, []byte(fmt.Sprintf(`{"clientId":"cert-client-id","clientCertificate":%q,"tenantId":%q}`, certPath, testTenantID)), 0o600))
	t.Setenv(azureAuthLocationEnvVar, authPath)

	entra := newFakeEntra()
	cred, err := newRateCardCredential("", "", "", clientOptionsFor(cloud.AzurePublic, entra))
	require.NoError(t, err)
	require.IsType(t, &azidentity.ClientCertificateCredential{}, cred)

	getToken(t, cred, cloud.AzurePublic)
	requireAllHosts(t, entra, "login.microsoftonline.com")
	form, err := url.ParseQuery(entra.tokenRequests()[0].body)
	require.NoError(t, err)
	require.Equal(t, "cert-client-id", form.Get("client_id"))
	require.NotEmpty(t, form.Get("client_assertion"), "certificate auth must send a signed client assertion")

	// With a password the file is read as PKCS12, so a PEM file failing to
	// parse shows the password was passed through.
	require.NoError(t, os.WriteFile(authPath, []byte(fmt.Sprintf(`{"clientId":"cert-client-id","clientCertificate":%q,"clientCertificatePassword":"pfx-password","tenantId":%q}`, certPath, testTenantID)), 0o600))
	_, err = newRateCardCredential("", "", "", clientOptionsFor(cloud.AzurePublic, newFakeEntra()))
	require.ErrorContains(t, err, "parsing client certificate from AZURE_AUTH_LOCATION file")
}

func TestNewRateCardCredential_AuthFileErrors(t *testing.T) {
	isolateAzureEnv(t)
	dir := t.TempDir()
	opts := clientOptionsFor(cloud.AzurePublic, newFakeEntra())

	t.Setenv(azureAuthLocationEnvVar, filepath.Join(dir, "missing.json"))
	_, err := newRateCardCredential("", "", "", opts)
	require.ErrorIs(t, err, os.ErrNotExist)

	noCreds := filepath.Join(dir, "nocreds.json")
	require.NoError(t, os.WriteFile(noCreds, []byte(`{"clientId":"id","tenantId":"tenant"}`), 0o600))
	t.Setenv(azureAuthLocationEnvVar, noCreds)
	_, err = newRateCardCredential("", "", "", opts)
	require.ErrorContains(t, err, "neither a client secret nor a client certificate")

	notJSON := filepath.Join(dir, "notjson.json")
	require.NoError(t, os.WriteFile(notJSON, []byte(`clientId=id`), 0o600))
	t.Setenv(azureAuthLocationEnvVar, notJSON)
	_, err = newRateCardCredential("", "", "", opts)
	require.ErrorContains(t, err, "parsing AZURE_AUTH_LOCATION file")
}

func TestResourceManagerCredential(t *testing.T) {
	isolateAzureEnv(t)
	az := &Azure{
		ClusterRegion:     "chinanorth3",
		loadedAzureSecret: true,
		azureSecret: &AzureServiceKey{
			SubscriptionID: "sub-id",
			ServiceKey:     &AzureAppKey{AppID: "sp-client-id", Password: "sp-secret", Tenant: testTenantID},
		},
	}
	config := &models.CustomPricing{}
	cred, opts, err := az.resourceManagerCredential(config)
	require.NoError(t, err)
	require.IsType(t, &azidentity.ClientSecretCredential{}, cred)
	require.Equal(t, cloud.AzureChina.ActiveDirectoryAuthorityHost, opts.Cloud.ActiveDirectoryAuthorityHost)
	require.Equal(t, "sub-id", config.AzureSubscriptionID)

	// Without a service key the subscription still comes from config, and the
	// credential falls through to DefaultAzureCredential.
	az = &Azure{ClusterRegion: "northeurope", loadedAzureSecret: true}
	config = &models.CustomPricing{AzureSubscriptionID: "config-sub-id"}
	cred, opts, err = az.resourceManagerCredential(config)
	require.NoError(t, err)
	require.IsType(t, &azidentity.DefaultAzureCredential{}, cred)
	require.Equal(t, cloud.AzurePublic.ActiveDirectoryAuthorityHost, opts.Cloud.ActiveDirectoryAuthorityHost)
	require.Equal(t, "config-sub-id", config.AzureSubscriptionID)
}

func selfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "opencost-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	out := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return append(out, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})...)
}
