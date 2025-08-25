package storage

import (
	"crypto/tls"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	assert2 "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	caFileName          = "testCA.pem"
	keyFileName         = "testkey.pem"
	invalidFileName     = "invalid.pem"
	nonExistentFileName = "no.exist"
	// valid CA File for test purposes only
	caContent = `-----BEGIN CERTIFICATE-----
MIIF2TCCA8GgAwIBAgIUIY1Kop8xSQEwlz4EeykhGEviRLIwDQYJKoZIhvcNAQEL
BQAwfDELMAkGA1UEBhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExDTALBgNVBAcM
BHRlc3QxDTALBgNVBAoMBHRlc3QxDTALBgNVBAsMBHRlc3QxDTALBgNVBAMMBHRl
c3QxHDAaBgkqhkiG9w0BCQEWDXRlc3RAdGVzdC5jb20wHhcNMjUwNjI0MjEwMTM0
WhcNMjUwNzI0MjEwMTM0WjB8MQswCQYDVQQGEwJVUzETMBEGA1UECAwKQ2FsaWZv
cm5pYTENMAsGA1UEBwwEdGVzdDENMAsGA1UECgwEdGVzdDENMAsGA1UECwwEdGVz
dDENMAsGA1UEAwwEdGVzdDEcMBoGCSqGSIb3DQEJARYNdGVzdEB0ZXN0LmNvbTCC
AiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBALYBm4UDowPTNvBxanFKdJ5g
+ZIKkzvIqlAVxKWPWopdlQinoRl6jyofDJ1yhuLiqz4CxDczTv+A1TjxH0RaSdj2
qebOqqhHl+EahV/stc16vOz4mywkrV+C5i5Vk2y1SXqxyzZQtthhvPquHD2C/Z8M
PVVzyN4+gGog0srdXffPhEI774uenkkcBZNh1ycvJJPv6nQ3Ib0Gjk/J7nnV5AvI
glfloy2sENZagtx2EuPxuQzeuJoR62hrBLLG/gR50Mqr1RRxn3BV61Z2q+8vmhK0
qyRF6RiFqrtJy67NGyhlBmlCttI5rZX8lBADCpaLRWDRlZlGtA08Mh9FbiawHRwd
pcN4AVpMfsqRxZJ18LLzZe9XzWrVpaWoF5JFNB8rcF5+eH7ry671AkK8nV0BrbdR
H+zJnbqi1ewQBpL49dYsUheqqZw9w/bq7fgvefxEL2urAfbEwnGfyygReYjiNw7D
z9uMudBoYNQyCTe/lYH0q6xP1Ycso0WfjKL50mcvMOQNaux0Nd/oH1B0WFe4OnFE
QqAvs6g1hxd4W8Q3mjbiTNVmpFU9O5W3yCvNgJt/5UKi/zfXClTsMicAPznvQTlB
+O7GxL4B9mgSNL+8qLwx8NmfJlskk4HrJZZkdk1hTIr9Lj/uu/ooR1tbRDkbeLuY
N6wqN7+jcBWoDZcHh+gDAgMBAAGjUzBRMB0GA1UdDgQWBBQsrXi8RmTF705Ct3RR
Fng02lj7+TAfBgNVHSMEGDAWgBQsrXi8RmTF705Ct3RRFng02lj7+TAPBgNVHRMB
Af8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQA7MLXyWIRghrsJeOtxfi2xmr2F
di417pi5cGrq98liRlISF/bqXLqLWcTSDpCLdHzLBbUA5CVGU//1g88HuHUC1mbp
ENxdKqRzlK5wkxDifGiLFqX87SOb+dSqkhn85fs6kBkHRBixZwoVcs/WQWQNSbPw
lFx6sVa5oKOPkJbre7AJf9BTWtCUftps0rwQfrlZw9FYmBSGQ1PkhJOdOkbWKhJ6
F55lWdfpcxZRPuemQFWzErQRLMXwzOm9MdmrISkQbRttTjZCvoo0njphckPjdrZJ
kqN2OSFUGmoia/0LfIOaC6LdWri2tEemS69JDJxIXxpxv5GE1PAu6zdQBmV5/rBn
e2EwLSLUxleNDGp4YhyXOXSS3Tm2zCxVmLUKLAqq6IqvIO155J0uTBFKpRGs+3DL
P+c0XFHgp3+tVWnkYGszh6hrKMgYWCneEDAdCXrv2GQjY7ObbRo3f4dR0Iswz8pR
KdrUSftz1CNVmQpjq06nUa9pdZpqwAxuyvKKzBrefprHqUS2WBsiGdmhjpNIagGl
jF3fZ24qJxDOv6vAvF+9jHxfTq+WUrR+tS6BpgvrvVVJkaWsj6s8wlRFny8zrWln
gy8s1O9SI6+368+p37nPPgIfBVjOJpGVLrG3QL/e1kg91mUdhXkfan81NVXdQif5
JXNAW1pC0i+sxoHYfg==
-----END CERTIFICATE-----
`
	// valid key file, only used for testing
	keyContent = `-----BEGIN PRIVATE KEY-----
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
Hef5W/FKNtov4fx8QuMhwA4lWIuliw==
-----END PRIVATE KEY-----`
	invalidContent = "invalid"
)

func createKeyFiles(t *testing.T, tmpDir string) func() {
	return createTempFiles(
		t,
		tmpDir,
		map[string]string{
			caFileName:  caContent,
			keyFileName: keyContent,
		},
	)
}

// createTempFiles takes a map of file name and content pairs, creates the files in a tmp dir and returns a cleanup
// function
func createTempFiles(t *testing.T, tmpDir string, files map[string]string) func() {
	var filesToRemove []string
	for name, content := range files {
		tmpFile, err := os.Create(path.Join(tmpDir, name))
		require.NoError(t, err)
		_, err = tmpFile.WriteString(content)
		require.NoError(t, err)
		filesToRemove = append(filesToRemove, tmpFile.Name())
		tmpFile.Close()
	}
	return func() {
		for _, name := range filesToRemove {
			os.Remove(name)
		}
	}
}

func TestHTTPConfig_GetHTTPTransport(t *testing.T) {
	testCases := map[string]struct {
		config        HTTPConfig
		wantError     bool
		validateFunc  func(t *testing.T, transport http.RoundTripper)
		errorContains string
	}{
		"default configuration": {
			config: HTTPConfig{
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 2 * time.Minute,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				MaxConnsPerHost:       0,
				DisableCompression:    false,
				InsecureSkipVerify:    false,
			},
			wantError: false,
			validateFunc: func(t *testing.T, transport http.RoundTripper) {
				httpTransport, ok := transport.(*http.Transport)
				require.True(t, ok, "Expected *http.Transport")
				assert2.Equal(t, 90*time.Second, httpTransport.IdleConnTimeout)
				assert2.Equal(t, 2*time.Minute, httpTransport.ResponseHeaderTimeout)
				assert2.Equal(t, 10*time.Second, httpTransport.TLSHandshakeTimeout)
				assert2.Equal(t, 1*time.Second, httpTransport.ExpectContinueTimeout)
				assert2.Equal(t, 100, httpTransport.MaxIdleConns)
				assert2.Equal(t, 100, httpTransport.MaxIdleConnsPerHost)
				assert2.Equal(t, 0, httpTransport.MaxConnsPerHost)
				assert2.False(t, httpTransport.DisableCompression)
				assert2.False(t, httpTransport.TLSClientConfig.InsecureSkipVerify)
			},
		},
		"with insecure skip verify": {
			config: HTTPConfig{
				InsecureSkipVerify: true,
			},
			wantError: false,
			validateFunc: func(t *testing.T, transport http.RoundTripper) {
				httpTransport, ok := transport.(*http.Transport)
				require.True(t, ok)
				assert2.True(t, httpTransport.TLSClientConfig.InsecureSkipVerify)
			},
		},
		"with injected transport": {
			config: HTTPConfig{
				Transport: &http.Transport{},
			},
			wantError: false,
			validateFunc: func(t *testing.T, transport http.RoundTripper) {
				_, ok := transport.(*http.Transport)
				require.True(t, ok)
			},
		},
		"with server name": {
			config: HTTPConfig{
				TLSConfig: TLSConfig{
					ServerName: "example.com",
				},
			},
			wantError: false,
			validateFunc: func(t *testing.T, transport http.RoundTripper) {
				httpTransport, ok := transport.(*http.Transport)
				require.True(t, ok)
				assert2.Equal(t, "example.com", httpTransport.TLSClientConfig.ServerName)
			},
		},
		"with disable compression": {
			config: HTTPConfig{
				DisableCompression: true,
			},
			wantError: false,
			validateFunc: func(t *testing.T, transport http.RoundTripper) {
				httpTransport, ok := transport.(*http.Transport)
				require.True(t, ok)
				assert2.True(t, httpTransport.DisableCompression)
			},
		},
		"with invalid TLS config": {
			config: HTTPConfig{
				TLSConfig: TLSConfig{
					CertFile: "cert.pem", // cert without key should cause error
				},
			},
			wantError:     true,
			errorContains: "client cert file",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			transport, err := tc.config.GetHTTPTransport()

			if tc.wantError {
				assert2.Error(t, err)
				if tc.errorContains != "" {
					assert2.Contains(t, err.Error(), tc.errorContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, transport)
			if tc.validateFunc != nil {
				tc.validateFunc(t, transport)
			}
		})
	}
}

func TestTLSConfig_ToConfig(t *testing.T) {
	tmpDir := os.TempDir()
	cleanupFn := createKeyFiles(t, tmpDir)
	t.Cleanup(cleanupFn)

	testCases := map[string]struct {
		config       *TLSConfig
		want         *tls.Config
		wantError    bool
		validateFunc func(t *testing.T, tlsConfig *tls.Config)
	}{
		"default configuration": {
			config:    &TLSConfig{},
			want:      &tls.Config{},
			wantError: false,
		},
		"with insecure skip verify": {
			config: &TLSConfig{
				InsecureSkipVerify: true,
			},
			want: &tls.Config{
				InsecureSkipVerify: true,
			},
			wantError: false,
		},
		"missing CA file": {
			config: &TLSConfig{
				CAFile: path.Join(tmpDir, nonExistentFileName),
			},
			wantError: true,
		},
		"invalid CA file": {
			config: &TLSConfig{
				CAFile: path.Join(tmpDir, invalidFileName),
			},
			wantError: true,
		},
		"with server name": {
			config: &TLSConfig{
				ServerName: "example.com",
			},
			want: &tls.Config{
				ServerName: "example.com",
			},
			wantError: false,
		},
		"cert file without key file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, caFileName),
			},
			wantError: true,
		},
		"key file without cert file": {
			config: &TLSConfig{
				KeyFile: path.Join(tmpDir, keyFileName),
			},
			wantError: true,
		},

		"invalid cert file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, invalidFileName),
				KeyFile:  path.Join(tmpDir, keyFileName),
			},
			wantError: true,
		},
		"invalid key file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, caFileName),
				KeyFile:  path.Join(tmpDir, invalidFileName),
			},
			wantError: true,
		},
		"valid Cert and Key file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, caFileName),
				KeyFile:  path.Join(tmpDir, keyFileName),
			},
			want: &tls.Config{
				GetClientCertificate: TLSConfig{
					CertFile: path.Join(tmpDir, caFileName),
					KeyFile:  path.Join(tmpDir, keyFileName),
				}.getClientCertificate,
			},
			wantError: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tlsConfig, err := tc.config.ToConfig()

			if tc.wantError {
				assert2.Error(t, err)
				return
			}

			require.NoError(t, err)
			tlsConfigEqual(t, tlsConfig, tc.want)
		})
	}
}

func tlsConfigEqual(t *testing.T, got, want *tls.Config) {
	if want == nil {
		assert2.Nil(t, got)
		return
	} else {
		assert2.NotNil(t, got)
	}

	assert2.Equal(t, got.InsecureSkipVerify, want.InsecureSkipVerify)
	assert2.Equal(t, got.ServerName, want.ServerName)
	assert2.Equal(t, got.GetClientCertificate == nil, want.GetClientCertificate == nil)
	if want.GetClientCertificate != nil {
		gotCert, gotError := got.GetClientCertificate(nil)
		assert2.NoError(t, gotError)
		wantCert, wantError := want.GetClientCertificate(nil)
		assert2.NoError(t, wantError)
		assert2.Equal(t, gotCert, wantCert)
	}
}

func TestReadCAFile(t *testing.T) {
	tmpDir := os.TempDir()
	cleanupFn := createKeyFiles(t, tmpDir)
	t.Cleanup(cleanupFn)

	testCases := map[string]struct {
		fileName  string
		content   string
		wantError bool
	}{
		"nonexistent file": {
			fileName:  nonExistentFileName,
			content:   "",
			wantError: true,
		},
		"invalid file": {
			fileName:  invalidFileName,
			content:   invalidContent,
			wantError: true,
		},
		"valid file": {
			fileName:  caFileName,
			content:   caContent,
			wantError: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			data, err := readCAFile(path.Join(tmpDir, tc.fileName))

			if tc.wantError {
				assert2.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert2.Equal(t, tc.content, string(data))
		})
	}
}

func TestUpdateRootCA(t *testing.T) {
	testCases := map[string]struct {
		input      []byte
		expectedOk bool
	}{
		"valid PEM certificate": {
			input:      []byte(caContent),
			expectedOk: true, // This is a valid certificate
		},
		"invalid PEM data": {
			input:      []byte(invalidContent),
			expectedOk: false,
		},
		"empty data": {
			input:      []byte(""),
			expectedOk: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tlsConfig := &tls.Config{}
			result := updateRootCA(tlsConfig, tc.input)

			assert2.Equal(t, tc.expectedOk, result)
			if result {
				assert2.NotNil(t, tlsConfig.RootCAs)
			} else {
				assert2.Nil(t, tlsConfig.RootCAs)
			}
		})
	}
}

func TestTLSConfig_getClientCertificate(t *testing.T) {
	tmpDir := os.TempDir()
	cleanupFn := createKeyFiles(t, tmpDir)
	t.Cleanup(cleanupFn)

	testCases := map[string]struct {
		config    *TLSConfig
		wantError bool
	}{
		"empty config": {
			config:    &TLSConfig{},
			wantError: true,
		},
		"nonexistent cert files": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, nonExistentFileName),
				KeyFile:  path.Join(tmpDir, nonExistentFileName),
			},
			wantError: true,
		},
		"missing cert file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, nonExistentFileName),
				KeyFile:  path.Join(tmpDir, keyFileName),
			},
			wantError: true,
		},
		"missing key file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, caFileName),
				KeyFile:  path.Join(tmpDir, nonExistentFileName),
			},
			wantError: true,
		},
		"invalid cert file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, invalidFileName),
				KeyFile:  path.Join(tmpDir, keyFileName),
			},
			wantError: true,
		},
		"invalid key file": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, caFileName),
				KeyFile:  path.Join(tmpDir, invalidFileName),
			},
			wantError: true,
		},
		"valid": {
			config: &TLSConfig{
				CertFile: path.Join(tmpDir, caFileName),
				KeyFile:  path.Join(tmpDir, keyFileName),
			},
			wantError: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := tc.config.getClientCertificate(nil)

			if tc.wantError {
				assert2.Error(t, err)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestHTTPConfigIntegration(t *testing.T) {
	config := HTTPConfig{
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: 1 * time.Minute,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 500 * time.Millisecond,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   25,
		MaxConnsPerHost:       10,
		DisableCompression:    true,
		InsecureSkipVerify:    true,
		TLSConfig: TLSConfig{
			ServerName:         "test-server.com",
			InsecureSkipVerify: false, // This should be overridden by HTTPConfig.InsecureSkipVerify
		},
	}

	transport, err := config.GetHTTPTransport()
	require.NoError(t, err)

	httpTransport, ok := transport.(*http.Transport)
	require.True(t, ok)

	// Verify all settings are applied correctly
	assert2.Equal(t, 30*time.Second, httpTransport.IdleConnTimeout)
	assert2.Equal(t, 1*time.Minute, httpTransport.ResponseHeaderTimeout)
	assert2.Equal(t, 5*time.Second, httpTransport.TLSHandshakeTimeout)
	assert2.Equal(t, 500*time.Millisecond, httpTransport.ExpectContinueTimeout)
	assert2.Equal(t, 50, httpTransport.MaxIdleConns)
	assert2.Equal(t, 25, httpTransport.MaxIdleConnsPerHost)
	assert2.Equal(t, 10, httpTransport.MaxConnsPerHost)
	assert2.True(t, httpTransport.DisableCompression)
	assert2.True(t, httpTransport.TLSClientConfig.InsecureSkipVerify) // Should be overridden
	assert2.Equal(t, "test-server.com", httpTransport.TLSClientConfig.ServerName)
	assert2.NotNil(t, httpTransport.Proxy)
	assert2.NotNil(t, httpTransport.DialContext)
}
