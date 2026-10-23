package huawei

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth"

	"github.com/opencost/opencost/pkg/env"
)

// TestMain keeps every test in this package off a real Huawei Cloud account,
// whatever the machine running it has: the credential environment is
// cleared, the IAM agency lookup fails, and the instance metadata service is
// a local server that answers 404. A test that needs credentials or a BSS
// endpoint sets its own (see withoutHuaweiCredentials for the opposite).
func TestMain(m *testing.M) {
	for _, name := range []string{
		env.HuaweiAccessKeyIDEnvVar,
		env.HuaweiAccessKeySecretEnvVar,
		env.HuaweiDomainIDEnvVar,
		env.HuaweiProjectIDEnvVar,
		env.HuaweiCCEClusterIDEnvVar,
		env.HuaweiServiceCategoriesEnvVar,
	} {
		os.Unsetenv(name)
	}

	agencyCredentials = func() (auth.ICredential, error) {
		return nil, fmt.Errorf("no IAM agency in tests")
	}

	metadata := httptest.NewServer(http.NotFoundHandler())
	metadataEndpoint = metadata.URL

	code := m.Run()
	metadata.Close()
	os.Exit(code)
}
