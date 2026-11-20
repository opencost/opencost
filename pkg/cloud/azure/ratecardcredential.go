package azure

import (
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/cloud/models"
)

const (
	azureAuthLocationEnvVar = "AZURE_AUTH_LOCATION"
	// go-autorest read the certificate from these; azidentity reads
	// AZURE_CLIENT_CERTIFICATE_PATH and AZURE_CLIENT_CERTIFICATE_PASSWORD.
	legacyCertificatePathEnvVar     = "AZURE_CERTIFICATE_PATH"
	legacyCertificatePasswordEnvVar = "AZURE_CERTIFICATE_PASSWORD"
)

// determineCloudByRegion uses region name to pick the correct Cloud Environment for the azure provider to use
func determineCloudByRegion(region string) cloud.Configuration {
	lcRegion := strings.ToLower(region)
	if strings.Contains(lcRegion, "china") {
		return cloud.AzureChina
	}
	if strings.Contains(lcRegion, "gov") || strings.Contains(lcRegion, "dod") {
		return cloud.AzureGovernment
	}
	// Default to public cloud
	return cloud.AzurePublic
}

// resourceManagerCredential loads the rate card auth into config and returns
// the credential and client options for the cluster's cloud.
func (az *Azure) resourceManagerCredential(config *models.CustomPricing) (azcore.TokenCredential, *arm.ClientOptions, error) {
	subscriptionID, clientID, clientSecret, tenantID := az.getAzureRateCardAuth(false, config)
	config.AzureSubscriptionID = subscriptionID
	config.AzureClientID = clientID
	config.AzureClientSecret = clientSecret
	config.AzureTenantID = tenantID

	opts := &arm.ClientOptions{
		ClientOptions: azcore.ClientOptions{Cloud: determineCloudByRegion(az.ClusterRegion)},
	}
	credential, err := newRateCardCredential(clientID, clientSecret, tenantID, opts)
	if err != nil {
		return nil, nil, err
	}
	return credential, opts, nil
}

// newRateCardCredential returns the credential used for the Resource Manager
// calls behind list pricing and disk discovery. In order of precedence:
//  1. the service principal from the azure service key secret or pricing config
//  2. a client secret, certificate or username in environment variables,
//     including go-autorest's AZURE_CERTIFICATE_PATH
//  3. an SDK auth file named by AZURE_AUTH_LOCATION
//  4. DefaultAzureCredential: workload identity, managed identity, then the
//     Azure CLI
func newRateCardCredential(clientID, clientSecret, tenantID string, opts *arm.ClientOptions) (azcore.TokenCredential, error) {
	if clientID != "" && clientSecret != "" && tenantID != "" {
		return azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, &azidentity.ClientSecretCredentialOptions{
			ClientOptions: opts.ClientOptions,
		})
	}

	// Environment credentials in go-autorest's order: client secret,
	// certificate, then username.
	envOpts := &azidentity.EnvironmentCredentialOptions{ClientOptions: opts.ClientOptions}
	if os.Getenv("AZURE_CLIENT_SECRET") != "" || os.Getenv("AZURE_CLIENT_CERTIFICATE_PATH") != "" {
		return azidentity.NewEnvironmentCredential(envOpts)
	}

	if path := os.Getenv(legacyCertificatePathEnvVar); path != "" {
		envTenantID, envClientID := os.Getenv("AZURE_TENANT_ID"), os.Getenv("AZURE_CLIENT_ID")
		if envTenantID == "" || envClientID == "" {
			return nil, fmt.Errorf("%s is set but AZURE_TENANT_ID or AZURE_CLIENT_ID is missing", legacyCertificatePathEnvVar)
		}
		return newCertificateCredential(envTenantID, envClientID, path, os.Getenv(legacyCertificatePasswordEnvVar), legacyCertificatePathEnvVar, opts.ClientOptions)
	}

	if os.Getenv("AZURE_USERNAME") != "" {
		return azidentity.NewEnvironmentCredential(envOpts)
	}

	if path := os.Getenv(azureAuthLocationEnvVar); path != "" {
		return newAuthFileCredential(path, opts.ClientOptions)
	}

	return azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		ClientOptions: opts.ClientOptions,
	})
}

// azureAuthFile is the file written by `az ad sp create-for-rbac --sdk-auth`.
type azureAuthFile struct {
	ClientID                  string `json:"clientId"`
	ClientSecret              string `json:"clientSecret"`
	ClientCertificate         string `json:"clientCertificate"`
	ClientCertificatePassword string `json:"clientCertificatePassword"`
	TenantID                  string `json:"tenantId"`
	ActiveDirectoryEndpoint   string `json:"activeDirectoryEndpointUrl"`
}

func newAuthFileCredential(path string, opts azcore.ClientOptions) (azcore.TokenCredential, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s file: %w", azureAuthLocationEnvVar, err)
	}
	// Windows tools often save this file as UTF-16 or with a UTF-8 BOM.
	decoded, _, err := transform.Bytes(unicode.BOMOverride(unicode.UTF8.NewDecoder()), contents)
	if err != nil {
		return nil, fmt.Errorf("decoding %s file %s: %w", azureAuthLocationEnvVar, path, err)
	}
	var f azureAuthFile
	if err := json.Unmarshal(decoded, &f); err != nil {
		return nil, fmt.Errorf("parsing %s file %s: %w", azureAuthLocationEnvVar, path, err)
	}

	if f.ActiveDirectoryEndpoint != "" {
		opts.Cloud.ActiveDirectoryAuthorityHost = f.ActiveDirectoryEndpoint
	}

	if f.ClientSecret != "" {
		return azidentity.NewClientSecretCredential(f.TenantID, f.ClientID, f.ClientSecret, &azidentity.ClientSecretCredentialOptions{
			ClientOptions: opts,
		})
	}

	if f.ClientCertificate != "" {
		return newCertificateCredential(f.TenantID, f.ClientID, f.ClientCertificate, f.ClientCertificatePassword, azureAuthLocationEnvVar+" file", opts)
	}

	return nil, fmt.Errorf("%s file %s has neither a client secret nor a client certificate", azureAuthLocationEnvVar, path)
}

// newCertificateCredential loads a PEM or PKCS12 certificate; source names
// where the path came from for error messages.
func newCertificateCredential(tenantID, clientID, certPath, password, source string, opts azcore.ClientOptions) (azcore.TokenCredential, error) {
	certData, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("reading client certificate from %s: %w", source, err)
	}
	certs, key, err := azidentity.ParseCertificates(certData, []byte(password))
	if err != nil {
		return nil, fmt.Errorf("parsing client certificate from %s: %w", source, err)
	}
	return azidentity.NewClientCertificateCredential(tenantID, clientID, certs, key, &azidentity.ClientCertificateCredentialOptions{
		ClientOptions: opts,
	})
}
