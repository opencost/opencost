package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/global"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
)

// huaweiGlobalCredentials resolves global.Credentials for both the BSS pricing
// client (pricing.go) and the OBS bucket client (costsource.go), the same way
// pkg/cloud/huawei's newBssClient does in the main module: static AK/SK
// (envAccessKeyID/envAccessKeySecret, optionally + envDomainID) take precedence
// when both are set; otherwise it falls back to the IAM agency attached to the
// node, fetched from the ECS/CCE instance metadata service via the SDK's own
// MetadataCredentialProvider -- the same mechanism other in-cluster tools (e.g.
// ces-exporter) use, so no static AK/SK need to be provisioned at all when the
// node already has an agency configured.
//
// envDomainID is optional in both cases: GlobalCredentials.ProcessAuthParams
// auto-discovers the domain ID from the credential's own IAM permissions when
// it's left empty.
//
// agencyCredsCacheTTL bounds how long the resolved agency credentials are
// reused before re-fetching from the metadata service. GetCustomCosts calls
// this once per ingestion window (hundreds of calls per build cycle when
// backfilling a month in 1h blocks), and every call to
// provider.GlobalCredentialMetadataProvider().GetCredentials() builds a brand
// new internal MetadataAccessor (no caching shared across calls) -- without
// caching here, that floods 169.254.169.254 until it starts responding 503
// "The request is too frequent. Please try again later!", failing every
// window in the batch. Huawei Cloud agency-assumed temporary credentials are
// valid well beyond this TTL, so re-using them for 10 minutes is safe.
const agencyCredsCacheTTL = 10 * time.Minute

var (
	agencyCredsCacheMu    sync.Mutex
	agencyCredsCacheValue *global.Credentials
	agencyCredsCacheAt    time.Time
)

func huaweiGlobalCredentials() (*global.Credentials, error) {
	ak := os.Getenv(envAccessKeyID)
	sk := os.Getenv(envAccessKeySecret)
	domainID := os.Getenv(envDomainID)

	if ak != "" && sk != "" {
		creds, err := global.NewCredentialsBuilder().
			WithAk(ak).
			WithSk(sk).
			WithDomainId(domainID).
			SafeBuild()
		if err != nil {
			return nil, fmt.Errorf("building huawei cloud credentials: %w", err)
		}
		return creds, nil
	}

	agencyCredsCacheMu.Lock()
	if agencyCredsCacheValue != nil && time.Since(agencyCredsCacheAt) < agencyCredsCacheTTL {
		cached := agencyCredsCacheValue
		agencyCredsCacheMu.Unlock()
		return cached, nil
	}
	agencyCredsCacheMu.Unlock()

	agencyCreds, err := provider.GlobalCredentialMetadataProvider().GetCredentials()
	if err != nil {
		return nil, fmt.Errorf(
			"huawei obs plugin requires either %s/%s to be set, or an IAM agency configured on the node: %w",
			envAccessKeyID, envAccessKeySecret, err)
	}
	creds, ok := agencyCreds.(*global.Credentials)
	if !ok {
		return nil, fmt.Errorf("huawei obs plugin: unexpected credential type %T from IAM agency", agencyCreds)
	}
	if domainID != "" {
		creds.DomainId = domainID
	}

	agencyCredsCacheMu.Lock()
	agencyCredsCacheValue = creds
	agencyCredsCacheAt = time.Now()
	agencyCredsCacheMu.Unlock()

	return creds, nil
}
