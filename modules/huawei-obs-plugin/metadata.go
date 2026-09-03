package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// metadataEndpoint is the ECS/CCE instance metadata service base URL. The IAM
// agency's temporary credentials (see huaweiGlobalCredentials in credentials.go)
// come from GetSecurityKeyPath on this same host, but that response carries no
// project_id field -- project ID is region-specific, not part of the
// AK/SK/security-token triple. meta_data.json is where it actually lives; this is
// the exact mechanism mlops-infra/images/ces-exporter-image/ces_exporter.py
// already uses to resolve project_id for CES without any static config.
var metadataEndpoint = "http://169.254.169.254"

const metadataPath = "/openstack/latest/meta_data.json"

// projectIDCacheTTL bounds how long a resolved project ID is reused before
// hitting the metadata service again. GetCustomCosts calls
// huaweiProjectIDFromMetadata once per ingestion window -- hundreds of calls
// in a single build cycle when backfilling a month in 1h blocks -- and without
// caching this floods 169.254.169.254 until it starts responding 503 "The
// request is too frequent. Please try again later!", failing every window in
// the batch. The project ID never changes for the life of the node, so this
// could be cached indefinitely, but a bounded TTL still allows recovery if the
// first lookup raced the metadata service being unready.
const projectIDCacheTTL = 30 * time.Minute

var (
	projectIDCacheMu    sync.Mutex
	projectIDCacheValue string
	projectIDCacheAt    time.Time
)

// huaweiProjectIDFromMetadata resolves the current node's Huawei Cloud IAM
// project ID from the instance metadata service, for nodes that have an IAM
// agency attached but no HUAWEICLOUD_PROJECT_ID provisioned anywhere. Results
// are cached (see projectIDCacheTTL).
func huaweiProjectIDFromMetadata() (string, error) {
	projectIDCacheMu.Lock()
	if projectIDCacheValue != "" && time.Since(projectIDCacheAt) < projectIDCacheTTL {
		cached := projectIDCacheValue
		projectIDCacheMu.Unlock()
		return cached, nil
	}
	projectIDCacheMu.Unlock()

	projectID, err := fetchHuaweiProjectIDFromMetadata()
	if err != nil {
		return "", err
	}

	projectIDCacheMu.Lock()
	projectIDCacheValue = projectID
	projectIDCacheAt = time.Now()
	projectIDCacheMu.Unlock()

	return projectID, nil
}

func fetchHuaweiProjectIDFromMetadata() (string, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(metadataEndpoint + metadataPath)
	if err != nil {
		return "", fmt.Errorf("fetching huawei cloud instance metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("fetching huawei cloud instance metadata: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading huawei cloud instance metadata: %w", err)
	}

	var meta struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return "", fmt.Errorf("parsing huawei cloud instance metadata: %w", err)
	}
	if meta.ProjectID == "" {
		return "", fmt.Errorf("huawei cloud instance metadata: project_id empty")
	}
	return meta.ProjectID, nil
}
