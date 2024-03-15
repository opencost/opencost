package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/cmd"
	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {
	//os.Setenv("KUBECONFIG", "/Users/r2k1/p/playground/costanalysis.yaml") // CHANGEME
	os.Setenv("PROMETHEUS_SERVER_ENDPOINT", "http://localhost:9092")
	os.Setenv("KUBERNETES_PORT", "443")
	os.Setenv("PPROF_ENABLED", "true")
	os.Setenv("CACHE_WARMING_ENABLED", "false")
	os.Setenv("DISABLE_AGGREGATE_COST_MODEL_CACHE", "true")
	os.Setenv("MAX_QUERY_CONCURRENCY", "5")

	go func() {
		err := cmd.Execute(nil)
		require.NoError(t, err)
	}()
	maxUsage := uint64(0)
	go trackMaxMemoryUsage(100*time.Microsecond, &maxUsage)

	time.Sleep(60 * time.Second) // wait for opencost to start

	generateLoad(t)

	t.Log("Max memory usage, KiB:", maxUsage/1024)
}

func generateLoad(t *testing.T) {
	end := time.Now()
	start := end.Add(-time.Hour * 24)
	testFetch(t, fmt.Sprintf("http://localhost:9003/metrics"))
	testFetch(t, fmt.Sprintf("http://localhost:9003/allocation/compute?aggregate=namespace,controllerKind,controller,label:app,label:team,label:pod_template_hash&idleByNode=true&includeIdle=true&includeProportionalAssetResourceCosts=true&step=window&window=%s,%s", start.Format("2006-01-02T15:04:05Z"), end.Format("2006-01-02T15:04:05Z")))
	testFetch(t, fmt.Sprintf("http://localhost:9003/assets?window=%s,%s", start.Format("2006-01-02T15:04:05Z"), end.Format("2006-01-02T15:04:05Z")))
}

func testFetch(t *testing.T, url string) {
	startTime := time.Now()
	resp, err := http.Get(url)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300)
	require.GreaterOrEqual(t, resp.StatusCode, 200)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("%s, %v MiB, %s", time.Since(startTime), len(data)/1024/1024, url)
}

func trackMaxMemoryUsage(interval time.Duration, maxUsage *uint64) {
	var memStats runtime.MemStats

	for {
		runtime.ReadMemStats(&memStats)

		// Update maxUsage if the current HeapAlloc is greater
		if memStats.HeapAlloc > *maxUsage {
			*maxUsage = memStats.HeapAlloc
		}

		time.Sleep(interval)
	}
}
