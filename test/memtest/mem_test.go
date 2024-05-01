// don't run this test by default

//go:build ignore

package memtest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/cmd"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func RunOpencost(t *testing.T, promEndpoint string) {
	t.Setenv("KUBECONFIG", kubeConfigPath())
	t.Setenv("PROMETHEUS_SERVER_ENDPOINT", fmt.Sprintf("http://%s", promEndpoint))
	t.Setenv("KUBERNETES_PORT", "443")
	t.Setenv("PPROF_ENABLED", "true")
	t.Setenv("CACHE_WARMING_ENABLED", "false")
	t.Setenv("DISABLE_AGGREGATE_COST_MODEL_CACHE", "true")
	t.Setenv("MAX_QUERY_CONCURRENCY", "5")
	t.Setenv("CONFIG_PATH", "../../configs")

	go func() {
		err := cmd.Execute(nil)
		require.NoError(t, err)
	}()
	maxUsage := uint64(0)
	go trackMaxMemoryUsage(context.TODO(), 100*time.Microsecond, &maxUsage)

	time.Sleep(10 * time.Second) // wait for opencost to start

	generateLoad(t)

	t.Log("Max memory usage, KiB:", maxUsage/1024)
	// uncomment to pause execution and explore the prometheus UI
	//t.Logf("Execution complete. Analyze http://%s for more results", promEndpoint)
	//select {} // block execution
}

func generateLoad(t *testing.T) {
	testFetch(t, fmt.Sprintf("http://localhost:9003/metrics"))
	//end := time.Now()
	//start := end.Add(-time.Hour * 24)
	//testFetch(t, fmt.Sprintf("http://localhost:9003/allocation/compute?aggregate=namespace,controllerKind,controller,label:app,label:team,label:pod_template_hash&idleByNode=true&includeIdle=true&includeProportionalAssetResourceCosts=true&step=window&window=%s,%s", start.Format("2006-01-02T15:04:05Z"), end.Format("2006-01-02T15:04:05Z")))
	//testFetch(t, fmt.Sprintf("http://localhost:9003/assets?window=%s,%s", start.Format("2006-01-02T15:04:05Z"), end.Format("2006-01-02T15:04:05Z")))
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

func trackMaxMemoryUsage(ctx context.Context, interval time.Duration, maxUsage *uint64) {
	var memStats runtime.MemStats
	t := time.NewTicker(interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runtime.ReadMemStats(&memStats)

			// Update maxUsage if the current HeapAlloc is greater
			if memStats.HeapAlloc > *maxUsage {
				*maxUsage = memStats.HeapAlloc
			}
		}
	}
}

func TestMemoryUsage(t *testing.T) {
	LaunchKubeProxy(t)
	endpoint := LaunchVictoriaMetrics(t)
	RunOpencost(t, endpoint)
}

func LaunchKubeProxy(t *testing.T) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "kubectl", "proxy", "--accept-hosts", `^localhost$,^127\.0\.0\.1$,^\[::1\]$,^host.testcontainers.internal$`)
	t.Cleanup(func() {
		err := cmd.Cancel()
		require.NoError(t, err)

	})
	cmd.Stdout = os.Stdout
	err := cmd.Start()
	require.NoError(t, err)
}

func LaunchVictoriaMetrics(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	vm, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{

			Image:        "victoriametrics/victoria-metrics:latest",
			ExposedPorts: []string{"8428/tcp"},
			WaitingFor:   wait.ForListeningPort("8428/tcp"), // note, container isn't accessible on this port, a random free port is used instead. Check logs for the actual port.
			HostAccessPorts: []int{
				8001, // kube proxy
				9003, // opencost
			},
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      "prometheus_config.yaml",
					ContainerFilePath: "/prometheus_config.yaml",
				},
				{
					ContainerFilePath: "/.kube/config",
					HostFilePath:      kubeConfigPath(),
				},
			},
			Cmd: []string{"-promscrape.config=/prometheus_config.yaml"},
			LogConsumerCfg: &testcontainers.LogConsumerConfig{
				Consumers: []testcontainers.LogConsumer{
					&testcontainers.StdoutLogConsumer{},
				},
			},
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := vm.Terminate(ctx)
		require.NoError(t, err)
	})
	endpoint, err := vm.Endpoint(ctx, "")
	require.NoError(t, err)
	t.Logf("VictoriaMetrics endpoint: %s", endpoint)
	return endpoint
}

func kubeConfigPath() string {
	env := os.Getenv("KUBECONFIG")
	if env != "" {
		return env
	}
	return os.ExpandEnv("$HOME/.kube/config")
}
