package model

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ClusterSize defines the configuration for different cluster sizes
type ClusterSize struct {
	Name                   string
	Nodes                  int
	Namespaces             int
	Pods                   int
	Containers             int
	Services               int
	Controllers            int
	Volumes                int
	PersistentVolumeClaims int
	GpuDevices             int
	GpuUsages              int
	LabelCount             int
}

// ClusterSizes defines various cluster size configurations
var ClusterSizes = map[string]ClusterSize{
	"xsm": {
		Name:                   "Extra Small",
		Nodes:                  3,
		Namespaces:             5,
		Pods:                   10,
		Containers:             15,
		Services:               5,
		Controllers:            5,
		Volumes:                8,
		PersistentVolumeClaims: 5,
		GpuDevices:             0,
		GpuUsages:              0,
		LabelCount:             3,
	},
	"sm": {
		Name:                   "Small",
		Nodes:                  5,
		Namespaces:             10,
		Pods:                   25,
		Containers:             50,
		Services:               15,
		Controllers:            10,
		Volumes:                20,
		PersistentVolumeClaims: 15,
		GpuDevices:             2,
		GpuUsages:              4,
		LabelCount:             5,
	},
	"md": {
		Name:                   "Medium",
		Nodes:                  10,
		Namespaces:             20,
		Pods:                   100,
		Containers:             250,
		Services:               50,
		Controllers:            25,
		Volumes:                75,
		PersistentVolumeClaims: 50,
		GpuDevices:             5,
		GpuUsages:              15,
		LabelCount:             8,
	},
	"lg": {
		Name:                   "Large",
		Nodes:                  25,
		Namespaces:             50,
		Pods:                   500,
		Containers:             1250,
		Services:               150,
		Controllers:            75,
		Volumes:                300,
		PersistentVolumeClaims: 200,
		GpuDevices:             15,
		GpuUsages:              75,
		LabelCount:             12,
	},
	"xlg": {
		Name:                   "Extra Large",
		Nodes:                  50,
		Namespaces:             100,
		Pods:                   2000,
		Containers:             5000,
		Services:               500,
		Controllers:            200,
		Volumes:                1000,
		PersistentVolumeClaims: 750,
		GpuDevices:             50,
		GpuUsages:              250,
		LabelCount:             15,
	},
	"2xlg": {
		Name:                   "2X Large",
		Nodes:                  100,
		Namespaces:             200,
		Pods:                   5000,
		Containers:             12500,
		Services:               1500,
		Controllers:            500,
		Volumes:                3000,
		PersistentVolumeClaims: 2000,
		GpuDevices:             100,
		GpuUsages:              500,
		LabelCount:             20,
	},
	"3xlg": {
		Name:                   "3X Large",
		Nodes:                  200,
		Namespaces:             500,
		Pods:                   10000,
		Containers:             25000,
		Services:               3000,
		Controllers:            1000,
		Volumes:                6000,
		PersistentVolumeClaims: 4000,
		GpuDevices:             200,
		GpuUsages:              1000,
		LabelCount:             25,
	},
	"4xlg": {
		Name:                   "4X Large",
		Nodes:                  500,
		Namespaces:             1000,
		Pods:                   25000,
		Containers:             62500,
		Services:               7500,
		Controllers:            2500,
		Volumes:                15000,
		PersistentVolumeClaims: 10000,
		GpuDevices:             500,
		GpuUsages:              2500,
		LabelCount:             30,
	},
	"5xlg": {
		Name:                   "5X Large",
		Nodes:                  1000,
		Namespaces:             2000,
		Pods:                   50000,
		Containers:             125000,
		Services:               15000,
		Controllers:            5000,
		Volumes:                30000,
		PersistentVolumeClaims: 20000,
		GpuDevices:             1000,
		GpuUsages:              5000,
		LabelCount:             35,
	},
	"10xlg": {
		Name:                   "10X Large",
		Nodes:                  2000,
		Namespaces:             5000,
		Pods:                   100000,
		Containers:             250000,
		Services:               30000,
		Controllers:            10000,
		Volumes:                60000,
		PersistentVolumeClaims: 40000,
		GpuDevices:             2000,
		GpuUsages:              10000,
		LabelCount:             40,
	},
}

// BenchmarkResult stores the results of a benchmark run
type BenchmarkResult struct {
	Size              string
	SerializationType string
	SerializedSize    int
	SerializeTime     time.Duration
	DeserializeTime   time.Duration
}

// generateGoCluster creates a cluster using the Go struct implementation
func generateGoCluster(size ClusterSize) *Cluster {
	now := time.Now()
	cluster := &Cluster{
		ID:       fmt.Sprintf("cluster-%s", randomString(8)),
		Provider: ProviderAWS,
		Account:  fmt.Sprintf("account-%s", randomString(6)),
		Name:     fmt.Sprintf("cluster-%s", size.Name),
		Window: &Window{
			Resolution: Resolution1H,
			Start:      now.Truncate(time.Hour),
		},
	}

	// Generate nodes
	for i := 0; i < size.Nodes; i++ {
		cluster.Nodes = append(cluster.Nodes, Node{
			ID:                   fmt.Sprintf("node-%d", i),
			ClusterID:            cluster.ID,
			ProviderResourceID:   fmt.Sprintf("i-%s", randomString(8)),
			Name:                 fmt.Sprintf("node-%d", i),
			Labels:               randomLabels(size.LabelCount),
			Annotations:          randomAnnotations(size.LabelCount),
			CreationTime:         &now,
			CpuCores:             int32(rand.Intn(16) + 2),
			RamBytes:             int64(rand.Intn(32)*1024*1024*1024 + 4*1024*1024*1024),
			CpuCost:              rand.Float32() * 100,
			RamCost:              rand.Float32() * 50,
			CpuCoreUsageAverage:  rand.Float32() * 4,
			CpuCoreUsageMax:      rand.Float32() * 8,
			RamBytesUsageAverage: int64(rand.Intn(16) * 1024 * 1024 * 1024),
			RamBytesUsageMax:     int64(rand.Intn(32) * 1024 * 1024 * 1024),
		})
	}

	// Generate namespaces
	for i := 0; i < size.Namespaces; i++ {
		cluster.Namespaces = append(cluster.Namespaces, Namespace{
			ID:           fmt.Sprintf("ns-%d", i),
			ClusterID:    cluster.ID,
			Name:         fmt.Sprintf("namespace-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: &now,
		})
	}

	// Generate controllers
	for i := 0; i < size.Controllers; i++ {
		cluster.Controllers = append(cluster.Controllers, Controller{
			ID:           fmt.Sprintf("ctrl-%d", i),
			NamespaceID:  fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			Name:         fmt.Sprintf("controller-%d", i),
			Kind:         ControllerKindDeployment,
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: &now,
		})
	}

	// Generate pods
	for i := 0; i < size.Pods; i++ {
		cluster.Pods = append(cluster.Pods, Pod{
			ID:                     fmt.Sprintf("pod-%d", i),
			NamespaceID:            fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			ControllerID:           fmt.Sprintf("ctrl-%d", rand.Intn(size.Controllers)),
			NodeID:                 fmt.Sprintf("node-%d", rand.Intn(size.Nodes)),
			Name:                   fmt.Sprintf("pod-%d", i),
			Labels:                 randomLabels(size.LabelCount),
			Annotations:            randomAnnotations(size.LabelCount),
			CreationTime:           &now,
			CpuCoreHours:           rand.Float32() * 10,
			CpuCoreRequestAverage:  rand.Float32() * 2,
			CpuCoreUsageAverage:    rand.Float32() * 1.5,
			CpuCoreUsageMax:        rand.Float32() * 3,
			RamByteHours:           int64(rand.Intn(1000) * 1024 * 1024),
			RamBytesRequestAverage: int64(rand.Intn(2048) * 1024 * 1024),
			RamBytesUsageAverage:   int64(rand.Intn(1536) * 1024 * 1024),
			RamBytesUsageMax:       int64(rand.Intn(3072) * 1024 * 1024),
			StorageByteHours:       int64(rand.Intn(10000) * 1024 * 1024),
			NetworkTransferBytes:   int64(rand.Intn(1000000)),
			NetworkReceiveBytes:    int64(rand.Intn(1000000)),
		})
	}

	// Generate containers
	for i := 0; i < size.Containers; i++ {
		cluster.Containers = append(cluster.Containers, Container{
			PodID:                  fmt.Sprintf("pod-%d", rand.Intn(size.Pods)),
			Name:                   fmt.Sprintf("container-%d", i),
			CreationTime:           &now,
			CpuCoreHours:           rand.Float32() * 5,
			CpuCoreRequestAverage:  rand.Float32() * 1,
			CpuCoreUsageAverage:    rand.Float32() * 0.8,
			CpuCoreUsageMax:        rand.Float32() * 2,
			RamByteHours:           int64(rand.Intn(500) * 1024 * 1024),
			RamBytesRequestAverage: int64(rand.Intn(1024) * 1024 * 1024),
			RamBytesUsageAverage:   int64(rand.Intn(768) * 1024 * 1024),
			RamBytesUsageMax:       int64(rand.Intn(1536) * 1024 * 1024),
		})
	}

	// Generate services
	for i := 0; i < size.Services; i++ {
		cluster.Services = append(cluster.Services, Service{
			ID:          fmt.Sprintf("svc-%d", i),
			ClusterID:   cluster.ID,
			NamespaceID: fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			Name:        fmt.Sprintf("service-%d", i),
			Type:        "ClusterIP",
			Labels:      randomLabels(size.LabelCount),
			Annotations: randomAnnotations(size.LabelCount),
			Ports: []ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: 8080,
					Protocol:   "TCP",
				},
			},
			CreationTime: &now,
		})
	}

	// Generate volumes
	for i := 0; i < size.Volumes; i++ {
		cluster.Volumes = append(cluster.Volumes, Volume{
			ID:           fmt.Sprintf("vol-%d", i),
			ClusterID:    cluster.ID,
			Name:         fmt.Sprintf("volume-%d", i),
			Namespace:    fmt.Sprintf("namespace-%d", rand.Intn(size.Namespaces)),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: &now,
			StorageClass: "gp2",
			Size:         int64(rand.Intn(100) + 10) * 1024 * 1024 * 1024,
			Cost:         rand.Float32() * 50,
		})
	}

	// Generate PVCs
	for i := 0; i < size.PersistentVolumeClaims; i++ {
		cluster.PersistentVolumeClaims = append(cluster.PersistentVolumeClaims, PersistentVolumeClaim{
			ID:           fmt.Sprintf("pvc-%d", i),
			NamespaceID:  fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			Name:         fmt.Sprintf("pvc-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: &now,
			StorageClass: "gp2",
			Size:         int64(rand.Intn(100) + 10) * 1024 * 1024 * 1024,
			VolumeName:   fmt.Sprintf("vol-%d", rand.Intn(size.Volumes)),
		})
	}

	// Generate GPU devices
	for i := 0; i < size.GpuDevices; i++ {
		cluster.GpuDevices = append(cluster.GpuDevices, GPUDevice{
			ID:     fmt.Sprintf("gpu-%d", i),
			NodeID: fmt.Sprintf("node-%d", rand.Intn(size.Nodes)),
			Name:   fmt.Sprintf("GPU-%d", i),
			Model:  "NVIDIA Tesla V100",
			Memory: 16 * 1024 * 1024 * 1024, // 16GB
			Count:  1,
		})
	}

	// Generate GPU usage
	for i := 0; i < size.GpuUsages; i++ {
		cluster.GpuUsage = append(cluster.GpuUsage, GPUUsage{
			ContainerID:        fmt.Sprintf("container-%d", rand.Intn(size.Containers)),
			GpuDeviceID:        fmt.Sprintf("gpu-%d", rand.Intn(maxInt(1, size.GpuDevices))),
			UtilizationAverage: rand.Float32() * 100,
			UtilizationMax:     rand.Float32() * 100,
			MemoryUsageAverage: int64(rand.Intn(16) * 1024 * 1024 * 1024),
			MemoryUsageMax:     int64(rand.Intn(16) * 1024 * 1024 * 1024),
		})
	}

	return cluster
}

// generateProtoCluster creates a cluster using the protobuf implementation
func generateProtoCluster(size ClusterSize) *kubemodel.Cluster {
	now := timestamppb.Now()
	cluster := &kubemodel.Cluster{
		ID:       fmt.Sprintf("cluster-%s", randomString(8)),
		Provider: kubemodel.Provider_PROVIDER_AWS,
		Account:  fmt.Sprintf("account-%s", randomString(6)),
		Name:     fmt.Sprintf("cluster-%s", size.Name),
		Window: &pb.Window{
			Resolution: pb.Resolution_RESOLUTION_1H,
			Start:      now,
		},
	}

	// Generate nodes
	for i := 0; i < size.Nodes; i++ {
		cluster.Nodes = append(cluster.Nodes, &kubemodel.Node{
			ID:                   fmt.Sprintf("node-%d", i),
			ClusterID:            cluster.ID,
			ProviderResourceID:   fmt.Sprintf("i-%s", randomString(8)),
			Name:                 fmt.Sprintf("node-%d", i),
			Labels:               randomLabels(size.LabelCount),
			Annotations:          randomAnnotations(size.LabelCount),
			CreationTime:         now,
		})
	}

	// Generate namespaces
	for i := 0; i < size.Namespaces; i++ {
		cluster.Namespaces = append(cluster.Namespaces, &kubemodel.Namespace{
			ID:           fmt.Sprintf("ns-%d", i),
			ClusterID:    cluster.ID,
			Name:         fmt.Sprintf("namespace-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: now,
		})
	}

	// Generate controllers
	for i := 0; i < size.Controllers; i++ {
		cluster.Controllers = append(cluster.Controllers, &kubemodel.Controller{
			ID:           fmt.Sprintf("ctrl-%d", i),
			NamespaceID:  fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			Name:         fmt.Sprintf("controller-%d", i),
			Kind:         kubemodel.ControllerKind_DEPLOYMENT,
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: now,
		})
	}

	// Generate pods
	for i := 0; i < size.Pods; i++ {
		cluster.Pods = append(cluster.Pods, &kubemodel.Pod{
			ID:           fmt.Sprintf("pod-%d", i),
			NamespaceID:  fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			ControllerID: fmt.Sprintf("ctrl-%d", rand.Intn(size.Controllers)),
			NodeID:       fmt.Sprintf("node-%d", rand.Intn(size.Nodes)),
			Name:         fmt.Sprintf("pod-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: now,
		})
	}

	// Generate containers
	for i := 0; i < size.Containers; i++ {
		cluster.Containers = append(cluster.Containers, &kubemodel.Container{
			PodID:        fmt.Sprintf("pod-%d", rand.Intn(size.Pods)),
			Name:         fmt.Sprintf("container-%d", i),
			CreationTime: now,
		})
	}

	// Generate services
	for i := 0; i < size.Services; i++ {
		cluster.Services = append(cluster.Services, &kubemodel.Service{
			ID:          fmt.Sprintf("svc-%d", i),
			ClusterID:   cluster.ID,
			Name:        fmt.Sprintf("service-%d", i),
			ServiceType: "ClusterIP",
			Labels:      randomLabels(size.LabelCount),
			Annotations: randomAnnotations(size.LabelCount),
			Ports: []*kubemodel.ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: 8080,
					Protocol:   "TCP",
				},
			},
			CreationTime: now,
		})
	}

	// Generate volumes
	for i := 0; i < size.Volumes; i++ {
		cluster.Volumes = append(cluster.Volumes, &kubemodel.Volume{
			ID:           fmt.Sprintf("vol-%d", i),
			ClusterID:    cluster.ID,
			Name:         fmt.Sprintf("volume-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: now,
		})
	}

	// Generate PVCs
	for i := 0; i < size.PersistentVolumeClaims; i++ {
		cluster.PersistentVolumeClaims = append(cluster.PersistentVolumeClaims, &kubemodel.PersistentVolumeClaim{
			ID:           fmt.Sprintf("pvc-%d", i),
			NamespaceID:  fmt.Sprintf("ns-%d", rand.Intn(size.Namespaces)),
			Name:         fmt.Sprintf("pvc-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomAnnotations(size.LabelCount),
			CreationTime: now,
		})
	}

	// Generate GPU devices
	for i := 0; i < size.GpuDevices; i++ {
		cluster.GpuDevices = append(cluster.GpuDevices, &kubemodel.GPUDevice{
			ID:           fmt.Sprintf("gpu-%d", i),
			NodeID:       fmt.Sprintf("node-%d", rand.Intn(size.Nodes)),
			DeviceNumber: int32(i),
			ModelName:    "NVIDIA Tesla V100",
			MemoryBytes:  16 * 1024 * 1024 * 1024, // 16GB
		})
	}

	// Generate GPU usage
	for i := 0; i < size.GpuUsages; i++ {
		cluster.GpuUsage = append(cluster.GpuUsage, &kubemodel.GPUUsage{
			ContainerID: fmt.Sprintf("container-%d", rand.Intn(size.Containers)),
			GpuDeviceID: fmt.Sprintf("gpu-%d", rand.Intn(maxInt(1, size.GpuDevices))),
		})
	}

	return cluster
}

// Helper functions
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randomLabels(count int) map[string]string {
	labels := make(map[string]string)

	labelKeys := []string{
		"app", "version", "component", "managed-by", "tier", "environment",
		"team", "service", "owner", "release", "stage", "region", "zone",
		"cluster", "namespace", "workload", "project", "cost-center", "department", "purpose",
	}

	for i := 0; i < count && i < len(labelKeys); i++ {
		key := labelKeys[i]
		switch key {
		case "version":
			labels[key] = fmt.Sprintf("v%d.%d.%d", rand.Intn(3)+1, rand.Intn(10), rand.Intn(20))
		case "environment":
			envs := []string{"production", "staging", "development", "test"}
			labels[key] = envs[rand.Intn(len(envs))]
		case "tier":
			tiers := []string{"frontend", "backend", "database", "cache"}
			labels[key] = tiers[rand.Intn(len(tiers))]
		case "managed-by":
			labels[key] = "opencost"
		default:
			labels[key] = randomString(8)
		}
	}

	return labels
}

func randomAnnotations(count int) map[string]string {
	annotations := make(map[string]string)

	annotationKeys := []string{
		"deployment.kubernetes.io/revision",
		"kubectl.kubernetes.io/last-applied-configuration",
		"prometheus.io/scrape",
		"prometheus.io/port",
		"prometheus.io/path",
		"fluentd.org/exclude",
		"sidecar.istio.io/inject",
		"linkerd.io/inject",
		"kubernetes.io/ingress.class",
		"cert-manager.io/cluster-issuer",
		"nginx.ingress.kubernetes.io/rewrite-target",
		"datadog.ad.check_names",
		"vault.hashicorp.com/agent-inject",
		"backup.velero.io/backup-volumes",
		"cluster-autoscaler.kubernetes.io/safe-to-evict",
		"scheduler.alpha.kubernetes.io/node-selector",
	}

	for i := 0; i < count && i < len(annotationKeys); i++ {
		key := annotationKeys[i]
		switch key {
		case "deployment.kubernetes.io/revision":
			annotations[key] = fmt.Sprintf("%d", rand.Intn(10)+1)
		case "kubectl.kubernetes.io/last-applied-configuration":
			annotations[key] = `{"apiVersion":"v1","kind":"Pod"}`
		case "prometheus.io/scrape":
			annotations[key] = "true"
		case "prometheus.io/port":
			annotations[key] = "8080"
		case "prometheus.io/path":
			annotations[key] = "/metrics"
		case "sidecar.istio.io/inject":
			annotations[key] = "true"
		case "linkerd.io/inject":
			annotations[key] = "enabled"
		case "kubernetes.io/ingress.class":
			annotations[key] = "nginx"
		default:
			annotations[key] = randomString(12)
		}
	}

	return annotations
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Benchmark functions
func benchmarkSerialization(b *testing.B, sizeName string, size ClusterSize) {
	goCluster := generateGoCluster(size)
	protoCluster := generateProtoCluster(size)

	b.Run(fmt.Sprintf("%s/JSON", sizeName), func(b *testing.B) {
		var serializedSize int
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := json.Marshal(goCluster)
			if err != nil {
				b.Fatal(err)
			}
			serializedSize = len(data)
		}
		b.ReportMetric(float64(serializedSize), "bytes")
	})

	b.Run(fmt.Sprintf("%s/Protobuf", sizeName), func(b *testing.B) {
		var serializedSize int
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := proto.Marshal(protoCluster)
			if err != nil {
				b.Fatal(err)
			}
			serializedSize = len(data)
		}
		b.ReportMetric(float64(serializedSize), "bytes")
	})
}

func benchmarkDeserialization(b *testing.B, sizeName string, size ClusterSize) {
	goCluster := generateGoCluster(size)
	protoCluster := generateProtoCluster(size)

	jsonData, _ := json.Marshal(goCluster)
	protoData, _ := proto.Marshal(protoCluster)

	b.Run(fmt.Sprintf("%s/JSON", sizeName), func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var cluster Cluster
			err := json.Unmarshal(jsonData, &cluster)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run(fmt.Sprintf("%s/Protobuf", sizeName), func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var cluster kubemodel.Cluster
			err := proto.Unmarshal(protoData, &cluster)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark tests for serialization
func BenchmarkSerialization(b *testing.B) {
	for sizeName, size := range ClusterSizes {
		benchmarkSerialization(b, sizeName, size)
	}
}

// Benchmark tests for deserialization
func BenchmarkDeserialization(b *testing.B) {
	for sizeName, size := range ClusterSizes {
		benchmarkDeserialization(b, sizeName, size)
	}
}

// Test function that generates comparison tables
func TestSerializationComparison(t *testing.T) {
	results := make(map[string][]BenchmarkResult)

	for sizeName, size := range ClusterSizes {
		goCluster := generateGoCluster(size)
		protoCluster := generateProtoCluster(size)

		// Test JSON serialization
		start := time.Now()
		jsonData, err := json.Marshal(goCluster)
		if err != nil {
			t.Fatal(err)
		}
		jsonSerializeTime := time.Since(start)

		start = time.Now()
		var jsonCluster Cluster
		err = json.Unmarshal(jsonData, &jsonCluster)
		if err != nil {
			t.Fatal(err)
		}
		jsonDeserializeTime := time.Since(start)

		// Test Protobuf serialization
		start = time.Now()
		protoData, err := proto.Marshal(protoCluster)
		if err != nil {
			t.Fatal(err)
		}
		protoSerializeTime := time.Since(start)

		start = time.Now()
		var protoClusterResult kubemodel.Cluster
		err = proto.Unmarshal(protoData, &protoClusterResult)
		if err != nil {
			t.Fatal(err)
		}
		protoDeserializeTime := time.Since(start)

		results[sizeName] = []BenchmarkResult{
			{
				Size:              sizeName,
				SerializationType: "JSON",
				SerializedSize:    len(jsonData),
				SerializeTime:     jsonSerializeTime,
				DeserializeTime:   jsonDeserializeTime,
			},
			{
				Size:              sizeName,
				SerializationType: "Protobuf",
				SerializedSize:    len(protoData),
				SerializeTime:     protoSerializeTime,
				DeserializeTime:   protoDeserializeTime,
			},
		}
	}

	// Print results table
	printComparisonTable(results)
}

func printComparisonTable(results map[string][]BenchmarkResult) {
	fmt.Printf("\n=== Serialization Performance Comparison ===\n\n")

	// Size comparison table
	fmt.Printf("%-8s | %-12s | %-15s | %-15s | %-18s | %-18s\n",
		"Size", "Format", "Serialized Size", "Size Ratio", "Serialize Time", "Deserialize Time")
	fmt.Printf("---------|--------------|-----------------|-----------------|--------------------|-----------------\n")

	sizeOrder := []string{"xsm", "sm", "md", "lg", "xlg", "2xlg", "3xlg", "4xlg", "5xlg", "10xlg"}
	for _, sizeName := range sizeOrder {
		if sizeResults, ok := results[sizeName]; ok {
			jsonResult := sizeResults[0]
			protoResult := sizeResults[1]

			sizeRatio := float64(protoResult.SerializedSize) / float64(jsonResult.SerializedSize)

			fmt.Printf("%-8s | %-12s | %13d B | %13s | %16s | %16s\n",
				sizeName, jsonResult.SerializationType,
				jsonResult.SerializedSize, "-",
				jsonResult.SerializeTime.String(),
				jsonResult.DeserializeTime.String())

			fmt.Printf("%-8s | %-12s | %13d B | %13.2fx | %16s | %16s\n",
				"", protoResult.SerializationType,
				protoResult.SerializedSize, sizeRatio,
				protoResult.SerializeTime.String(),
				protoResult.DeserializeTime.String())
			fmt.Printf("---------|--------------|-----------------|-----------------|--------------------|-----------------\n")
		}
	}

	// Summary table
	fmt.Printf("\n=== Performance Summary ===\n\n")
	fmt.Printf("%-8s | %-20s | %-20s | %-20s\n", "Size", "JSON vs Proto Size", "JSON vs Proto Ser", "JSON vs Proto Deser")
	fmt.Printf("---------|----------------------|----------------------|---------------------\n")

	for _, sizeName := range sizeOrder {
		if sizeResults, ok := results[sizeName]; ok {
			jsonResult := sizeResults[0]
			protoResult := sizeResults[1]

			sizeRatio := float64(protoResult.SerializedSize) / float64(jsonResult.SerializedSize)
			serRatio := float64(protoResult.SerializeTime.Nanoseconds()) / float64(jsonResult.SerializeTime.Nanoseconds())
			deserRatio := float64(protoResult.DeserializeTime.Nanoseconds()) / float64(jsonResult.DeserializeTime.Nanoseconds())

			fmt.Printf("%-8s | %18.2fx | %18.2fx | %18.2fx\n",
				sizeName, sizeRatio, serRatio, deserRatio)
		}
	}
}