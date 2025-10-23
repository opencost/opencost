package model

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pb"
	"github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
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
		Namespaces:             3,
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
		Nodes:                  10,
		Namespaces:             20,
		Pods:                   50,
		Containers:             100,
		Services:               25,
		Controllers:            20,
		Volumes:                40,
		PersistentVolumeClaims: 30,
		GpuDevices:             5,
		GpuUsages:              10,
		LabelCount:             5,
	},
	"md": {
		Name:                   "Medium",
		Nodes:                  50,
		Namespaces:             100,
		Pods:                   500,
		Containers:             1000,
		Services:               250,
		Controllers:            100,
		Volumes:                400,
		PersistentVolumeClaims: 300,
		GpuDevices:             50,
		GpuUsages:              100,
		LabelCount:             10,
	},
	"lg": {
		Name:                   "Large",
		Nodes:                  200,
		Namespaces:             500,
		Pods:                   2000,
		Containers:             5000,
		Services:               1000,
		Controllers:            500,
		Volumes:                1500,
		PersistentVolumeClaims: 1000,
		GpuDevices:             200,
		GpuUsages:              500,
		LabelCount:             15,
	},
	"xlg": {
		Name:                   "Extra Large",
		Nodes:                  1000,
		Namespaces:             1000,
		Pods:                   5000,
		Containers:             10000,
		Services:               2500,
		Controllers:            1000,
		Volumes:                3000,
		PersistentVolumeClaims: 2000,
		GpuDevices:             1000,
		GpuUsages:              2000,
		LabelCount:             20,
	},
	"2xlg": {
		Name:                   "2X Large",
		Nodes:                  5000,
		Namespaces:             2000,
		Pods:                   15000,
		Containers:             30000,
		Services:               7500,
		Controllers:            3000,
		Volumes:                10000,
		PersistentVolumeClaims: 7500,
		GpuDevices:             5000,
		GpuUsages:              10000,
		LabelCount:             30,
	},
	"3xlg": {
		Name:                   "3X Large",
		Nodes:                  10000,
		Namespaces:             3000,
		Pods:                   30000,
		Containers:             60000,
		Services:               15000,
		Controllers:            6000,
		Volumes:                20000,
		PersistentVolumeClaims: 15000,
		GpuDevices:             10000,
		GpuUsages:              20000,
		LabelCount:             40,
	},
	"4xlg": {
		Name:                   "4X Large",
		Nodes:                  25000,
		Namespaces:             5000,
		Pods:                   60000,
		Containers:             120000,
		Services:               30000,
		Controllers:            12000,
		Volumes:                40000,
		PersistentVolumeClaims: 30000,
		GpuDevices:             25000,
		GpuUsages:              50000,
		LabelCount:             60,
	},
	"5xlg": {
		Name:                   "5X Large",
		Nodes:                  50000,
		Namespaces:             7500,
		Pods:                   100000,
		Containers:             200000,
		Services:               50000,
		Controllers:            20000,
		Volumes:                60000,
		PersistentVolumeClaims: 50000,
		GpuDevices:             50000,
		GpuUsages:              75000,
		LabelCount:             80,
	},
	"10xlg": {
		Name:                   "10X Large",
		Nodes:                  100000,
		Namespaces:             10000,
		Pods:                   150000,
		Containers:             300000,
		Services:               100000,
		Controllers:            100000,
		Volumes:                100000,
		PersistentVolumeClaims: 100000,
		GpuDevices:             100000,
		GpuUsages:              100000,
		LabelCount:             100,
	},
}

// BenchmarkResult stores the results of a benchmark run
type BenchmarkResult struct {
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
			Annotations:          randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
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
			Annotations:            randomLabels(size.LabelCount),
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
			Annotations: randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
			CreationTime: &now,
			StorageClass: "gp2",
			Size:         int64(rand.Intn(100)+10) * 1024 * 1024 * 1024,
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
			Annotations:  randomLabels(size.LabelCount),
			CreationTime: &now,
			StorageClass: "gp2",
			Size:         int64(rand.Intn(100)+10) * 1024 * 1024 * 1024,
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
			ID:                 fmt.Sprintf("node-%d", i),
			ClusterID:          cluster.ID,
			ProviderResourceID: fmt.Sprintf("i-%s", randomString(8)),
			Name:               fmt.Sprintf("node-%d", i),
			Labels:             randomLabels(size.LabelCount),
			Annotations:        randomLabels(size.LabelCount),
			CreationTime:       now,
		})
	}

	// Generate namespaces
	for i := 0; i < size.Namespaces; i++ {
		cluster.Namespaces = append(cluster.Namespaces, &kubemodel.Namespace{
			ID:           fmt.Sprintf("ns-%d", i),
			ClusterID:    cluster.ID,
			Name:         fmt.Sprintf("namespace-%d", i),
			Labels:       randomLabels(size.LabelCount),
			Annotations:  randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
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
			Annotations: randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
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
			Annotations:  randomLabels(size.LabelCount),
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
	labels := make(map[string]string, count)
	for range count {
		key := randomString(12)
		value := randomString(12)
		labels[key] = value
	}
	return labels
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// GobSerializer implements Go's gob serialization

func GobDecoder[T any](data []byte) (*T, error) {
	var target *T = new(T)

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(target)
	if err != nil {
		return nil, fmt.Errorf("failed to decode with GOB: %w", err)
	}
	return target, nil
}

type GobEncoder[T any] struct{}

func NewGobEncoder[T any]() exporter.Encoder[T] {
	return new(GobEncoder[T])
}

func (g *GobEncoder[T]) FileExt() string {
	return "gob"
}

func (g *GobEncoder[T]) Encode(data *T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	return buf.Bytes(), err
}

type SerializationTester[T any] struct {
	Name    string
	Decoder exporter.Decoder[T]
	Encoder exporter.Encoder[T]
}

func (s *SerializationTester[T]) Run(data *T) (*BenchmarkResult, error) {

	// Test serialization
	start := time.Now()
	serializedData, err := s.Encoder.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("serializer %s failed to encode data: %w", s.Name, err)
	}
	serializeTime := time.Since(start)

	// Test deserialization
	start = time.Now()

	_, err = s.Decoder(serializedData)

	if err != nil {
		return nil, fmt.Errorf("serializer %s failed to decode data: %w", s.Name, err)
	}
	deserializeTime := time.Since(start)

	result := &BenchmarkResult{
		SerializationType: s.Name,
		SerializedSize:    len(serializedData),
		SerializeTime:     serializeTime,
		DeserializeTime:   deserializeTime,
	}

	return result, nil
}

// TestMultiSerializationComparison generates comparison tables for multiple serialization types
func TestMultiSerializationComparison(t *testing.T) {
	results := make(map[string][]BenchmarkResult)

	serializationTesters := []*SerializationTester[Cluster]{
		{
			Name:    "JSON",
			Decoder: exporter.JSONDecoder[Cluster],
			Encoder: exporter.NewJSONEncoder[Cluster](),
		},
		{
			Name:    "JSON GZIP",
			Decoder: exporter.GetGzipDecoder(exporter.JSONDecoder[Cluster]),
			Encoder: exporter.NewGZipEncoder(exporter.NewJSONEncoder[Cluster]()),
		},
		{
			Name:    "Bingen",
			Decoder: exporter.BingenDecoder[Cluster],
			Encoder: exporter.NewBingenEncoder[Cluster](),
		},
		{
			Name:    "Bingen GZIP",
			Decoder: exporter.GetGzipDecoder(exporter.BingenDecoder[Cluster]),
			Encoder: exporter.NewGZipEncoder(exporter.NewBingenEncoder[Cluster]()),
		},
		{
			Name:    "GOB",
			Decoder: GobDecoder[Cluster],
			Encoder: NewGobEncoder[Cluster](),
		},
		{
			Name:    "GOB GZIP",
			Decoder: exporter.GetGzipDecoder(GobDecoder[Cluster]),
			Encoder: exporter.NewGZipEncoder(NewGobEncoder[Cluster]()),
		},
	}

	protoSerializationTesters := []*SerializationTester[kubemodel.Cluster]{
		{
			Name:    "Protobuf",
			Decoder: exporter.ProtobufDecoder[kubemodel.Cluster],
			Encoder: exporter.NewProtobufEncoder[kubemodel.Cluster](),
		},
		{
			Name:    "Protobuf GZIP",
			Decoder: exporter.GetGzipDecoder(exporter.ProtobufDecoder[kubemodel.Cluster]),
			Encoder: exporter.NewGZipEncoder(exporter.NewProtobufEncoder[kubemodel.Cluster]()),
		},
	}

	for sizeName, size := range ClusterSizes {
		log.Infof("Running benchmarks for size %s", sizeName)
		var sizeResults []BenchmarkResult

		goCluster := generateGoCluster(size)
		for _, st := range serializationTesters {
			log.Infof("Running benchmarks for %s", st.Name)
			result, err := st.Run(goCluster)
			if err != nil {
				t.Fatal(err)
			}

			sizeResults = append(sizeResults, *result)
		}

		protoCluster := generateProtoCluster(size)
		for _, st := range protoSerializationTesters {
			log.Infof("Running benchmarks for %s", st.Name)
			result, err := st.Run(protoCluster)
			if err != nil {
				t.Fatal(err)
			}

			sizeResults = append(sizeResults, *result)
		}

		results[sizeName] = sizeResults
	}

	// Print results table
	printMultiSerializationTable(results)
}

func printMultiSerializationTable(results map[string][]BenchmarkResult) {
	fmt.Printf("\n=== Multi-Type Serialization Performance Comparison ===\n\n")

	// Size comparison table
	fmt.Printf("%-8s | %-12s | %-15s | %-15s | %-18s | %-18s\n",
		"Size", "Format", "Serialized Size", "Size Ratio", "Serialize Time", "Deserialize Time")
	fmt.Printf("---------|--------------|-----------------|-----------------|--------------------|-----------------\n")

	sizeOrder := []string{"xsm", "sm", "md", "lg", "xlg", "2xlg", "3xlg", "4xlg", "5xlg", "10xlg"}

	for _, sizeName := range sizeOrder {
		if sizeResults, ok := results[sizeName]; ok {
			// Find JSON result as baseline
			var jsonResult *BenchmarkResult
			for _, result := range sizeResults {
				if result.SerializationType == "JSON" {
					jsonResult = &result
					break
				}
			}

			if jsonResult == nil {
				continue
			}

			// Print all results for this size
			for i, result := range sizeResults {
				sizeRatio := "-"
				if result.SerializationType != "JSON" {
					sizeRatio = fmt.Sprintf("%.2fx", float64(result.SerializedSize)/float64(jsonResult.SerializedSize))
				}

				fmt.Printf("%-8s | %-12s | %13d B | %13s | %16s | %16s\n",
					func() string {
						if i == 0 {
							return sizeName
						}
						return ""
					}(),
					result.SerializationType,
					result.SerializedSize, sizeRatio,
					result.SerializeTime.String(),
					result.DeserializeTime.String())
			}
			fmt.Printf("---------|--------------|-----------------|-----------------|--------------------|-----------------\n")
		}
	}

	// Summary table comparing all formats to JSON
	fmt.Printf("\n=== Performance Summary (Relative to JSON) ===\n\n")
	fmt.Printf("%-8s | %-12s | %-15s | %-15s | %-15s\n", "Size", "Format", "Size Ratio", "Ser Ratio", "Deser Ratio")
	fmt.Printf("---------|--------------|-----------------|-----------------|----------------\n")

	for _, sizeName := range sizeOrder {
		if sizeResults, ok := results[sizeName]; ok {
			// Find JSON result as baseline
			var jsonResult *BenchmarkResult
			for _, result := range sizeResults {
				if result.SerializationType == "JSON" {
					jsonResult = &result
					break
				}
			}

			if jsonResult == nil {
				continue
			}

			// Print ratios for all formats
			for i, result := range sizeResults {
				sizeRatio := float64(result.SerializedSize) / float64(jsonResult.SerializedSize)
				serRatio := float64(result.SerializeTime.Nanoseconds()) / float64(jsonResult.SerializeTime.Nanoseconds())
				deserRatio := float64(result.DeserializeTime.Nanoseconds()) / float64(jsonResult.DeserializeTime.Nanoseconds())

				fmt.Printf("%-8s | %-12s | %13.2fx | %13.2fx | %13.2fx\n",
					func() string {
						if i == 0 {
							return sizeName
						}
						return ""
					}(),
					result.SerializationType,
					sizeRatio, serRatio, deserRatio)
			}
		}
	}
}
