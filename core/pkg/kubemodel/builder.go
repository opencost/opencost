package kubemodel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
	kubepb "github.com/opencost/opencost/core/pkg/model/pb/kubemodel"
	"github.com/opencost/opencost/core/pkg/source"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// Types and Interfaces
// ============================================================================

// ModelHydrator is a pluggable function that populates specific resources in a model.
//
// Hydrators query the datasource for specific resource types (nodes, pods, etc.)
// and transform the results into protobuf messages stored in the model.
//
// Multiple hydrators can be composed together, each responsible for different
// resource types or data sources. They are executed sequentially in the order
// provided to the Builder.
//
// Example hydrators:
//   - BasicHydrator: populates nodes, namespaces, pods from Prometheus labels
//   - ResourceHydrator: populates CPU/RAM/GPU usage metrics
//   - StorageHydrator: populates volumes and PVCs
type ModelHydrator func(ctx context.Context, model *Model, ds source.OpenCostDataSource, start, end time.Time) error

// ============================================================================
// Errors
// ============================================================================

var (
	// ErrNoDataSource indicates that a Builder was created without a datasource
	ErrNoDataSource = errors.New("kubemodel: no datasource configured")

	// ErrNoHydrators indicates that a Builder was created without any hydrators
	ErrNoHydrators = errors.New("kubemodel: no hydrators configured")
)

// ============================================================================
// Builder
// ============================================================================

// Builder orchestrates the construction of Kubernetes cluster models using a
// pluggable hydrator architecture.
//
// The Builder pattern separates the concerns of:
//   - Data source configuration (where to get data)
//   - Data transformation (how to convert raw data to protobuf)
//   - Cluster metadata (cluster identity and provider information)
//
// Usage:
//
//	builder, err := NewBuilder(Config{
//	    DataSource: prometheusDataSource,
//	    Hydrators: []ModelHydrator{
//	        prometheus.NewBasicHydrator(clusterID),
//	        prometheus.NewResourceHydrator(),
//	    },
//	    ClusterName: "production",
//	    Provider: kubepb.Provider_PROVIDER_AWS,
//	})
//
//	model, err := builder.ComputeModel(ctx, start, end)
type Builder struct {
	// Data source for querying metrics (e.g., Prometheus)
	datasource source.OpenCostDataSource

	// Hydrators to populate different resource types
	// Executed sequentially in the order provided
	hydrators []ModelHydrator

	// Cluster metadata applied to all resources
	clusterID   string           // Unique cluster identifier (defaults to kube-system UID)
	clusterName string           // Human-readable cluster name
	account     string           // Cloud account ID (optional)
	provider    kubepb.Provider  // Cloud provider type
}

// Config contains the configuration for creating a Builder.
//
// Required fields:
//   - DataSource: Source for querying cluster metrics
//   - Hydrators: At least one hydrator to populate resources
//   - ClusterName: Human-readable name for the cluster
//
// Optional fields:
//   - ClusterID: Unique identifier (auto-detected from kube-system UID if not provided)
//   - Account: Cloud account identifier
//   - Provider: Cloud provider type (defaults to PROVIDER_UNSPECIFIED)
type Config struct {
	// DataSource provides access to cluster metrics
	DataSource source.OpenCostDataSource

	// Hydrators define what resources to populate and from which sources
	// Example: []ModelHydrator{prometheus.NewBasicHydrator(clusterID)}
	Hydrators []ModelHydrator

	// ClusterID is a unique identifier for the cluster
	// If empty, will be auto-detected using kube-system namespace UID
	ClusterID string

	// ClusterName is a human-readable name (required)
	ClusterName string

	// Account is the cloud provider account ID (optional)
	Account string

	// Provider identifies the cloud provider or infrastructure type
	Provider kubepb.Provider
}

// ============================================================================
// Builder Construction
// ============================================================================

// NewBuilder creates a new Builder with the given configuration.
//
// The function performs the following validations and setup:
//  1. Validates required fields (DataSource, Hydrators, ClusterName)
//  2. Auto-detects ClusterID from kube-system namespace UID if not provided
//  3. Initializes the Builder with all configuration
//
// ClusterID Auto-detection:
// If ClusterID is not provided, the function queries the kube-system namespace
// from the datasource and uses its UID. The kube-system namespace exists in all
// Kubernetes clusters and has a UID that is:
//   - Unique across clusters
//   - Stable for the lifetime of the cluster
//   - Automatically available without configuration
//
// Returns an error if:
//   - DataSource is nil
//   - No hydrators are provided
//   - ClusterName is empty
//   - ClusterID auto-detection fails
func NewBuilder(cfg Config) (*Builder, error) {
	// Validate required configuration
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	// Resolve cluster ID (auto-detect if not provided)
	clusterID, err := resolveClusterID(cfg.ClusterID, cfg.DataSource)
	if err != nil {
		return nil, err
	}

	return &Builder{
		datasource:  cfg.DataSource,
		hydrators:   cfg.Hydrators,
		clusterID:   clusterID,
		clusterName: cfg.ClusterName,
		account:     cfg.Account,
		provider:    cfg.Provider,
	}, nil
}

// validateConfig checks that all required fields are present.
func validateConfig(cfg Config) error {
	if cfg.DataSource == nil {
		return ErrNoDataSource
	}
	if len(cfg.Hydrators) == 0 {
		return ErrNoHydrators
	}
	if cfg.ClusterName == "" {
		return fmt.Errorf("kubemodel: cluster name must be provided")
	}
	return nil
}

// resolveClusterID returns the provided clusterID or auto-detects it from kube-system.
func resolveClusterID(clusterID string, ds source.OpenCostDataSource) (string, error) {
	if clusterID != "" {
		return clusterID, nil
	}

	// Auto-detect from kube-system namespace UID
	uid, err := getKubeSystemNamespaceUID(ds)
	if err != nil {
		return "", fmt.Errorf("kubemodel: cluster ID not provided and failed to get kube-system namespace UID: %w", err)
	}

	return uid, nil
}

// getKubeSystemNamespaceUID fetches the UID of the kube-system namespace from the datasource.
// This UID is unique per cluster and stable across its lifetime, making it a good default cluster ID.
func getKubeSystemNamespaceUID(ds source.OpenCostDataSource) (string, error) {
	metrics := ds.Metrics()

	// Query namespace labels from the last hour to find kube-system
	end := time.Now()
	start := end.Add(-1 * time.Hour)

	future := metrics.QueryNamespaceLabels(start, end)
	results, err := future.Await()
	if err != nil {
		return "", fmt.Errorf("failed to query namespace labels: %w", err)
	}

	// Find kube-system namespace and return its UID
	for _, ns := range results {
		if ns.Namespace == "kube-system" && ns.UID != "" {
			return ns.UID, nil
		}
	}

	return "", fmt.Errorf("kube-system namespace UID not found in metrics")
}

// ============================================================================
// Model Building
// ============================================================================

// ComputeModel builds a complete Kubernetes cluster model for the specified time window.
//
// The process consists of three main steps:
//  1. Validate inputs and calculate window resolution
//  2. Initialize model with cluster metadata and window information
//  3. Execute hydrators sequentially to populate resources
//
// Time Window:
// The time window determines what data is queried and aggregated. The duration
// between start and end must match one of the supported resolutions:
//   - 10 minutes (10m resolution)
//   - 1 hour (1h resolution)
//   - 24 hours (1d resolution)
//
// Model Structure:
// The returned Model contains:
//   - Window: timeframe and resolution
//   - Cluster: metadata (ID, name, provider, account)
//   - Resources: nodes, pods, namespaces, etc. (populated by hydrators)
//
// Context Cancellation:
// The context is passed to all hydrators, allowing for graceful cancellation
// of long-running queries.
//
// Returns an error if:
//   - Inputs are invalid (nil builder, zero times, end before start)
//   - Window duration doesn't match a supported resolution
//   - Any hydrator fails during execution
func (b *Builder) ComputeModel(ctx context.Context, start, end time.Time) (*Model, error) {
	// Step 1: Validate inputs
	if err := b.validateComputeInputs(start, end); err != nil {
		return nil, err
	}

	// Normalize times to UTC for consistency
	start = start.UTC()
	end = end.UTC()

	// Step 2: Calculate window resolution and create window metadata
	window, err := b.createWindow(start, end)
	if err != nil {
		return nil, err
	}

	// Step 3: Initialize model with cluster metadata
	model := b.initializeModel(window)

	// Step 4: Run hydrators to populate resources
	if err := b.runHydrators(ctx, model, start, end); err != nil {
		return nil, err
	}

	return model, nil
}

// validateComputeInputs checks that the Builder and time parameters are valid.
func (b *Builder) validateComputeInputs(start, end time.Time) error {
	if b == nil {
		return fmt.Errorf("kubemodel: builder is nil")
	}
	if start.IsZero() {
		return fmt.Errorf("kubemodel: start time must be set")
	}
	if end.IsZero() {
		return fmt.Errorf("kubemodel: end time must be set")
	}
	if end.Before(start) {
		return fmt.Errorf("kubemodel: end %s is before start %s", end, start)
	}
	return nil
}

// createWindow calculates the resolution from the time window and creates a Window protobuf.
func (b *Builder) createWindow(start, end time.Time) (*pb.Window, error) {
	duration := end.Sub(start)
	resolution, err := DurationToResolution(duration)
	if err != nil {
		return nil, err
	}

	return &pb.Window{
		Resolution: resolution,
		Start:      timestamppb.New(start),
	}, nil
}

// initializeModel creates a new Model and populates it with cluster metadata and window info.
func (b *Builder) initializeModel(window *pb.Window) *Model {
	model := NewModel()

	// Set window (deep copy to prevent mutations)
	model.Window = proto.Clone(window).(*pb.Window)

	// Set cluster metadata
	model.Cluster = &kubepb.Cluster{
		ID:       b.clusterID,
		Provider: b.provider,
		Account:  b.account,
		Name:     b.clusterName,
		Window:   proto.Clone(window).(*pb.Window),
	}

	return model
}

// runHydrators executes all configured hydrators sequentially to populate the model.
// Returns the first error encountered, or nil if all succeed.
func (b *Builder) runHydrators(ctx context.Context, model *Model, start, end time.Time) error {
	for i, hydrator := range b.hydrators {
		if err := hydrator(ctx, model, b.datasource, start, end); err != nil {
			return fmt.Errorf("kubemodel: hydrator %d failed: %w", i, err)
		}
	}
	return nil
}
