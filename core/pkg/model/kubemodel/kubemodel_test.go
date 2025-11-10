package kubemodel

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TODO: what tests, specifically, do we need here? Register funcs? Constructor?

func TestKubeModel(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	kms := NewKubeModelSet(start, end)

	// Test adding a Cluster
	// 1. Invalid Cluster
	kms.RegisterCluster("")
	if kms.Cluster != nil {
		t.Errorf("Cluster should be nil")
	}
	// 2. Valid Cluster
	kms.RegisterCluster(uuid.New().String())
	if kms.Cluster == nil {
		t.Errorf("Cluster should not be nil")
	}
	if kms.Cluster.UID == "" {
		t.Errorf("Cluster should not have empty UID")
	}

	t.Run("RegisterNamespace", func(t *testing.T) {
		// Test registering Namespaces
		// 1. Invalid Namespace
		// 2. Valid Namespace
	})

	t.Run("RegisterResourceQuota", func(t *testing.T) {
		// Test registering ResourceQuotas
		// 1. Invalid ResourceQuota
		// 2. Valid ResourceQuota in a valid Namespace
		// 3. Valid ResourceQuota in an invalid Namespace
	})
}
