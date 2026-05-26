package kubemodel

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKubeModelMarshalBinary(t *testing.T) {
	s := time.Now().UTC().Truncate(time.Hour)
	e := s.Add(time.Hour)

	// Test empty KubeModelSet

	kms := NewKubeModelSet(s, e)

	b, err := kms.MarshalBinary()
	require.NoError(t, err)

	var act = new(KubeModelSet)
	err = act.UnmarshalBinary(b)
	require.NoError(t, err)

	require.Equal(t, kms.Metadata, act.Metadata)
	require.Equal(t, kms.Window, act.Window)
	require.Equal(t, kms.Cluster, act.Cluster)
	require.Equal(t, kms.Namespaces, act.Namespaces)
	require.Equal(t, kms.ResourceQuotas, act.ResourceQuotas)

	// Test non-empty KubeModelSet

	kms = NewKubeModelSet(s, e)

	kms.Metadata.CreatedAt = time.Now().UTC()

	kms.RegisterCluster("cluster")
	kms.Cluster.Start = s
	kms.Cluster.End = e

	kms.RegisterNamespace("ns1", "ns1")
	kms.Namespaces["ns1"].Start = s
	kms.Namespaces["ns1"].End = e
	kms.Namespaces["ns1"].Labels = map[string]string{"label1": "label1"}
	kms.Namespaces["ns1"].Annotations = map[string]string{"anno1": "anno1"}

	kms.RegisterNamespace("ns2", "ns2")
	kms.Namespaces["ns2"].Start = s
	kms.Namespaces["ns2"].End = e
	kms.Namespaces["ns2"].Labels = map[string]string{"label2": "label2"}
	kms.Namespaces["ns2"].Annotations = map[string]string{"anno2": "anno2"}

	kms.RegisterResourceQuota("rq1", "rq1", "ns1")
	kms.ResourceQuotas["rq1"].Start = s
	kms.ResourceQuotas["rq1"].End = e
	kms.ResourceQuotas["rq1"].Spec = &ResourceQuotaSpec{
		Hard: &ResourceQuotaSpecHard{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}
	kms.ResourceQuotas["rq1"].Status = &ResourceQuotaStatus{
		Used: &ResourceQuotaStatusUsed{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}

	kms.RegisterResourceQuota("rq2", "rq2", "ns1")
	kms.ResourceQuotas["rq2"].Start = s
	kms.ResourceQuotas["rq2"].End = e
	kms.ResourceQuotas["rq2"].Spec = &ResourceQuotaSpec{
		Hard: &ResourceQuotaSpecHard{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}
	kms.ResourceQuotas["rq2"].Status = &ResourceQuotaStatus{
		Used: &ResourceQuotaStatusUsed{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}

	kms.RegisterResourceQuota("rq3", "rq3", "ns2")
	kms.ResourceQuotas["rq3"].Start = s
	kms.ResourceQuotas["rq3"].End = e
	kms.ResourceQuotas["rq3"].Spec = &ResourceQuotaSpec{
		Hard: &ResourceQuotaSpecHard{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}
	kms.ResourceQuotas["rq3"].Status = &ResourceQuotaStatus{
		Used: &ResourceQuotaStatusUsed{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}

	kms.RegisterResourceQuota("rq4", "rq4", "ns2")
	kms.ResourceQuotas["rq4"].Start = s
	kms.ResourceQuotas["rq4"].End = e
	kms.ResourceQuotas["rq4"].Spec = &ResourceQuotaSpec{
		Hard: &ResourceQuotaSpecHard{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}
	kms.ResourceQuotas["rq4"].Status = &ResourceQuotaStatus{
		Used: &ResourceQuotaStatusUsed{
			Requests: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
			Limits: ResourceQuantities{
				ResourceCPU: ResourceQuantity{
					Resource: ResourceCPU,
					Unit:     UnitMillicore,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
				ResourceMemory: ResourceQuantity{
					Resource: ResourceMemory,
					Unit:     UnitByte,
					Values: Stats{
						StatAvg: 1,
						StatMax: 1,
						StatP85: 1,
						StatP95: 1,
					},
				},
			},
		},
	}

	kms.Error(errors.New("test error"))
	kms.Warn("test warning")
	kms.Info("test info")
	kms.Debug("test debug")
	kms.Trace("test trace")

	kms.Metadata.CompletedAt = time.Now().UTC()

	b, err = kms.MarshalBinary()
	require.NoError(t, err)

	act = new(KubeModelSet)
	err = act.UnmarshalBinary(b)
	require.NoError(t, err)

	require.Equal(t, kms.Metadata, act.Metadata)
	require.Equal(t, kms.Window, act.Window)
	require.Equal(t, kms.Cluster, act.Cluster)
	require.Equal(t, kms.Namespaces, act.Namespaces)
	require.Equal(t, kms.ResourceQuotas, act.ResourceQuotas)
}
