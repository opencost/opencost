package cloudcost

import (
	"time"

	"github.com/opencost/opencost/core/pkg/kubecost"
)

func DefaultMockCloudCostSet(start, end time.Time, provider, integration string) *kubecost.CloudCostSet {
	ccs := kubecost.NewCloudCostSet(start, end)

	ccs.Integration = integration

	ccs.Insert(&kubecost.CloudCost{
		Window: ccs.Window,
		Properties: &kubecost.CloudCostProperties{
			Provider:        provider,
			AccountID:       "account1",
			InvoiceEntityID: "invoiceEntity1",
			Service:         provider + "-storage",
			Category:        kubecost.StorageCategory,
			Labels: kubecost.CloudCostLabels{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			},
			ProviderID: "id1",
		},
		ListCost: kubecost.CostMetric{
			Cost:              100,
			KubernetesPercent: 0,
		},
		NetCost: kubecost.CostMetric{
			Cost:              100,
			KubernetesPercent: 0,
		},
	})

	ccs.Insert(&kubecost.CloudCost{
		Window: ccs.Window,
		Properties: &kubecost.CloudCostProperties{
			Provider:        provider,
			AccountID:       "account1",
			InvoiceEntityID: "invoiceEntity1",
			Service:         provider + "-compute",
			Category:        kubecost.ComputeCategory,
			Labels: kubecost.CloudCostLabels{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			},
			ProviderID: "id2",
		},
		ListCost: kubecost.CostMetric{
			Cost:              2000,
			KubernetesPercent: 1,
		},
		NetCost: kubecost.CostMetric{
			Cost:              1800,
			KubernetesPercent: 1,
		},
	})

	ccs.Insert(&kubecost.CloudCost{
		Window: ccs.Window,
		Properties: &kubecost.CloudCostProperties{
			Provider:        provider,
			AccountID:       "account2",
			InvoiceEntityID: "invoiceEntity2",
			Service:         provider + "-compute",
			Category:        kubecost.ComputeCategory,
			Labels: kubecost.CloudCostLabels{
				"label1": "value1",
				"label2": "value2",
				"label3": "value3",
			},
			ProviderID: "id3",
		},
		ListCost: kubecost.CostMetric{
			Cost:              8000,
			KubernetesPercent: 1,
		},
		NetCost: kubecost.CostMetric{
			Cost:              8000,
			KubernetesPercent: 1,
		},
	})

	return ccs
}
