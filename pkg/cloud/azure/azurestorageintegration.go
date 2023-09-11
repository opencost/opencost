package azure

import (
	"strings"
	"time"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/util/timeutil"
)

type AzureStorageIntegration struct {
	AzureStorageBillingParser
	ConnectionStatus cloud.ConnectionStatus
}

func (asi *AzureStorageIntegration) GetCloudCost(start, end time.Time) (*kubecost.CloudCostSetRange, error) {
	ccsr, err := kubecost.NewCloudCostSetRange(start, end, timeutil.Day, asi.Key())
	if err != nil {
		return nil, err
	}

	status, err := asi.ParseBillingData(start, end, func(abv *BillingRowValues) error {
		s := abv.Date
		e := abv.Date.Add(timeutil.Day)
		window := kubecost.NewWindow(&s, &e)

		k8sPtc := 0.0
		if AzureIsK8s(abv.Tags) {
			k8sPtc = 1.0
		}

		// Create CloudCost
		// Using the NetCost as a 'placeholder' for Invoiced and Amortized Net costs now,
		// until we can revisit and spend the time to do the calculations correctly
		cc := &kubecost.CloudCost{
			Properties: &kubecost.CloudCostProperties{
				ProviderID:      AzureSetProviderID(abv),
				Provider:        kubecost.AzureProvider,
				AccountID:       abv.SubscriptionID,
				InvoiceEntityID: abv.InvoiceEntityID,
				Service:         abv.Service,
				Category:        SelectAzureCategory(abv.MeterCategory),
				Labels:          abv.Tags,
			},
			Window: window,
			AmortizedNetCost: kubecost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
			InvoicedCost: kubecost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
			ListCost: kubecost.CostMetric{
				Cost:              abv.Cost,
				KubernetesPercent: k8sPtc,
			},
			NetCost: kubecost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
			// NOTE: on Azure, there is no "AmortizedCost" per se, so we use
			// AmortizedNetCost, or NetCost, instead.
			AmortizedCost: kubecost.CostMetric{
				Cost:              abv.NetCost,
				KubernetesPercent: k8sPtc,
			},
		}

		// Check if Item
		if abv.IsCompute(cc.Properties.Category) {
			// TODO: Will need to split VMSS for other features
			ccsr.LoadCloudCost(cc)
		}
		return nil
	})
	if err != nil {
		asi.ConnectionStatus = status
		return nil, err
	}
	return ccsr, nil
}

// Check for the presence of k8s labels
func AzureIsK8s(labels map[string]string) bool {
	for key := range labels {
		if strings.HasPrefix(key, "aks-managed-") {
			return true
		}
		if strings.HasPrefix(key, "kubernetes.io-created-") {
			return true
		}
		if strings.HasPrefix(key, "k8s-azure-created-") {
			return true
		}
	}
	return false
}
