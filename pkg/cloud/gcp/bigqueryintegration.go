package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/timeutil"
	"google.golang.org/api/iterator"
)

type BigQueryIntegration struct {
	BigQueryQuerier
}

const (
	UsageDateColumnName          = "usage_date"
	BillingAccountIDColumnName   = "billing_id"
	ProjectIDColumnName          = "project_id"
	ServiceDescriptionColumnName = "service"
	SKUDescriptionColumnName     = "description"
	LabelsColumnName             = "labels"
	ResourceNameColumnName       = "resource"
	CostColumnName               = "cost"
	CreditsColumnName            = "credits"
)

const BiqQueryWherePartitionFmt = `DATE(_PARTITIONTIME) >= "%s" AND DATE(_PARTITIONTIME) < "%s"`
const BiqQueryWhereDateFmt = `usage_start_time >= "%s" AND usage_start_time < "%s"`

func (bqi *BigQueryIntegration) GetCloudCost(start time.Time, end time.Time) (*kubecost.CloudCostSetRange, error) {
	// Build Query

	selectColumns := []string{
		fmt.Sprintf("TIMESTAMP_TRUNC(usage_start_time, day) as %s", UsageDateColumnName),
		fmt.Sprintf("billing_account_id as %s", BillingAccountIDColumnName),
		fmt.Sprintf("project.id as %s", ProjectIDColumnName),
		fmt.Sprintf("service.description as %s", ServiceDescriptionColumnName),
		fmt.Sprintf("sku.description as %s", SKUDescriptionColumnName),
		fmt.Sprintf("resource.name as %s", ResourceNameColumnName),
		fmt.Sprintf("TO_JSON_STRING(labels) as %s", LabelsColumnName),
		fmt.Sprintf("SUM(cost) as %s", CostColumnName),
		fmt.Sprintf("IFNULL(SUM((Select SUM(amount) FROM bd.credits)),0) as %s", CreditsColumnName),
	}

	groupByColumns := []string{
		UsageDateColumnName,
		BillingAccountIDColumnName,
		ProjectIDColumnName,
		ServiceDescriptionColumnName,
		SKUDescriptionColumnName,
		LabelsColumnName,
		ResourceNameColumnName,
	}

	partitionStart := start
	partitionEnd := end.AddDate(0, 0, 2)
	wherePartition := fmt.Sprintf(BiqQueryWherePartitionFmt, partitionStart.Format("2006-01-02"), partitionEnd.Format("2006-01-02"))
	whereDate := fmt.Sprintf(BiqQueryWhereDateFmt, start.Format("2006-01-02"), end.Format("2006-01-02"))

	whereConjuncts := []string{
		wherePartition,
		whereDate,
	}

	columnStr := strings.Join(selectColumns, ", ")
	table := fmt.Sprintf(" `%s` bd ", bqi.GetBillingDataDataset())
	whereClause := strings.Join(whereConjuncts, " AND ")
	groupByStr := strings.Join(groupByColumns, ", ")
	queryStr := `
		SELECT %s
		FROM %s
		WHERE %s
		GROUP BY %s
	`

	querystr := fmt.Sprintf(queryStr, columnStr, table, whereClause, groupByStr)

	// Perform Query and parse values

	ccsr, err := kubecost.NewCloudCostSetRange(start, end, timeutil.Day, bqi.Key())
	if err != nil {
		return ccsr, fmt.Errorf("error creating new CloudCostSetRange: %s", err)
	}

	iter, err := bqi.Query(context.Background(), querystr)
	if err != nil {
		return ccsr, fmt.Errorf("error querying: %s", err)
	}

	// Parse query into CloudCostSetRange
	for {
		var ccl CloudCostLoader
		err = iter.Next(&ccl)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return ccsr, err
		}
		if ccl.CloudCost == nil {
			continue
		}
		ccsr.LoadCloudCost(ccl.CloudCost)

	}
	return ccsr, nil

}

type CloudCostLoader struct {
	CloudCost *kubecost.CloudCost
}

// Load populates the fields of a CloudCostValues with bigquery.Value from provided slice
func (ccl *CloudCostLoader) Load(values []bigquery.Value, schema bigquery.Schema) error {

	// Create Cloud Cost Properties
	properties := kubecost.CloudCostProperties{
		Provider: kubecost.GCPProvider,
	}
	var window kubecost.Window
	var description string
	var listCost float64
	var credits float64

	for i, field := range schema {
		if field == nil {
			log.DedupedErrorf(5, "GCP: BigQuery: found nil field in schema")
			continue
		}

		switch field.Name {
		case UsageDateColumnName:
			usageDate, ok := values[i].(time.Time)
			if !ok {
				// It would be very surprising if an unparsable time came back from the API, so it should be ok to return here.
				return fmt.Errorf("error parsing usage date: %v", values[0])
			}
			// start and end will be the day that the usage occurred on
			s := usageDate
			e := s.Add(timeutil.Day)
			window = kubecost.NewWindow(&s, &e)
		case BillingAccountIDColumnName:
			invoiceEntityID, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", BillingAccountIDColumnName, values[i])
				invoiceEntityID = ""
			}
			properties.InvoiceEntityID = invoiceEntityID
		case ProjectIDColumnName:
			accountID, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ProjectIDColumnName, values[i])
				accountID = ""
			}
			properties.AccountID = accountID
		case ServiceDescriptionColumnName:
			service, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ServiceDescriptionColumnName, values[i])
				service = ""
			}
			properties.Service = service
		case SKUDescriptionColumnName:
			d, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", SKUDescriptionColumnName, values[i])
				d = ""
			}
			description = d
		case LabelsColumnName:
			labelJSON, ok := values[i].(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", LabelsColumnName, values[i])
			}
			labelList := []map[string]string{}
			err := json.Unmarshal([]byte(labelJSON), &labelList)
			if err != nil {
				log.Warnf("GCP Cloud Assets: error unmarshaling GCP CloudCost labels: %s", err)
			}
			labels := map[string]string{}
			for _, pair := range labelList {
				key := pair["key"]
				value := pair["value"]
				labels[key] = value
			}
			properties.Labels = labels
		case ResourceNameColumnName:
			resouceNameValue := values[i]
			if resouceNameValue == nil {
				properties.ProviderID = ""
				continue
			}
			resource, ok := resouceNameValue.(string)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", ResourceNameColumnName, values[i])
				properties.ProviderID = ""
				continue
			}

			properties.ProviderID = ParseProviderID(resource)
		case CostColumnName:
			cost, ok := values[i].(float64)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", CostColumnName, values[i])
				cost = 0.0
			}
			listCost = cost
		case CreditsColumnName:
			creditSum, ok := values[i].(float64)
			if !ok {
				log.DedupedErrorf(5, "error parsing GCP CloudCost %s: %v", CreditsColumnName, values[i])
				creditSum = 0.0
			}
			credits = creditSum
		default:
			log.DedupedErrorf(5, "GCP: BigQuery: found unrecognized column name %s", field.Name)
		}
	}

	// Check required Fields
	if window.IsOpen() {
		return fmt.Errorf("GCP: BigQuery: error parsing, item had invalid window")
	}

	// Determine Category
	properties.Category = SelectCategory(properties.Service, description)

	// sum credit and cost for NetCost
	netCost := listCost + credits

	// Using the NetCost as a 'placeholder' for these costs now, until we can revisit and spend the time to do
	// the calculations correctly
	amortizedCost := netCost
	amortizedNetCost := netCost
	invoicedCost := netCost

	// percent k8s is determined by the presence of labels
	k8sPercent := 0.0
	if IsK8s(properties.Labels) {
		k8sPercent = 1.0
	}

	ccl.CloudCost = &kubecost.CloudCost{
		Properties: &properties,
		Window:     window,
		ListCost: kubecost.CostMetric{
			Cost:              listCost,
			KubernetesPercent: k8sPercent,
		},
		AmortizedCost: kubecost.CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: k8sPercent,
		},
		AmortizedNetCost: kubecost.CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: k8sPercent,
		},
		InvoicedCost: kubecost.CostMetric{
			Cost:              invoicedCost,
			KubernetesPercent: k8sPercent,
		},
		NetCost: kubecost.CostMetric{
			Cost:              netCost,
			KubernetesPercent: k8sPercent,
		},
	}

	return nil
}

func IsK8s(labels map[string]string) bool {
	if _, ok := labels["goog-gke-volume"]; ok {
		return true
	}

	if _, ok := labels["goog-gke-node"]; ok {
		return true
	}

	if _, ok := labels["goog-k8s-cluster-name"]; ok {
		return true
	}

	return false
}

var parseProviderIDRx = regexp.MustCompile("^.+\\/(.+)?") // Capture "gke-cluster-3-default-pool-xxxx-yy" from "projects/###/instances/gke-cluster-3-default-pool-xxxx-yy"

func ParseProviderID(id string) string {
	match := parseProviderIDRx.FindStringSubmatch(id)
	if len(match) == 0 {
		return id
	}
	return match[len(match)-1]
}

func SelectCategory(service, description string) string {
	s := strings.ToLower(service)
	d := strings.ToLower(description)

	// Network descriptions
	if strings.Contains(d, "download") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "network") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "ingress") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "egress") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "static ip") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "external ip") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "load balanced") {
		return kubecost.NetworkCategory
	}
	if strings.Contains(d, "licensing fee") {
		return kubecost.OtherCategory
	}

	// Storage Descriptions
	if strings.Contains(d, "storage") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd capacity") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd iops") {
		return kubecost.StorageCategory
	}
	if strings.Contains(d, "pd snapshot") {
		return kubecost.StorageCategory
	}

	// Service Defaults
	if strings.Contains(s, "storage") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "compute") {
		return kubecost.ComputeCategory
	}
	if strings.Contains(s, "sql") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "bigquery") {
		return kubecost.StorageCategory
	}
	if strings.Contains(s, "kubernetes") {
		return kubecost.ManagementCategory
	} else if strings.Contains(s, "pub/sub") {
		return kubecost.NetworkCategory
	}

	return kubecost.OtherCategory
}
