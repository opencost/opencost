package aws

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/timeutil"
)

const LabelColumnPrefix = "resource_tags_user_"

// athenaDateLayout is the default AWS date format
const AthenaDateLayout = "2006-01-02 15:04:05.000"

// Cost Columns
const AthenaPricingColumn = "line_item_unblended_cost"

// Amortized Cost Columns
const AthenaRIPricingColumn = "reservation_effective_cost"
const AthenaSPPricingColumn = "savings_plan_savings_plan_effective_cost"

// Net Cost Columns
const AthenaNetPricingColumn = "line_item_net_unblended_cost"

// Amortized Net Cost Columns
const AthenaNetRIPricingColumn = "reservation_net_effective_cost"
const AthenaNetSPPricingColumn = "savings_plan_net_savings_plan_effective_cost"

// Category Columns
const AthenaIsNode = "SUBSTRING(line_item_resource_id,1,2) = 'i-'"
const AthenaIsVol = "SUBSTRING(line_item_resource_id, 1, 4) = 'vol-'"
const AthenaIsNetwork = "line_item_usage_type LIKE '%Bytes'"

// athenaDateTruncColumn Aggregates line items from the hourly level to daily. "line_item_usage_start_date" is used because at
// all time values 00:00-23:00 it will truncate to the correct date.
const AthenaDateColumn = "line_item_usage_start_date"
const AthenaDateTruncColumn = "DATE_TRUNC('day'," + AthenaDateColumn + ") as usage_date"

const AthenaWhereDateFmt = `line_item_usage_start_date >= date '%s' AND line_item_usage_start_date < date '%s'`
const AthenaWhereUsage = "(line_item_line_item_type = 'Usage' OR line_item_line_item_type = 'DiscountedUsage' OR line_item_line_item_type = 'SavingsPlanCoveredUsage' OR line_item_line_item_type = 'EdpDiscount' OR line_item_line_item_type = 'PrivateRateDiscount')"

// AthenaQueryIndexes is a struct for holding the context of a query
type AthenaQueryIndexes struct {
	Query                     string
	ColumnIndexes             map[string]int
	TagColumns                []string
	ListCostColumn            string
	ListK8sCostColumn         string
	NetCostColumn             string
	NetK8sCostColumn          string
	AmortizedNetCostColumn    string
	AmortizedNetK8sCostColumn string
	AmortizedCostColumn       string
	AmortizedK8sCostColumn    string
	InvoicedCostColumn        string
	InvoicedK8sCostColumn     string
}

type AthenaIntegration struct {
	AthenaQuerier
}

// Query Athena for CUR data and build a new CloudCostSetRange containing the info
func (ai *AthenaIntegration) GetCloudCost(start, end time.Time) (*kubecost.CloudCostSetRange, error) {
	log.Infof("AthenaIntegration[%s]: StoreCloudCost: %s", ai.Key(), kubecost.NewWindow(&start, &end).String())
	// Query for all column names
	allColumns, err := ai.GetColumns()
	if err != nil {
		return nil, fmt.Errorf("GetCloudCost: error getting Athena columns: %w", err)
	}

	// List known, hard-coded columns to query
	groupByColumns := []string{
		AthenaDateTruncColumn,
		"line_item_resource_id",
		"bill_payer_account_id",
		"line_item_usage_account_id",
		"line_item_product_code",
		"line_item_usage_type",
		AthenaIsNode,
		AthenaIsVol,
		AthenaIsNetwork,
	}

	// Create query indices
	aqi := AthenaQueryIndexes{}

	// Determine which columns are user-defined tags and add those to the list
	// of columns to query.
	for column := range allColumns {
		if strings.HasPrefix(column, LabelColumnPrefix) {
			groupByColumns = append(groupByColumns, column)
			aqi.TagColumns = append(aqi.TagColumns, column)
		}
	}
	var selectColumns []string

	// Duplicate GroupBy Columns into select columns
	selectColumns = append(selectColumns, groupByColumns...)

	// Clean Up group by columns
	ai.RemoveColumnAliases(groupByColumns)

	// Build list cost column and add it to the select columns
	listCostColumn := fmt.Sprintf("SUM(%s) as list_cost", ai.GetListCostColumn())
	selectColumns = append(selectColumns, listCostColumn)
	aqi.ListCostColumn = listCostColumn
	listK8sCostColumn := fmt.Sprintf(
		"SUM(%s) as list_kubernetes_cost",
		ai.GetKubernetesCostColumn(allColumns, ai.GetListCostColumn()),
	)
	selectColumns = append(selectColumns, listK8sCostColumn)
	aqi.ListK8sCostColumn = listK8sCostColumn

	// Build net cost column and add it to select columns
	netCostColumn := fmt.Sprintf("SUM(%s) as net_cost", ai.GetNetCostColumn(allColumns))
	selectColumns = append(selectColumns, netCostColumn)
	aqi.NetCostColumn = netCostColumn
	netK8sCostColumn := fmt.Sprintf(
		"SUM(%s) as net_kubernetes_cost",
		ai.GetKubernetesCostColumn(allColumns, ai.GetNetCostColumn(allColumns)),
	)
	selectColumns = append(selectColumns, netK8sCostColumn)
	aqi.NetK8sCostColumn = netK8sCostColumn

	// Build amortized net cost column and add it to select columns
	amortizedNetCostColumn := fmt.Sprintf("SUM(%s) as amortized_net_cost", ai.GetAmortizedNetCostColumn(allColumns))
	selectColumns = append(selectColumns, amortizedNetCostColumn)
	aqi.AmortizedNetCostColumn = amortizedNetCostColumn
	amortizedNetK8sCostColumn := fmt.Sprintf(
		"SUM(%s) as amortized_net_kubernetes_cost",
		ai.GetKubernetesCostColumn(allColumns, ai.GetNetCostColumn(allColumns)),
	)
	selectColumns = append(selectColumns, amortizedNetK8sCostColumn)
	aqi.AmortizedNetK8sCostColumn = amortizedNetK8sCostColumn

	// Build Amortized cost column and add it to select columns
	amortizedCostColumn := fmt.Sprintf("SUM(%s) as amortized_cost", ai.GetAmortizedCostCase(allColumns))
	selectColumns = append(selectColumns, amortizedCostColumn)
	aqi.AmortizedCostColumn = amortizedCostColumn
	amortizedK8sCostColumn := fmt.Sprintf(
		"SUM(%s) as amortized_kubernetes_cost",
		ai.GetKubernetesCostColumn(allColumns, ai.GetAmortizedCostCase(allColumns)),
	)
	selectColumns = append(selectColumns, amortizedK8sCostColumn)
	aqi.AmortizedK8sCostColumn = amortizedK8sCostColumn

	// We are using Net Cost for Invoiced Cost for now as it is the closest approximation
	invoicedCostColumn := netCostColumn
	selectColumns = append(selectColumns, invoicedCostColumn)
	aqi.InvoicedCostColumn = invoicedCostColumn
	invoicedK8sCostColumn := netK8sCostColumn
	selectColumns = append(selectColumns, invoicedK8sCostColumn)
	aqi.InvoicedK8sCostColumn = invoicedK8sCostColumn

	// Build map of query columns to use for parsing query
	aqi.ColumnIndexes = map[string]int{}
	for i, column := range selectColumns {
		aqi.ColumnIndexes[column] = i
	}
	athenaWhereDate := fmt.Sprintf(AthenaWhereDateFmt, start.Format("2006-01-02"), end.Format("2006-01-02"))

	// Query for all line items with a resource_id or from AWS Marketplace, which did not end before
	// the range or start after it. This captures all costs with any amount of
	// overlap with the range, for which we will only extract the relevant costs
	whereConjuncts := []string{
		athenaWhereDate,
		AthenaWhereUsage,
	}
	columnStr := strings.Join(selectColumns, ", ")
	whereClause := strings.Join(whereConjuncts, " AND ")
	groupByStr := strings.Join(groupByColumns, ", ")
	queryStr := `
		SELECT %s
		FROM %s
		WHERE %s
		GROUP BY %s
	`
	aqi.Query = fmt.Sprintf(queryStr, columnStr, ai.Table, whereClause, groupByStr)

	CCSR, err := kubecost.NewCloudCostSetRange(start, end, timeutil.Day, ai.Key())
	if err != nil {
		return nil, err
	}

	// Generate row handling function.
	rowHandler := func(row types.Row) {
		err2 := ai.RowToCloudCost(row, aqi, CCSR)
		if err2 != nil {
			log.Errorf("AthenaIntegration: queryCloudCostCompute: error while parsing row: %s", err2.Error())
		}
	}
	log.Debugf("AthenaIntegration[%s]: queryCloudCostCompute: querying: %s", ai.Key(), aqi.Query)
	// Query CUR data and fill out CCSR
	err = ai.Query(context.TODO(), aqi.Query, GetAthenaQueryFunc(rowHandler))
	if err != nil {
		return nil, err
	}

	// TODO: May not be needed anymore?
	for _, ccs := range CCSR.CloudCostSets {
		log.Debugf("AthenaIntegration[%s]: queryCloudCostCompute: writing compute items for window %s: %d", ai.Key(), ccs.Window, len(ccs.CloudCosts))
		ai.ConnectionStatus = ai.GetConnectionStatusFromResult(ccs, ai.ConnectionStatus)
	}
	return CCSR, nil

}

func (ai *AthenaIntegration) GetListCostColumn() string {
	var listCostBuilder strings.Builder
	listCostBuilder.WriteString("CASE line_item_line_item_type")
	listCostBuilder.WriteString(" WHEN 'EdpDiscount' THEN 0")
	listCostBuilder.WriteString(" WHEN 'PrivateRateDiscount' THEN 0")
	listCostBuilder.WriteString(" ELSE ")
	listCostBuilder.WriteString(AthenaPricingColumn)
	listCostBuilder.WriteString(" END")
	return listCostBuilder.String()
}

func (ai *AthenaIntegration) GetNetCostColumn(allColumns map[string]bool) string {
	netCostColumn := ""
	if allColumns[AthenaNetPricingColumn] { // if Net pricing exists
		netCostColumn = AthenaNetPricingColumn
	} else { // Non-net for if there's no net pricing.
		netCostColumn = AthenaPricingColumn
	}
	return netCostColumn
}

func (ai *AthenaIntegration) GetAmortizedNetCostColumn(allColumns map[string]bool) string {
	amortizedNetCostCase := ""
	if allColumns[AthenaNetPricingColumn] { // if Net pricing exists
		amortizedNetCostCase = ai.GetAmortizedNetCostCase(allColumns)
	} else { // Non-net for if there's no net pricing.
		amortizedNetCostCase = ai.GetAmortizedCostCase(allColumns)
	}
	return amortizedNetCostCase
}

// getIsKubernetesColumn generates a boolean column which determines whether a line item is from kubernetes
func (ai *AthenaIntegration) GetIsKubernetesColumn(allColumns map[string]bool) string {
	return ai.GetIsKubernetesCase(allColumns)
}

// getKubernetesCostColumn generates a double column which determines the cost of k8s items in an aggregate
func (ai *AthenaIntegration) GetKubernetesCostColumn(allColumns map[string]bool, pricingCase string) string {
	k8sCase := ai.GetIsKubernetesCase(allColumns)
	return fmt.Sprintf("CAST((%s) as double) * (%s)", k8sCase, pricingCase)

}

func (ai *AthenaIntegration) RemoveColumnAliases(columns []string) {
	for i, column := range columns {
		if strings.Contains(column, " as ") {
			columnValues := strings.Split(column, " as ")
			columns[i] = columnValues[0]
		}
	}
}

func (ai *AthenaIntegration) ConvertLabelToAWSTag(label string) string {
	// if the label already has the column prefix assume that it is in the correct format
	if strings.HasPrefix(label, LabelColumnPrefix) {
		return label
	}
	// replace characters with underscore
	tag := label
	tag = strings.ReplaceAll(tag, ".", "_")
	tag = strings.ReplaceAll(tag, "/", "_")
	tag = strings.ReplaceAll(tag, ":", "_")
	tag = strings.ReplaceAll(tag, "-", "_")
	// add prefix and return
	return LabelColumnPrefix + tag
}

func (ai *AthenaIntegration) GetAmortizedCostCase(allColumns map[string]bool) string {
	// Use unblended costs if Reserved Instances/Savings Plans aren't in use
	if !allColumns[AthenaRIPricingColumn] && !allColumns[AthenaSPPricingColumn] {
		return AthenaPricingColumn
	}

	var costBuilder strings.Builder
	costBuilder.WriteString("CASE line_item_line_item_type")
	if allColumns[AthenaRIPricingColumn] {
		costBuilder.WriteString(" WHEN 'DiscountedUsage' THEN ")
		costBuilder.WriteString(AthenaRIPricingColumn)
	}

	if allColumns[AthenaSPPricingColumn] {
		costBuilder.WriteString(" WHEN 'SavingsPlanCoveredUsage' THEN ")
		costBuilder.WriteString(AthenaSPPricingColumn)
	}

	costBuilder.WriteString(" ELSE ")
	costBuilder.WriteString(AthenaPricingColumn)
	costBuilder.WriteString(" END")
	return costBuilder.String()
}

func (ai *AthenaIntegration) GetAmortizedNetCostCase(allColumns map[string]bool) string {
	// Use net unblended costs if Reserved Instances/Savings Plans aren't in use
	if !allColumns[AthenaNetRIPricingColumn] && !allColumns[AthenaNetSPPricingColumn] {
		return AthenaNetPricingColumn
	}

	var costBuilder strings.Builder
	costBuilder.WriteString("CASE line_item_line_item_type")
	if allColumns[AthenaNetRIPricingColumn] {
		costBuilder.WriteString(" WHEN 'DiscountedUsage' THEN ")
		costBuilder.WriteString(AthenaNetRIPricingColumn)
	}

	if allColumns[AthenaNetSPPricingColumn] {
		costBuilder.WriteString(" WHEN 'SavingsPlanCoveredUsage' THEN ")
		costBuilder.WriteString(AthenaNetSPPricingColumn)
	}

	costBuilder.WriteString(" ELSE ")
	costBuilder.WriteString(AthenaNetPricingColumn)
	costBuilder.WriteString(" END")
	return costBuilder.String()
}

// GetIsKubernetesCase builds a "CASE" clause which attempts to determine if a line item is kubernetes based on labels
// that may be available in the CUR
func (ai *AthenaIntegration) GetIsKubernetesCase(allColumns map[string]bool) string {
	// k8sColumns is a list of columns where the presence of a value indicates that a resource is part of a kubernetes cluster
	k8sColumns := []string{
		"resource_tags_aws_eks_cluster_name",
		"resource_tags_user_eks_cluster_name",
		"resource_tags_user_alpha_eksctl_io_cluster_name",
		"resource_tags_user_kubernetes_io_service_name",
		"resource_tags_user_kubernetes_io_created_for_pvc_name",
		"resource_tags_user_kubernetes_io_created_for_pv_name",
	}
	var k8sBuilder strings.Builder

	k8sBuilder.WriteString("CASE ")
	// EKS is always kubernetes
	k8sBuilder.WriteString("WHEN line_item_product_code = 'AmazonEKS' THEN TRUE ")
	for _, k8sColumn := range k8sColumns {
		if _, ok := allColumns[k8sColumn]; ok {
			k8sBuilder.WriteString("WHEN ")
			k8sBuilder.WriteString(k8sColumn)
			k8sBuilder.WriteString(" <> '' THEN TRUE ")
		}
	}

	k8sBuilder.WriteString("ELSE FALSE END")
	return k8sBuilder.String()
}

func (ai *AthenaIntegration) RowToCloudCost(row types.Row, aqi AthenaQueryIndexes, ccsr *kubecost.CloudCostSetRange) error {
	if len(row.Data) < len(aqi.ColumnIndexes) {
		return fmt.Errorf("rowToCloudCost: row with fewer than %d columns (has only %d)", len(aqi.ColumnIndexes), len(row.Data))
	}

	// Iterate through the slice of tag columns, assigning
	// values to the column names, minus the tag prefix.
	labels := kubecost.CloudCostLabels{}
	labelValues := []string{}
	for _, tagColumnName := range aqi.TagColumns {
		labelName := strings.TrimPrefix(tagColumnName, LabelColumnPrefix)
		value := GetAthenaRowValue(row, aqi.ColumnIndexes, tagColumnName)
		if value != "" {
			labels[labelName] = value
			labelValues = append(labelValues, value)
		}
	}

	invoiceEntityID := GetAthenaRowValue(row, aqi.ColumnIndexes, "bill_payer_account_id")
	accountID := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_usage_account_id")
	startStr := GetAthenaRowValue(row, aqi.ColumnIndexes, AthenaDateTruncColumn)
	providerID := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_resource_id")
	productCode := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_product_code")
	usageType := GetAthenaRowValue(row, aqi.ColumnIndexes, "line_item_usage_type")
	isNode, _ := strconv.ParseBool(GetAthenaRowValue(row, aqi.ColumnIndexes, AthenaIsNode))
	isVol, _ := strconv.ParseBool(GetAthenaRowValue(row, aqi.ColumnIndexes, AthenaIsVol))
	isNetwork, _ := strconv.ParseBool(GetAthenaRowValue(row, aqi.ColumnIndexes, AthenaIsNetwork))

	listCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.ListCostColumn)
	if err != nil {
		return err
	}

	listK8sCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.ListK8sCostColumn)
	if err != nil {
		return err
	}

	netCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.NetCostColumn)
	if err != nil {
		return err
	}

	netK8sCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.NetK8sCostColumn)
	if err != nil {
		return err
	}

	amortizedNetCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.AmortizedNetCostColumn)
	if err != nil {
		return err
	}

	amortizedNetK8sCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.AmortizedNetK8sCostColumn)
	if err != nil {
		return err
	}
	amortizedCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.AmortizedCostColumn)
	if err != nil {
		return err
	}

	amortizedK8sCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.AmortizedK8sCostColumn)
	if err != nil {
		return err
	}

	invoicedCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.InvoicedCostColumn)
	if err != nil {
		return err
	}

	invoicedK8sCost, err := GetAthenaRowValueFloat(row, aqi.ColumnIndexes, aqi.InvoicedK8sCostColumn)
	if err != nil {
		return err
	}

	// Identify resource category in the CUR
	category := SelectAWSCategory(isNode, isVol, isNetwork, providerID, productCode)

	// Retrieve final stanza of product code for ProviderID
	if productCode == "AWSELB" || productCode == "AmazonFSx" {
		providerID = ParseARN(providerID)
	}

	if productCode == "AmazonEKS" && category == kubecost.ComputeCategory {
		if strings.Contains(usageType, "CPU") {
			providerID = fmt.Sprintf("%s/CPU", providerID)
		} else if strings.Contains(usageType, "GB") {
			providerID = fmt.Sprintf("%s/RAM", providerID)
		}
	}

	properties := kubecost.CloudCostProperties{
		ProviderID:      providerID,
		Provider:        kubecost.AWSProvider,
		AccountID:       accountID,
		InvoiceEntityID: invoiceEntityID,
		Service:         productCode,
		Category:        category,
		Labels:          labels,
	}

	start, err := time.Parse(AthenaDateLayout, startStr)
	if err != nil {
		return fmt.Errorf("unable to parse %s: '%s'", AthenaDateTruncColumn, err.Error())
	}
	end := start.AddDate(0, 0, 1)

	cc := &kubecost.CloudCost{
		Properties: &properties,
		Window:     kubecost.NewWindow(&start, &end),
		ListCost: kubecost.CostMetric{
			Cost:              listCost,
			KubernetesPercent: ai.CalculateK8sPercent(listCost, listK8sCost),
		},
		NetCost: kubecost.CostMetric{
			Cost:              netCost,
			KubernetesPercent: ai.CalculateK8sPercent(netCost, netK8sCost),
		},
		AmortizedNetCost: kubecost.CostMetric{
			Cost:              amortizedNetCost,
			KubernetesPercent: ai.CalculateK8sPercent(amortizedNetCost, amortizedNetK8sCost),
		},
		AmortizedCost: kubecost.CostMetric{
			Cost:              amortizedCost,
			KubernetesPercent: ai.CalculateK8sPercent(amortizedCost, amortizedK8sCost),
		},
		InvoicedCost: kubecost.CostMetric{
			Cost:              invoicedCost,
			KubernetesPercent: ai.CalculateK8sPercent(invoicedCost, invoicedK8sCost),
		},
	}

	ccsr.LoadCloudCost(cc)
	return nil
}

func (ai *AthenaIntegration) CalculateK8sPercent(cost, k8sCost float64) float64 {
	// Calculate percent of cost that is k8s with the k8sCost
	k8sPercent := 0.0
	if k8sCost != 0.0 && cost != 0.0 {
		k8sPercent = k8sCost / cost
	}
	return k8sPercent
}

func (ai *AthenaIntegration) GetConnectionStatusFromResult(result cloud.EmptyChecker, currentStatus cloud.ConnectionStatus) cloud.ConnectionStatus {
	if result.IsEmpty() && currentStatus != cloud.SuccessfulConnection {
		return cloud.MissingData
	}
	return cloud.SuccessfulConnection
}

func (ai *AthenaIntegration) GetConnectionStatus() string {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if ai.ConnectionStatus.String() == "" {
		ai.ConnectionStatus = cloud.InitialStatus
	}

	return ai.ConnectionStatus.String()
}
