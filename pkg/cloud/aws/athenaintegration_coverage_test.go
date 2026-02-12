package aws

import (
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/cloud"
)

func TestAthenaIntegration_GetListCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}
	expected := "SUM(CASE line_item_line_item_type WHEN 'EdpDiscount' THEN 0 WHEN 'PrivateRateDiscount' THEN 0 ELSE line_item_unblended_cost END) as list_cost"
	actual := ai.GetListCostColumn()
	if actual != expected {
		t.Errorf("GetListCostColumn() = %v, want %v", actual, expected)
	}
}

func TestAthenaIntegration_GetNetCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	// Test case where net pricing column exists
	allColumnsWithNet := map[string]bool{
		"line_item_net_unblended_cost": true,
	}
	expectedWithNet := "SUM(COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0)) as net_cost"
	actualWithNet := ai.GetNetCostColumn(allColumnsWithNet)
	if actualWithNet != expectedWithNet {
		t.Errorf("GetNetCostColumn() with net pricing = %v, want %v", actualWithNet, expectedWithNet)
	}

	// Test case where net pricing column doesn't exist
	allColumnsWithoutNet := map[string]bool{
		"line_item_unblended_cost": true,
	}
	expectedWithoutNet := "SUM(line_item_unblended_cost) as net_cost"
	actualWithoutNet := ai.GetNetCostColumn(allColumnsWithoutNet)
	if actualWithoutNet != expectedWithoutNet {
		t.Errorf("GetNetCostColumn() without net pricing = %v, want %v", actualWithoutNet, expectedWithoutNet)
	}
}

func TestAthenaIntegration_GetAmortizedCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}
	allColumns := map[string]bool{
		"reservation_effective_cost":               true,
		"savings_plan_savings_plan_effective_cost": true,
		"line_item_unblended_cost":                 true,
	}

	result := ai.GetAmortizedCostColumn(allColumns)
	if !strings.Contains(result, "SUM(") || !strings.Contains(result, " as amortized_cost") {
		t.Errorf("GetAmortizedCostColumn() should return a SUM expression with amortized_cost alias, got: %v", result)
	}
}

func TestAthenaIntegration_GetAmortizedNetCostColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	// Test case where net pricing columns exist
	allColumnsWithNet := map[string]bool{
		"line_item_net_unblended_cost":                 true,
		"reservation_net_effective_cost":               true,
		"savings_plan_net_savings_plan_effective_cost": true,
		"line_item_unblended_cost":                     true,
	}
	resultWithNet := ai.GetAmortizedNetCostColumn(allColumnsWithNet)
	if !strings.Contains(resultWithNet, "SUM(") || !strings.Contains(resultWithNet, " as amortized_net_cost") {
		t.Errorf("GetAmortizedNetCostColumn() with net pricing should return a SUM expression with amortized_net_cost alias, got: %v", resultWithNet)
	}

	// Test case where net pricing columns don't exist
	allColumnsWithoutNet := map[string]bool{
		"reservation_effective_cost":               true,
		"savings_plan_savings_plan_effective_cost": true,
		"line_item_unblended_cost":                 true,
	}
	resultWithoutNet := ai.GetAmortizedNetCostColumn(allColumnsWithoutNet)
	if !strings.Contains(resultWithoutNet, "SUM(") || !strings.Contains(resultWithoutNet, " as amortized_net_cost") {
		t.Errorf("GetAmortizedNetCostColumn() without net pricing should return a SUM expression with amortized_net_cost alias, got: %v", resultWithoutNet)
	}
}

func TestAthenaIntegration_GetAmortizedCostCase(t *testing.T) {
	ai := &AthenaIntegration{}

	// Test case where RI and SP pricing columns exist
	allColumnsWithRIAndSP := map[string]bool{
		"reservation_effective_cost":               true,
		"savings_plan_savings_plan_effective_cost": true,
		"line_item_unblended_cost":                 true,
	}
	resultWithRIAndSP := ai.GetAmortizedCostCase(allColumnsWithRIAndSP)
	if !strings.Contains(resultWithRIAndSP, "CASE line_item_line_item_type") ||
		!strings.Contains(resultWithRIAndSP, "DiscountedUsage") ||
		!strings.Contains(resultWithRIAndSP, "SavingsPlanCoveredUsage") {
		t.Errorf("GetAmortizedCostCase() with RI and SP should contain CASE statement with DiscountedUsage and SavingsPlanCoveredUsage, got: %v", resultWithRIAndSP)
	}

	// Test case where neither RI nor SP pricing columns exist
	allColumnsWithoutRIOrSP := map[string]bool{
		"line_item_unblended_cost": true,
	}
	resultWithoutRIOrSP := ai.GetAmortizedCostCase(allColumnsWithoutRIOrSP)
	expectedWithoutRIOrSP := "line_item_unblended_cost"
	if resultWithoutRIOrSP != expectedWithoutRIOrSP {
		t.Errorf("GetAmortizedCostCase() without RI or SP should return line_item_unblended_cost, got: %v, want: %v", resultWithoutRIOrSP, expectedWithoutRIOrSP)
	}
}

func TestAthenaIntegration_GetAmortizedNetCostCase(t *testing.T) {
	ai := &AthenaIntegration{}

	// Test case where net RI and SP pricing columns exist
	allColumnsWithNetRIAndSP := map[string]bool{
		"reservation_net_effective_cost":               true,
		"savings_plan_net_savings_plan_effective_cost": true,
		"line_item_net_unblended_cost":                 true,
		"line_item_unblended_cost":                     true,
	}
	resultWithNetRIAndSP := ai.GetAmortizedNetCostCase(allColumnsWithNetRIAndSP)
	if !strings.Contains(resultWithNetRIAndSP, "CASE line_item_line_item_type") ||
		!strings.Contains(resultWithNetRIAndSP, "DiscountedUsage") ||
		!strings.Contains(resultWithNetRIAndSP, "SavingsPlanCoveredUsage") {
		t.Errorf("GetAmortizedNetCostCase() with net RI and SP should contain CASE statement with DiscountedUsage and SavingsPlanCoveredUsage, got: %v", resultWithNetRIAndSP)
	}

	// Test case where neither net RI nor net SP pricing columns exist
	allColumnsWithoutNetRIOrSP := map[string]bool{
		"line_item_net_unblended_cost": true,
		"line_item_unblended_cost":     true,
	}
	resultWithoutNetRIOrSP := ai.GetAmortizedNetCostCase(allColumnsWithoutNetRIOrSP)
	expectedStr := "COALESCE(line_item_net_unblended_cost, line_item_unblended_cost, 0)"
	if resultWithoutNetRIOrSP != expectedStr {
		t.Errorf("GetAmortizedNetCostCase() without net RI or SP should return COALESCE expression, got: %v, want: %v", resultWithoutNetRIOrSP, expectedStr)
	}
}

func TestAthenaIntegration_RemoveColumnAliases(t *testing.T) {
	ai := &AthenaIntegration{}
	columns := []string{
		"column1 as alias1",
		"column2",
		"column3 as alias3",
		"column4",
	}

	ai.RemoveColumnAliases(columns)

	if columns[0] != "column1" {
		t.Errorf("RemoveColumnAliases() should remove alias from 'column1 as alias1', got: %v", columns[0])
	}
	if columns[1] != "column2" {
		t.Errorf("RemoveColumnAliases() should not modify 'column2', got: %v", columns[1])
	}
	if columns[2] != "column3" {
		t.Errorf("RemoveColumnAliases() should remove alias from 'column3 as alias3', got: %v", columns[2])
	}
	if columns[3] != "column4" {
		t.Errorf("RemoveColumnAliases() should not modify 'column4', got: %v", columns[3])
	}
}

func TestAthenaIntegration_ConvertLabelToAWSTag(t *testing.T) {
	ai := &AthenaIntegration{}

	// Test case where label already has prefix
	labelWithPrefix := "resource_tags_user_test_label"
	resultWithPrefix := ai.ConvertLabelToAWSTag(labelWithPrefix)
	if resultWithPrefix != labelWithPrefix {
		t.Errorf("ConvertLabelToAWSTag() should return label unchanged if it already has prefix, got: %v, want: %v", resultWithPrefix, labelWithPrefix)
	}

	// Test case where label needs prefix
	labelWithoutPrefix := "test.label/with:characters-here"
	resultWithoutPrefix := ai.ConvertLabelToAWSTag(labelWithoutPrefix)
	expectedWithoutPrefix := "resource_tags_user_test_label_with_characters_here"
	if resultWithoutPrefix != expectedWithoutPrefix {
		t.Errorf("ConvertLabelToAWSTag() should add prefix and replace characters, got: %v, want: %v", resultWithoutPrefix, expectedWithoutPrefix)
	}
}

func TestAthenaIntegration_GetIsKubernetesColumn(t *testing.T) {
	ai := &AthenaIntegration{}

	// Test with some tag columns present
	allColumns := map[string]bool{
		"resource_tags_user_eks_cluster_name":             true,
		"resource_tags_user_alpha_eksctl_io_cluster_name": true,
		"resource_tags_user_kubernetes_io_service_name":   true,
		"some_other_column":                               true,
	}

	result := ai.GetIsKubernetesColumn(allColumns)
	if !strings.Contains(result, "line_item_product_code = 'AmazonEKS'") {
		t.Errorf("GetIsKubernetesColumn() should always include EKS check, got: %v", result)
	}
	if !strings.Contains(result, "resource_tags_user_eks_cluster_name <> ''") {
		t.Errorf("GetIsKubernetesColumn() should include checks for tag columns, got: %v", result)
	}
	if !strings.Contains(result, " as is_kubernetes") {
		t.Errorf("GetIsKubernetesColumn() should alias result as is_kubernetes, got: %v", result)
	}
}

func TestAthenaQuerier_GetStatus(t *testing.T) {
	aq := &AthenaQuerier{}

	// Test initial status
	status := aq.GetStatus()
	if status.String() != cloud.InitialStatus.String() {
		t.Errorf("GetStatus() should return InitialStatus for uninitialized querier, got: %v", status)
	}

	// Test setting a specific status
	aq.ConnectionStatus = cloud.SuccessfulConnection
	status = aq.GetStatus()
	if status != cloud.SuccessfulConnection {
		t.Errorf("GetStatus() should return set status, got: %v", status)
	}
}

func TestAthenaQuerier_Equals(t *testing.T) {
	aq1 := &AthenaQuerier{
		AthenaConfiguration: AthenaConfiguration{
			Bucket:   "bucket1",
			Region:   "region1",
			Database: "database1",
			Table:    "table1",
			Account:  "account1",
			Authorizer: &AccessKey{
				ID:     "id1",
				Secret: "secret1",
			},
		},
	}

	aq2 := &AthenaQuerier{
		AthenaConfiguration: AthenaConfiguration{
			Bucket:   "bucket1",
			Region:   "region1",
			Database: "database1",
			Table:    "table1",
			Account:  "account1",
			Authorizer: &AccessKey{
				ID:     "id1",
				Secret: "secret1",
			},
		},
	}

	aq3 := &AthenaQuerier{
		AthenaConfiguration: AthenaConfiguration{
			Bucket:   "bucket2", // Different bucket
			Region:   "region1",
			Database: "database1",
			Table:    "table1",
			Account:  "account1",
			Authorizer: &AccessKey{
				ID:     "id1",
				Secret: "secret1",
			},
		},
	}

	// Test equality
	if !aq1.Equals(aq2) {
		t.Errorf("Equals() should return true for identical configurations")
	}

	// Test inequality
	if aq1.Equals(aq3) {
		t.Errorf("Equals() should return false for different configurations")
	}

	// Test comparison with non-AthenaQuerier
	accessKey := &AccessKey{
		ID:     "id1",
		Secret: "secret1",
	}
	if aq1.Equals(accessKey) {
		t.Errorf("Equals() should return false when comparing with different type")
	}
}

// Helper function for parsing time in tests
func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}
