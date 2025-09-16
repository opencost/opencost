package env

import (
	"github.com/opencost/opencost/core/pkg/env"
)

const (
	CloudCostConfigControllerStateFile = "cloud-configurations.json"
	CloudIntegrationConfigFile         = "cloud-integration.json"
	AzureBillingDataDownloadPath       = "db/cloudcost"
)

const (
	CloudCostEnvVarPrefix           = "CLOUD_COST_"
	CloudCostEnabledEnvVar          = "CLOUD_COST_ENABLED"
	CloudCostMonthToDateIntervalVar = "CLOUD_COST_MONTH_TO_DATE_INTERVAL"
	CloudCostRefreshRateHoursEnvVar = "CLOUD_COST_REFRESH_RATE_HOURS"
	CloudCostQueryWindowDaysEnvVar  = "CLOUD_COST_QUERY_WINDOW_DAYS"
	CloudCostRunWindowDaysEnvVar    = "CLOUD_COST_RUN_WINDOW_DAYS"

	CustomCostEnvVarPrefix          = "CUSTOM_COST_"
	CustomCostEnabledEnvVar         = "CUSTOM_COST_ENABLED"
	CustomCostQueryWindowDaysEnvVar = "CUSTOM_COST_QUERY_WINDOW_DAYS"

	PluginConfigDirEnvVar     = "PLUGIN_CONFIG_DIR"
	PluginExecutableDirEnvVar = "PLUGIN_EXECUTABLE_DIR"

	AzureDownloadBillingDataToDiskEnvVar = "AZURE_DOWNLOAD_BILLING_DATA_TO_DISK"
)

func IsCloudCostEnabled() bool {
	return env.GetBool(CloudCostEnabledEnvVar, false)
}

func IsCustomCostEnabled() bool {
	return env.GetBool(CustomCostEnabledEnvVar, false)
}

func GetCloudCostConfigPath() string {
	return env.GetPathFromConfig(CloudIntegrationConfigFile)
}

func GetCloudCostMonthToDateInterval() int {
	return env.GetInt(CloudCostMonthToDateIntervalVar, 6)
}

func GetCloudCostRefreshRateHours() int {
	return env.GetInt(CloudCostRefreshRateHoursEnvVar, 6)
}

func GetCloudCostQueryWindowDays() int {
	return env.GetInt(CloudCostQueryWindowDaysEnvVar, 7)
}

func GetCloudCostRunWindowDays() int {
	return env.GetInt(CloudCostRunWindowDaysEnvVar, 3)
}

func GetCloudCost1dRetention() int {
	return env.GetPrefixInt(CloudCostEnvVarPrefix, env.Resolution1dRetentionEnvVar, 30)
}

func GetCustomCostQueryWindowHours() int {
	return env.GetInt(CustomCostQueryWindowDaysEnvVar, 1)
}

func GetCustomCostQueryWindowDays() int {
	return env.GetInt(CustomCostQueryWindowDaysEnvVar, 7)
}

func GetCustomCost1dRetention() int {
	return env.GetPrefixInt(CustomCostEnvVarPrefix, env.Resolution1dRetentionEnvVar, 30)
}

func GetCustomCost1hRetention() int {
	return env.GetPrefixInt(CustomCostEnvVarPrefix, env.Resolution1hRetentionEnvVar, 49)
}

func GetPluginConfigDir() string {
	return env.Get(PluginConfigDirEnvVar, "/opt/opencost/plugin/config")
}

func GetPluginExecutableDir() string {
	return env.Get(PluginExecutableDirEnvVar, "/opt/opencost/plugin/bin")
}

func GetAzureDownloadBillingDataPath() string {
	return env.GetPathFromConfig(AzureBillingDataDownloadPath)
}

func GetCloudCostConfigControllerStateFile() string {
	return env.GetPathFromConfig(CloudCostConfigControllerStateFile)
}
