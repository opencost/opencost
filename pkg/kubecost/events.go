package kubecost

// TODO prefix all with "ETL" and have "ETL*"?

const (
	AllocationEvents                = "Allocation*"
	AllocationComputeBeginEvent     = "AllocationComputeBeginEvent"
	AllocationComputeFailureEvent   = "AllocationComputeFailureEvent"
	AllocationComputeSuccessEvent   = "AllocationComputeSuccessEvent"
	AllocationInsertSuccessEvent    = "AllocationInsertSuccessEvent"
	AllocationReconcileSuccessEvent = "AllocationReconcileSuccessEvent"
	AllocationQueryBeginEvent       = "AllocationQueryBeginEvent"
	AllocationQueryFailureEvent     = "AllocationQueryFailureEvent"
	AllocationQuerySuccessEvent     = "AllocationQuerySuccessEvent"
	AllocationRepairBeginEvent      = "AllocationRepairBeginEvent"
	AllocationRepairFailureEvent    = "AllocationRepairFailureEvent"
	AllocationRepairSuccessEvent    = "AllocationRepairSuccessEvent"

	AssetEvents                = "Asset*"
	AssetComputeBeginEvent     = "AssetComputeBeginEvent"
	AssetComputeFailureEvent   = "AssetComputeFailureEvent"
	AssetComputeSuccessEvent   = "AssetComputeSuccessEvent"
	AssetInsertSuccessEvent    = "AssetInsertSuccessEvent"
	AssetQueryBeginEvent       = "AssetQueryBeginEvent"
	AssetQueryFailureEvent     = "AssetQueryFailureEvent"
	AssetQuerySuccessEvent     = "AssetQuerySuccessEvent"
	AssetReconcileSuccessEvent = "AssetReconcileSuccessEvent"
	AssetRepairBeginEvent      = "AssetRepairBeginEvent"
	AssetRepairFailureEvent    = "AssetRepairFailureEvent"
	AssetRepairSuccessEvent    = "AssetRepairSuccessEvent"

	CloudEvents              = "Cloud*"
	CloudComputeBeginEvent   = "CloudComputeBeginEvent"
	CloudComputeFailureEvent = "CloudComputeFailureEvent"
	CloudComputeSuccessEvent = "CloudComputeSuccessEvent"
	CloudInsertSuccessEvent  = "CloudInsertSuccessEvent"
	CloudQueryBeginEvent     = "CloudQueryBeginEvent"
	CloudQueryFailureEvent   = "CloudQueryFailureEvent"
	CloudQuerySuccessEvent   = "CloudQuerySuccessEvent"
	CloudRepairBeginEvent    = "CloudRepairBeginEvent"
	CloudRepairFailureEvent  = "CloudRepairFailureEvent"
	CloudRepairSuccessEvent  = "CloudRepairSuccessEvent"
)
