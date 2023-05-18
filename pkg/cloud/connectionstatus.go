package cloud

// ConnectionStatus communicates the status of a cloud connection in a way that is general enough to apply to each
// Cloud Provider, but still give actionable information on how to trouble shoot one the four failing statuses.
type ConnectionStatus string

const (
	// InitialStatus is the zero value of CloudConnectionStatus and means that cloud connection is untested. Once
	// CloudConnection Status has been changed in should not return to this value. This status is assigned on creation
	// to the cloud provider
	InitialStatus ConnectionStatus = "No Connection"

	// InvalidConfiguration means that Cloud Configuration is missing required values to connect to cloud provider.
	// This status is assigned during failures in the provider implementation of getCloudConfig()
	InvalidConfiguration = "Invalid Configuration"

	// FailedConnection means that all required Cloud Configuration values are filled in, but a connection with the
	// Cloud Provider cannot be established. This is indicative of a typo in one of the Cloud Configuration values or an
	// issue in how the connection was set up in the Cloud Provider's Console. The assignment of this status varies
	// between implementations, but should happen if an error is thrown when an interaction with an object from
	// the Cloud Service Provider's sdk occurs.
	FailedConnection = "Failed Connection"

	// ParseError indicates an issue with our functions which parse responses
	ParseError = "Parse Error"

	// MissingData means that the Cloud Integration is properly configured, but the cloud provider is not returning
	// billing/cost and usage data. This status is indicative of the billing/cost and usage data export of the Cloud Provider
	// being incorrectly set up or the export being set up in the last 48 hours and not having started populating data yet.
	// This status is set when a query has been successfully made but the results come back empty. If the cloud provider,
	// already has a SUCCESSFUL_CONNECTION status then this status should not be set, because this indicates that the specific
	// query made may have been empty.
	MissingData = "Data Missing"

	// SuccessfulConnection means that the Cloud Integration is properly configured and returning data. This status is
	// set on any successful query where data is returned
	SuccessfulConnection = "Connection Successful"
)

func (cs ConnectionStatus) String() string {
	return string(cs)
}

// EmptyChecker provides an interface for to check if a result is empty which can be useful for setting a MissingData status
type EmptyChecker interface {
	IsEmpty() bool
}
