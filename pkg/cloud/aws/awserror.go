package aws

import (
	"errors"
	"fmt"

	"github.com/aws/smithy-go"
)

// formatAWSError builds a service/operation-scoped error message for a failed AWS SDK call, so
// that failures are always attributed to the AWS service and operation that actually failed
// (e.g. Athena StartQueryExecution) rather than being confused with unrelated failures earlier
// in the same request chain, such as an AssumeRole credential failure.
func formatAWSError(service, operation string, err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return fmt.Errorf("AWS %s %s failed: code=%s message=%s: %w", service, operation, apiErr.ErrorCode(), apiErr.ErrorMessage(), err)
	}
	return fmt.Errorf("AWS %s %s failed: %w", service, operation, err)
}
