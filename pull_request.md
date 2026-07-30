## Description
This PR implements a cost anomaly detection API endpoint `GET /anomaly` in OpenCost.

Key changes:
1. Created a new package `pkg/anomaly` implementing rolling Z-score and rolling Median Absolute Deviation (MAD) anomaly detection algorithms.
2. Added `GET /anomaly` handler in `pkg/costmodel/router.go`.
3. Allowed query parameters:
   - `window`: total time window (lookback + detection), e.g., `30d` (default).
   - `step`: resolution step size, e.g., `1d` (default).
   - `lookback`: window of historical data to build baseline, e.g., `7d` (default).
   - `algorithm`: `mad` (default) or `zscore`.
   - `threshold`: standard deviation/MAD multiplier (default `3.5` for MAD, `3.0` for Z-score).
   - `minCost`: ignore micro-costs below this value to avoid noise (default `$0.10`).
   - `aggregate`: cost grouping (default `namespace`).
   - `filter`: allocation filters.

## Related Issues
Closes #4156

## User Impact
Users/FinOps teams can query `GET /anomaly` to identify sudden unexpected spikes in cost data, grouped by namespace, pod, cluster, etc.

## Testing
1. Added unit tests for statistical functions and anomaly detection algorithms in `pkg/anomaly/anomaly_test.go`.
2. Added handler routing and validation tests in `pkg/costmodel/anomaly_handler_test.go`.
3. Verified all tests passed successfully using `go test ./...`.
