---
name: OpenCost Feature request
about: Suggest an idea for this project
title: 'feat: Add cost anomaly detection API endpoint (GET /anomaly)'
labels: 'feature'
assignees: ''

---

**Is your feature request related to a problem? Please describe.**
FinOps teams need to monitor sudden, unexpected spikes in daily or hourly cost data (e.g., a namespace's CPU cost suddenly doubling or cloud storage costs spiking). Currently, OpenCost has no native anomaly detection.

**Describe the solution you'd like**
Implement a cost anomaly detection API endpoint `GET /anomaly` that analyzes time-series metrics.
We want to:
1. Create a package `pkg/anomaly` to implement simple statistical anomaly detection algorithms on historical allocations (e.g., seasonal/rolling Z-score or rolling Median Absolute Deviation (MAD)).
2. Add a `GET /anomaly` router handler in `router.go`.
3. Allow query parameters to define lookback windows (e.g., 7 days or 30 days) and sensitivity thresholds.

**Describe alternatives you've considered**
External time-series anomaly detection tools, but built-in native anomaly detection in OpenCost provides faster, zero-configuration cost monitoring for FinOps teams.

**Additional context**
The endpoint will return reports detailing the cost values, expected baseline values (mean/median), deviations (stddev/MAD), and anomaly scores.
