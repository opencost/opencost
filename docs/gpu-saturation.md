# GPU Saturation Signals

OpenCost derives GPU **saturation** signals from
[dcgm-exporter](https://github.com/NVIDIA/dcgm-exporter) metrics, following
the [USE method](https://www.brendangregg.com/usemethod.html):

- **Utilization** — how busy the GPU was (already exposed as
  `gpuAllocation.gpuUsageAverage` from `DCGM_FI_PROF_GR_ENGINE_ACTIVE`).
- **Saturation** — work that was queued, rejected, or slowed because the GPU
  could not service demand. That is what the signals below report.

Every signal is an independent primitive. OpenCost deliberately does **not**
compute a composite saturation score; consumers combine the primitives as
they see fit.

## Absence semantics

A missing field always means *the source metric was unavailable* — no
dcgm-exporter in the cluster, the DCGM field is not in the exporter's
configuration, or the GPU lacks DCP profiling support. OpenCost never emits
a zero in place of missing data, so `0` can be trusted to mean "observed,
and not saturated".

## Allocation API

Saturation appears on the Allocation API under
`gpuAllocation.saturation`, per container:

```json
{
  "gpuAllocation": {
    "gpuDevice": "nvidia0",
    "gpuModel": "Tesla T4",
    "gpuUUID": "GPU-...",
    "saturation": {
      "throttleViolationRatios": { "power": 0.12, "thermal": 0.01 },
      "throttleReasonRatios": { "sw_power_cap": 0.15 },
      "memoryUsedRatioAvg": 0.81,
      "memoryUsedRatioMax": 0.97,
      "memoryPressureRatio": 0.25,
      "xidErrorCount": 0,
      "dramActiveAvg": 0.62,
      "smActiveAvg": 0.55,
      "smOccupancyAvg": 0.31,
      "pcieTxBytesAvg": 1.2e9,
      "pcieRxBytesAvg": 2.0e9
    }
  }
}
```

Controlled by `GPU_SATURATION_METRICS_ENABLED` (default `true`).

## Signal reference

Ratios are fractions of the queried window in `[0, 1]` unless noted.

| Field | DCGM source | In default dcgm-exporter config? | Needs DCP? | Meaning |
|---|---|---|---|---|
| `throttleViolationRatios` (`power`, `thermal`, `sync_boost`, `board_limit`) | `DCGM_FI_DEV_POWER_VIOLATION`, `DCGM_FI_DEV_THERMAL_VIOLATION`, `DCGM_FI_DEV_SYNC_BOOST_VIOLATION`, `DCGM_FI_DEV_BOARD_LIMIT_VIOLATION` | Yes | No | Fraction of the window the GPU spent throttled for the reason, from cumulative microsecond violation counters. The strongest direct saturation signal available by default. |
| `throttleReasonRatios` (`sw_power_cap`, `hw_slowdown`, `sync_boost`, `sw_thermal`, `hw_thermal`, `hw_power_brake`) | `DCGM_FI_DEV_CLOCK_THROTTLE_REASONS` (renamed `DCGM_FI_DEV_CLOCKS_EVENT_REASONS` in DCGM 3.3+; OpenCost queries both) | **No — must be enabled** | No | Fraction of samples in which each saturation-relevant bit of the NVML throttle-reasons bitmask was set. Richer reason breakdown than the violation counters (hardware slowdown, power brake). Idle/configured-clock bits are excluded by design. Reported for the whole physical GPU even under MIG or time-slicing. |
| `memoryUsedRatioAvg`, `memoryUsedRatioMax` | `DCGM_FI_DEV_FB_USED`, `DCGM_FI_DEV_FB_FREE` | Yes | No | Framebuffer occupancy `used / (used + free)`. Sustained values near 1.0 mean new allocations are likely to fail or force eviction. |
| `memoryPressureRatio` | same | Yes | No | Fraction of the window occupancy was at or above the configured threshold (`GPU_MEMORY_SATURATION_THRESHOLD`, default `0.9`). |
| `xidErrorCount` | `DCGM_FI_DEV_XID_ERRORS` | Yes | No | XID error events observed in the window, a rejected-work signal. The DCGM field reports the *last* XID code, so consecutive identical errors are undercounted. |
| `dramActiveAvg`, `dramActiveMax` | `DCGM_FI_PROF_DRAM_ACTIVE` | Yes | **Yes** | Ratio of cycles the device memory interface was active. Near-ceiling values with low `smOccupancyAvg` indicate a memory-bandwidth-bound workload. |
| `smActiveAvg` | `DCGM_FI_PROF_SM_ACTIVE` | **No — must be enabled** | **Yes** | Ratio of cycles at least one warp was resident on any SM. |
| `smOccupancyAvg` | `DCGM_FI_PROF_SM_OCCUPANCY` | **No — must be enabled** | **Yes** | Ratio of resident warps to the SM maximum. Together with `smActiveAvg` and `dramActiveAvg`, lets consumers distinguish compute-bound vs bandwidth-bound vs latency-bound saturation. |
| `pcieTxBytesAvg`, `pcieRxBytesAvg` | `DCGM_FI_PROF_PCIE_TX_BYTES`, `DCGM_FI_PROF_PCIE_RX_BYTES` | Yes | **Yes** | Average PCIe throughput in bytes/sec. DCGM does not export link capacity, so these are raw rates; deriving a capacity ratio is future work. |
| `nvlinkTxBytesAvg`, `nvlinkRxBytesAvg` | `DCGM_FI_PROF_NVLINK_TX_BYTES`, `DCGM_FI_PROF_NVLINK_RX_BYTES` | **No — must be enabled** | **Yes** | Average NVLink throughput in bytes/sec; same capacity caveat as PCIe. |

"Needs DCP" means the field comes from DCGM's profiling module (DCP), which
requires Volta or newer GPUs and is unavailable in some virtualized
environments. "Must be enabled" means the field exists in the dcgm-exporter
default configuration file but is commented out (or absent), and must be
uncommented/added for the signal to appear.

### Interpretation guidance (USE)

- High utilization with **zero** throttle/pressure ratios: the GPU is busy
  but keeping up — buying a faster GPU may not help.
- `throttleViolationRatios.power` or `sw_power_cap` sustained above zero:
  the GPU is power-limited; demand exceeds what the power envelope can
  service.
- `memoryUsedRatioMax` near 1.0 or `memoryPressureRatio` > 0 alongside
  `xidErrorCount` > 0: work is likely being rejected (OOM-style failures).
- `dramActiveAvg` near ceiling with low `smOccupancyAvg`: bandwidth-bound;
  with high `smOccupancyAvg`: genuinely compute-saturated.

### Attribution caveat

Device-level signals (throttling, framebuffer, XID) are attributed to
containers via the pod labels dcgm-exporter attaches. For exclusively
assigned GPUs this is exact. For time-sliced or MPS-shared GPUs, every
sharing container sees the same device-level saturation; the signal tells
you the *device* was saturated, not which tenant caused it. MIG slices are
reported as distinct devices (dcgm-exporter `GPU_I_ID` / `GPU_I_PROFILE`
labels), except the throttle-reasons bitmask, which is physical-GPU-scoped.

## Scheduler-level saturation metrics

DCGM cannot see work that never reached a GPU. OpenCost additionally emits
cluster-scoped gauges on `/metrics` from the Kubernetes scheduler's view,
one series per observed GPU resource name (`nvidia.com/gpu`,
`nvidia.com/gpu.shared`, `nvidia.com/mig-*`):

| Metric | Meaning |
|---|---|
| `cluster_gpu_pending_pod_count{resource}` | Pods in `Pending` phase requesting the resource. |
| `cluster_gpu_pending_request_total{resource}` | GPU units requested by those pending pods. |
| `cluster_gpu_requested_allocatable_ratio{resource}` | Units requested by all non-terminated pods divided by allocatable units. Values near or above 1 indicate scheduler-level saturation, including exhaustion of time-sliced/MPS replicas. Only emitted when allocatable capacity exists. |

These can be disabled individually through the standard metrics
configuration (the same mechanism as `node_gpu_count` et al.).

## Configuration

| Variable | Default | Description |
|---|---|---|
| `GPU_SATURATION_METRICS_ENABLED` | `true` | Query and apply GPU saturation signals in the Allocation pipeline. |
| `GPU_MEMORY_SATURATION_THRESHOLD` | `0.9` | Framebuffer occupancy ratio above which the GPU counts as memory-pressured. Values outside `(0, 1]` fall back to the default. |

## Data source differences

- **Prometheus source**: the throttle-bitmask and memory-pressure signals
  use PromQL subqueries at the configured query resolution.
- **Collector source**: framebuffer occupancy is joined from FB_USED and
  FB_FREE per scrape by a metric synthesizer, producing a per-sample ratio
  that the occupancy and pressure aggregations consume. All signals are
  supported.

## Future work

- Non-NVIDIA GPUs (AMD ROCm SMI exporter, Intel XPU manager) — the signal
  taxonomy is vendor-neutral, the queries are not.
- PCIe/NVLink capacity ratios, once link capacity can be derived per model.
- Device-level saturation in the kubemodel pipeline is modeled
  (`Device.Saturation`, `GPUDevice.saturation` in the protobuf) but not yet
  populated; it will be wired when device population lands.
