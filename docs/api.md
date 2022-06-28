# API Usage


```
GET /allocation/compute
```

Argument | Default | Description
--: | :--: | :--
window (required) | — | Duration of time over which to query. Accepts: words like `today`, `week`, `month`, `yesterday`, `lastweek`, `lastmonth`; durations like `30m`, `12h`, `7d`; RFC3339 date pairs like `2021-01-02T15:04:05Z,2021-02-02T15:04:05Z`; unix timestamps like `1578002645,1580681045`.
resolution | 1m | Duration to use as resolution in Prometheus queries. Smaller values (i.e. higher resolutions) will provide better accuracy, but worse performance (i.e. slower query time, higher memory use). Larger values (i.e. lower resolutions) will perform better, but at the expense of lower accuracy for short-running workloads. (See [error bounds](#theoretical-error-bounds) for details.)
step | `window` | Duration of a single allocation set. If unspecified, this defaults to the `window`, so that you receive exactly one set for the entire window. If specified, it works chronologically backward, querying in durations of `step` until the full window is covered.
aggregate | | Field by which to aggregate the results. Accepts: `cluster`, `namespace`, `controllerKind`, `controller`, `service`, `label:<name>`, and `annotation:<name>`. Also accepts comma-separated lists for multi-aggregation, like `namespace,label:app`.
accumulate | false | If `true`, sum the entire range of sets into a single set.

## On-demand query examples

Allocation data for the last 60m, in steps of 10m, with resolution 1m, aggregated by namespace.

```sh
curl http://localhost:9090/allocation/compute \
  -d window=60m \
  -d resolution=1m \
  -d aggregate=namespace \
  -d accumulate=true \
  -G
```

```json
{
  "code": 200,
  "data": [
    {
      "kube-system": { ... },
      "default": { ... },
      "cost-model": { ... }
    }
  ]
}
```

Allocation data for the last 9d, in steps of 3d, with a 10m resolution, aggregated by namespace.

```sh
curl http://localhost:9090/allocation/compute \
  -d window=9d \
  -d step=3d \
  -d resolution=10m
  -d aggregate=namespace \
  -d accumulate=false \
  -G
```

```json
{
  "code": 200,
  "data": [
    {
      "default": { ... },
      "open-cost": { ... },
      "kube-system": { ... }
    },
    {
      "default": { ... },
      "open-cost": { ... },
      "kube-system": { ... }
    },
    {
      "default": { ... },
      "open-cost": { ... },
      "kube-system": { ... }
    }
  ]
}
```

## Theoretical error bounds

Tuning the resolution parameter allows the querier to make tradeoffs between accuracy and performance. For long-running pods (>1d) resolution can be tuned aggressively low (>10m) with relatively little effect on accuracy. However, even modestly low resolutions (5m) can result in significant accuracy degradation for short-running pods (<1h).

Here, we provide theoretical error bounds for different resolution values given pods of differing running durations. The tuple represents lower- and upper-bounds for accuracy as a percentage of the actual value. For example:
- 1.00, 1.00 means that results should always be accurate to less than 0.5% error
- 0.83, 1.00 means that results should never be high by more than 0.5% error, but could be low by as much as 17% error
- -1.00, 10.00 means that the result could be as high as 1000% error (e.g. 30s pod being counted for 5m) or the pod could be missed altogether, i.e. -100% error.

| resolution | 30s pod | 5m pod | 1h pod | 1d pod | 7d pod |
|--:|:-:|:-:|:-:|:-:|:-:|
| 1m | -1.00, 2.00 |  0.80, 1.00 |  0.98, 1.00 | 1.00, 1.00 | 1.00, 1.00 |
| 2m | -1.00, 4.00 |  0.80, 1.20 |  0.97, 1.00 | 1.00, 1.00 | 1.00, 1.00 |
| 5m | -1.00, 10.00 | -1.00, 1.00 |  0.92, 1.00 | 1.00, 1.00 | 1.00, 1.00 |
| 10m | -1.00, 20.00 | -1.00, 2.00 |  0.83, 1.00 | 0.99, 1.00 | 1.00, 1.00 |
| 30m | -1.00, 60.00 | -1.00, 6.00 |  0.50, 1.00 | 0.98, 1.00 | 1.00, 1.00 |
| 60m | -1.00, 120.00 | -1.00, 12.00 | -1.00, 1.00 | 0.96, 1.00 | 0.99, 1.00 |