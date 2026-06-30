# Upstream Watchlist: Kubernetes & Cloud Provider Changes for FinOps

This document tracks changes in Kubernetes and the major cloud providers (AWS, Azure, GCP) that
create new opportunities — or new correctness requirements — for OpenCost and Kubecost. It exists
because cost-allocation accuracy depends on staying current with upstream primitives: a new
scheduling or billing primitive that we don't model shows up as a silent accuracy gap, not an error.

The canonical example that motivated this doc: [in-place Pod resource resize](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/)
graduated to GA in Kubernetes 1.35. Pods can now have their CPU/memory requests changed without a
restart. OpenCost's allocation engine currently treats a Pod's resource requests as fixed for its
whole lifetime (one request value per Pod UID) — that assumption is now wrong for any cluster using
resize, and right-sizing recommendations can no longer assume "apply this suggestion" means "restart
the Pod."

## How to use this doc

- Each entry is an **upstream change**, not a OpenCost/Kubecost feature spec. The "Implication" line
  is the seed of a future design doc / issue, not a commitment.
- Status reflects what's GA/stable vs. still in beta/preview as of June 2026 — re-verify before
  scoping work, since these move fast (several items below moved stages in the last two quarters).
- Items are roughly ordered by impact × urgency within each section.
- See [How to Influence the Roadmap](ROADMAP.md#how-to-influence-the-roadmap) to turn an item here
  into an actual roadmap commitment.

---

## Kubernetes upstream

### 1. In-place Pod resize — GA in 1.35
KEP-1287. Alpha in 1.27 → beta in 1.33 → **GA/stable (on by default) in 1.35** (Dec 2025). 1.34 lifted
the prior restriction on memory *limit* decreases (kubelet allows it on a best-effort basis if current
usage is below the new limit). 1.35 adds dedicated kubelet metrics and Pod events for resize
operations.
**Implication:** OpenCost's allocation model assumes one request/limit value per Pod for its entire
window. With resize GA and on-by-default, accurate allocation requires time-slicing requests within a
Pod's lifetime (request value as a function of time, not a constant), and the MCP server's right-sizing
recommendations should be able to express "apply in place" vs. "requires recreate." This is the
highest-leverage item on this list — it's a correctness gap, not just a missed feature.
Source: [Kubernetes 1.35 blog](https://kubernetes.io/blog/2025/12/19/kubernetes-v1-35-in-place-pod-resize-ga/), [KEP-1287](https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/1287-in-place-update-pod-resources)

### 2. VPA `InPlaceOrRecreate` mode
VPA 1.2+ added an `InPlaceOrRecreate` update mode (alpha at VPA 1.7.0) that uses in-place resize when
possible and only falls back to evict+recreate when it can't. Requires k8s 1.33+ with the
`InPlacePodVerticalScaling` gate.
**Implication:** This is the most likely path by which clusters actually adopt #1 at scale. Worth
validating that OpenCost's allocation correctly tracks VPA-driven in-place changes (vs. only catching
resize on Pod recreation, which is what most allocation pipelines implicitly assume today).
Source: [VPA docs](https://kubernetes.io/docs/concepts/workloads/autoscaling/vertical-pod-autoscale/), [autoscaler repo](https://github.com/kubernetes/autoscaler/blob/master/vertical-pod-autoscaler/docs/quickstart.md)

### 3. Dynamic Resource Allocation (DRA) — GA in 1.34
KEP-3063. **GA and enabled by default since Kubernetes 1.34** (Sept 2025), with continued feature
growth in 1.35/1.36. DRA replaces the old "extended resource count" model for GPUs/accelerators with
`ResourceClaim`/`ResourceSlice`/`DeviceClass` objects, including a new "consumable capacity" model
where multiple unrelated Pods can share fractional access to one physical device.
**Implication:** Today, GPU cost attribution in cost tools is generally coarse (whole-device or
extended-resource-count based). DRA gives per-claim, per-device visibility that could let OpenCost
allocate fractional GPU cost much more precisely than counting `nvidia.com/gpu: 1` on a Pod spec —
directly relevant given GPU/accelerator spend is now often the single largest line item on AI-heavy
clusters.
Source: [Kubernetes 1.34 DRA blog](https://kubernetes.io/blog/2025/09/01/kubernetes-v1-34-dra-updates/), [DRA concepts](https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/)

### 4. Karpenter consolidation/bin-packing performance work
Continued upstream investment through 2025–2026 in Karpenter scale-out/consolidation performance
(faster scheduling loops, concurrent disruption execution) — not a new capability so much as Karpenter
becoming the de-facto default autoscaler across AWS, and increasingly adapted to other clouds.
**Implication:** Faster, more aggressive consolidation means idle/unallocated cost changes more
frequently within a window. Worth checking that OpenCost's idle-cost calculation samples frequently
enough to not systematically under- or over-state idle cost on Karpenter-managed clusters relative to
static-node-pool clusters.
Source: [AWS EKS Auto Mode blog](https://aws.amazon.com/blogs/containers/faster-nodes-smarter-scaling-whats-new-inside-amazon-elastic-kubernetes-service-amazon-eks-auto-mode/)

---

## AWS

### 5. Split Cost Allocation Data (SCAD) for EKS — converging fast on OpenCost's own model
SCAD generates per-Pod (and per-ECS-task) usage records in CUR, with cost allocation tags
(`aws:eks:cluster-name`, `aws:eks:deployment`, `aws:eks:namespace`, `aws:eks:node`,
`aws:eks:workload-name`, `aws:eks:workload-type`). It's been extended rapidly:
- **Nov 2024**: added CloudWatch Container Insights / Amazon Managed Prometheus as an allocation
  source, so costs can be split by `max(request, actual usage)` — the same formula OpenCost uses.
- **Sept 2025 (GA)**: extended to GPU/accelerator resources (NVIDIA, AMD, Trainium, Inferentia) — direct
  overlap with GPU cost attribution.
- **Oct 30, 2025 (GA)**: imports up to 50 Kubernetes custom labels per Pod directly as CUR cost
  allocation tags (cost center, application, environment, etc.) — AWS's most direct replication yet of
  OpenCost/Kubecost's label-based allocation.
**Implication:** AWS is converging on OpenCost's own allocation math (request-vs-usage, label-based
splitting) as a free, native CUR feature. The remaining gap is entirely in the product layer, not the
math: SCAD has **no UI** (CUR/Athena/QuickSight only), no real-time view, and no optimization
recommendations. Two concrete moves: (a) use SCAD as an independent ground-truth cross-check for
OpenCost's own AWS allocation output; (b) lean into the product gap — real-time visibility, UX, and
actionable right-sizing recommendations are where OpenCost/Kubecost still clearly win, and that
framing should inform where engineering effort goes vs. duplicating cost-splitting math AWS now does
for free.
Source: [AWS split cost allocation data docs](https://docs.aws.amazon.com/cur/latest/userguide/split-cost-allocation-data.html), [EKS GPU/accelerator SCAD](https://aws.amazon.com/about-aws/whats-new/2025/09/split-cost-allocation-data-amazon-eks-nvidia-amd-gpu-trainium-inferentia-ec2/), [EKS custom labels SCAD](https://aws.amazon.com/about-aws/whats-new/2025/10/split-cost-allocation-data-amazon-eks-kubernetes-labels/)

### 6. EKS Container Network Observability (Network Flow Monitor) — pod-level cross-AZ/NAT cost visibility
**GA Nov 19, 2025.** Adds an eBPF-based Network Flow Monitor add-on to EKS: a Service Map of
inter-workload traffic, a "top talkers" Flow Table (by AWS service / cluster / external), and
Kubernetes-enriched, Prometheus-scrapable flow metrics (ingress/egress counts, bytes transferred) that
can be exported to Amazon Managed Prometheus/Grafana. AWS's companion blog post is framed explicitly
around identifying and reducing inter-AZ and NAT gateway traffic costs.
**Implication:** This is the most directly actionable item in this whole document. Cross-AZ data
transfer and NAT gateway cost are a long-standing blind spot for OpenCost/Kubecost — today that cost
is only visible at aggregate VPC Flow Log granularity, not attributed to individual Pods. Network Flow
Monitor's Prometheus-exported, pod-attributed flow data is ingestible by OpenCost's existing
Prometheus-based collection model with no new integration pattern needed. **Recommend a concrete
engineering spike**: evaluate scraping Network Flow Monitor's metrics as a new OpenCost network-cost
data source for EKS clusters specifically.
Source: [EKS Container Network Observability launch](https://aws.amazon.com/about-aws/whats-new/2025/11/amazon-eks-enhanced-container-network-observability/), [AWS blog: track inter-AZ/NAT traffic with EKS Container Network Observability](https://aws.amazon.com/blogs/containers/track-inter-az-and-nat-gateway-traffic-with-eks-container-network-observability/)

### 7. CUR 2.0 native FOCUS export
AWS CUR 2.0 can output **FOCUS 1.2**-formatted Parquet files directly to S3 — AWS was first of the big
three to ship this, and publishes a conformance report (11 spec gaps closed, 8 still open as of this
writing).
**Implication:** See the cross-cutting FOCUS section below — this is the AWS half of a cross-cloud
opportunity.
Source: [FOCUS v1.2 spec](https://focus.finops.org/focus-specification/v1-2/)

### 8. EKS Auto Mode: performance gains + new capacity-reservation awareness
39% faster node boot, 43% faster scale-out, up to 69% faster consolidation with ~30% more usable
cluster capacity from 2025–2026 Karpenter optimizations under EKS Auto Mode. Separately, re:Invent 2025
added a `capacityReservationSelectorTerms` field on NodeClass and a `reserved` Capacity Type on
NodePool, letting Auto Mode target On-Demand Capacity Reservations, ML Capacity Blocks, and a new
"static capacity" mode that pins a fixed instance count.
**Implication:** Same idle-cost sampling-frequency concern as #4, specific to EKS Auto Mode customers.
The capacity-reservation piece is the more urgent one: a cost tool that doesn't recognize
reservation-backed nodes risks double-counting (treating pre-purchased/sunk capacity as new on-demand
spend) or generating misleading "switch to reserved capacity" recommendations for nodes that are
already on reserved capacity.
Source: [AWS EKS Auto Mode "what's new" blog](https://aws.amazon.com/blogs/containers/faster-nodes-smarter-scaling-whats-new-inside-amazon-elastic-kubernetes-service-amazon-eks-auto-mode/), [EKS Auto Mode ODCR docs](https://docs.aws.amazon.com/eks/latest/userguide/auto-odcr.html)

### 9. AWS Compute Optimizer: moving from recommend to auto-remediate (EKS still excluded)
**Nov 21, 2025 (GA)**: Compute Optimizer added **Automation Rules** — scheduled, tag-filtered
auto-remediation (e.g., delete EBS volumes unattached 32+ days, gp2→gp3 upgrades) with a rollback
dashboard. Lookback windows and idle-detection coverage have also expanded (32-day lookback option for
EBS/ECS rightsizing; idle detection added for DynamoDB, ElastiCache, MemoryDB, DocumentDB, WorkSpaces,
SageMaker endpoints). Notably, **EKS Pod/container-level rightsizing is still not covered** — Compute
Optimizer's container support remains ECS-on-Fargate only.
**Implication:** Two-sided signal. AWS pushing from recommendation-only to auto-remediation raises the
bar on Kubecost's recommendation-only posture for the resources Compute Optimizer *does* cover — but
EKS Pod-level rightsizing remains explicitly open ground, and is exactly where OpenCost/Kubecost's MCP
right-sizing recommendations already operate. Worth tracking whether AWS extends Compute Optimizer to
EKS Pods, since that would be a direct collision rather than an adjacent feature.
Source: [Compute Optimizer Automation Rules](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-compute-optimizer-automation-rules/)

### 10. New pricing/commitment data to ingest: Graviton5 and Database Savings Plans
**AWS Graviton5** (EC2 M9g/M9gd) entered preview at re:Invent 2025 (Dec 2025) and GA by mid-2026 — new
SKUs OpenCost's AWS Price List ingestion needs to pick up, and another entry in the
Graviton-migration-savings recommendation story. Separately, **Database Savings Plans** (Dec 2025, GA;
expanded to OpenSearch/Neptune Analytics Mar 2026) is a new commitment type covering
Aurora/RDS/DynamoDB/ElastiCache/DocumentDB/Neptune/Keyspaces/OpenSearch, alongside **Savings Plans/RI
Group Sharing** reaching GA (Nov 2025) for granular org-level discount-sharing control.
**Implication:** Pricing-source maintenance item (new SKUs) plus a correctness item — if OpenCost ever
amortizes Savings Plans/RI coverage against AWS spend, Group Sharing changes which accounts a discount
can legitimately be attributed to, and Database Savings Plans is a new coverage/utilization API shape
to handle.
Source: [Graviton5 M9g preview](https://aws.amazon.com/about-aws/whats-new/2025/12/ec2-m9g-instances-graviton5-processors-preview/), [Database Savings Plans](https://aws.amazon.com/about-aws/whats-new/2025/12/database-savings-plans-savings/), [RISP Group Sharing GA](https://aws.amazon.com/about-aws/whats-new/2025/11/savings-plans-reserved-instances-group-sharing-generally-available/)

---

## Azure

### 11. AKS Cost Analysis add-on is built on OpenCost
Confirmed and worth stating plainly for the roadmap: Azure's native AKS cost view (in Azure Portal Cost
Management, Standard/Premium tier only) **runs OpenCost under the hood**, reconciled against Azure
invoice data.
**Implication:** This isn't a competitive threat to plan around defensively — it's a distribution
channel. Every AKS customer using this feature is already running OpenCost; the roadmap question is
how upstream OpenCost can better serve that path (e.g., does our release cadence/feature set track
what Azure ships in the add-on, are there AKS-specific quirks we should test against in CI). Worth a
direct conversation with the Azure AKS team via the OpenCost working group.
Source: [Microsoft Learn: AKS cost analysis](https://learn.microsoft.com/en-us/azure/aks/cost-analysis)

### 12. Azure Cost Management exports overhaul — FOCUS GA, Parquet, Fabric destination
**Enhanced Cost Management exports went GA April 28, 2025**, bundling several changes at once:
- **FOCUS-format export GA** (FOCUS 1.0/1.0r2; a **1.2-preview** schema with 105 columns exists but had
  not reached GA as of mid-2026), combining actual + amortized cost in one file.
- New Price Sheet, Reservation Recommendations/Details/Transactions export types.
- Parquet+Snappy output (alongside existing CSV+Gzip), mandatory file partitioning for large exports,
  and a `dataOverwriteBehavior` option that collapses daily exports into one continuously-overwritten
  file instead of 30 files/month.
- A newer (still **preview-only** as of mid-2026) Exports API revision adds **Microsoft Fabric/OneLake
  Lakehouse** as an export destination, alongside the existing Blob Storage destination.
- Note: FOCUS export does not support management-group scope or legacy MOSP/EA billing scopes.
**Implication:** Second half of the cross-cloud FOCUS opportunity (see cross-cutting section below).
The Fabric/OneLake destination is also worth tracking separately — if it reaches GA, it changes the
"how would a customer pipe Azure billing data into a data warehouse for OpenCost/Kubecost to ingest"
answer for Fabric-centric shops.
Source: [Azure FOCUS export schema](https://learn.microsoft.com/en-us/azure/cost-management-billing/dataset-schema/cost-usage-details-focus), [Cost Management exports tutorial](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-improved-exports)

### 13. Legacy Azure Consumption/EA billing APIs being retired — check for any lingering dependency
Microsoft has retired the old **EA Reporting APIs** (`consumption.azure.com`, API-key auth) outright,
and has the **Consumption Usage Details, Marketplaces, and Forecasts APIs** marked deprecated (no firm
shutoff date set, but explicitly "do not build new pipelines on these"). Separately, the standalone
**"Connector for AWS"** multi-cloud view inside Azure Cost Management was retired March 31, 2025, with
Microsoft's stated replacement path being FOCUS-format exports. Replacement throughout is the modern
Cost Management Exports API / Cost Details API (Entra ID auth) described in #12.
**Implication:** Lower urgency than other items — a `grep` of `pkg/cloud/azure/` found no current
OpenCost dependency on these legacy `consumption.azure.com` endpoints, so this is a "stay off this
path" note rather than an active migration. Worth keeping on the watchlist only because any
community-contributed Azure ingestion code or docs that reference these endpoints should be flagged
for removal before they bit-rot into broken instructions.
Source: [Migrate from EA Reporting APIs](https://learn.microsoft.com/en-us/azure/cost-management-billing/automate/migrate-ea-usage-details-api), [Migrate from Consumption Usage Details API](https://learn.microsoft.com/en-us/azure/cost-management-billing/automate/migrate-consumption-usage-details-api), [Automation FAQ](https://learn.microsoft.com/en-us/azure/cost-management-billing/automate/automation-faq)

### 14. AKS Node Auto-Provisioning (Karpenter-on-Azure)
Azure's adaptation of Karpenter for AKS. Exact GA timeline for the 2025–2026 window needs direct
confirmation (Microsoft Learn docs), but directionally this brings the same bin-packing/idle-cost
sampling consideration from #4 to AKS clusters.
**Implication:** Track GA date; same idle-cost sampling concern applies once broadly adopted.
Source: needs verification — check `learn.microsoft.com/azure/aks/node-autoprovision` for current status.

---

## GCP

### 15. GKE cost allocation export to BigQuery — `kube:system-overhead` / `kube:unallocated`
GKE's built-in cost allocation feature adds cluster/namespace labels to the BigQuery billing export,
plus two synthetic namespaces: `kube:system-overhead` (node resources unavailable to Pods) and
`kube:unallocated` (resources neither requested by workloads nor reserved for overhead). Backfill is
not supported — data only starts from when the feature is enabled.
**Implication:** Those two synthetic namespace concepts map closely to OpenCost's own idle/overhead
accounting. Worth a direct comparison of GKE's idle/overhead methodology vs. OpenCost's to find
discrepancies — those discrepancies are exactly what GCP customers will ask about when running both
side-by-side (which AKS customers already implicitly do, per #11).
Source: [GKE cost allocation docs](https://cloud.google.com/kubernetes-engine/docs/how-to/cost-allocations)

### 16. GCP FOCUS export now in Preview (separate from the older FOCUS view)
GCP shipped a native **FOCUS export table** (`gcp_billing_export_focus_<BILLING_ACCOUNT_ID>`) in
**Preview** — an immutable, Google-hosted table (no storage cost, 2-year retention), distinct from the
older FOCUS-format BigQuery *view* that reached GA back in mid-2024. Google is also overhauling the
billing export schema around spend-based Committed Use Discounts (effective Jan 21, 2026), adding new
pricing/commitment columns.
**Implication:** This walks back the "GCP lags on FOCUS" framing somewhat — a native export table is
now in flight, just not GA yet. Worth tracking the Preview→GA transition as the trigger for treating
GCP as a true FOCUS-conformant third leg alongside AWS/Azure (see cross-cutting section). The CUD
schema overhaul is a more immediate item: any GCP billing ingestion code that assumes one row per CUD
SKU needs to handle the new two-row-per-SKU shape after Jan 21, 2026.
Source: GKE/BigQuery billing export schema docs (`cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/focus-export`, `cloud.google.com/docs/cuds-multiprice-datamodel`) — direct fetch was blocked in this research session; re-verify against live docs before treating dates as final.

### 17. GKE Custom Compute Classes
Lets platform teams define a prioritized list of compute properties (machine family, Spot vs.
on-demand, reservations) for GKE's autoscaler to pick from. Status (preview vs. GA) as of mid-2026
needs direct confirmation against GKE release notes.
**Implication:** Same as Karpenter/NAP — changes the "what would this Pod have run on" assumption
behind cost modeling once adopted. Lower urgency until GA status and adoption are confirmed.
Source: needs verification — check `cloud.google.com/kubernetes-engine/docs/release-notes`.

---

## Cross-cutting: FOCUS as a unifying ingestion format

[FOCUS](https://focus.finops.org/) (FinOps Open Cost and Usage Specification) is the FinOps
Foundation's vendor-neutral billing schema. **v1.3 ratified December 2025**, adding a Contract
Commitment dataset, shared-cost-splitting columns, and data-recency/completeness flags.

Conformance status as of mid-2026:
- **AWS**: native FOCUS 1.2 export via CUR 2.0, conformance report published, 8 known gaps remain.
- **Azure**: native FOCUS export (1.0/1.0r2 GA, 1.2-preview in flight) to Blob Storage, conformance
  report published.
- **GCP**: a native export table is now in Preview (#16), not yet GA — closing the gap with AWS/Azure
  but not there yet.

**Implication:** Once GCP's FOCUS export reaches GA, a shared FOCUS-format ingestion layer could
replace some proportion of the bespoke per-cloud billing parsers in `pkg/cloud/{aws,azure,gcp}`,
reducing maintenance surface and making it easier to onboard new clouds (Alibaba, Oracle, OTC,
Scaleway) that also adopt FOCUS. This is a multi-quarter architecture bet, not a quick win — track
GCP's FOCUS export GA as the trigger to scope it seriously.

---

## Suggested process for keeping this current

This list is a snapshot; the underlying ask was for a *daily* watch, which a static doc can't provide
on its own. Recommended split:
1. **This doc** stays the curated, human-reviewed roadmap artifact — updated when something here
   actually changes status (alpha→beta→GA) or a new item clears the bar for inclusion.
2. **A scheduled research routine** (separate from this doc) does the daily scanning — k8s KEP tracker,
   AWS/Azure/GCP "what's new" feeds, FOCUS spec changes — and surfaces only items that look
   roadmap-worthy, rather than every changelog line. Noise control matters more than coverage here;
   a daily feed that flags ten irrelevant SKU changes a week will get ignored.
3. Items that survive a few research cycles get promoted into this doc via PR, then discussed in the
   biweekly [OpenCost Working Group](ROADMAP.md#how-to-influence-the-roadmap) meeting before becoming
   an actual roadmap commitment.
