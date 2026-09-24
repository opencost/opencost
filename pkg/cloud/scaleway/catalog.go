package scaleway

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	productcatalog "github.com/scaleway/scaleway-sdk-go/api/product_catalog/v2alpha1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

// ProductCatalogPricing is the canonical pricing-source name for the Scaleway
// Product Catalog, as reported by PricingSourceStatus.
const ProductCatalogPricing = "Scaleway Product Catalog"

// catalogPageSize is the page size used when paginating the product catalog.
// A full in-scope retrieval is ~1,012 products, i.e. 2 pages (research R3).
const catalogPageSize = 1000

// catalogPageCap is a safety bound against anomalous pagination (FR-009).
const catalogPageCap = 100

// catalogProductTypes are the in-scope product types (spec Assumptions).
var catalogProductTypes = []productcatalog.ListPublicCatalogProductsRequestProductType{
	productcatalog.ListPublicCatalogProductsRequestProductTypeInstance,
	productcatalog.ListPublicCatalogProductsRequestProductTypeBlockStorage,
	productcatalog.ListPublicCatalogProductsRequestProductTypeLoadBalancer,
	productcatalog.ListPublicCatalogProductsRequestProductTypeKubernetes,
}

// localityKind ranks locality specificity for conflict resolution
// (most specific wins: zone > region > global — research R14).
type localityKind int

const (
	localityGlobal localityKind = iota
	localityRegion
	localityZone
	localityDatacenter
)

// catalogProduct is one row of the public catalog parsed into the fields the
// cost model uses (data-model.md §1).
type catalogProduct struct {
	SKU           string
	ProductType   string // "instance" | "block_storage" | "load_balancer" | "kubernetes"
	Name          string
	Variant       string
	Locality      localityKind
	LocalityValue string // zone or region string; "global" for global products
	Price         float64
	CurrencyCode  string
	Unit          string // "hour", "gigabyte", "node", "ip", ...
	UnitSize      uint64
	Status        string

	// product-specific identifiers
	OfferID       string // instance products: == k8s instance-type label (R5)
	VolumeSKUName string // block_storage products: SKU segment, e.g. "volume-bssd"
	LBIsNode      bool   // load_balancer products: true for node products, false for ipv4
	K8sTier       string // kubernetes products: SKU segment, e.g. "kapsule" (mutualized)

	// environmental impact, per product unit (0 = absent)
	KgCo2ePerUnit  float64
	M3WaterPerUnit float64
}

// instanceKey identifies a catalog instance price entry. Locality is the zone
// or region string, or "global".
type instanceKey struct {
	Locality string
	OfferID  string
}

// volumeKey identifies a catalog volume price entry.
type volumeKey struct {
	Locality     string
	StorageClass string
}

// carbonKey identifies a node carbon coefficient derived from the catalog.
type carbonKey struct {
	Region  string
	OfferID string
}

// catalogStore is the in-memory, catalog-backed pricing cache. It is built
// atomically from a full fetch and swapped in whole under the provider's
// RWMutex so in-flight readers always observe one complete set (FR-012).
type catalogStore struct {
	// locality-aware price sections; lookups cascade zone → region → global (FR-008)
	instances map[instanceKey]float64
	volumes   map[volumeKey]float64
	// locality string ("fr-par-1", "global") → price
	loadBalancers     map[string]float64
	controlPlanes     map[string]float64 // locality → kapsule mutualized €/hour
	controlPlaneTiers map[string]float64 // "locality/tier" → €/hour (recorded, not selected in v1)

	// node carbon coefficients (tonnes CO2e/hour), keyed by (region, offerID)
	carbon map[carbonKey]float64

	// summary fields (data-model.md §4)
	byType      map[string]int
	zones       map[string]struct{}
	regions     map[string]struct{}
	dropped     map[string]int
	lastFetched time.Time
}

func newCatalogStore() *catalogStore {
	return &catalogStore{
		instances:         map[instanceKey]float64{},
		volumes:           map[volumeKey]float64{},
		loadBalancers:     map[string]float64{},
		controlPlanes:     map[string]float64{},
		controlPlaneTiers: map[string]float64{},
		carbon:            map[carbonKey]float64{},
		byType:            map[string]int{},
		zones:             map[string]struct{}{},
		regions:           map[string]struct{}{},
		dropped:           map[string]int{},
	}
}

// zoneSuffixRe strips the trailing "-<digit>" zone suffix: "fr-par-1" → "fr-par".
var zoneSuffixRe = regexp.MustCompile(`-\d+$`)

// regionFromZone derives the region from a Scaleway zone label.
func regionFromZone(zone string) string {
	return zoneSuffixRe.ReplaceAllString(zone, "")
}

// catalogVolumeToStorageClass maps catalog block-storage SKU names to the
// Kubernetes storage class names OpenCost observes (research R6).
var catalogVolumeToStorageClass = map[string]string{
	"volume-bssd":            "volume-bssd",
	"volume-low-latency-5k":  "volume-ll-5k",
	"volume-low-latency-15k": "volume-ll-15k",
}

// listProductsFn is the product-catalog list endpoint signature used by
// fetchAllProducts. It is indirect so unit tests can inject a fake and test
// pagination behavior without network access (T007).
type listProductsFn func(req *productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest) (*productcatalog.ListPublicCatalogProductsResponse, error)

// fetchCatalog retrieves the full in-scope public catalog (all pages), parses
// it into a new store, and returns it. clusterCurrency is the cluster's
// configured currency (FR-010). It performs no mutation of provider state —
// the caller swaps the store in (FR-012, edge case 1).
func fetchCatalog(clusterCurrency string) (*catalogStore, error) {
	client, err := scw.NewClient(scw.WithoutAuth())
	if err != nil {
		return nil, fmt.Errorf("create unauthenticated client: %w", err)
	}
	api := productcatalog.NewPublicCatalogAPI(client)
	products, err := fetchAllProducts(func(req *productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest) (*productcatalog.ListPublicCatalogProductsResponse, error) {
		return api.ListPublicCatalogProducts(req)
	}, catalogPageSize)
	if err != nil {
		return nil, err
	}
	return buildStore(products, clusterCurrency), nil
}

// fetchAllProducts paginates the in-scope catalog until the accumulated
// product count reaches the first response's TotalCount (research R3, R12;
// FR-001, FR-009). A page that is empty or repeats the previous page before
// reaching TotalCount is a hard error — no infinite loop, no partial store.
func fetchAllProducts(list listProductsFn, pageSize uint32) ([]*productcatalog.PublicCatalogProduct, error) {
	var (
		products   []*productcatalog.PublicCatalogProduct
		totalCount uint64
		page       int32  = 1
		prevLast   string // last SKU of the previous page (anomaly detection, R12)
		haveTotal  bool
	)

	for {
		resp, err := list(&productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest{
			Page:         &page,
			PageSize:     &pageSize,
			ProductTypes: catalogProductTypes,
		})
		if err != nil {
			return nil, fmt.Errorf("page %d: %w", page, err)
		}
		if !haveTotal {
			totalCount = resp.TotalCount
			haveTotal = true
		}
		if len(resp.Products) == 0 {
			if !haveTotal || uint64(len(products)) < totalCount {
				return nil, fmt.Errorf("empty page %d before reaching total_count %d", page, totalCount)
			}
			break
		}
		// Anomalous pagination: the page repeats the previous page's last SKU.
		if prevLast != "" && resp.Products[0].Sku == prevLast {
			return nil, fmt.Errorf("repeated page detected at page %d (first SKU %s equals previous last SKU)", page, prevLast)
		}
		products = append(products, resp.Products...)
		prevLast = resp.Products[len(resp.Products)-1].Sku
		if uint64(len(products)) >= totalCount {
			break
		}
		page++
		if page > catalogPageCap {
			return nil, fmt.Errorf("pagination safety cap reached (%d pages) without reaching total_count %d", page, totalCount)
		}
	}

	return products, nil
}

// buildStore parses the fetched products into a complete store (data-model.md
// §1–§2). Products are processed in sorted SKU order so every tie-break is
// deterministic (R14). It returns a store even when all products are dropped;
// the caller decides whether it is usable.
func buildStore(products []*productcatalog.PublicCatalogProduct, clusterCurrency string) *catalogStore {
	store := newCatalogStore()
	store.lastFetched = time.Now()
	sort.Slice(products, func(i, j int) bool { return products[i].Sku < products[j].Sku })

	for _, p := range products {
		prod, err := parseCatalogProduct(p)
		if err != nil {
			store.dropped[err.Error()]++
			log.DedupedWarningf(10, "Scaleway catalog: dropping product %s: %s", p.Sku, err)
			continue
		}
		if prod.CurrencyCode != clusterCurrency {
			// FR-010: record the mismatch and use the price as-is (documented behavior).
			store.dropped["currency_mismatch"]++
			log.DedupedWarningf(10, "Scaleway catalog: product %s currency %s does not match cluster currency %s; using price as-is", prod.SKU, prod.CurrencyCode, clusterCurrency)
		}
		store.add(prod)
	}

	return store
}

// isSellableStatus reports whether a catalog product can still be purchased.
// end_of_new_features and end_of_growth products are no longer extended (new
// features/regions) but remain fully sold and priced — the live catalog
// marks volume-bssd volumes as end_of_growth (verified 2026-09-24), and the
// quickstart expects catalog pricing for them. R10 excludes non-availability
// states; end_of_deployment and later mean "must not be used for new
// deployments" (SDK doc) and are excluded, along with pre-release states.
func isSellableStatus(s productcatalog.PublicCatalogProductStatus) bool {
	switch s {
	case productcatalog.PublicCatalogProductStatusGeneralAvailability,
		productcatalog.PublicCatalogProductStatusEndOfNewFeatures,
		productcatalog.PublicCatalogProductStatusEndOfGrowth:
		return true
	default:
		return false
	}
}

// parseCatalogProduct converts a raw catalog product into the parsed model,
// applying the validation rules from data-model.md §1 and research R10.
func parseCatalogProduct(p *productcatalog.PublicCatalogProduct) (*catalogProduct, error) {
	if p.Price == nil || p.Price.RetailPrice == nil {
		return nil, fmt.Errorf("missing_price")
	}
	if p.Locality == nil {
		return nil, fmt.Errorf("missing_locality")
	}
	kind, value := parseLocality(p.Locality)
	if kind == localityGlobal && value == "" {
		return nil, fmt.Errorf("missing_locality")
	}
	if !isSellableStatus(p.Status) {
		// Products that are no longer available for new deployments
		// (end_of_deployment and later), pre-release, or retired are not used
		// for pricing (R10).
		return nil, fmt.Errorf("non_ga_status")
	}

	prod := &catalogProduct{
		SKU:           p.Sku,
		Name:          p.Product,
		Variant:       p.Variant,
		Locality:      kind,
		LocalityValue: value,
		Price:         p.Price.RetailPrice.ToFloat(),
		CurrencyCode:  p.Price.RetailPrice.CurrencyCode,
		Status:        string(p.Status),
	}
	if p.UnitOfMeasure != nil {
		prod.Unit = string(p.UnitOfMeasure.Unit)
		prod.UnitSize = p.UnitOfMeasure.Size
	}
	if p.EnvironmentalImpactEstimation != nil {
		if p.EnvironmentalImpactEstimation.KgCo2Equivalent != nil {
			prod.KgCo2ePerUnit = float64(*p.EnvironmentalImpactEstimation.KgCo2Equivalent)
		}
		if p.EnvironmentalImpactEstimation.M3WaterUsage != nil {
			prod.M3WaterPerUnit = float64(*p.EnvironmentalImpactEstimation.M3WaterUsage)
		}
	}

	props := p.Properties
	if props == nil {
		return nil, fmt.Errorf("unknown_type")
	}
	switch {
	case props.Instance != nil:
		prod.ProductType = "instance"
		prod.OfferID = props.Instance.OfferID
		if prod.OfferID == "" {
			return nil, fmt.Errorf("missing_offer_id")
		}
	case props.BlockStorage != nil:
		prod.ProductType = "block_storage"
		// SKU shape: /storage/block/<volume-name>/<locality>
		segments := strings.Split(p.Sku, "/")
		if len(segments) >= 4 && segments[1] == "storage" && segments[2] == "block" {
			prod.VolumeSKUName = segments[3]
		}
	case props.LoadBalancer != nil:
		prod.ProductType = "load_balancer"
		prod.LBIsNode = props.LoadBalancer.Node != nil
	case props.Kubernetes != nil:
		prod.ProductType = "kubernetes"
		// SKU shape: /k8s/control-plane/<tier>/<locality>. The mutualized
		// (default) tier omits the <tier> segment entirely — the locality
		// value appears where the tier would be (verified live 2026-09-24:
		// /k8s/control-plane/fr-par = 0 EUR/h). Products outside the
		// control-plane path (e.g. /k8s/multicloud/...) keep an empty tier
		// and are recorded but not stored (out of v1 scope).
		segments := strings.Split(p.Sku, "/")
		if len(segments) >= 4 && segments[1] == "k8s" && segments[2] == "control-plane" {
			if segments[3] == value {
				prod.K8sTier = "kapsule" // no tier segment: the mutualized tier
			} else {
				prod.K8sTier = segments[3]
			}
		}
	default:
		return nil, fmt.Errorf("unknown_type")
	}

	return prod, nil
}

// parseLocality extracts (kind, value) from the locality oneof.
func parseLocality(l *productcatalog.PublicCatalogProductLocality) (localityKind, string) {
	switch {
	case l.Zone != nil:
		return localityZone, l.Zone.String()
	case l.Region != nil:
		return localityRegion, l.Region.String()
	case l.Datacenter != nil:
		return localityDatacenter, *l.Datacenter
	case l.Global != nil && *l.Global:
		return localityGlobal, "global"
	default:
		return localityGlobal, ""
	}
}

// add inserts a parsed product into the store. The locality is part of every
// key, so zone/region/global products coexist; lookups cascade zone → region
// → global to apply FR-008 (most specific locality wins). Same-key duplicates
// are broken by deterministic stable order (products are pre-sorted by SKU)
// with a warning (R14).
func (s *catalogStore) add(p *catalogProduct) {
	s.byType[p.ProductType]++
	if p.Locality == localityZone {
		s.zones[p.LocalityValue] = struct{}{}
	} else if p.Locality == localityRegion {
		s.regions[p.LocalityValue] = struct{}{}
	}

	price := p.Price
	if p.UnitSize > 0 {
		price = p.Price / float64(p.UnitSize)
	}

	switch p.ProductType {
	case "instance":
		key := instanceKey{Locality: p.LocalityValue, OfferID: p.OfferID}
		s.instancesSet(key, price, p.SKU)
		// Carbon section: only for hourly-priced instances with impact data (R11).
		if p.KgCo2ePerUnit > 0 && p.Unit == "hour" {
			region := p.LocalityValue
			if p.Locality == localityZone {
				region = regionFromZone(region)
			}
			ck := carbonKey{Region: region, OfferID: p.OfferID}
			if prev, ok := s.carbon[ck]; ok && math.Abs(prev-p.KgCo2ePerUnit) > 1e-12 {
				log.DedupedWarningf(10, "Scaleway catalog: conflicting carbon impact for %s in region %s (%g vs %g kg/h); keeping %s", p.OfferID, region, prev, p.KgCo2ePerUnit, p.SKU)
			} else {
				s.carbon[ck] = p.KgCo2ePerUnit
			}
		}
	case "block_storage":
		if p.VolumeSKUName == "snapshot" {
			// Snapshots are not separately charged by the cost model (R6).
			return
		}
		storageClass, ok := catalogVolumeToStorageClass[p.VolumeSKUName]
		if !ok {
			s.dropped["unmapped_volume_class"]++
			return
		}
		key := volumeKey{Locality: p.LocalityValue, StorageClass: storageClass}
		s.volumesSet(key, price, p.SKU)
	case "load_balancer":
		if !p.LBIsNode {
			// LB IPv4 products are recorded in the count but not used for v1 pricing (R4/R7).
			return
		}
		// R7: the LB's node size is not detectable, so the cheapest node-unit
		// price per locality is the deterministic selection — collapsing the
		// tiers is the rule itself, not a same-key conflict.
		if prev, ok := s.loadBalancers[p.LocalityValue]; ok {
			if price < prev {
				s.loadBalancers[p.LocalityValue] = price
			}
		} else {
			s.loadBalancers[p.LocalityValue] = price
		}
	case "kubernetes":
		if p.K8sTier == "" {
			// Not a control-plane product (e.g. /k8s/multicloud/... SKUs):
			// counted in byType but not stored (out of v1 scope).
			return
		}
		tierKey := p.LocalityValue + "/" + p.K8sTier
		s.controlPlaneTiers[tierKey] = price
		if p.K8sTier == "kapsule" { // mutualized control plane (R8)
			if prev, ok := s.controlPlanes[p.LocalityValue]; ok && math.Abs(prev-price) > 1e-12 {
				s.dropped["conflicts"]++
				log.DedupedWarningf(10, "Scaleway catalog: control plane price conflict at %s (%s %g vs kept %g); keeping first by SKU order", p.LocalityValue, p.SKU, price, prev)
			} else {
				s.controlPlanes[p.LocalityValue] = price
			}
		}
	}
}

// instancesSet stores an instance price under key, applying the conflict rule:
// for same-locality duplicates keep the first by sorted SKU order (products are
// pre-sorted) and count the conflict.
func (s *catalogStore) instancesSet(key instanceKey, price float64, sku string) {
	if prev, ok := s.instances[key]; ok && math.Abs(prev-price) > 1e-12 {
		s.dropped["conflicts"]++
		log.DedupedWarningf(10, "Scaleway catalog: price conflict for instance %s in %s (%s %g vs kept %g); keeping first by SKU order", key.OfferID, key.Locality, sku, price, prev)
		return
	}
	s.instances[key] = price
}

// volumesSet stores a volume price under key, applying the conflict rule:
// for same-locality duplicates keep the first by sorted SKU order (products
// are pre-sorted) and count the conflict.
func (s *catalogStore) volumesSet(key volumeKey, price float64, sku string) {
	if prev, ok := s.volumes[key]; ok && math.Abs(prev-price) > 1e-12 {
		s.dropped["conflicts"]++
		log.DedupedWarningf(10, "Scaleway catalog: price conflict for volume class %s in %s (%s %g vs kept %g); keeping first by SKU order", key.StorageClass, key.Locality, sku, price, prev)
		return
	}
	s.volumes[key] = price
}

// instancePrice resolves a node price by cascading zone → region → global (FR-008).
func (s *catalogStore) instancePrice(zone, offerID string) (float64, bool) {
	if p, ok := s.instances[instanceKey{Locality: zone, OfferID: offerID}]; ok {
		return p, true
	}
	if region := regionFromZone(zone); region != zone {
		if p, ok := s.instances[instanceKey{Locality: region, OfferID: offerID}]; ok {
			return p, true
		}
	}
	if p, ok := s.instances[instanceKey{Locality: "global", OfferID: offerID}]; ok {
		return p, true
	}
	return 0, false
}

// volumePrice resolves a per-GB-hour volume price by cascading zone → region → global (FR-008).
func (s *catalogStore) volumePrice(zone, storageClass string) (float64, bool) {
	if p, ok := s.volumes[volumeKey{Locality: zone, StorageClass: storageClass}]; ok {
		return p, true
	}
	if region := regionFromZone(zone); region != zone {
		if p, ok := s.volumes[volumeKey{Locality: region, StorageClass: storageClass}]; ok {
			return p, true
		}
	}
	if p, ok := s.volumes[volumeKey{Locality: "global", StorageClass: storageClass}]; ok {
		return p, true
	}
	return 0, false
}

// loadBalancerPrice resolves the cheapest-LB-node price by cascading zone → region → global.
func (s *catalogStore) loadBalancerPrice(zone string) (float64, bool) {
	if p, ok := s.loadBalancers[zone]; ok {
		return p, true
	}
	if region := regionFromZone(zone); region != zone {
		if p, ok := s.loadBalancers[region]; ok {
			return p, true
		}
	}
	if p, ok := s.loadBalancers["global"]; ok {
		return p, true
	}
	return 0, false
}

// controlPlanePrice resolves the Kapsule mutualized control-plane price by
// cascading region → global.
func (s *catalogStore) controlPlanePrice(region string) (float64, bool) {
	if p, ok := s.controlPlanes[region]; ok {
		return p, true
	}
	if p, ok := s.controlPlanes["global"]; ok {
		return p, true
	}
	return 0, false
}

// zoneSorted returns the covered zones in stable order (for summaries/tests).
func (s *catalogStore) zonesSorted() []string {
	out := make([]string, 0, len(s.zones))
	for z := range s.zones {
		out = append(out, z)
	}
	sort.Strings(out)
	return out
}

func (s *catalogStore) regionsSorted() []string {
	out := make([]string, 0, len(s.regions))
	for r := range s.regions {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// catalogSummary is the structured catalog section of the pricing source
// summary (contracts/pricing-source-status.md §2).
type catalogSummary struct {
	Source      string         `json:"source"`
	Products    int            `json:"products"`
	ByType      map[string]int `json:"byType"`
	Zones       []string       `json:"zones"`
	Regions     []string       `json:"regions"`
	Dropped     map[string]int `json:"dropped"`
	LastFetched string         `json:"lastFetched"`
}

// summary renders the store as the additive catalog section of the pricing
// source summary.
func (s *catalogStore) summary() *catalogSummary {
	products := 0
	for _, n := range s.byType {
		products += n
	}
	return &catalogSummary{
		Source:      ProductCatalogPricing,
		Products:    products,
		ByType:      s.byType,
		Zones:       s.zonesSorted(),
		Regions:     s.regionsSorted(),
		Dropped:     s.dropped,
		LastFetched: s.lastFetched.UTC().Format(time.RFC3339),
	}
}

// formatPrice renders a price the same way the provider renders legacy prices.
func formatPrice(p float64) string {
	return strconv.FormatFloat(p, 'f', -1, 64)
}
