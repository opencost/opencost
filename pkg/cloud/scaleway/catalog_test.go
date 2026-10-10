package scaleway

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/carbon"
	productcatalog "github.com/scaleway/scaleway-sdk-go/api/product_catalog/v2alpha1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

// closeEnough compares two floats with the same tolerance used for conflict
// detection in the store (1e-12).
func closeEnough(a, b float64) bool {
	return math.Abs(a-b) < 1e-12
}

func boolPtr(b bool) *bool { return &b }

func zonePtr(z scw.Zone) *scw.Zone {
	tmp := z
	return &tmp
}

func regionPtr(r scw.Region) *scw.Region {
	tmp := r
	return &tmp
}

// money builds an scw.Money from a float amount in the given currency.
func money(currency string, amount float64) *scw.Money {
	units := int64(amount)
	nanos := int32((amount - float64(units)) * 1e9)
	return &scw.Money{CurrencyCode: currency, Units: units, Nanos: nanos}
}

// productFixture returns a GA instance product in zone fr-par-1 at 0.039449
// EUR/hour (the 2026-09-24 live reference value for BASIC3-X2C-4G). mutate
// overrides fields for each test case.
func productFixture(mutate func(p *productcatalog.PublicCatalogProduct)) *productcatalog.PublicCatalogProduct {
	p := &productcatalog.PublicCatalogProduct{
		Sku:     "/instance/server/basic3-x2c-4g/fr-par-1",
		Product: "Instance",
		Variant: "Basic 3 4G",
		Status:  productcatalog.PublicCatalogProductStatusGeneralAvailability,
		Locality: &productcatalog.PublicCatalogProductLocality{
			Zone: zonePtr(scw.Zone("fr-par-1")),
		},
		Price: &productcatalog.PublicCatalogProductPrice{
			RetailPrice: money("EUR", 0.039449),
		},
		UnitOfMeasure: &productcatalog.PublicCatalogProductUnitOfMeasure{
			Unit: productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("hour"),
			Size: 1,
		},
		Properties: &productcatalog.PublicCatalogProductProperties{
			Instance: &productcatalog.PublicCatalogProductPropertiesInstance{
				OfferID: "BASIC3-X2C-4G",
			},
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

// impact sets environmental impact on the product.
func impact(p *productcatalog.PublicCatalogProduct, kgCo2e, m3Water float32) {
	p.EnvironmentalImpactEstimation = &productcatalog.PublicCatalogProductEnvironmentalImpactEstimation{
		KgCo2Equivalent: &kgCo2e,
		M3WaterUsage:    &m3Water,
	}
}

func TestParseCatalogProduct(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(p *productcatalog.PublicCatalogProduct)
		wantErr string
		check   func(t *testing.T, prod *catalogProduct)
	}{
		{
			name: "instance GA (reference price)",
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "instance" || prod.OfferID != "BASIC3-X2C-4G" {
					t.Fatalf("unexpected product: %+v", prod)
				}
				if !closeEnough(prod.Price, 0.039449) {
					t.Fatalf("price = %v, want 0.039449", prod.Price)
				}
				if prod.Locality != localityZone || prod.LocalityValue != "fr-par-1" {
					t.Fatalf("locality = (%v, %q), want (zone, fr-par-1)", prod.Locality, prod.LocalityValue)
				}
				if prod.CurrencyCode != "EUR" || prod.Unit != "hour" || prod.UnitSize != 1 {
					t.Fatalf("unit/currency = (%q, %q, %d)", prod.CurrencyCode, prod.Unit, prod.UnitSize)
				}
			},
		},
		{
			name: "money conversion units + nanos",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Price.RetailPrice = &scw.Money{CurrencyCode: "EUR", Units: 1, Nanos: 500000000}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if !closeEnough(prod.Price, 1.5) {
					t.Fatalf("price = %v, want 1.5", prod.Price)
				}
			},
		},
		{
			name: "volume bssd",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/storage/block/volume-bssd/fr-par-1"
				p.Price.RetailPrice = money("EUR", 0.000129999)
				p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("gigabyte")
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
						Volume: &productcatalog.PublicCatalogProductPropertiesBlockStorageVolumeType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "block_storage" || prod.VolumeSKUName != "volume-bssd" {
					t.Fatalf("unexpected product: %+v", prod)
				}
				if !closeEnough(prod.Price, 0.000129999) {
					t.Fatalf("price = %v, want 0.000129999", prod.Price)
				}
			},
		},
		{
			name: "volume ll-15k",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/storage/block/volume-low-latency-15k/fr-par-1"
				p.Price.RetailPrice = money("EUR", 0.000177)
				p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("gigabyte")
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
						Volume: &productcatalog.PublicCatalogProductPropertiesBlockStorageVolumeType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.VolumeSKUName != "volume-low-latency-15k" || !closeEnough(prod.Price, 0.000177) {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "snapshot parsed (dropped at store level)",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/storage/block/snapshot/fr-par-1"
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
						Snapshot: &productcatalog.PublicCatalogProductPropertiesBlockStorageSnapshotType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "block_storage" || prod.VolumeSKUName != "snapshot" {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "LB node",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/lb/node/lb-s/fr-par-1"
				p.Price.RetailPrice = money("EUR", 0.023)
				p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("node")
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					LoadBalancer: &productcatalog.PublicCatalogProductPropertiesLoadBalancer{
						Node: &productcatalog.PublicCatalogProductPropertiesLoadBalancerNodeType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "load_balancer" || !prod.LBIsNode || !closeEnough(prod.Price, 0.023) {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "LB ipv4",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/lb/ip/v1/fr-par-1"
				p.Price.RetailPrice = money("EUR", 0.005)
				p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("ip")
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					LoadBalancer: &productcatalog.PublicCatalogProductPropertiesLoadBalancer{
						IPv4: &productcatalog.PublicCatalogProductPropertiesLoadBalancerIPV4Type{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "load_balancer" || prod.LBIsNode {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "kapsule mutualized (zero price is valid)",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/k8s/control-plane/kapsule/fr-par"
				p.Locality = &productcatalog.PublicCatalogProductLocality{
					Region: regionPtr(scw.Region("fr-par")),
				}
				p.Price.RetailPrice = money("EUR", 0)
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
						KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "kubernetes" || prod.K8sTier != "kapsule" || prod.Price != 0 {
					t.Fatalf("unexpected product: %+v", prod)
				}
				if prod.Locality != localityRegion || prod.LocalityValue != "fr-par" {
					t.Fatalf("locality = (%v, %q)", prod.Locality, prod.LocalityValue)
				}
			},
		},
		{
			name: "kapsule dedicated-4",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Sku = "/k8s/control-plane/kapsule-dedicated-4/fr-par"
				p.Locality = &productcatalog.PublicCatalogProductLocality{
					Region: regionPtr(scw.Region("fr-par")),
				}
				p.Price.RetailPrice = money("EUR", 0.11)
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
						KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.K8sTier != "kapsule-dedicated-4" || !closeEnough(prod.Price, 0.11) {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "kapsule mutualized without tier segment (live SKU shape)",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				// The live catalog omits the tier segment for the mutualized
				// control plane: /k8s/control-plane/<region> (verified 2026-09-24).
				p.Sku = "/k8s/control-plane/fr-par"
				p.Locality = &productcatalog.PublicCatalogProductLocality{
					Region: regionPtr(scw.Region("fr-par")),
				}
				p.Price.RetailPrice = money("EUR", 0)
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
						KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "kubernetes" || prod.K8sTier != "kapsule" || prod.Price != 0 {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "multicloud SKU keeps empty tier",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				// /k8s/multicloud/... products are outside the control-plane
				// path: tier must stay empty so add() records but does not store.
				p.Sku = "/k8s/multicloud/control-plane/fr-par"
				p.Locality = &productcatalog.PublicCatalogProductLocality{
					Region: regionPtr(scw.Region("fr-par")),
				}
				p.Price.RetailPrice = money("EUR", 0.1444)
				p.Properties = &productcatalog.PublicCatalogProductProperties{
					Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
						KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
					},
				}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.ProductType != "kubernetes" || prod.K8sTier != "" {
					t.Fatalf("unexpected product: %+v", prod)
				}
			},
		},
		{
			name: "global locality",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Locality = &productcatalog.PublicCatalogProductLocality{Global: boolPtr(true)}
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.Locality != localityGlobal || prod.LocalityValue != "global" {
					t.Fatalf("locality = (%v, %q)", prod.Locality, prod.LocalityValue)
				}
			},
		},
		{
			name: "impact data parsed",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				impact(p, 0.0005840293, 0.0000061068)
			},
			check: func(t *testing.T, prod *catalogProduct) {
				// Impact values are float32 in the API; compare against the
				// round-tripped value.
				if !closeEnough(prod.KgCo2ePerUnit, float64(float32(0.0005840293))) || !closeEnough(prod.M3WaterPerUnit, float64(float32(0.0000061068))) {
					t.Fatalf("impact = (%v, %v)", prod.KgCo2ePerUnit, prod.M3WaterPerUnit)
				}
			},
		},
		{
			name: "retired dropped",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Status = productcatalog.PublicCatalogProductStatusRetired
			},
			wantErr: "non_ga_status",
		},
		{
			name: "end of life dropped",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Status = productcatalog.PublicCatalogProductStatusEndOfLife
			},
			wantErr: "non_ga_status",
		},
		{
			name: "public beta dropped",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Status = productcatalog.PublicCatalogProductStatusPublicBeta
			},
			wantErr: "non_ga_status",
		},
		{
			name: "end of deployment dropped",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Status = productcatalog.PublicCatalogProductStatusEndOfDeployment
			},
			wantErr: "non_ga_status",
		},
		{
			name: "end of growth kept (still sold)",
			mutate: func(p *productcatalog.PublicCatalogProduct) {
				p.Status = productcatalog.PublicCatalogProductStatusEndOfGrowth
			},
			check: func(t *testing.T, prod *catalogProduct) {
				if prod.Status != "end_of_growth" {
					t.Fatalf("status = %q", prod.Status)
				}
			},
		},
		{
			name:    "missing price dropped",
			mutate:  func(p *productcatalog.PublicCatalogProduct) { p.Price = nil },
			wantErr: "missing_price",
		},
		{
			name:    "missing locality dropped",
			mutate:  func(p *productcatalog.PublicCatalogProduct) { p.Locality = nil },
			wantErr: "missing_locality",
		},
		{
			name:    "unknown properties dropped",
			mutate:  func(p *productcatalog.PublicCatalogProduct) { p.Properties = nil },
			wantErr: "unknown_type",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prod, err := parseCatalogProduct(productFixture(tc.mutate))
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tc.check(t, prod)
		})
	}
}

// buildTestStore builds a store from the 2026-09-24 reference fixture set.
func buildTestStore(t *testing.T) *catalogStore {
	t.Helper()
	products := []*productcatalog.PublicCatalogProduct{
		// instance GA with impact data (R11)
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			impact(p, 0.0005840293, 0.0000061068)
		}),
		// retired instance (R10: retired products are returned by the API)
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/instance/server/vc1l/fr-par-1"
			p.Status = productcatalog.PublicCatalogProductStatusRetired
			p.Properties.Instance.OfferID = "VC1L"
		}),
		// volumes
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/storage/block/volume-bssd/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.000129999)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("gigabyte")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
					Volume: &productcatalog.PublicCatalogProductPropertiesBlockStorageVolumeType{},
				},
			}
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/storage/block/volume-low-latency-15k/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.000177)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("gigabyte")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
					Volume: &productcatalog.PublicCatalogProductPropertiesBlockStorageVolumeType{},
				},
			}
		}),
		// unmapped volume class (e.g. lssd) falls back to legacy pricing
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/storage/block/volume-lssd/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.0001)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("gigabyte")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
					Volume: &productcatalog.PublicCatalogProductPropertiesBlockStorageVolumeType{},
				},
			}
		}),
		// snapshot: recorded in the count, never priced
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/storage/block/snapshot/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.00005)
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
					Snapshot: &productcatalog.PublicCatalogProductPropertiesBlockStorageSnapshotType{},
				},
			}
		}),
		// LB nodes: cheapest wins (R7); ipv4 excluded
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/lb/node/lb-s/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.023)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("node")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				LoadBalancer: &productcatalog.PublicCatalogProductPropertiesLoadBalancer{
					Node: &productcatalog.PublicCatalogProductPropertiesLoadBalancerNodeType{},
				},
			}
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/lb/node/lb-gp-m/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.054)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("node")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				LoadBalancer: &productcatalog.PublicCatalogProductPropertiesLoadBalancer{
					Node: &productcatalog.PublicCatalogProductPropertiesLoadBalancerNodeType{},
				},
			}
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/lb/ip/v1/fr-par-1"
			p.Price.RetailPrice = money("EUR", 0.005)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("ip")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				LoadBalancer: &productcatalog.PublicCatalogProductPropertiesLoadBalancer{
					IPv4: &productcatalog.PublicCatalogProductPropertiesLoadBalancerIPV4Type{},
				},
			}
		}),
		// Kapsule control planes (R8)
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/k8s/control-plane/kapsule/fr-par"
			p.Locality = &productcatalog.PublicCatalogProductLocality{
				Region: regionPtr(scw.Region("fr-par")),
			}
			p.Price.RetailPrice = money("EUR", 0)
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
					KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
				},
			}
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/k8s/control-plane/kapsule-dedicated-4/fr-par"
			p.Locality = &productcatalog.PublicCatalogProductLocality{
				Region: regionPtr(scw.Region("fr-par")),
			}
			p.Price.RetailPrice = money("EUR", 0.11)
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
					KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
				},
			}
		}),
		// currency mismatch (FR-010): recorded, price used as-is
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/instance/server/usd-type/fr-par-1"
			p.Price.RetailPrice = money("USD", 0.05)
			p.Properties.Instance.OfferID = "USD-TYPE"
		}),
	}
	return buildStore(products, "EUR")
}

func TestBuildStoreAllTypes(t *testing.T) {
	before := time.Now()
	store := buildTestStore(t)

	// Instances
	if price, ok := store.instancePrice("fr-par-1", "BASIC3-X2C-4G"); !ok || !closeEnough(price, 0.039449) {
		t.Errorf("instance BASIC3-X2C-4G @ fr-par-1 = (%v, %v), want (0.039449, true)", price, ok)
	}
	if _, ok := store.instancePrice("fr-par-1", "VC1L"); ok {
		t.Error("retired VC1L must not be in the store")
	}
	if price, ok := store.instancePrice("fr-par-1", "USD-TYPE"); !ok || !closeEnough(price, 0.05) {
		t.Errorf("currency-mismatch product must be used as-is: got (%v, %v)", price, ok)
	}

	// Volumes (per GB-hour)
	if price, ok := store.volumePrice("fr-par-1", "volume-bssd"); !ok || !closeEnough(price, 0.000129999) {
		t.Errorf("volume bssd = (%v, %v), want (0.000129999, true)", price, ok)
	}
	if price, ok := store.volumePrice("fr-par-1", "volume-ll-15k"); !ok || !closeEnough(price, 0.000177) {
		t.Errorf("volume ll-15k = (%v, %v), want (0.000177, true)", price, ok)
	}
	if _, ok := store.volumePrice("fr-par-1", "volume-lssd"); ok {
		t.Error("unmapped volume class must not be in the store (legacy fallback)")
	}
	if _, ok := store.volumePrice("fr-par-1", "snapshot"); ok {
		t.Error("snapshot must not be priced")
	}

	// Load balancer: cheapest node unit price per zone (R7)
	if price, ok := store.loadBalancerPrice("fr-par-1"); !ok || !closeEnough(price, 0.023) {
		t.Errorf("LB @ fr-par-1 = (%v, %v), want (0.023, true)", price, ok)
	}

	// Control planes (R8): mutualized 0 is a valid, present value
	if price, ok := store.controlPlanePrice("fr-par"); !ok || price != 0 {
		t.Errorf("Kapsule mutualized @ fr-par = (%v, %v), want (0, true)", price, ok)
	}
	if _, ok := store.controlPlanePrice("nl-ams"); ok {
		t.Error("Kapsule must not be in nl-ams")
	}
	if tier, ok := store.controlPlaneTiers["fr-par/kapsule-dedicated-4"]; !ok || !closeEnough(tier, 0.11) {
		t.Errorf("dedicated tier = (%v, %v), want (0.11, true)", tier, ok)
	}

	// Carbon (R11): kg/h retained in the store (float32 round-trip from the API)
	if coeff, ok := store.carbon[carbonKey{Region: "fr-par", OfferID: "BASIC3-X2C-4G"}]; !ok || !closeEnough(coeff, float64(float32(0.0005840293))) {
		t.Errorf("carbon = (%v, %v), want (float64(float32(0.0005840293)) kg/h, true)", coeff, ok)
	}

	// byType counts parsed (GA) products; the retired instance is dropped
	if got := store.byType["instance"]; got != 2 {
		t.Errorf("byType[instance] = %d, want 2", got)
	}
	if got := store.byType["block_storage"]; got != 4 {
		t.Errorf("byType[block_storage] = %d, want 4", got)
	}
	if got := store.byType["load_balancer"]; got != 3 {
		t.Errorf("byType[load_balancer] = %d, want 3", got)
	}
	if got := store.byType["kubernetes"]; got != 2 {
		t.Errorf("byType[kubernetes] = %d, want 2", got)
	}
	if !contains(store.zonesSorted(), "fr-par-1") {
		t.Errorf("zones = %v, want to contain fr-par-1", store.zonesSorted())
	}
	if !contains(store.regionsSorted(), "fr-par") {
		t.Errorf("regions = %v, want to contain fr-par", store.regionsSorted())
	}
	if got := store.dropped["non_ga_status"]; got != 1 {
		t.Errorf("dropped[non_ga_status] = %d, want 1", got)
	}
	if got := store.dropped["unmapped_volume_class"]; got != 1 {
		t.Errorf("dropped[unmapped_volume_class] = %d, want 1", got)
	}
	if got := store.dropped["currency_mismatch"]; got != 1 {
		t.Errorf("dropped[currency_mismatch] = %d, want 1", got)
	}
	if store.lastFetched.Before(before) || store.lastFetched.IsZero() {
		t.Errorf("lastFetched = %v, want >= %v", store.lastFetched, before)
	}

	// Summary contract shape (contracts/pricing-source-status.md §2)
	sum := store.summary()
	if sum.Source != ProductCatalogPricing {
		t.Errorf("summary.source = %q", sum.Source)
	}
	wantProducts := 0
	for _, n := range sum.ByType {
		wantProducts += n
	}
	if sum.Products != wantProducts {
		t.Errorf("summary.products = %d, want %d", sum.Products, wantProducts)
	}
	if _, err := time.Parse(time.RFC3339, sum.LastFetched); err != nil {
		t.Errorf("summary.lastFetched %q is not RFC3339: %v", sum.LastFetched, err)
	}
}

// TestBuildStoreLiveShapes covers the shapes observed in the live catalog on
// 2026-09-24, where the research-era fixture assumptions did not hold:
// the mutualized control-plane SKU omits the tier segment, /k8s/multicloud/
// SKUs exist and must be ignored, and volume-bssd carries end_of_growth
// (still sold) rather than general_availability status.
func TestBuildStoreLiveShapes(t *testing.T) {
	products := []*productcatalog.PublicCatalogProduct{
		// live mutualized SKU: no tier segment (R8 live correction)
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/k8s/control-plane/fr-par"
			p.Locality = &productcatalog.PublicCatalogProductLocality{
				Region: regionPtr(scw.Region("fr-par")),
			}
			p.Price.RetailPrice = money("EUR", 0)
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
					KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
				},
			}
		}),
		// live multicloud SKU: out of v1 scope, counted but not stored
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/k8s/multicloud/control-plane/fr-par"
			p.Locality = &productcatalog.PublicCatalogProductLocality{
				Region: regionPtr(scw.Region("fr-par")),
			}
			p.Price.RetailPrice = money("EUR", 0.1444)
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				Kubernetes: &productcatalog.PublicCatalogProductPropertiesKubernetes{
					KapsuleControlPlane: &productcatalog.PublicCatalogProductPropertiesKubernetesKapsuleControlPlaneType{},
				},
			}
		}),
		// live volume-bssd: end_of_growth status (still sold, R10)
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/storage/block/volume-bssd/fr-par-1"
			p.Status = productcatalog.PublicCatalogProductStatusEndOfGrowth
			p.Price.RetailPrice = money("EUR", 0.000129999)
			p.UnitOfMeasure.Unit = productcatalog.PublicCatalogProductUnitOfMeasureCountableUnit("gigabyte")
			p.Properties = &productcatalog.PublicCatalogProductProperties{
				BlockStorage: &productcatalog.PublicCatalogProductPropertiesBlockStorage{
					Volume: &productcatalog.PublicCatalogProductPropertiesBlockStorageVolumeType{},
				},
			}
		}),
	}
	store := buildStore(products, "EUR")

	if price, ok := store.controlPlanePrice("fr-par"); !ok || price != 0 {
		t.Errorf("live-shape mutualized @ fr-par = (%v, %v), want (0, true)", price, ok)
	}
	if price, ok := store.volumePrice("fr-par-1", "volume-bssd"); !ok || !closeEnough(price, 0.000129999) {
		t.Errorf("end_of_growth bssd @ fr-par-1 = (%v, %v), want (0.000129999, true)", price, ok)
	}
	if _, ok := store.controlPlaneTiers["fr-par/"]; ok {
		t.Error("multicloud SKU must not create an empty-tier entry")
	}
	// Both kubernetes products are parsed (and counted); only the
	// control-plane one is stored.
	if got := store.byType["kubernetes"]; got != 2 {
		t.Errorf("byType[kubernetes] = %d, want 2", got)
	}
	if got := store.dropped["non_ga_status"]; got != 0 {
		t.Errorf("dropped[non_ga_status] = %d, want 0 (end_of_growth is sellable)", got)
	}
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// TestConflictResolution verifies the locality cascade (FR-008) and the
// same-key tie-break (R14: deterministic sorted-SKU order + counted conflict).
func TestConflictResolution(t *testing.T) {
	offerID := "CONFLICT-TYPE"
	products := []*productcatalog.PublicCatalogProduct{
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/instance/server/conflict-a/fr-par-1" // sorts first
			p.Properties.Instance.OfferID = offerID
			p.Price.RetailPrice = money("EUR", 0.1)
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/instance/server/conflict-b/fr-par-1"
			p.Properties.Instance.OfferID = offerID
			p.Price.RetailPrice = money("EUR", 0.2) // same-key conflict
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/instance/server/conflict-c/fr-par"
			p.Locality = &productcatalog.PublicCatalogProductLocality{
				Region: regionPtr(scw.Region("fr-par")),
			}
			p.Properties.Instance.OfferID = offerID
			p.Price.RetailPrice = money("EUR", 0.2)
		}),
		productFixture(func(p *productcatalog.PublicCatalogProduct) {
			p.Sku = "/instance/server/conflict-d/global"
			p.Locality = &productcatalog.PublicCatalogProductLocality{Global: boolPtr(true)}
			p.Properties.Instance.OfferID = offerID
			p.Price.RetailPrice = money("EUR", 0.3)
		}),
	}
	store := buildStore(products, "EUR")

	// Zone-specific wins in fr-par-1 (first by SKU order wins the same-key conflict)
	if price, ok := store.instancePrice("fr-par-1", offerID); !ok || !closeEnough(price, 0.1) {
		t.Errorf("zone price = (%v, %v), want (0.1, true)", price, ok)
	}
	// Another zone of the same region cascades to the region entry
	if price, ok := store.instancePrice("fr-par-2", offerID); !ok || !closeEnough(price, 0.2) {
		t.Errorf("region cascade = (%v, %v), want (0.2, true)", price, ok)
	}
	// An unrelated region cascades to the global entry
	if price, ok := store.instancePrice("nl-ams-1", offerID); !ok || !closeEnough(price, 0.3) {
		t.Errorf("global cascade = (%v, %v), want (0.3, true)", price, ok)
	}
	// The same-key conflict was counted exactly once
	if got := store.dropped["conflicts"]; got != 1 {
		t.Errorf("dropped[conflicts] = %d, want 1", got)
	}

	// A type genuinely absent from the catalog misses
	if _, ok := store.instancePrice("fr-par-1", "MISSING"); ok {
		t.Error("absent offer must miss")
	}
}

func TestRegionFromZone(t *testing.T) {
	cases := map[string]string{
		"fr-par-1": "fr-par",
		"fr-par-3": "fr-par",
		"nl-ams-2": "nl-ams",
		"pl-waw-1": "pl-waw",
		"":         "",
	}
	for in, want := range cases {
		if got := regionFromZone(in); got != want {
			t.Errorf("regionFromZone(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCatalogCarbonCoefficients(t *testing.T) {
	store := newCatalogStore()
	store.carbon[carbonKey{Region: "fr-par", OfferID: "BASIC3-X2C-4G"}] = 0.0005840293 // kg/h
	coeffs := catalogCarbonCoefficients(store)
	want := 0.0005840293 / 1000.0 // t/h
	if got, ok := coeffs[carbon.NodeCoefficient{Region: "fr-par", InstanceType: "BASIC3-X2C-4G"}]; !ok || !closeEnough(got, want) {
		t.Fatalf("coeff = (%v, %v), want (%v, true)", got, ok, want)
	}
	if len(coeffs) != 1 {
		t.Fatalf("len(coeffs) = %d, want 1", len(coeffs))
	}

	// Empty store → empty (no registration happens upstream).
	if got := catalogCarbonCoefficients(newCatalogStore()); len(got) != 0 {
		t.Fatalf("empty store produced %d coefficients", len(got))
	}
}

// skuProducts builds products with the given SKUs for pagination fixtures.
func skuProducts(skus ...string) []*productcatalog.PublicCatalogProduct {
	out := make([]*productcatalog.PublicCatalogProduct, 0, len(skus))
	for _, sku := range skus {
		out = append(out, productFixture(func(p *productcatalog.PublicCatalogProduct) { p.Sku = sku }))
	}
	return out
}

// pageResponse builds a list response for the pagination fixtures.
func pageResponse(total uint64, products ...*productcatalog.PublicCatalogProduct) *productcatalog.ListPublicCatalogProductsResponse {
	return &productcatalog.ListPublicCatalogProductsResponse{
		TotalCount: total,
		Products:   products,
	}
}

func TestFetchAllProductsPagination(t *testing.T) {
	t.Run("completes across pages at TotalCount", func(t *testing.T) {
		var gotPages []int32
		products, err := fetchAllProducts(func(req *productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest) (*productcatalog.ListPublicCatalogProductsResponse, error) {
			gotPages = append(gotPages, *req.Page)
			switch *req.Page {
			case 1:
				return pageResponse(3, skuProducts("/a/1")[0], skuProducts("/a/2")[0]), nil
			case 2:
				return pageResponse(3, skuProducts("/a/3")[0]), nil
			}
			return nil, errors.New("unexpected page")
		}, 1000)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(products) != 3 {
			t.Fatalf("got %d products, want 3", len(products))
		}
		if len(gotPages) != 2 || gotPages[0] != 1 || gotPages[1] != 2 {
			t.Fatalf("pages requested = %v, want [1 2]", gotPages)
		}
	})

	t.Run("empty page before TotalCount is an error", func(t *testing.T) {
		_, err := fetchAllProducts(func(req *productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest) (*productcatalog.ListPublicCatalogProductsResponse, error) {
			if *req.Page == 1 {
				return pageResponse(3, skuProducts("/a/1")[0]), nil
			}
			return pageResponse(3), nil // empty page, only 1 of 3 products seen
		}, 1000)
		if err == nil || !strings.Contains(err.Error(), "empty page") {
			t.Fatalf("err = %v, want empty-page error", err)
		}
	})

	t.Run("repeated page is an error", func(t *testing.T) {
		_, err := fetchAllProducts(func(req *productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest) (*productcatalog.ListPublicCatalogProductsResponse, error) {
			if *req.Page == 1 {
				return pageResponse(4, skuProducts("/a/1")[0], skuProducts("/a/2")[0]), nil
			}
			// Page 2 starts with the SKU that ended page 1: repeated page.
			return pageResponse(4, skuProducts("/a/2")[0], skuProducts("/a/3")[0]), nil
		}, 1000)
		if err == nil || !strings.Contains(err.Error(), "repeated page") {
			t.Fatalf("err = %v, want repeated-page error", err)
		}
	})

	t.Run("lister error propagates", func(t *testing.T) {
		_, err := fetchAllProducts(func(req *productcatalog.PublicCatalogAPIListPublicCatalogProductsRequest) (*productcatalog.ListPublicCatalogProductsResponse, error) {
			return nil, errors.New("network down")
		}, 1000)
		if err == nil || !strings.Contains(err.Error(), "network down") {
			t.Fatalf("err = %v, want wrapped lister error", err)
		}
	})
}
