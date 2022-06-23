package cloud

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kubecost/opencost/pkg/clustercache"
	"github.com/kubecost/opencost/pkg/util"

	"github.com/kubecost/opencost/pkg/log"
	v1 "k8s.io/api/core/v1"

	"github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

type ScalewayPricing struct {
	NodesInfos map[string]*instance.ServerType
	PVCost     float64
}

type Scaleway struct {
	Clientset               clustercache.ClusterCache
	Config                  *ProviderConfig
	Pricing                 map[string]*ScalewayPricing
	DownloadPricingDataLock sync.RWMutex
}

func (c *Scaleway) DownloadPricingData() error {
	c.DownloadPricingDataLock.Lock()
	defer c.DownloadPricingDataLock.Unlock()

	// TODO wait for an official Pricing API from Scaleway
	// Let's use a static map and an old API

	if len(c.Pricing) != 0 {
		// Already initialized
		return nil
	}

	// PV pricing per AZ
	pvPrice := map[string]float64{
		"fr-par-1": 0.00011,
		"fr-par-2": 0.00011,
		"fr-par-3": 0.00032,
		"nl-ams-1": 0.00008,
		"nl-ams-2": 0.00008,
		"pl-waw-1": 0.00011,
	}

	c.Pricing = make(map[string]*ScalewayPricing)

	client, err := scw.NewClient(scw.WithAuth("SCWXXXXXXXXXXXXXXXXX", "00000000-0000-0000-0000-000000000000"), scw.WithDefaultProjectID("00000000-0000-0000-0000-000000000000"))
	if err != nil {
		return err
	}

	instanceAPI := instance.NewAPI(client)

	for _, zone := range scw.AllZones {
		resp, err := instanceAPI.ListServersTypes(&instance.ListServersTypesRequest{Zone: zone})
		if err != nil {
			log.Errorf("Could not get Scaleway pricing data from instance API in zone %s: %+v", zone, err)
			continue
		}
		c.Pricing[zone.String()] = &ScalewayPricing{
			PVCost:     pvPrice[zone.String()],
			NodesInfos: map[string]*instance.ServerType{},
		}

		for name, infos := range resp.Servers {
			c.Pricing[zone.String()].NodesInfos[name] = infos
		}
	}

	return nil
}

type scalewayKey struct {
	Labels map[string]string
}

func (k *scalewayKey) Features() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	zone, _ := util.GetZone(k.Labels)

	return zone + "," + instanceType
}

func (k *scalewayKey) GPUType() string {
	instanceType, _ := util.GetInstanceType(k.Labels)
	if strings.HasPrefix(instanceType, "RENDER") || strings.HasPrefix(instanceType, "GPU") {
		return instanceType
	}
	return ""
}
func (k *scalewayKey) ID() string {
	return ""
}

func (c *Scaleway) NodePricing(key Key) (*Node, error) {
	// There is only the zone and the instance ID in the providerID, hence we must use the features
	split := strings.Split(key.Features(), ",")
	if pricing, ok := c.Pricing[split[0]]; ok {
		if info, ok := pricing.NodesInfos[split[1]]; ok {
			log.Infof("Using features:`%s`", key.Features())
			return &Node{
				Cost:        fmt.Sprintf("%f", info.HourlyPrice),
				PricingType: DefaultPrices,
				VCPU:        fmt.Sprintf("%d", info.Ncpus),
				RAM:         fmt.Sprintf("%d", info.RAM),
				// This is tricky, as instances can have local volumes or not
				Storage:      fmt.Sprintf("%d", info.PerVolumeConstraint.LSSD.MinSize),
				GPU:          fmt.Sprintf("%d", info.Gpu),
				InstanceType: split[1],
				Region:       split[0],
				GPUName:      key.GPUType(),
			}, nil

		}

	}
	return nil, fmt.Errorf("Unable to find Node matching `%s`:`%s`", key.ID(), key.Features())
}

func (c *Scaleway) GetKey(l map[string]string, n *v1.Node) Key {
	return &scalewayKey{
		Labels: l,
	}
}

type scalewayPVKey struct {
	Labels                 map[string]string
	StorageClassName       string
	StorageClassParameters map[string]string
	Name                   string
	Zone                   string
}

func (key *scalewayPVKey) ID() string {
	return ""
}

func (key *scalewayPVKey) GetStorageClass() string {
	return key.StorageClassName
}

func (key *scalewayPVKey) Features() string {
	// Only 1 type of PV for now
	return key.Zone
}

func (c *Scaleway) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string, defaultRegion string) PVKey {
	// the csi volume handle is the form <az>/<volume-id>
	zone := strings.Split(pv.Spec.CSI.VolumeHandle, "/")[0]
	return &scalewayPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Name:                   pv.Name,
		Zone:                   zone,
	}
}

func (c *Scaleway) PVPricing(pvk PVKey) (*PV, error) {
	pricing, ok := c.Pricing[pvk.Features()]
	if !ok {
		log.Infof("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &PV{}, nil
	}
	return &PV{
		Cost:  fmt.Sprintf("%f", pricing.PVCost),
		Class: pvk.GetStorageClass(),
	}, nil
}

func (c *Scaleway) ServiceAccountStatus() *ServiceAccountStatus {
	return &ServiceAccountStatus{
		Checks: []*ServiceAccountCheck{},
	}
}

func (*Scaleway) ClusterManagementPricing() (string, float64, error) {
	return "", 0.0, nil
}

func (c *Scaleway) CombinedDiscountForNode(instanceType string, isPreemptible bool, defaultDiscount, negotiatedDiscount float64) float64 {
	return 1.0 - ((1.0 - defaultDiscount) * (1.0 - negotiatedDiscount))
}

func (c *Scaleway) Regions() []string {
	// These are zones but hey, its 2022
	zones := []string{}
	for _, zone := range scw.AllZones {
		zones = append(zones, zone.String())
	}
	return zones
}
