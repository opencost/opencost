package cloud

import (
	"encoding/csv"
	"io"
	"os"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"

	"github.com/jszwec/csvutil"
)

type CSVProvider struct {
	*CustomProvider
	CSVLocation             string
	Pricing                 map[string]*price
	NodeMapField            string
	PricingPV               map[string]*price
	PVMapField              string
	DownloadPricingDataLock sync.RWMutex
}
type price struct {
	EndTimestamp      string `csv:"EndTimestamp"`
	InstanceID        string `csv:"InstanceID"`
	AssetClass        string `csv:"AssetClass"`
	InstanceIDField   string `csv:"InstanceIDField"`
	InstanceType      string `csv:"InstanceType"`
	MarketPriceHourly string `csv:"MarketPriceHourly"`
	Version           string `csv:"Version"`
}

func parseMapField(mf string) {

}

func GetCsv(location string) (io.Reader, error) {
	return os.Open(location)
}

func (c *CSVProvider) DownloadPricingData() error {
	c.DownloadPricingDataLock.Lock()
	defer c.DownloadPricingDataLock.Unlock()
	pricing := make(map[string]*price)
	pvpricing := make(map[string]*price)
	header, err := csvutil.Header(price{}, "csv")
	if err != nil {
		return err
	}
	fieldsPerRecord := len(header)
	csvr, err := GetCsv(c.CSVLocation)
	csvReader := csv.NewReader(csvr)
	csvReader.Comma = '\t'
	csvReader.FieldsPerRecord = fieldsPerRecord

	dec, err := csvutil.NewDecoder(csvReader, header...)
	if err != nil {
		return err
	}
	for {
		p := price{}
		err := dec.Decode(&p)
		csvParseErr, isCsvParseErr := err.(*csv.ParseError)
		if err == io.EOF {
			break
		} else if err == csvutil.ErrFieldCount || (isCsvParseErr && csvParseErr.Err == csv.ErrFieldCount) {
			rec := dec.Record()
			if len(rec) != 1 {
				klog.V(2).Infof("Expected %d price info fields but received %d: %s", fieldsPerRecord, len(rec), rec)
				continue
			}
			if strings.Index(rec[0], "#") == 0 {
				continue
			} else {
				klog.V(3).Infof("skipping non-CSV line: %s", rec)
				continue
			}
		} else if err != nil {
			klog.V(2).Infof("Error during spot info decode: %+v", err)
			continue
		}

		klog.V(4).Infof("Found price info %+v", p)
		if p.AssetClass == "pv" {
			pvpricing[p.InstanceID] = &p
			c.PVMapField = p.InstanceIDField
		} else if p.AssetClass == "node" {
			pricing[p.InstanceID] = &p
			c.NodeMapField = p.InstanceIDField
		} else {
			klog.Infof("Unrecognized asset class %s, defaulting to node", p.AssetClass)
			pricing[p.InstanceID] = &p
			c.NodeMapField = p.InstanceIDField
		}
	}
	c.Pricing = pricing
	c.PricingPV = pvpricing
	return nil
}

type csvKey struct {
	Labels     map[string]string
	ProviderID string
}

func (k *csvKey) Features() string {
	return ""
}
func (k *csvKey) GPUType() string {
	return ""
}
func (k *csvKey) ID() string {
	return k.ProviderID
}

func (c *CSVProvider) NodePricing(key Key) (*Node, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()
	if p, ok := c.Pricing[key.ID()]; ok {
		return &Node{
			Cost: p.MarketPriceHourly,
		}, nil
	} else {
		klog.Infof("Unable to find Node matching %s", key.ID())
		return &Node{}, nil
	}
}

func NodeValueFromMapField(m string, n *v1.Node) string {
	mf := strings.Split(m, ".")
	if len(mf) == 2 && mf[0] == "spec" && mf[1] == "providerID" {
		return n.Spec.ProviderID
	} else if len(mf) > 1 && mf[0] == "metadata" {
		if mf[1] == "name" {
			return n.Name
		} else if mf[1] == "labels" {
			lkey := strings.Join(mf[2:len(mf)], "")
			return n.Labels[lkey]
		} else if mf[1] == "annotations" {
			akey := strings.Join(mf[2:len(mf)], "")
			return n.Annotations[akey]
		} else {
			klog.Infof("[ERROR] Unsupported InstanceIDField %s in CSV For Node", m)
			return ""
		}
	} else {
		klog.Infof("[ERROR] Unsupported InstanceIDField %s in CSV For Node", m)
		return ""
	}
}

func PVValueFromMapField(m string, n *v1.PersistentVolume) string {
	mf := strings.Split(m, ".")
	if len(mf) > 1 && mf[0] == "metadata" {
		if mf[1] == "name" {
			return n.Name
		} else if mf[1] == "labels" {
			lkey := strings.Join(mf[2:len(mf)], "")
			return n.Labels[lkey]
		} else if mf[1] == "annotations" {
			akey := strings.Join(mf[2:len(mf)], "")
			return n.Annotations[akey]
		} else {
			klog.Infof("[ERROR] Unsupported InstanceIDField %s in CSV For PV", m)
			return ""
		}
	} else {
		klog.Infof("[ERROR] Unsupported InstanceIDField %s in CSV For PV", m)
		return ""
	}
}

func (c *CSVProvider) GetKey(l map[string]string, n *v1.Node) Key {
	id := NodeValueFromMapField(c.NodeMapField, n)
	return &csvKey{
		ProviderID: id,
		Labels:     l,
	}
}

type csvPVKey struct {
	Labels                 map[string]string
	ProviderID             string
	StorageClassName       string
	StorageClassParameters map[string]string
	Name                   string
	DefaultRegion          string
}

func (key *csvPVKey) GetStorageClass() string {
	return key.StorageClassName
}

func (key *csvPVKey) Features() string {
	return key.ProviderID
}

func (c *CSVProvider) GetPVKey(pv *v1.PersistentVolume, parameters map[string]string, defaultRegion string) PVKey {
	id := PVValueFromMapField(c.PVMapField, pv)
	return &csvPVKey{
		Labels:                 pv.Labels,
		ProviderID:             id,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
		Name:                   pv.Name,
		DefaultRegion:          defaultRegion,
	}
}

func (c *CSVProvider) PVPricing(pvk PVKey) (*PV, error) {
	c.DownloadPricingDataLock.RLock()
	defer c.DownloadPricingDataLock.RUnlock()
	pricing, ok := c.PricingPV[pvk.Features()]
	if !ok {
		klog.V(4).Infof("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.Features())
		return &PV{}, nil
	}
	return &PV{
		Cost: pricing.MarketPriceHourly,
	}, nil
}
