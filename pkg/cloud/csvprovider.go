package cloud

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"

	"github.com/jszwec/csvutil"
)

const refreshMinutes = 60

type CSVProvider struct {
	*CustomProvider
	CSVLocation             string
	Pricing                 map[string]*price
	NodeMapField            string
	PricingPV               map[string]*price
	PVMapField              string
	UsesRegion              bool
	DownloadPricingDataLock sync.RWMutex
}
type price struct {
	EndTimestamp      string `csv:"EndTimestamp"`
	InstanceID        string `csv:"InstanceID"`
	Region            string `csv:"Region"`
	AssetClass        string `csv:"AssetClass"`
	InstanceIDField   string `csv:"InstanceIDField"`
	InstanceType      string `csv:"InstanceType"`
	MarketPriceHourly string `csv:"MarketPriceHourly"`
	Version           string `csv:"Version"`
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
	var csvr io.Reader
	var csverr error
	if strings.HasPrefix(c.CSVLocation, "s3://") {
		region := os.Getenv("CSV_REGION")
		conf := aws.NewConfig().WithRegion(region).WithCredentialsChainVerboseErrors(true)
		s3Client := s3.New(session.New(conf))
		bucketAndKey := strings.Split(strings.TrimPrefix(c.CSVLocation, "s3://"), "/")
		if len(bucketAndKey) == 2 {
			out, err := s3Client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucketAndKey[0]),
				Key:    aws.String(bucketAndKey[1]),
			})
			csverr = err
			csvr = out.Body
		} else {
			c.Pricing = pricing
			c.PricingPV = pvpricing
			return fmt.Errorf("Invalid s3 URI: %s", c.CSVLocation)
		}
	} else {
		csvr, csverr = GetCsv(c.CSVLocation)
	}
	if csverr != nil {
		klog.Infof("Error reading csv at %s: %s", c.CSVLocation, csverr)
		c.Pricing = pricing
		c.PricingPV = pvpricing
		return nil
	}
	csvReader := csv.NewReader(csvr)
	csvReader.Comma = ','
	csvReader.FieldsPerRecord = fieldsPerRecord

	dec, err := csvutil.NewDecoder(csvReader, header...)
	if err != nil {
		c.Pricing = pricing
		c.PricingPV = pvpricing
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
		key := p.InstanceID
		if p.Region != "" { // strip the casing from region and add to key.
			key = fmt.Sprintf("%s,%s", strings.ToLower(p.Region), p.InstanceID)
			c.UsesRegion = true
		}
		if p.AssetClass == "pv" {
			pvpricing[key] = &p
			c.PVMapField = p.InstanceIDField
		} else if p.AssetClass == "node" {
			pricing[key] = &p
			c.NodeMapField = p.InstanceIDField
		} else {
			klog.Infof("Unrecognized asset class %s, defaulting to node", p.AssetClass)
			pricing[key] = &p
			c.NodeMapField = p.InstanceIDField
		}
	}
	if len(pricing) > 0 {
		c.Pricing = pricing
		c.PricingPV = pvpricing
	} else {
		klog.Infof("[WARNING] No data received from csv")
	}
	time.AfterFunc(refreshMinutes*time.Minute, func() { c.DownloadPricingData() })
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
	}
	s := strings.Split(key.ID(), ",") // Try without a region to be sure
	if len(s) == 2 {
		if p, ok := c.Pricing[s[1]]; ok {
			return &Node{
				Cost: p.MarketPriceHourly,
			}, nil
		}
	}
	return nil, fmt.Errorf("Unable to find Node matching %s", key.ID())
}

func NodeValueFromMapField(m string, n *v1.Node, useRegion bool) string {
	mf := strings.Split(m, ".")
	toReturn := ""
	if useRegion {
		toReturn = n.Labels[v1.LabelZoneRegion] + ","
	}
	if len(mf) == 2 && mf[0] == "spec" && mf[1] == "providerID" {
		provIdRx := regexp.MustCompile("aws:///([^/]+)/([^/]+)") // It's of the form aws:///us-east-2a/i-0fea4fd46592d050b and we want i-0fea4fd46592d050b, if it exists
		for matchNum, group := range provIdRx.FindStringSubmatch(n.Spec.ProviderID) {
			if matchNum == 2 {
				return toReturn + group
			}
		}
		if strings.HasPrefix(n.Spec.ProviderID, "azure://") {
			vmOrScaleSet := strings.TrimPrefix(n.Spec.ProviderID, "azure://")
			return toReturn + vmOrScaleSet
		}
		return toReturn + n.Spec.ProviderID
	} else if len(mf) > 1 && mf[0] == "metadata" {
		if mf[1] == "name" {
			return toReturn + n.Name
		} else if mf[1] == "labels" {
			lkey := strings.Join(mf[2:len(mf)], "")
			return toReturn + n.Labels[lkey]
		} else if mf[1] == "annotations" {
			akey := strings.Join(mf[2:len(mf)], "")
			return toReturn + n.Annotations[akey]
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
			klog.V(4).Infof("[ERROR] Unsupported InstanceIDField %s in CSV For PV", m)
			return ""
		}
	} else {
		klog.V(4).Infof("[ERROR] Unsupported InstanceIDField %s in CSV For PV", m)
		return ""
	}
}

func (c *CSVProvider) GetKey(l map[string]string, n *v1.Node) Key {
	id := NodeValueFromMapField(c.NodeMapField, n, c.UsesRegion)
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
