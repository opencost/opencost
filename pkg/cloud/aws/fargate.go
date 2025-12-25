package aws

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/env"
)

const (
	usageTypeFargateLinuxX86CPU    = "Fargate-vCPU-Hours:perCPU"
	usageTypeFargateLinuxX86RAM    = "Fargate-GB-Hours"
	usageTypeFargateLinuxArmCPU    = "Fargate-ARM-vCPU-Hours:perCPU"
	usageTypeFargateLinuxArmRAM    = "Fargate-ARM-GB-Hours"
	usageTypeFargateWindowsCPU     = "Fargate-Windows-vCPU-Hours:perCPU"
	usageTypeFargateWindowsLicense = "Fargate-Windows-OS-Hours:perCPU"
	usageTypeFargateWindowsRAM     = "Fargate-Windows-GB-Hours"
)

var fargateUsageTypes = []string{
	usageTypeFargateLinuxX86CPU,
	usageTypeFargateLinuxX86RAM,
	usageTypeFargateLinuxArmCPU,
	usageTypeFargateLinuxArmRAM,
	usageTypeFargateWindowsCPU,
	usageTypeFargateWindowsLicense,
	usageTypeFargateWindowsRAM,
}

type FargateRegionPricing map[string]float64

func (f FargateRegionPricing) Validate() error {
	for _, usageType := range fargateUsageTypes {
		if _, ok := f[usageType]; !ok {
			return fmt.Errorf("missing pricing for usageType %s", usageType)
		}
	}
	return nil
}

type FargatePricing struct {
	regions map[string]FargateRegionPricing
}

func NewFargatePricing() *FargatePricing {
	return &FargatePricing{
		regions: make(map[string]FargateRegionPricing),
	}
}

func (f *FargatePricing) Initialize(nodeList []*clustercache.Node) error {
	url := f.getPricingURL(nodeList)

	log.Infof("Downloading Fargate pricing data from %s", url)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("downloading pricing data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pricing download failed: status=%d", resp.StatusCode)
	}

	var pricing AWSPricing
	if err := json.NewDecoder(resp.Body).Decode(&pricing); err != nil {
		return fmt.Errorf("parsing pricing data: %w", err)
	}

	return f.populatePricing(&pricing)
}

func (f *FargatePricing) getPricingURL(nodeList []*clustercache.Node) string {
	// Allow override of pricing URL for air-gapped environments
	if override := env.GetAWSECSPricingURLOverride(); override != "" {
		return override
	}
	return getPricingListURL("AmazonECS", nodeList)
}

func (f *FargatePricing) populatePricing(pricing *AWSPricing) error {
	// Populate pricing for each region
productLoop:
	for sku, product := range pricing.Products {
		for _, usageType := range fargateUsageTypes {
			if strings.HasSuffix(product.Attributes.UsageType, usageType) {
				region := product.Attributes.RegionCode
				if _, ok := f.regions[region]; !ok {
					f.regions[region] = make(FargateRegionPricing)
				}

				skuPrice, err := f.getPricingOfSKU(sku, &pricing.Terms)
				if err != nil {
					return fmt.Errorf("error getting pricing for sku %s: %s", sku, err)
				}
				f.regions[region][usageType] = skuPrice
				continue productLoop
			}
		}
	}

	// Validate pricing - do we have all the pricing we need?
	for region, regionPricing := range f.regions {
		err := regionPricing.Validate()
		if err != nil {
			// Be failsafe here and just log warnings
			log.Warnf("Fargate pricing data is (partially) missing pricing for %s: %s", region, err)
		}
	}
	return nil
}

func (f *FargatePricing) getPricingOfSKU(sku string, allTerms *AWSPricingTerms) (float64, error) {
	skuTerm, ok := allTerms.OnDemand[sku]
	if !ok {
		return 0, fmt.Errorf("missing pricing for sku %s", sku)
	}
	for _, offerTerm := range skuTerm {
		if _, isMatch := OnDemandRateCodes[offerTerm.OfferTermCode]; isMatch {
			priceDimensionKey := strings.Join([]string{sku, offerTerm.OfferTermCode, HourlyRateCode}, ".")
			if dimension, ok := offerTerm.PriceDimensions[priceDimensionKey]; ok {
				return strconv.ParseFloat(dimension.PricePerUnit.USD, 64)
			}
		} else if _, isMatch := OnDemandRateCodesCn[offerTerm.OfferTermCode]; isMatch {
			priceDimensionKey := strings.Join([]string{sku, offerTerm.OfferTermCode, HourlyRateCodeCn}, ".")
			if dimension, ok := offerTerm.PriceDimensions[priceDimensionKey]; ok {
				return strconv.ParseFloat(dimension.PricePerUnit.CNY, 64)
			}
		}
	}
	return 0, fmt.Errorf("missing pricing for sku %s", sku)
}

func (f *FargatePricing) GetHourlyPricing(region string, os, arch string) (cpu, memory float64, err error) {
	regionPricing, ok := f.regions[region]
	if !ok {
		return 0, 0, fmt.Errorf("missing pricing for region %s", region)
	}

	switch os {
	case "linux":
		switch arch {
		case "amd64":
			cpu = regionPricing[usageTypeFargateLinuxX86CPU]
			memory = regionPricing[usageTypeFargateLinuxX86RAM]
			return
		case "arm64":
			cpu = regionPricing[usageTypeFargateLinuxArmCPU]
			memory = regionPricing[usageTypeFargateLinuxArmRAM]
			return
		}
	case "windows":
		cpuOnly := regionPricing[usageTypeFargateWindowsCPU]
		cpuLicense := regionPricing[usageTypeFargateWindowsLicense]
		cpu = cpuOnly + cpuLicense
		memory = regionPricing[usageTypeFargateWindowsRAM]
		return
	}

	return 0, 0, fmt.Errorf("unknown os/arch combination: %s/%s", os, arch)
}
