package aws

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
)

const pricingCacheTTL = 24 * time.Hour
const pricingCacheDir = "pricingsource/aws"
const pricingCacheFile = "cached_ec2_pricingmodelset"

type PublicAPIPricingSource struct{}

func (p PublicAPIPricingSource) cacheFilePath() (string, error) {
	dir := env.GetPathFromConfig(pricingCacheDir)
	if _, e := os.Stat(dir); e != nil && os.IsNotExist(e) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(dir, pricingCacheFile), nil
}

func (p PublicAPIPricingSource) loadFromCache() (*pricingmodel.PricingModelSet, bool) {
	path, err := p.cacheFilePath()
	if err != nil {
		return nil, false
	}
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > pricingCacheTTL {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	pms := &pricingmodel.PricingModelSet{}
	if err := pms.UnmarshalBinary(data); err != nil {
		log.Warnf("failed to unmarshal cached pricing data: %s", err.Error())
		return nil, false
	}
	return pms, true
}

func (p PublicAPIPricingSource) saveToCache(pms *pricingmodel.PricingModelSet) {
	path, err := p.cacheFilePath()
	if err != nil {
		log.Warnf("failed to determine pricing cache path: %s", err.Error())
		return
	}
	data, err := pms.MarshalBinary()
	if err != nil {
		log.Warnf("failed to marshal pricing data for cache: %s", err.Error())
		return
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		log.Warnf("failed to write pricing cache: %s", err.Error())
	}
}

const PublicAPIPricingSourceKey = "aws/public_api_ec2_pricing"

func (p PublicAPIPricingSource) PricingSourceKey() string {
	return PublicAPIPricingSourceKey
}

func (p PublicAPIPricingSource) GetPricing() (*pricingmodel.PricingModelSet, error) {
	if cached, ok := p.loadFromCache(); ok {
		return cached, nil
	}
	now := time.Now().UTC()
	pms := pricingmodel.NewPricingModelSet(now, PublicAPIPricingSourceKey)
	skuToNodeKey := make(map[string]pricingmodel.NodeKey)

	// When parsing product we create keys based off of product attributes and link those to a SKU.
	handleProduct := func(product *PriceListEC2Product) {
		attr := product.Attributes
		if attr.LocationType != "AWS Region" {
			return
		}

		if !((strings.HasPrefix(attr.UsageType, "BoxUsage") || strings.Contains(attr.UsageType, "-BoxUsage")) &&
			(attr.CapacityStatus == "Used" || attr.CapacityStatus == "") &&
			(attr.MarketOption == "OnDemand" || attr.MarketOption == "")) {
			return
		}

		if attr.OperatingSystem != "" && attr.OperatingSystem != "NA" && attr.OperatingSystem != "Linux" {
			return
		}

		if attr.PreInstalledSw != "" && attr.PreInstalledSw != "NA" {

		}

		if attr.RegionCode == "" || attr.InstanceType == "" {
			return
		}

		skuToNodeKey[product.Sku] = pricingmodel.NodeKey{
			Provider: "AWS",
			Region:   attr.RegionCode,
			NodeType: attr.InstanceType,
		}
	}

	// Terms are used to define pricing and have the sku to look up the appropriate key.
	handleTerm := func(term *PriceListEC2Term) {
		nodeKey, ok := skuToNodeKey[term.Sku]
		if !ok {
			return
		}
		isCN := false
		hourlyRateCode := HourlyRateCode
		if _, ok = OnDemandRateCodes[term.OfferTermCode]; !ok {
			if _, okCN := OnDemandRateCodesCn[term.OfferTermCode]; !okCN {
				// Skip if term is not OnDemand
				return
			}
			isCN = true
			hourlyRateCode = HourlyRateCodeCn
		}
		priceDimensionKey := strings.Join([]string{term.Sku, term.OfferTermCode, hourlyRateCode}, ".")

		pricingDimension, ok := term.PriceDimensions[priceDimensionKey]
		if !ok {
			return
		}

		priceStr := pricingDimension.PricePerUnit.USD
		if isCN {
			priceStr = pricingDimension.PricePerUnit.CNY
		}
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Errorf("failed to parse str to float '%s': %s", priceStr, err.Error())
			return
		}
		pms.NodePricing[nodeKey] = pricingmodel.NodePricing{
			TotalHourlyRate: price,
		}
	}

	err := QueryEC2PriceList("", handleProduct, handleTerm)
	if err != nil {
		return nil, fmt.Errorf("failed to query list pricing data %w", err)
	}

	p.saveToCache(pms)

	return pms, nil
}
