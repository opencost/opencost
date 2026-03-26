package aws

import (
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
)

type PublicAPIPricingSource struct{}

func (p PublicAPIPricingSource) GetPricing() (*pricingmodel.PricingModelSet, error) {
	pms := &pricingmodel.PricingModelSet{}
	skuToNodeKey := make(map[string]pricingmodel.NodeKey)

	// One counter map per field of PriceListEC2ProductAttributes.
	serviceCodeCounts := make(map[string]int)
	instanceTypeCounts := make(map[string]int)
	usageTypeCounts := make(map[string]int)
	operationCounts := make(map[string]int)
	locationCounts := make(map[string]int)
	locationTypeCounts := make(map[string]int)
	regionCodeCounts := make(map[string]int)
	serviceNameCounts := make(map[string]int)
	memoryCounts := make(map[string]int)
	storageCounts := make(map[string]int)
	vCpuCounts := make(map[string]int)
	operatingSystemCounts := make(map[string]int)
	preInstalledSwCounts := make(map[string]int)
	instanceFamilyCounts := make(map[string]int)
	capacityStatusCounts := make(map[string]int)
	gpuCounts := make(map[string]int)
	marketOptionCounts := make(map[string]int)

	// When parsing product we create keys based off of product attributes and link those to a SKU.
	handleProduct := func(product *PriceListEC2Product) {
		attr := product.Attributes
		serviceCodeCounts[attr.ServiceCode]++
		instanceTypeCounts[attr.InstanceType]++
		usageTypeCounts[attr.UsageType]++
		operationCounts[attr.Operation]++
		locationCounts[attr.Location]++
		locationTypeCounts[attr.LocationType]++
		regionCodeCounts[attr.RegionCode]++
		serviceNameCounts[attr.ServiceName]++
		memoryCounts[attr.Memory]++
		storageCounts[attr.Storage]++
		vCpuCounts[attr.VCpu]++
		operatingSystemCounts[attr.OperatingSystem]++
		preInstalledSwCounts[attr.PreInstalledSw]++
		instanceFamilyCounts[attr.InstanceFamily]++
		capacityStatusCounts[attr.CapacityStatus]++
		gpuCounts[attr.GPU]++
		marketOptionCounts[attr.MarketOption]++

		_ = skuToNodeKey // will be used when building NodeKey from qualifying products
	}

	// Terms are used to define pricing and have the sku to look up the appropriate key.
	handleTerm := func(term *PriceListEC2Term) {

	}

	QueryEC2PriceList("", handleProduct, handleTerm)

	_ = serviceCodeCounts
	_ = instanceTypeCounts
	_ = usageTypeCounts
	_ = operationCounts
	_ = locationCounts
	_ = locationTypeCounts
	_ = regionCodeCounts
	_ = serviceNameCounts
	_ = memoryCounts
	_ = storageCounts
	_ = vCpuCounts
	_ = operatingSystemCounts
	_ = preInstalledSwCounts
	_ = instanceFamilyCounts
	_ = capacityStatusCounts
	_ = gpuCounts
	_ = marketOptionCounts

	return pms, nil
}
