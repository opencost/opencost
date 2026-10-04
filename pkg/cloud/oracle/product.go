package oracle

import (
	"embed"
	"encoding/json"
	"log"
	"strconv"
	"strings"
)

const blockVolumePartNumber = "B91961"
const loadBalancerPartNumber = "B93030"

// egressT1PartNumber for egress to NA, EU, and UK
const egress1PartNumber = "B88327"

// egressT2PartNumber for egress to APAC, and SA
const egress2PartNumber = "B93455"

// egressT3PartNumber for egress to ME, and AF
const egress3PartNumber = "B93456"
const enhancedClusterPartNumber = "B96545"
const virualNodePartNumber = "B96109"

// Compiled from cost estimator data at https://www.oracle.com/cloud/costestimator.html
// The shapes.json endpoint can be queried for a snapshot of this data.
// Note that this endpoint is subject to change and should not be queried directly.
//
//go:embed partnumbers
var partNumbers embed.FS

func init() {
	shapeInfo, err := partNumbers.ReadFile("partnumbers/shape_part_numbers.json")
	if err != nil {
		log.Fatalln("unable to read OCI Shape part numbers", err)
	}
	instanceProducts = map[string]Product{}
	if err := json.Unmarshal(shapeInfo, &instanceProducts); err != nil {
		log.Fatalln("unable to unmarshal OCI Shape part numbers", err)
	}
}

type Product struct {
	// OCPU product name
	OCPU string
	// Memory product name
	Memory string
	// GPU product name
	GPU  string
	Disk string
}

type DefaultPricing struct {
	OCPU    string
	Memory  string
	GPU     string
	Storage string
	LB      string
	Egress  string
}

type instanceProduct map[string]Product

// instanceProducts maps instance types to associated part numbers.
var instanceProducts instanceProduct

// normalizeOCIInstanceShape parses synthetic flex shape labels in the form
// <base-shape>.<ocpus>o.<memory>g.<baseline>b. Supported baselines are 1_1,
// 1_2, and 1_8, which map to the burstable CPU price multipliers below.
func normalizeOCIInstanceShape(shape string) (string, float64, bool) {
	const defaultCPUPriceMultiplier = 1.0

	parts := strings.Split(shape, ".")
	if len(parts) < 4 {
		return "", defaultCPUPriceMultiplier, false
	}

	ocpuPart := parts[len(parts)-3]
	memoryPart := parts[len(parts)-2]
	baselinePart := parts[len(parts)-1]

	if !strings.HasSuffix(ocpuPart, "o") || !strings.HasSuffix(memoryPart, "g") || !strings.HasSuffix(baselinePart, "b") {
		return "", defaultCPUPriceMultiplier, false
	}
	if _, err := strconv.ParseFloat(strings.TrimSuffix(ocpuPart, "o"), 64); err != nil {
		return "", defaultCPUPriceMultiplier, false
	}
	if _, err := strconv.ParseFloat(strings.TrimSuffix(memoryPart, "g"), 64); err != nil {
		return "", defaultCPUPriceMultiplier, false
	}

	var cpuPriceMultiplier float64
	switch strings.TrimSuffix(baselinePart, "b") {
	case "1_1":
		cpuPriceMultiplier = 1.0
	case "1_2":
		cpuPriceMultiplier = 0.5
	case "1_8":
		cpuPriceMultiplier = 0.125
	default:
		return "", defaultCPUPriceMultiplier, false
	}

	baseShape := strings.Join(parts[:len(parts)-3], ".")
	return baseShape, cpuPriceMultiplier, true
}

func (i instanceProduct) get(shape string) Product {
	if product, ok := i[shape]; ok {
		return product
	}
	// If no product mapping exists, provide a default product
	return Product{}
}

func (d DefaultPricing) TotalInstanceCost() (float64, error) {
	var totalCost float64
	addValue := func(val string) error {
		if val != "" {
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return err
			}
			totalCost += f
		}
		return nil
	}
	if err := addValue(d.OCPU); err != nil {
		return totalCost, err
	}
	if err := addValue(d.Memory); err != nil {
		return totalCost, err
	}
	if err := addValue(d.GPU); err != nil {
		return totalCost, err
	}
	return totalCost, nil
}
func egressRegionPartNumber(region string) string {
	split := strings.Split(region, "-")
	switch split[0] {
	case "us", "ca", "eu", "uk", "mx":
		return egress1PartNumber
	case "ap", "sa":
		return egress2PartNumber
	case "me", "af", "il":
		return egress3PartNumber
	default:
		return ""
	}
}
