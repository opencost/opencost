package main

import (
	"fmt"
	"os"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

const (
	pluginDomain  = "huaweiobs"
	pluginVersion = "0.1.0"

	bytesPerGB = 1024 * 1024 * 1024
)

// ObsCostSource implements core/pkg/plugin.CustomCostSource, pricing Huawei Cloud
// OBS bucket storage for a requested window using live bucket sizes (OBS API) and
// live on-demand storage pricing (BSS demandPrice API).
type ObsCostSource struct {
	cfg *Config
}

func NewObsCostSource(cfg *Config) *ObsCostSource {
	return &ObsCostSource{cfg: cfg}
}

// GetCustomCosts implements plugin.CustomCostSource. Any failure (missing
// credentials, an OBS or BSS API error) is reported as a single response with
// Errors set, matching how other CustomCostSource implementations in this codebase
// (and the ingestor that calls them, see pkg/customcost/ingestor.go) surface plugin
// failures without crashing the pipeline.
func (s *ObsCostSource) GetCustomCosts(req *pb.CustomCostRequest) []*pb.CustomCostResponse {
	errResp := func(err error) []*pb.CustomCostResponse {
		return []*pb.CustomCostResponse{{
			Domain: pluginDomain,
			Errors: []string{err.Error()},
		}}
	}

	projectID := os.Getenv(envProjectID)
	if projectID == "" {
		return errResp(fmt.Errorf("huawei obs plugin: required environment variable not set: %s", envProjectID))
	}

	ak := os.Getenv(envAccessKeyID)
	sk := os.Getenv(envAccessKeySecret)
	if ak == "" || sk == "" {
		return errResp(fmt.Errorf("huawei obs plugin: required environment variables not set: %s / %s", envAccessKeyID, envAccessKeySecret))
	}

	obsClient, err := obs.New(ak, sk, obsEndpoint(s.cfg.Region))
	if err != nil {
		return errResp(err)
	}
	defer obsClient.Close()

	bucketFilter := map[string]bool{}
	for _, b := range s.cfg.Buckets {
		bucketFilter[b] = true
	}

	usage, err := listBucketUsage(obsClient, bucketFilter)
	if err != nil {
		return errResp(err)
	}

	pricePerGBHour, currency, err := obsGBHourPrice(projectID, s.cfg.Region)
	if err != nil {
		return errResp(err)
	}

	start := req.Start.AsTime()
	end := req.End.AsTime()
	hours := end.Sub(start).Hours()

	costs := make([]*pb.CustomCost, 0, len(usage))
	for _, b := range usage {
		sizeGB := float64(b.SizeBytes) / bytesPerGB
		priceF, _ := pricePerGBHour.Float64()
		billedCost := float32(sizeGB * hours * priceF)

		costs = append(costs, &pb.CustomCost{
			Zone:          s.cfg.Region,
			ResourceName:  b.Name,
			ResourceType:  "OBS Bucket",
			Id:            b.Name,
			ProviderId:    b.Name,
			BilledCost:    billedCost,
			ListCost:      billedCost,
			ListUnitPrice: float32(priceF),
			UsageQuantity: float32(sizeGB * hours),
			UsageUnit:     "GB-Hours",
		})
	}

	return []*pb.CustomCostResponse{{
		Metadata:   map[string]string{"region": s.cfg.Region},
		CostSource: pluginDomain,
		Domain:     pluginDomain,
		Version:    pluginVersion,
		Currency:   currency,
		Start:      timestamppb.New(start),
		End:        timestamppb.New(end),
		Costs:      costs,
	}}
}
