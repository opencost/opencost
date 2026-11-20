package aws

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDescribeVolumes struct {
	region string
	fn     func(ctx context.Context, region string, params *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error)
}

func (m *mockDescribeVolumes) DescribeVolumes(ctx context.Context, params *ec2.DescribeVolumesInput, _ ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	return m.fn(ctx, m.region, params)
}

type stubProviderConfig struct{}

func (stubProviderConfig) ConfigFileManager() *config.ConfigFileManager { return nil }
func (stubProviderConfig) GetCustomPricingData() (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}
func (stubProviderConfig) Update(func(*models.CustomPricing) error) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}
func (stubProviderConfig) UpdateFromMap(map[string]string) (*models.CustomPricing, error) {
	return &models.CustomPricing{}, nil
}

func awsWithMockEC2(fn func(ctx context.Context, region string, params *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error)) *AWS {
	return &AWS{
		Config:               stubProviderConfig{},
		ServiceAccountChecks: models.NewServiceAccountChecks(),
		ValidPricingKeys:     map[string]bool{},
		ClusterRegion:        "us-east-2",
		ec2ClientForRegionFunc: func(_ context.Context, region string) (describeVolumesAPI, error) {
			return &mockDescribeVolumes{region: region, fn: fn}, nil
		},
	}
}

func volumePage(nextToken string, ids ...string) *ec2.DescribeVolumesOutput {
	out := &ec2.DescribeVolumesOutput{}
	for _, id := range ids {
		out.Volumes = append(out.Volumes, ec2Types.Volume{VolumeId: awsSDK.String(id)})
	}
	if nextToken != "" {
		out.NextToken = awsSDK.String(nextToken)
	}
	return out
}

func listedVolumeIDs(t *testing.T, aws *AWS, ctx context.Context) []string {
	t.Helper()

	volumes, err := aws.getAllDisks(ctx)
	require.NoError(t, err)

	ids := make([]string, 0, len(volumes))
	for _, v := range volumes {
		if v.VolumeId != nil {
			ids = append(ids, *v.VolumeId)
		}
	}
	return ids
}

func TestAWS_getAllDisks_FollowsNextTokenWithAPageSize(t *testing.T) {
	var calls atomic.Int64

	aws := awsWithMockEC2(func(_ context.Context, region string, params *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error) {
		if params.MaxResults == nil || *params.MaxResults != describeVolumesPageSize {
			return nil, fmt.Errorf("expected MaxResults %d, got %v", describeVolumesPageSize, params.MaxResults)
		}
		if region != "us-east-2" {
			return volumePage(""), nil
		}

		switch calls.Add(1) {
		case 1:
			if params.NextToken != nil {
				return nil, fmt.Errorf("first page must not carry a token")
			}
			return volumePage("page2", "vol-1", "vol-2"), nil
		default:
			if params.NextToken == nil || *params.NextToken != "page2" {
				return nil, fmt.Errorf("second page must carry the first page's token, got %v", params.NextToken)
			}
			return volumePage("", "vol-3"), nil
		}
	})

	ids := listedVolumeIDs(t, aws, context.Background())

	assert.Equal(t, int64(2), calls.Load(), "the next token must be followed")
	assert.ElementsMatch(t, []string{"vol-1", "vol-2", "vol-3"}, ids)
}

func TestAWS_getAllDisks_SweepsEveryRegion(t *testing.T) {
	aws := awsWithMockEC2(func(_ context.Context, region string, _ *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error) {
		return volumePage("", "vol-"+region), nil
	})

	var want []string
	for _, region := range aws.Regions() {
		want = append(want, "vol-"+region)
	}
	require.NotEmpty(t, want)
	assert.ElementsMatch(t, want, listedVolumeIDs(t, aws, context.Background()))
}

func TestAWS_getAllDisks_PermissionDeniedRegionIsSkipped(t *testing.T) {
	aws := awsWithMockEC2(func(_ context.Context, region string, _ *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error) {
		switch region {
		case "us-west-2":
			return nil, &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "no"}
		case "us-east-2":
			return volumePage("", "vol-allowed"), nil
		}
		return volumePage(""), nil
	})

	assert.Equal(t, []string{"vol-allowed"}, listedVolumeIDs(t, aws, context.Background()))
}

func TestAWS_getAllDisks_OtherErrorsAreReported(t *testing.T) {
	aws := awsWithMockEC2(func(_ context.Context, region string, _ *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error) {
		if region == "us-east-2" {
			return nil, &smithy.GenericAPIError{Code: "RequestLimitExceeded", Message: "slow down"}
		}
		return volumePage(""), nil
	})

	_, err := aws.getAllDisks(context.Background())
	assert.Error(t, err)
}

func TestAWS_getAllDisks_NoPartialInventoryOnTimeout(t *testing.T) {
	aws := awsWithMockEC2(func(ctx context.Context, region string, _ *ec2.DescribeVolumesInput) (*ec2.DescribeVolumesOutput, error) {
		switch region {
		case "us-west-2":
			<-ctx.Done()
			return nil, ctx.Err()
		case "us-east-2":
			return volumePage("", "vol-fast"), nil
		}
		return volumePage(""), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	started := time.Now()
	volumes, err := aws.getAllDisks(ctx)

	assert.Error(t, err, "a truncated sweep must not be reported as a complete one")
	assert.Nil(t, volumes, "the volumes one region did return are not the inventory")
	assert.Less(t, time.Since(started), 10*time.Second, "the sweep must give up when its context does")
}
