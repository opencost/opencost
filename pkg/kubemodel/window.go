package kubemodel

import (
	"fmt"
	"time"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

func DurationToResolution(d time.Duration) (pb.Resolution, error) {
	switch d {
	case 10 * time.Minute:
		return pb.Resolution_RESOLUTION_10M, nil
	case time.Hour:
		return pb.Resolution_RESOLUTION_1H, nil
	case 24 * time.Hour:
		return pb.Resolution_RESOLUTION_1D, nil
	default:
		return pb.Resolution_RESOLUTION_10M, fmt.Errorf("kubemodel: unsupported window duration %s", d)
	}
}

func ResolutionToDuration(res pb.Resolution) (time.Duration, error) {
	switch res {
	case pb.Resolution_RESOLUTION_10M:
		return 10 * time.Minute, nil
	case pb.Resolution_RESOLUTION_1H:
		return time.Hour, nil
	case pb.Resolution_RESOLUTION_1D:
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("kubemodel: unsupported window resolution %v", res)
	}
}
