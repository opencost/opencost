package pricing

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/opencost/opencost/pkg/log"
)

func GetPricingURL(region, service string) string {
	pricingURL := "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/%s/current/%s/index.json"
	if strings.HasPrefix(region, "cn-") {
		pricingURL = "https://pricing.cn-north-1.amazonaws.com.cn/offers/v1.0/cn/%s/current/%s/index.json"
	}
	return fmt.Sprintf(pricingURL, service, region)
}

func FetchPricing(pricingURL string) (*http.Response, error) {
	log.Infof("starting download of \"%s\", which is quite large ...", pricingURL)
	resp, err := http.Get(pricingURL)
	if err != nil {
		log.Errorf("Bogus fetch of \"%s\": %v", pricingURL, err)
		return nil, err
	}
	return resp, nil
}
