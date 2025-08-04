package otc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/opencost/opencost/core/pkg/log"
)

// Fetches and flattens all product entries across multiple services with pagination
func (otc *OTC) fetchPaginatedProducts(serviceNames []string) ([]Product, error) {
	const baseURL = "https://calculator.otc-service.com/de/open-telekom-price-api/"
	var allProducts []Product

	limitFrom := 0
	query := buildServiceNameQueryParam(serviceNames)

	for {
		url := fmt.Sprintf("%s?%s&columns%%5B0%%5D=productIdParameter&columns%%5B1%%5D=opiFlavour&columns%%5B2%%5D=osUnit&columns%%5B3%%5D=vCpu&columns%%5B4%%5D=ram&columns%%5B5%%5D=priceAmount&limitFrom=%d", baseURL, query, limitFrom)

		resp, err := http.Get(url)
		if err != nil {
			log.Errorf("Error fetching products from OTC API: %v", err)
			return nil, err
		}
		defer resp.Body.Close()

		pageData, stats, err := otc.loadPaginatedResponse(resp)
		if err != nil {
			log.Errorf("Error loading paginated response: %v", err)
			return nil, err
		}

		for _, products := range pageData {
			allProducts = append(allProducts, products...)
		}

		if stats.CurrentPage >= stats.MaxPages {
			log.Infof("Fetched all products for services: %v", serviceNames)
			break
		}

		limitFrom += stats.RecordsPerPage
	}

	return allProducts, nil
}

// Parses the OTC API response into a map of service → []Product and pagination stats
func (otc *OTC) loadPaginatedResponse(resp *http.Response) (map[string][]Product, *OTCStats, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Error reading OTC API response: %v", err)
		return nil, nil, err
	}

	var raw map[string]map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		log.Errorf("Error unmarshalling OTC API response: %v", err)
		return nil, nil, err
	}

	var data map[string][]Product
	if err := json.Unmarshal(raw["response"]["result"], &data); err != nil {
		log.Errorf("Error unmarshalling result section: %v", err)
		return nil, nil, err
	}

	var stats OTCStats
	if err := json.Unmarshal(raw["response"]["stats"], &stats); err != nil {
		log.Errorf("Error unmarshalling stats section: %v", err)
		return nil, nil, err
	}

	return data, &stats, nil
}
