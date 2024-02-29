package aws

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
)

const S3SelectDateLayout = "2006-01-02T15:04:05Z"

// S3Object is aliased as "s" in queries
const S3SelectAccountID = `s."bill/PayerAccountId"`

const S3SelectItemType = `s."lineItem/LineItemType"`
const S3SelectStartDate = `s."lineItem/UsageStartDate"`
const S3SelectProductCode = `s."lineItem/ProductCode"`
const S3SelectResourceID = `s."lineItem/ResourceId"`
const S3SelectUsageType = `s."lineItem/UsageType"`

const S3SelectListCost = `s."lineItem/UnblendedCost"`
const S3SelectNetCost = `s."lineItem/NetUnblendedCost"`

// These two may be used for Amortized<Net>Cost
const S3SelectRICost = `s."reservation/EffectiveCost"`
const S3SelectSPCost = `s."savingsPlan/SavingsPlanEffectiveCost"`

type S3SelectIntegration struct {
	S3SelectQuerier
}

func (s3si *S3SelectIntegration) GetCloudCost(
	start,
	end time.Time,
) (*opencost.CloudCostSetRange, error) {
	log.Infof(
		"S3SelectIntegration[%s]: GetCloudCost: %s",
		s3si.Key(),
		opencost.NewWindow(&start, &end).String(),
	)

	// Set midnight yesterday as last point in time reconciliation data
	// can be pulled from to ensure complete days of data
	midnightYesterday := time.Now().In(
		time.UTC,
	).Truncate(time.Hour*24).AddDate(0, 0, -1)
	if end.After(midnightYesterday) {
		end = midnightYesterday
	}

	// ccsr to populate with cloudcosts.
	ccsr, err := opencost.NewCloudCostSetRange(
		start,
		end,
		opencost.AccumulateOptionDay,
		s3si.Key(),
	)
	if err != nil {
		return nil, err
	}
	// acquire S3 client
	client, err := s3si.GetS3Client()
	if err != nil {
		return nil, err
	}
	// Acquire query keys
	queryKeys, err := s3si.GetQueryKeys(start, end, client)
	if err != nil {
		return nil, err
	}
	// Acquire headers
	headers, err := s3si.GetHeaders(queryKeys, client)
	if err != nil {
		return nil, err
	}
	// Exactly what it says on the tin. Though is there a set equivalent
	// in Go? This seems like a good use case for that.
	allColumns := map[string]bool{}
	for _, header := range headers {
		allColumns[header] = true
	}

	formattedStart := start.Format("2006-01-02")
	formattedEnd := end.Format("2006-01-02")
	selectColumns := []string{
		S3SelectStartDate,
		S3SelectAccountID,
		S3SelectResourceID,
		S3SelectItemType,
		S3SelectProductCode,
		S3SelectUsageType,
		S3SelectListCost,
	}
	// OC equivalent to KCM env flags relevant at all?
	// Check for Reservation columns in CUR and query if available
	checkReservations := allColumns[S3SelectRICost]
	if checkReservations {
		selectColumns = append(selectColumns, S3SelectRICost)
	}

	// Check for Savings Plan Columns in CUR and query if available
	checkSavingsPlan := allColumns[S3SelectSPCost]
	if checkSavingsPlan {
		selectColumns = append(selectColumns, S3SelectSPCost)
	}

	// Build map of query columns to use for parsing query
	columnIndexes := map[string]int{}
	for i, column := range selectColumns {
		columnIndexes[column] = i
	}
	// Build query
	selectStr := strings.Join(selectColumns, ", ")
	queryStr := `SELECT %s FROM s3object s
	WHERE (CAST(s."lineItem/UsageStartDate" AS TIMESTAMP) BETWEEN CAST('%s' AS TIMESTAMP) AND CAST('%s' AS TIMESTAMP))
	AND s."lineItem/ResourceId" <> ''
	AND (
		(
			s."lineItem/ProductCode" = 'AmazonEC2' AND (
				SUBSTRING(s."lineItem/ResourceId",1,2) = 'i-'
				OR SUBSTRING(s."lineItem/ResourceId",1,4) = 'vol-'
			)
		)
		OR s."lineItem/ProductCode" = 'AWSELB'
       OR s."lineItem/ProductCode" = 'AmazonFSx'
	)`
	query := fmt.Sprintf(queryStr, selectStr, formattedStart, formattedEnd)

	processResults := func(reader *csv.Reader) error {
		_, err2 := reader.Read()
		if err2 == io.EOF {
			return nil
		}
		for {
			row, err3 := reader.Read()
			if err3 == io.EOF {
				return nil
			}

			startStr := GetCSVRowValue(row, columnIndexes, S3SelectStartDate)
			itemAccountID := GetCSVRowValue(row, columnIndexes, S3SelectAccountID)
			itemProviderID := GetCSVRowValue(row, columnIndexes, S3SelectResourceID)
			lineItemType := GetCSVRowValue(row, columnIndexes, S3SelectItemType)
			itemProductCode := GetCSVRowValue(row, columnIndexes, S3SelectProductCode)
			usageType := GetCSVRowValue(row, columnIndexes, S3SelectUsageType)

			var (
				amortizedCost float64
				listCost      float64
				netCost       float64
			)
			// Get list and net costs
			listCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectListCost)
			if err != nil {
				return err
			}
			netCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectNetCost)
			if err != nil {
				return err
			}

			// If there is a reservation_reservation_a_r_n on the line item use the awsRIPricingSUMColumn as cost
			if checkReservations && lineItemType == "DiscountedUsage" {
				amortizedCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectRICost)
				if err != nil {
					log.Errorf(err.Error())
					continue
				}
				// If there is a lineItemType of SavingsPlanCoveredUsage use the awsSPPricingSUMColumn
			} else if checkSavingsPlan && lineItemType == "SavingsPlanCoveredUsage" {
				amortizedCost, err = GetCSVRowValueFloat(row, columnIndexes, S3SelectSPCost)
				if err != nil {
					log.Errorf(err.Error())
					continue
				}
			} else {
				// Default to listCost
				amortizedCost = listCost
			}
			category := SelectAWSCategory(itemProviderID, usageType, itemProductCode)
			// Retrieve final stanza of product code for ProviderID
			if itemProductCode == "AWSELB" || itemProductCode == "AmazonFSx" {
				itemProviderID = ParseARN(itemProviderID)
			}

			properties := opencost.CloudCostProperties{}
			properties.Provider = opencost.AWSProvider
			properties.AccountID = itemAccountID
			properties.Category = category
			properties.Service = itemProductCode
			properties.ProviderID = itemProviderID

			itemStart, err := time.Parse(S3SelectDateLayout, startStr)
			if err != nil {
				log.Infof(
					"Unable to parse '%s': '%s'",
					S3SelectStartDate,
					err.Error(),
				)
				itemStart = time.Now()
			}
			itemStart = itemStart.Truncate(time.Hour * 24)
			itemEnd := itemStart.AddDate(0, 0, 1)

			cc := &opencost.CloudCost{
				Properties: &properties,
				Window:     opencost.NewWindow(&itemStart, &itemEnd),
				ListCost: opencost.CostMetric{
					Cost: listCost,
				},
				NetCost: opencost.CostMetric{
					Cost: netCost,
				},
				AmortizedNetCost: opencost.CostMetric{
					Cost: amortizedCost,
				},
				AmortizedCost: opencost.CostMetric{
					Cost: amortizedCost,
				},
				InvoicedCost: opencost.CostMetric{
					Cost: netCost,
				},
			}
			ccsr.LoadCloudCost(cc)
		}
	}
	err = s3si.Query(query, queryKeys, client, processResults)
	if err != nil {
		return nil, err
	}

	return ccsr, nil
}

func (s3si *S3SelectIntegration) GetHeaders(queryKeys []string, client *s3.Client) ([]string, error) {
	// Query to grab only header line from file
	query := "SELECT * FROM S3OBJECT LIMIT 1"
	var record []string

	proccessheaders := func(reader *csv.Reader) error {
		var err error
		record, err = reader.Read()
		if err != nil {
			return err
		}
		return nil
	}

	// Use only the first query key with assumption that files share schema
	err := s3si.Query(query, []string{queryKeys[0]}, client, proccessheaders)
	if err != nil {
		return nil, err
	}

	return record, nil
}
