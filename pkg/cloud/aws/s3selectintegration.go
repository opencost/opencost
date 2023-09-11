package aws

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"github.com/opencost/opencost/pkg/util/timeutil"
)

const s3SelectDateLayout = "2006-01-02T15:04:05Z"

// S3Object is aliased as "s" in queries
const s3SelectAccountID = `s."bill/PayerAccountId"`

const s3SelectItemType = `s."lineItem/LineItemType"`
const s3SelectStartDate = `s."lineItem/UsageStartDate"`
const s3SelectProductCode = `s."lineItem/ProductCode"`
const s3SelectResourceID = `s."lineItem/ResourceId"`

const s3SelectIsNode = `SUBSTRING(s."lineItem/ResourceId",1,2) = 'i-'`
const s3SelectIsVol = `SUBSTRING(s."lineItem/ResourceId", 1, 4) = 'vol-'`
const s3SelectIsNetwork = `s."lineItem/UsageType" LIKE '%Bytes'`

const s3SelectListCost = `s."lineItem/UnblendedCost"`
const s3SelectNetCost = `s."lineItem/NetUnblendedCost"`

// These two may be used for Amortized<Net>Cost
const s3SelectRICost = `s."reservation/EffectiveCost"`
const s3SelectSPCost = `s."savingsPlan/SavingsPlanEffectiveCost"`

type S3SelectIntegration struct {
	S3SelectQuerier
}

func (s3si *S3SelectIntegration) GetCloudCost(
	start,
	end time.Time,
) (*kubecost.CloudCostSetRange, error) {
	log.Infof(
		"S3SelectIntegration[%s]: GetCloudCost: %s",
		s3si.Key(),
		kubecost.NewWindow(&start, &end).String(),
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
	ccsr, err := kubecost.NewCloudCostSetRange(
		start,
		end,
		timeutil.Day,
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
		s3SelectStartDate,
		s3SelectAccountID,
		s3SelectResourceID,
		s3SelectItemType,
		s3SelectProductCode,
		s3SelectIsNode,
		s3SelectIsVol,
		s3SelectIsNetwork,
		s3SelectListCost,
	}
	// OC equivalent to KCM env flags relevant at all?
	// Check for Reservation columns in CUR and query if available
	checkReservations := allColumns[s3SelectRICost]
	if checkReservations {
		selectColumns = append(selectColumns, s3SelectRICost)
	}

	// Check for Savings Plan Columns in CUR and query if available
	checkSavingsPlan := allColumns[s3SelectSPCost]
	if checkSavingsPlan {
		selectColumns = append(selectColumns, s3SelectSPCost)
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

			startStr := GetCSVRowValue(row, columnIndexes, s3SelectStartDate)
			itemAccountID := GetCSVRowValue(row, columnIndexes, s3SelectAccountID)
			itemProviderID := GetCSVRowValue(row, columnIndexes, s3SelectResourceID)
			lineItemType := GetCSVRowValue(row, columnIndexes, s3SelectItemType)
			itemProductCode := GetCSVRowValue(row, columnIndexes, s3SelectProductCode)
			isNode, _ := strconv.ParseBool(GetCSVRowValue(row, columnIndexes, s3SelectIsNode))
			isVol, _ := strconv.ParseBool(GetCSVRowValue(row, columnIndexes, s3SelectIsVol))
			isNetwork, _ := strconv.ParseBool(GetCSVRowValue(row, columnIndexes, s3SelectIsNetwork))
			var (
				amortizedCost float64
				listCost      float64
				netCost       float64
			)
			// Get list and net costs
			listCost, err = GetCSVRowValueFloat(row, columnIndexes, s3SelectListCost)
			if err != nil {
				return err
			}
			netCost, err = GetCSVRowValueFloat(row, columnIndexes, s3SelectNetCost)
			if err != nil {
				return err
			}

			// If there is a reservation_reservation_a_r_n on the line item use the awsRIPricingSUMColumn as cost
			if checkReservations && lineItemType == "DiscountedUsage" {
				amortizedCost, err = GetCSVRowValueFloat(row, columnIndexes, s3SelectRICost)
				if err != nil {
					log.Errorf(err.Error())
					continue
				}
				// If there is a lineItemType of SavingsPlanCoveredUsage use the awsSPPricingSUMColumn
			} else if checkSavingsPlan && lineItemType == "SavingsPlanCoveredUsage" {
				amortizedCost, err = GetCSVRowValueFloat(row, columnIndexes, s3SelectSPCost)
				if err != nil {
					log.Errorf(err.Error())
					continue
				}
			} else {
				// Default to listCost
				amortizedCost = listCost
			}
			category := SelectAWSCategory(isNode, isVol, isNetwork, itemProductCode, "")
			// Retrieve final stanza of product code for ProviderID
			if itemProductCode == "AWSELB" || itemProductCode == "AmazonFSx" {
				itemProviderID = ParseARN(itemProviderID)
			}

			properties := kubecost.CloudCostProperties{}
			properties.Provider = kubecost.AWSProvider
			properties.AccountID = itemAccountID
			properties.Category = category
			properties.Service = itemProductCode
			properties.ProviderID = itemProviderID

			itemStart, err := time.Parse(s3SelectDateLayout, startStr)
			if err != nil {
				log.Infof(
					"Unable to parse '%s': '%s'",
					s3SelectStartDate,
					err.Error(),
				)
				itemStart = time.Now()
			}
			itemStart = itemStart.Truncate(time.Hour * 24)
			itemEnd := itemStart.AddDate(0, 0, 1)

			cc := &kubecost.CloudCost{
				Properties: &properties,
				Window:     kubecost.NewWindow(&itemStart, &itemEnd),
				ListCost: kubecost.CostMetric{
					Cost: listCost,
				},
				NetCost: kubecost.CostMetric{
					Cost: netCost,
				},
				AmortizedNetCost: kubecost.CostMetric{
					Cost: amortizedCost,
				},
				AmortizedCost: kubecost.CostMetric{
					Cost: amortizedCost,
				},
				InvoicedCost: kubecost.CostMetric{
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
