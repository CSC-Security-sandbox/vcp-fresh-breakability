package usage

import (
	"context"
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
	"strings"
	"sync"
	"time"

	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/googlePusher"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// GoogleUsageSink is responsible for delivering metrics to Google.
type GoogleUsageSink struct {
	metricClient googlePusher.GoogleMetricsClient
	metricsdb    database2.Storage
	logger       log.Logger
	config       *common.TelemetryConfig
}

func NewSink(ctx context.Context, config *common.TelemetryConfig, metricsdb database2.Storage) *GoogleUsageSink {
	return &GoogleUsageSink{
		metricClient: *googlePusher.NewGoogleMetricsClient(ctx, config.RootUrl, config),
		logger:       util.GetLogger(ctx),
		metricsdb:    metricsdb,
		config:       config,
	}
}

func (s *GoogleUsageSink) DeliverMetrics(ctx context.Context, aggregatedRecords []datamodel.AggregatedUsage) (count int, err error) {
	s.logger.Debugf("MapAggregatedRecordsToBilling: Received Number of records to map", "Number of records:", len(aggregatedRecords))
	validUsage, err := s.filterValidUsage(aggregatedRecords)
	if err != nil {
		return 0, err
	}
	if len(validUsage) != len(aggregatedRecords) {
		s.logger.Errorf("In total, records were dropped during mapping because they were invalid.",
			"Dropped Records", len(aggregatedRecords)-len(validUsage))
	}
	googleMetrics := s.completeRecords(validUsage)
	s.processGcpUnifiedMetrics(ctx, googleMetrics)
	return len(validUsage), nil
}

func (s *GoogleUsageSink) completeRecords(records []datamodel.AggregatedUsage) []common.GoogleMetric {
	var firstPartyGoogleMetrics []common.GoogleMetric
	for _, record := range records {
		switch record.MeasuredType {
		case metadata.BackupLogicalSize:
			record.Quantity = float64(utils2.MibtoKib(record.Quantity))
		case metadata.BackupEnabledVolumeAllocatedSize:
			record.Quantity = float64(utils2.MibHoursToGibHoursWithRoundOff(record.Quantity))
		default:
			record.Quantity = float64(utils2.MibHoursToGibHours(record.Quantity))
		}

		googleMetric := &common.GoogleMetric{Record: &record}
		missingFields := googleMetric.Validate()

		if len(missingFields) == 0 {
			s.logger.Debugf("Google Usage Mapping with ID is ready for billing", "Record ID: ", record.ID)
			firstPartyGoogleMetrics = append(firstPartyGoogleMetrics, *googleMetric)
		} else {
			record.State = datamodel.Invalid
			s.logger.Errorf("Google Usage Mapping with ID %d failed GoogleMetric validation: missing fields %s", record.ID, strings.Join(missingFields, ", "))
		}
	}
	return firstPartyGoogleMetrics
}

func (s *GoogleUsageSink) filterValidUsage(aggregatedRecords []datamodel.AggregatedUsage) ([]datamodel.AggregatedUsage, error) {
	validUsage := make([]datamodel.AggregatedUsage, 0, len(aggregatedRecords))
	for _, usage := range aggregatedRecords {
		u := usage
		if !u.IsBillable {
			s.logger.Debugf("Skipping usage: Not mapping usage record as it is non-billable usage.",
				"Record ID", u.ID)
			continue
		}
		if u.ID == 0 {
			s.logger.Errorf("Skipping usage: Not mapping usage record due to unset id.", "Record ID", u.ID)
			continue
		}
		if s.isValid(u) {
			validUsage = append(validUsage, u)
		}
	}
	if len(validUsage) != len(aggregatedRecords) {
		s.logger.Errorf("Found records that are not appropriate for billing. Not mapping them. Number of records: %d", len(aggregatedRecords)-len(validUsage))
	}
	return validUsage, nil
}

func (s *GoogleUsageSink) isValid(usage datamodel.AggregatedUsage) bool {
	if usage.VendorCustomerID == nil || *usage.VendorCustomerID == "" {
		s.logger.Errorf("Skipping usage: Not mapping usage record due to missing project ID/number.",
			"Record ID", usage.ID)
		return false
	}
	return true
}

func (s *GoogleUsageSink) processGcpUnifiedMetrics(ctx context.Context, googleMetrics []common.GoogleMetric) {
	if len(googleMetrics) == 0 {
		s.logger.Info("No Google usage metrics processed in this run.")
		return
	}

	s.push(ctx, googleMetrics)
}

func (s *GoogleUsageSink) push(ctx context.Context, googleMetrics []common.GoogleMetric) {
	wg := sync.WaitGroup{}
	fpResultChan := make(chan []common.MetricsResult)
	if len(googleMetrics) > 0 {
		wg.Add(3)
		go func() {
			defer wg.Done()
			s.logger.Debugf("Reporting First Party Google Billing Metrics", "Google Metric First Party list count: ", len(googleMetrics))
			go s.metricClient.ReportMetrics(ctx, googleMetrics, time.Now().Unix(), time.Now().Unix(), &wg, fpResultChan)
			go s.processResponse(ctx, &wg, fpResultChan)
		}()
	} else {
		s.logger.Warn("Google first party billing metrics not found, hence not reporting anything.")
	}
	wg.Wait()
}

func (s *GoogleUsageSink) processResponse(ctx context.Context, wg *sync.WaitGroup, resultChan chan []common.MetricsResult) {
	defer wg.Done()

	// Extract correlation ID from context for logging
	logger := util.GetLogger(ctx)
	correlationID := "unknown"
	if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if corrIDStr, exists := loggerFields["requestCorrelationID"].(string); exists {
			correlationID = corrIDStr
		}
	}

	logger.Infof("Processing usage metrics results with correlation ID: %s", correlationID)

	for result := range resultChan {
		s.processMetricsResults(ctx, result)
	}

	logger.Info("Finished processing Google Usage Metrics Results")
}

func (s *GoogleUsageSink) processMetricsResults(ctx context.Context, gcpResults []common.MetricsResult) {
	goodResults, errorResults, exceptionResults := common.FilterErrorResults(gcpResults)

	s.logger.Infof("%d metrics were successfully reported.", len(goodResults))
	s.logger.Infof("%d metrics were not reported.", len(errorResults)+len(exceptionResults))

	for _, result := range gcpResults {
		billingRecord, err := result.GoogleMetric.GetAsUsageBillingMetric()
		if err != nil {
			s.logger.Warnf("It appears that a non-billing Google Metric was reported by the billing paradigm. "+
				"This should not happen, and its response cannot be processed.", "The error is:", err,
				"The Google Metric is:", result)
			continue
		}

		resultCode := common.GetCode(&result)
		s.logger.Debugf("Processing the Google Metric Result for service: %s; result is %v, resultCode: %s", s.config.PusherServiceName, spew.Sdump(result), resultCode)
		billingRecord.Submission = nillable.ToPointer(result.GetOperationID())
		errorMessage := ""
		if isSuccessful(result.ReportResponse) {
			billingRecord.State = datamodel.Submitted
			billingRecord.ErrorMessage = nil
		} else {
			errorMessage = common.GetErrorMessage(&result)
			billingRecord.ErrorMessage = &errorMessage
			billingRecord.ErrorCount = billingRecord.ErrorCount + 1
			billingRecord.State = datamodel.Error
		}
		s.logger.Infof("Updating usage information for billingRecord ID: %d, state: %s", billingRecord.ID, billingRecord.State)

		// Create update map with only the fields that need to be updated
		updates := map[string]interface{}{
			"state":         billingRecord.State,
			"error_message": billingRecord.ErrorMessage,
			"error_count":   billingRecord.ErrorCount,
		}

		// Handle submission field - it's a JSONB field in database, so JSON-encode the string value
		if billingRecord.Submission != nil {
			// JSON-encode the UUID string for JSONB storage
			submissionJSON, err := json.Marshal(*billingRecord.Submission)
			if err != nil {
				s.logger.Warnf("Error marshaling submission for database update - submission: %s, error: %v", *billingRecord.Submission, err)
			} else {
				updates["submission"] = string(submissionJSON)
			}
		}

		if err := s.metricsdb.UpdateAggregatedUsage(ctx, billingRecord.ID, updates); err != nil {
			s.logger.Warnf("Error updating usage information - billingRecord ID: %d, error: %v", billingRecord.ID, err)
		}
	}
}

func isSuccessful(response *common.ReportResponse) bool {
	return response != nil && (len(response.ReportErrors) == 0)
}
