package googlePusher

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/servicecontrol/v1"
	"google.golang.org/api/transport"
)

type Operation servicecontrol.Operation

type MetricValue *servicecontrol.MetricValue
type MetricValueSet servicecontrol.MetricValueSet

var (
	re = regexp.MustCompile(`^\d+$`)
)

type ResourceInfo struct {
	CustomerId   string
	ResourceId   string
	ResourceName string
}

type GoogleMetricsClient struct {
	rootURL                 string
	nameAndKeyLabelOfMetric map[metadata.CombinedKeyResourceTypeMeasuredType]common.Triple
	logger                  log.Logger
	config                  *common.TelemetryConfig
}

func NewGoogleMetricsClient(ctx context.Context, rootURL string, config *common.TelemetryConfig) *GoogleMetricsClient {
	return &GoogleMetricsClient{
		rootURL:                 rootURL,
		nameAndKeyLabelOfMetric: common.CreateMetricsMappingMap(),
		logger:                  util.GetLogger(ctx),
		config:                  config,
	}
}

func (client *GoogleMetricsClient) ReportMetrics(ctx context.Context, metrics []common.GoogleMetric, operationStartTime, operationEndTime int64, wg *sync.WaitGroup, resultChan chan []common.MetricsResult) {
	defer wg.Done()
	defer close(resultChan)

	// Extract correlation ID from context for logging
	logger := util.GetLogger(ctx)
	correlationID := "unknown"
	if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if corrIDStr, exists := loggerFields["requestCorrelationID"].(string); exists {
			correlationID = corrIDStr
		}
	}

	logger.Infof("Starting metrics reporting with correlation ID: %s, metrics count: %d", correlationID, len(metrics))

	if len(metrics) == 0 {
		logger.Info("No metrics to report")
		return
	}
	operationsToPush := client.createOperationsForMetrics(metrics, operationStartTime, operationEndTime)

	if len(operationsToPush) == 0 {
		logger.Debugf("No operations to push for metrics")
		return
	}

	var operationBatchList [][]*Operation
	operationMap := make(map[string]*Operation)

	logger.Infof("Operation batch size is %d", client.config.OperationBatchSize)
	var tempOperationList []*Operation
	var i int64 = 0

	for operation := range operationsToPush {
		if operation != nil {
			tempOperationList = append(tempOperationList, operation)
			i++
			if i == client.config.OperationBatchSize {
				operationBatchList = append(operationBatchList, tempOperationList)
				tempOperationList = []*Operation{}
				i = 0
			}
			operationMap[operation.OperationId] = operation
		}
	}

	if i != 0 && i < client.config.OperationBatchSize {
		operationBatchList = append(operationBatchList, tempOperationList)
	}
	client.reportOperationList(ctx, operationBatchList, operationsToPush, operationMap, resultChan)
}

func (client *GoogleMetricsClient) reportOperationList(ctx context.Context, operationBatchList [][]*Operation, operationsToPush map[*Operation][]common.GoogleMetric, operationMap map[string]*Operation, resultChan chan []common.MetricsResult) {
	logger := util.GetLogger(ctx)

	for _, operationList := range operationBatchList {
		var results []common.MetricsResult
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("Exception %v is thrown while reporting the operation list %v", r, operationList)
					for _, operation := range operationList {
						for _, reportedMetric := range operationsToPush[operation] {
							results = append(results, common.MetricsResult{
								GoogleMetric:  reportedMetric,
								Exception:     fmt.Errorf("%v", r),
								OperationID:   operation.OperationId,
								OperationName: operation.OperationName,
							})
						}
					}
				}
			}()

			// logger.Infof("For service: %s; Google Metrics Operation list is %v", client.config.PusherServiceName, spew.Sdump(operationList))

			serviceControl, err := createServiceControlClient(client.config.PusherServiceProject, client.rootURL, logger)
			if err != nil {
				logger.Errorf("Could not create Service Control Client: %v", err)
				return
			}
			response, err := client.reportOperation(operationList, serviceControl, logger)
			if err != nil {
				logger.Errorf("Error reporting operation list: %v", err)
			}

			logger.Infof("Report response: %v", response.ReportErrors)
			responseWithError := &common.ReportResponse{}
			responseWithNoError := &common.ReportResponse{}
			if len(response.ReportErrors) != 0 {
				for _, reportError := range response.ReportErrors {
					responseWithError.ReportErrors = append(responseWithError.ReportErrors, reportError)
					operation := operationMap[reportError.OperationId]
					for _, reportedMetric := range operationsToPush[operationMap[reportError.OperationId]] {
						results = append(results, common.MetricsResult{
							GoogleMetric:   reportedMetric,
							ReportResponse: responseWithError,
							OperationID:    reportError.OperationId,
							OperationName:  operation.OperationName,
						})
					}
					operationList = removeOperation(operationList, operationMap[reportError.OperationId])
				}
			}

			for _, operation := range operationList {
				for _, reportedMetric := range operationsToPush[operation] {
					responseWithNoError.ReportErrors = nil
					results = append(results, common.MetricsResult{
						GoogleMetric:   reportedMetric,
						ReportResponse: responseWithNoError,
						OperationID:    operation.OperationId,
						OperationName:  operation.OperationName,
					})
				}
			}
		}()
		resultChan <- results
	}
}

func (client *GoogleMetricsClient) createOperationsForMetrics(metrics []common.GoogleMetric, opStart, opEnd int64) map[*Operation][]common.GoogleMetric {
	metricsByVolume := make(map[ResourceInfo][]common.GoogleMetric)
	for _, metric := range metrics {
		customerId, err := metric.GetCustomerId()
		if err != nil {
			client.logger.Errorf("Error getting customer ID: %v", err)
			continue
		}

		resourceName, err := metric.GetResourceName()
		if err != nil {
			client.logger.Errorf("Error getting resource ID: %v", err)
			continue
		}

		info := ResourceInfo{
			CustomerId:   customerId,
			ResourceName: resourceName,
		}
		metricsByVolume[info] = append(metricsByVolume[info], metric)
	}

	operationAndMetrics := make(map[*Operation][]common.GoogleMetric)

	for info, googleMetrics := range metricsByVolume {
		if len(googleMetrics) == 0 {
			client.logger.Debugf("There were zero Google metrics to report for resource %s", info.ResourceId)
			continue
		}

		totalDropCount := 0
		partitionedMetrics := partitionMetrics(googleMetrics, client.logger)
		for _, partition := range partitionedMetrics {
			operation, droppedMetrics, err := client.createOperationForMetric(uuid.New().String(), partition, info.CustomerId, info.ResourceId, opStart, opEnd)
			if err != nil {
				client.logger.Errorf("Operation creation for %s failed: %v", info.ResourceId, err)
				continue
			}

			totalDropCount += len(droppedMetrics)

			if operation != nil && len(operation.MetricValueSets) > 0 {
				operationAndMetrics[operation] = partition
			} else {
				client.logger.Warnf("Operation creation method succeeded, but no operation returned. ResourceId", info.ResourceId)
			}
		}

		if totalDropCount > 0 {
			client.logger.Infof("Dropped %d ignored or invalid metrics from operations this run.", totalDropCount)
		}
	}
	return operationAndMetrics
}

func (client *GoogleMetricsClient) createOperationForMetric(operationId string, googleMetrics []common.GoogleMetric, customerId string, resourceUuid string, opStart int64, opEnd int64) (*Operation, []common.GoogleMetric, error) {
	if len(googleMetrics) == 0 {
		return nil, nil, nil
	}

	op := &Operation{}
	googleMetric := googleMetrics[0]
	metricType := googleMetric.GetType()
	dataCenter := client.config.RegionName

	err := SetCommonLabels(op, customerId, dataCenter, resourceUuid, googleMetric)
	if err != nil {
		return nil, nil, err
	}

	if metricType == common.BillingMetric { // TODO Implement Billing Labels
		if err != nil {
			client.logger.Warnf("Error getting billing labels: %v", err)
		}
	}

	op.OperationName = fmt.Sprintf("OperationMetrics_%s-%d", resourceUuid, opStart)
	op.OperationId = operationId
	op.StartTime = time.Unix(opStart, 0).Format(time.RFC3339)
	op.EndTime = time.Unix(opEnd, 0).Format(time.RFC3339)

	consumerId := toGoogleProject(customerId)
	if consumerId != "" {
		op.ConsumerId = consumerId
	} else {
		client.logger.Errorf("Consumer ID is null. Aborting operation creation.")
		return nil, nil, nil
	}

	var metricValueSets []*MetricValueSet
	metricsByName := make(map[string][]common.GoogleMetric)
	droppedMetrics := make(map[metadata.MeasuredType][]common.GoogleMetric)

	for _, value := range metadata.CombinedKeyResourceTypeMeasuredTypeMap {
		droppedMetrics[value.MeasuredType] = []common.GoogleMetric{}
	}

	for _, metric := range googleMetrics {
		googleMetricName, err := client.GetMetricName(metric)
		if err != nil {
			resourceType, _ := metric.GetResourceType()
			measuredType, _ := metric.GetMeasuredType()
			client.logger.Errorf("Valid google endpoint not found for metric for resource type %s and measured type %s", resourceType, measuredType)
		}
		if googleMetricName != "" {
			metricsByName[googleMetricName] = append(metricsByName[googleMetricName], metric)
		} else {
			measuredType, _ := metric.GetMeasuredType()
			droppedMetrics[measuredType] = append(droppedMetrics[measuredType], metric)
		}
	}

	for metricName, metric := range metricsByName {
		metricValueSet, err := client.createMetricValueSet(metricName, metric)

		if err != nil {
			return nil, nil, err
		}
		metricValueSets = append(metricValueSets, metricValueSet)
	}

	var serviceControlMetricValueSets []*servicecontrol.MetricValueSet
	for _, mvs := range metricValueSets {
		serviceControlMetricValueSets = append(serviceControlMetricValueSets, (*servicecontrol.MetricValueSet)(mvs))
	}

	logDroppedMetrics(droppedMetrics, client.logger)
	op.MetricValueSets = serviceControlMetricValueSets

	return op, flattenDroppedMetrics(droppedMetrics), nil
}

func flattenDroppedMetrics(droppedMetrics map[metadata.MeasuredType][]common.GoogleMetric) []common.GoogleMetric {
	var result []common.GoogleMetric
	for _, droppedMetric := range droppedMetrics {
		result = append(result, droppedMetric...)
	}
	return result
}

func partitionMetrics(googleMetrics []common.GoogleMetric, logger log.Logger) [][]common.GoogleMetric {
	if !hasDuplicateMeasuredTypes(googleMetrics, logger) {
		return [][]common.GoogleMetric{googleMetrics}
	}

	metricsByMeasuredType := make(map[metadata.MeasuredType][]common.GoogleMetric)
	for _, metric := range googleMetrics {
		measuredType, err := metric.GetMeasuredType()
		if err != nil {
			logger.Debugf("Error getting measured type in partition metrics method.", "error", err, "metric", metric)
			continue
		}
		metricsByMeasuredType[measuredType] = append(metricsByMeasuredType[measuredType], metric)
	}

	var partitionedMetrics [][]common.GoogleMetric
	for len(metricsByMeasuredType) > 0 {
		var partition []common.GoogleMetric
		for _, value := range metadata.CombinedKeyResourceTypeMeasuredTypeMap {
			if metricsOfType, exists := metricsByMeasuredType[value.MeasuredType]; exists && len(metricsOfType) > 0 {
				metric := metricsOfType[0]
				metricsByMeasuredType[value.MeasuredType] = metricsOfType[1:]
				if len(metricsByMeasuredType[value.MeasuredType]) == 0 {
					delete(metricsByMeasuredType, value.MeasuredType)
				}
				partition = append(partition, metric)
			}
		}
		partitionedMetrics = append(partitionedMetrics, partition)
	}
	return partitionedMetrics
}

func hasDuplicateMeasuredTypes(googleMetrics []common.GoogleMetric, logger log.Logger) bool {
	measuredTypeMap := make(map[metadata.MeasuredType]bool)
	for _, metric := range googleMetrics {
		measuredType, err := metric.GetMeasuredType()
		if err != nil {
			logger.Debugf("Error getting measured type in has duplicate measured types method.", "error", err, "metric", metric)
			continue
		}
		val, ok := measuredTypeMap[measuredType]
		if ok && val {
			return true
		}
		measuredTypeMap[measuredType] = true
	}
	return false
}

func logDroppedMetrics(droppedMetrics map[metadata.MeasuredType][]common.GoogleMetric, logger log.Logger) {
	for measuredType, metric := range droppedMetrics {
		logger.Debugf("Dropped %d metric of unsupported metric type %s.", len(metric), measuredType)
	}
}

func (client *GoogleMetricsClient) createMetricValueSet(metricName string, metrics []common.GoogleMetric) (*MetricValueSet, error) {
	if len(metrics) == 0 {
		return nil, nil
	}

	metricValueSet := &MetricValueSet{
		MetricName:   metricName,
		MetricValues: []*servicecontrol.MetricValue{},
	}

	for _, metric := range metrics {
		metricValue, err := client.CreateMetricValue(metric)
		if err != nil {
			return nil, fmt.Errorf("error creating metric value for metric %s", metricName)
		}
		metricValueSet.MetricValues = append(metricValueSet.MetricValues, metricValue)
	}
	return metricValueSet, nil
}

func (client *GoogleMetricsClient) CreateMetricValue(metric common.GoogleMetric) (MetricValue, error) {
	metricValue := &servicecontrol.MetricValue{}
	mibToKibConverter := 1024
	metricMeasuredType, _ := metric.GetMeasuredType()

	switch metricMeasuredType {
	case metadata.VolumeAllocatedThroughput:
		volAllocatedThroughputInMibPerSec, _ := metric.GetDoubleQuantity()
		volAllocatedThroughputInKibPerSec := int64(volAllocatedThroughputInMibPerSec * float64(mibToKibConverter))
		metricValue.Int64Value = &volAllocatedThroughputInKibPerSec
	default:
		val, _ := metric.GetQuantity()
		metricValue.Int64Value = nillable.ToPointer(val)

		client.logger.Debugf("Set Metric Value for MeasuredType %s as %d", metricMeasuredType, val)
	}

	startTime, err := metric.GetStartTime()
	if err != nil {
		client.logger.Errorf("error getting start time for MeasuredType %s", metricMeasuredType)
	}

	var endTime int64
	if metric.GetType() == common.HydratedMetric {
		secondsRemaining := startTime % 60
		startTime -= secondsRemaining
		endTime = startTime + 59
	} else {
		endTime, err = metric.GetEndTime()
		if err != nil {
			return nil, err
		}
	}

	metricValue.StartTime = time.Unix(startTime, 0).Format(time.RFC3339)
	metricValue.EndTime = time.Unix(endTime, 0).Format(time.RFC3339)

	labelKeys := GetLabelKey(metric)
	if err != nil {
		return nil, err
	}
	valueLabels := make(map[string]string)
	if labelKeys != nil {
		for _, labelKey := range labelKeys {
			metricLabelValue, err := GetLabelValue(labelKey, metric, client.logger)
			if err != nil {
				return nil, err
			}
			if metricLabelValue != "" {
				valueLabels[labelKey] = metricLabelValue
			}
		}
	}
	metricValue.Labels = valueLabels

	client.logger.Debugf("Created metric value - StartTime: %s, EndTime: %s, Labels: %d, Value: %v",
		metricValue.StartTime, metricValue.EndTime, len(metricValue.Labels), getMetricValueSummary(metricValue))
	return metricValue, nil
}

// getMetricValueSummary returns a user-friendly summary of the metric value
func getMetricValueSummary(mv *servicecontrol.MetricValue) interface{} {
	if mv.Int64Value != nil {
		return *mv.Int64Value
	}
	if mv.DoubleValue != nil {
		return *mv.DoubleValue
	}
	if mv.StringValue != nil {
		return *mv.StringValue
	}
	if mv.BoolValue != nil {
		return *mv.BoolValue
	}
	return "unknown"
}

func SetCommonLabels(op *Operation, consumerId, dataCenter, resourceId string, googleMetric common.GoogleMetric) error {
	labels := make(map[string]string)

	metricType := googleMetric.GetType()
	if metricType == common.BillingMetric {
		labels["cloud.googleapis.com/location"] = dataCenter
	} else {
		labels["location"] = dataCenter
		labels["resource_container"] = "projects/" + consumerId
		labels["name"], _ = googleMetric.GetResourceName()
	}
	op.Labels = labels
	return nil
}

func toGoogleProject(customerId string) string {
	if customerId == "" {
		return ""
	}

	if startsWith(customerId, "project:") || startsWith(customerId, "project_number:") {
		return customerId
	} else {
		if isNumeric(customerId) {
			return "project_number:" + customerId
		} else {
			return "project:" + customerId
		}
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func isNumeric(s string) bool {
	return re.MatchString(s)
}

func createServiceControlClient(serviceProject string, rootUrl string, logger log.Logger) (*servicecontrol.Service, error) {
	ctx := context.Background()
	httpClient, _, err := transport.NewHTTPClient(ctx, option.WithTokenSource(google.ComputeTokenSource("", servicecontrol.ServicecontrolScope)))
	if err != nil {
		logger.Errorf("Failed to create http client for creating service control: %v", err)
		return nil, err
	}

	service, err := servicecontrol.NewService(ctx,
		option.WithHTTPClient(httpClient),
		option.WithEndpoint(rootUrl),
		option.WithScopes(servicecontrol.ServicecontrolScope),
	)
	if err != nil {
		logger.Errorf("Failed to create service control client: %v", err)
		return nil, err
	}

	logger.Infof("Created client with workload identity: serviceProject: %s, rootUrl: %s", serviceProject, rootUrl)
	return service, nil
}

func (client *GoogleMetricsClient) reportOperation(operationBatchList []*Operation, serviceControl *servicecontrol.Service, logger log.Logger) (*servicecontrol.ReportResponse, error) {
	if operationBatchList == nil {
		return nil, errors.New("operation batch list was nil. This operation list was dropped")
	}
	var serviceControlOperations []*servicecontrol.Operation
	for _, op := range operationBatchList {
		serviceControlOperations = append(serviceControlOperations, (*servicecontrol.Operation)(op))
	}
	req := &servicecontrol.ReportRequest{
		Operations: serviceControlOperations,
	}
	logger.Info("Reporting operation to service control")
	report := serviceControl.Services.Report(client.config.PusherServiceName, req)
	return report.Do()
}

func removeOperation(operations []*Operation, operation *Operation) []*Operation {
	if operation == nil {
		return operations
	}
	for i, op := range operations {
		if op.OperationId == operation.OperationId {
			return append(operations[:i], operations[i+1:]...)
		}
	}
	return operations
}

func (client *GoogleMetricsClient) GetMetricName(metric common.GoogleMetric) (string, error) {
	resourceType, _ := metric.GetResourceType()
	measuredType, _ := metric.GetMeasuredType()
	if metric.GetType() == common.BillingMetric {
		jobDef, exists := common.DefaultAggregationJobDefinitions[metadata.CombinedKeyResourceTypeMeasuredType{ResourceType: resourceType, MeasuredType: measuredType}]
		if !exists {
			client.logger.Warnf("No job definition found for resource type %s and measured type %s", resourceType, measuredType)
			return "", fmt.Errorf("unsupported measured type or resource type received: %s, %s", measuredType, resourceType) // If the key does not exist
		}
		return common.BillingMetricsNamePrefix + jobDef.SKU, nil
	} else {
		nameAndKeyLabel, exists := client.nameAndKeyLabelOfMetric[metadata.CombinedKeyResourceTypeMeasuredType{ResourceType: resourceType, MeasuredType: measuredType}]
		if !exists {
			return "", fmt.Errorf("unsupported measured type or resource type received: %s, %s", measuredType, resourceType)
		}

		var metricsName string
		switch resourceType {
		case metadata.VolumePool, metadata.VolumePoolRegionalHA:
			metricsName = metadata.MetricsNamePrefixPoolFirstParty + nameAndKeyLabel.Left
		case metadata.Volume, metadata.VolumeRegionalHA:
			metricsName = metadata.MetricsNamePrefixVolumeFirstParty + nameAndKeyLabel.Left
		default:
			return "", fmt.Errorf("unrecognized resource type: %s", resourceType)
		}
		return metricsName, nil
	}
}

func GetLabelKey(metric common.GoogleMetric) []string {
	metricMeasuredType, _ := metric.GetMeasuredType()
	metricResourceType, _ := metric.GetResourceType()
	switch metricResourceType {
	case metadata.VolumeReplicationRelationship:
		return []string{"/resource_id", "/replication/frequency", "/replication/source_continent", "/replication/destination_continent", "/replication/source_service_level", "/replication/destination_service_level"}
	case metadata.Volume:
		switch metricMeasuredType {
		case metadata.CbsVolumeBackupSize:
			return []string{"/resource_id", "/backups/location"}
		}
	}
	return nil
}

func GetLabelValue(key string, metric common.GoogleMetric, logger log.Logger) (string, error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Infof("Recovered in GetLabelValue: %v", r)
		}
	}()

	metricResourceType, _ := metric.GetResourceType()
	switch metricResourceType {
	case metadata.VolumeReplicationRelationship:
		switch key {
		case "/resource_id":
			return "dummyLabelValue", nil
		case "/replication/frequency":
			return "dummyLabelValue", nil
		case "/replication/source_continent":
			return "dummyLabelValue", nil
		case "/replication/destination_continent":
			return "dummyLabelValue", nil
		case "/replication/source_service_level", "/replication/destination_service_level":
			return "dummyLabelValue", nil
		}
	}
	return "", nil
}
