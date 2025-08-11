package googlePusher

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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

func (client *GoogleMetricsClient) ReportMetrics(metrics []entity.HydratedMetric, operationStartTime, operationEndTime int64, wg *sync.WaitGroup, resultChan chan []common.MetricsResult) {
	defer wg.Done()
	defer close(resultChan)

	client.logger.Debugf("In Report metrics")
	if len(metrics) == 0 {
		client.logger.Info("No metrics to report")
		return
	}
	operationsToPush := client.createOperationsForMetrics(metrics, operationStartTime, operationEndTime)

	if len(operationsToPush) == 0 {
		client.logger.Debugf("No operations to push for metrics")
		return
	}

	var operationBatchList [][]*Operation
	operationMap := make(map[string]*Operation)

	client.logger.Infof("Operation batch size is %d", client.config.OperationBatchSize)
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
	client.reportOperationList(operationBatchList, operationsToPush, operationMap, resultChan)
}

func (client *GoogleMetricsClient) reportOperationList(operationBatchList [][]*Operation, operationsToPush map[*Operation][]entity.HydratedMetric, operationMap map[string]*Operation, resultChan chan []common.MetricsResult) {
	for _, operationList := range operationBatchList {
		var results []common.MetricsResult
		func() {
			defer func() {
				if r := recover(); r != nil {
					client.logger.Errorf("Exception %v is thrown while reporting the operation list %v", r, operationList)
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

			client.logger.Infof("For service: %s; Google Metrics Operation list is %v", client.config.PusherServiceName, spew.Sdump(operationList))

			serviceControl, err := createServiceControlClient(client.config.PusherServiceProject, client.rootURL, client.logger)
			if err != nil {
				client.logger.Errorf("Could not create Service Control Client. Error:", err)
				return
			}
			response, err := client.reportOperation(operationList, serviceControl, client.logger)
			if err != nil {
				client.logger.Errorf("Error reporting operation list: %v", err)
			}

			client.logger.Infof("Report response: %v", response.ReportErrors)
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
func (client *GoogleMetricsClient) createOperationsForMetrics(metrics []entity.HydratedMetric, opStart, opEnd int64) map[*Operation][]entity.HydratedMetric {
	metricsByVolume := make(map[ResourceInfo][]entity.HydratedMetric)
	var customerId, resourceId string
	for _, metric := range metrics {
		if metric.Metadata.AccountName == nil || metric.Metadata.ResourceUUID == nil {
			client.logger.Warnf("Metric metadata missing AccountUUID or ResourceUUID: %v", metric)
			continue
		}
		if metric.Metadata.AccountName != nil {
			customerId = *metric.Metadata.AccountName
		}
		if metric.Metadata.ResourceUUID != nil {
			resourceId = *metric.Metadata.ResourceUUID
		}

		info := ResourceInfo{
			CustomerId: customerId,
			ResourceId: resourceId,
		}
		if metric.Metadata.ResourceDisplayName != nil {
			info.ResourceName = *metric.Metadata.ResourceDisplayName
		}

		metricsByVolume[info] = append(metricsByVolume[info], metric)
	}

	operationAndMetrics := make(map[*Operation][]entity.HydratedMetric)

	for info, googleMetrics := range metricsByVolume {
		if len(googleMetrics) == 0 {
			client.logger.Debugf("There were zero Google metrics to report for resource %s", info.ResourceId)
			continue
		}

		totalDropCount := 0
		partitionedMetrics := partitionMetrics(googleMetrics)
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

func (client *GoogleMetricsClient) createOperationForMetric(operationId string, googleMetrics []entity.HydratedMetric, customerId string, resourceUuid string, opStart int64, opEnd int64) (*Operation, []entity.HydratedMetric, error) {
	if len(googleMetrics) == 0 {
		return nil, nil, nil
	}

	op := &Operation{}
	googleMetric := googleMetrics[0]
	var dataCenter string
	if googleMetric.Metadata.RegionName != nil {
		dataCenter = *googleMetric.Metadata.RegionName
	}

	err := SetCommonLabels(op, customerId, dataCenter, resourceUuid, googleMetric)
	if err != nil {
		return nil, nil, err
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
	metricsByName := make(map[string][]entity.HydratedMetric)
	droppedMetrics := make(map[metadata.MeasuredType][]entity.HydratedMetric)

	for _, CombinedMap := range metadata.CombinedKeyResourceTypeMeasuredTypeMap {
		droppedMetrics[CombinedMap.MeasuredType] = []entity.HydratedMetric{}
	}

	for _, metric := range googleMetrics {
		googleMetricName, err := client.GetMetricName(metric.MeasuredType, metric.Metadata.ResourceType)
		if err != nil {
			client.logger.Errorf("Valid google endpoint not found for metric for resource type %s and measured type %s", metric.Metadata.ResourceType, metric.MeasuredType)
		}
		if googleMetricName != "" {
			metricsByName[googleMetricName] = append(metricsByName[googleMetricName], metric)
		} else {
			droppedMetrics[metric.MeasuredType] = append(droppedMetrics[metric.MeasuredType], metric)
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

	// logDroppedMetrics(droppedMetrics, logger)
	op.MetricValueSets = serviceControlMetricValueSets

	return op, flattenDroppedMetrics(droppedMetrics), nil
}

func flattenDroppedMetrics(droppedMetrics map[metadata.MeasuredType][]entity.HydratedMetric) []entity.HydratedMetric {
	var result []entity.HydratedMetric
	for _, droppedMetric := range droppedMetrics {
		result = append(result, droppedMetric...)
	}
	return result
}

func partitionMetrics(googleMetrics []entity.HydratedMetric) [][]entity.HydratedMetric {
	if !hasDuplicateMeasuredTypes(googleMetrics) {
		return [][]entity.HydratedMetric{googleMetrics}
	}

	metricsByMeasuredType := make(map[metadata.MeasuredType][]entity.HydratedMetric)
	for _, metric := range googleMetrics {
		measuredType := metric.MeasuredType
		metricsByMeasuredType[measuredType] = append(metricsByMeasuredType[measuredType], metric)
	}

	var partitionedMetrics [][]entity.HydratedMetric
	for len(metricsByMeasuredType) > 0 {
		var partition []entity.HydratedMetric
		for _, CombinedType := range metadata.CombinedKeyResourceTypeMeasuredTypeMap {
			if metricsOfType, exists := metricsByMeasuredType[CombinedType.MeasuredType]; exists && len(metricsOfType) > 0 {
				metric := metricsOfType[0]
				metricsByMeasuredType[CombinedType.MeasuredType] = metricsOfType[1:]
				if len(metricsByMeasuredType[CombinedType.MeasuredType]) == 0 {
					delete(metricsByMeasuredType, CombinedType.MeasuredType)
				}
				partition = append(partition, metric)
			}
		}
		partitionedMetrics = append(partitionedMetrics, partition)
	}
	return partitionedMetrics
}

func hasDuplicateMeasuredTypes(googleMetrics []entity.HydratedMetric) bool {
	measuredTypeMap := make(map[metadata.MeasuredType]bool)
	for _, metric := range googleMetrics {
		measuredType := metric.MeasuredType

		val, ok := measuredTypeMap[measuredType]
		if ok && val {
			return true
		}
		measuredTypeMap[measuredType] = true
	}
	return false
}

func (client *GoogleMetricsClient) createMetricValueSet(metricName string, metrics []entity.HydratedMetric) (*MetricValueSet, error) {
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

func (client *GoogleMetricsClient) CreateMetricValue(metric entity.HydratedMetric) (MetricValue, error) {
	metricValue := &servicecontrol.MetricValue{}

	metricMeasuredType := metric.MeasuredType
	switch metricMeasuredType {
	default:
		val := int64(metric.Quantity)
		metricValue.Int64Value = nillable.ToPointer(val)

		client.logger.Debugf("Set Metric Value for MeasuredType %s as %d", metricMeasuredType, val)
	}

	startTime := metric.Timestamp.ToTime().Unix()
	var endTime int64

	secondsRemaining := startTime % 60
	startTime -= secondsRemaining
	endTime = startTime + 59

	metricValue.StartTime = time.Unix(startTime, 0).Format(time.RFC3339)
	metricValue.EndTime = time.Unix(endTime, 0).Format(time.RFC3339)

	client.logger.Debugf("setting metricValue as %+v", metricValue)
	return metricValue, nil
}

func SetCommonLabels(op *Operation, consumerId, dataCenter, resourceId string, googleMetric entity.HydratedMetric) error {
	labels := make(map[string]string)

	labels["location"] = dataCenter
	labels["resource_container"] = "projects/" + consumerId
	if googleMetric.Metadata.ResourceDisplayName != nil {
		labels["name"] = *googleMetric.Metadata.ResourceDisplayName
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

func (client *GoogleMetricsClient) GetMetricName(measuredType metadata.MeasuredType, resourceType metadata.ResourceType) (string, error) {
	nameAndKeyLabel, exists := client.nameAndKeyLabelOfMetric[metadata.CombinedKeyResourceTypeMeasuredType{ResourceType: resourceType, MeasuredType: measuredType}]
	if !exists {
		return "", fmt.Errorf("unsupported measured type or resource type received: %s, %s", measuredType, resourceType)
	}

	var metricsName string
	switch resourceType {
	case metadata.VolumePool:
		metricsName = metadata.MetricsNamePrefixPoolFirstParty + nameAndKeyLabel.Left
	default:
		return "", fmt.Errorf("unrecognized resource type: %s", resourceType)
	}
	return metricsName, nil
}
