package common

import (
	_ "embed"
	"log"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"gopkg.in/yaml.v3"
)

const (
	Gcp string = "gcp"
	Dev string = "dev"
)

type TelemetryConfig struct {
	// Server configuration
	OperationBatchSize                    int64
	PusherServiceName                     string
	PusherServiceProject                  string
	PerformanceRootUrl                    string
	UsageRootUrl                          string
	RegionName                            string
	EnableVolumeMetrics                   bool
	EnableBackupMetrics                   bool
	EnableBackupBillingMetrics            bool
	EnableFilesBackupBilling              bool
	EnableCrossRegionBackupBillingMetrics bool
	EnableReplicationBillingMetrics       bool
	EnableFilesReplicationBillingMetrics  bool
	SFRMetricsEnabled                     bool
	PushBatchSize                         int64
	Environment                           string
	MaxGoogleBillingPushRetry             int64
	PageSize                              int32
	NumWorkersPerformance                 int
	NumWorkersUsage                       int
	NumWorkersCollection                  int
	NumWorkersBizOps                      int
	GoogleBillingLabelsMaxEntries         int
	PoolVolumeLabelPageSize               int
	EnableBatchUsageUpdates               bool // Feature flag for batch usage updates
	ResultUpdateBatchSize                 int
	TargetMinute                          int
	IntervalBackfillLimitMinutes          int
	CounterBackfillLimitMinutes           int
}

type MetricItem struct {
	Metric       string `yaml:"metric"`
	ResourceType string `yaml:"resourceType"`
	MetricType   string `yaml:"metricType"`
}
type MetricsConfig struct {
	VolumeMetrics []MetricItem `yaml:"metrics"`
}

//go:embed metricList.yaml
var metricListYAML []byte

func LoadConfig() *TelemetryConfig {
	performanceRootURL := env.GetString("PERFORMANCE_ROOT_URL", "https://servicecontrol.googleapis.com")
	usageRootURL := env.GetString("USAGE_ROOT_URL", "https://servicecontrol.googleapis.com")
	operationBatchSize := env.GetInt64("OPERATION_BATCH_SIZE", 200)
	pusherServiceName := env.GetString("PUSHER_SERVICE_NAME", "autopush-netapp.sandbox.googleapis.com")
	pusherServiceProject := env.GetString("PUSHER_SERVICE_PROJECT", "netapp-au-se1-autopush-sde-tst")
	regionName := env.GetString("LOCAL_REGION", "")
	enableVolumeMetrics := env.GetBool("ENABLE_VOLUME_METRICS", false)
	enableBackupMetrics := env.GetBool("ENABLE_BACKUP_METRICS", false)
	enableBackupBillingMetrics := env.GetBool("ENABLE_BACKUP_BILLING_METRICS", false)
	enableFilesBackupBilling := env.GetBool("ENABLE_FILES_BACKUP_BILLING", false)
	enableReplicationBillingMetrics := env.GetBool("ENABLE_REPLICATION_BILLING_METRICS", false)
	enableFilesReplicationBillingMetrics := env.GetBool("ENABLE_FILES_REPLICATION_BILLING_METRICS", false)
	sfrMetricsEnabled := env.GetBool("ENABLE_SFR_METRICS", false)
	enableCrossRegionBackupBillingMetrics := env.GetBool("ENABLE_CROSS_REGION_BACKUP_BILLING_METRICS", false)
	pushBatchSize := env.GetInt64("PUSH_BATCH_SIZE", 1000)
	environment := env.GetString("ENVIRONMENT", Dev)
	maxGoogleBillingPushRetry := env.GetInt64("MAX_GOOGLE_BILLING_PUSH_RETRY", 5)
	pageSize := env.GetInt64("PAGE_SIZE", 1000)
	numWorkersPerformance := env.GetInt("NUM_WORKERS_PERFORMANCE", 10)
	numWorkersUsage := env.GetInt("NUM_WORKERS_USAGE", 1)
	numWorkersCollection := env.GetInt("NUM_WORKERS_COLLECTION", 10)
	numWorkersBizOps := env.GetInt("NUM_WORKERS_BIZOPS", 10)
	googleBillingLabelsMaxEntries := env.GetInt("GOOGLE_BILLING_LABELS_MAX_ENTRIES", 64)
	poolVolumeLabelPageSize := env.GetInt("POOL_VOLUME_LABEL_PAGE_SIZE", 5000)
	enableBatchUsageUpdates := env.GetBool("ENABLE_BATCH_USAGE_UPDATES", false)
	resultUpdateBatchSize := env.GetInt("RESULT_UPDATE_BATCH_SIZE", 100)
	intervalBackfillLimitMinutes := env.GetInt("INTERVAL_BACKFILL_LIMIT_MINUTES", 60)
	counterBackfillLimitMinutes := env.GetInt("COUNTER_BACKFILL_LIMIT_MINUTES", 0)
	targetMinute := env.GetInt("TARGET_MINUTE", 15)

	return &TelemetryConfig{
		PerformanceRootUrl:                    performanceRootURL,
		UsageRootUrl:                          usageRootURL,
		PusherServiceName:                     pusherServiceName,
		PusherServiceProject:                  pusherServiceProject,
		OperationBatchSize:                    operationBatchSize,
		RegionName:                            regionName,
		EnableVolumeMetrics:                   enableVolumeMetrics,
		PushBatchSize:                         pushBatchSize,
		Environment:                           environment,
		MaxGoogleBillingPushRetry:             maxGoogleBillingPushRetry,
		PageSize:                              int32(pageSize),
		EnableBackupMetrics:                   enableBackupMetrics,
		EnableBackupBillingMetrics:            enableBackupBillingMetrics,
		EnableFilesBackupBilling:              enableFilesBackupBilling,
		EnableCrossRegionBackupBillingMetrics: enableCrossRegionBackupBillingMetrics,
		EnableReplicationBillingMetrics:       enableReplicationBillingMetrics,
		EnableFilesReplicationBillingMetrics:  enableFilesReplicationBillingMetrics,
		SFRMetricsEnabled:                     sfrMetricsEnabled,
		NumWorkersPerformance:                 numWorkersPerformance,
		NumWorkersUsage:                       numWorkersUsage,
		NumWorkersCollection:                  numWorkersCollection,
		NumWorkersBizOps:                      numWorkersBizOps,
		GoogleBillingLabelsMaxEntries:         googleBillingLabelsMaxEntries,
		PoolVolumeLabelPageSize:               poolVolumeLabelPageSize,
		EnableBatchUsageUpdates:               enableBatchUsageUpdates,
		ResultUpdateBatchSize:                 resultUpdateBatchSize,
		TargetMinute:                          targetMinute,
		IntervalBackfillLimitMinutes:          intervalBackfillLimitMinutes,
		CounterBackfillLimitMinutes:           counterBackfillLimitMinutes,
	}
}

func LoadMetricsConfigFromBytes() *MetricsConfig {
	var config MetricsConfig
	if err := yaml.Unmarshal(metricListYAML, &config); err != nil {
		log.Fatalf("Failed to unmarshal metrics config: %v", err)
	}

	return &config
}
