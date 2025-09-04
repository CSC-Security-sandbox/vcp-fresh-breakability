package common

import (
	_ "embed"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"gopkg.in/yaml.v3"
	"log"
)

const (
	Gcp string = "gcp"
	Dev string = "dev"
)

type TelemetryConfig struct {
	// Server configuration
	OperationBatchSize        int64
	PusherServiceName         string
	PusherServiceProject      string
	RootUrl                   string
	RegionName                string
	EnableVolumeMetrics       bool
	PushBatchSize             int64
	Environment               string
	MaxGoogleBillingPushRetry int64
}

type MetricItem struct {
	Metric       string `yaml:"metric"`
	ResourceType string `yaml:"resourceType"`
}
type MetricsConfig struct {
	VolumeMetrics []MetricItem `yaml:"metrics"`
}

//go:embed metricList.yaml
var metricListYAML []byte

func LoadConfig() *TelemetryConfig {
	rootUrl := env.GetString("ROOT_URL", "https://servicecontrol.googleapis.com")
	operationBatchSize := env.GetInt64("OPERATION_BATCH_SIZE", 200)
	pusherServiceName := env.GetString("PUSHER_SERVICE_NAME", "autopush-netapp.sandbox.googleapis.com")
	pusherServiceProject := env.GetString("PUSHER_SERVICE_PROJECT", "netapp-au-se1-autopush-sde-tst")
	regionName := env.GetString("LOCAL_REGION", "")
	enableVolumeMetrics := env.GetBool("ENABLE_VOLUME_METRICS", false)
	pushBatchSize := env.GetInt64("PUSH_BATCH_SIZE", 1000)
	environment := env.GetString("ENVIRONMENT", Dev)
	maxGoogleBillingPushRetry := env.GetInt64("MAX_GOOGLE_BILLING_PUSH_RETRY", 5)

	return &TelemetryConfig{
		RootUrl:                   rootUrl,
		PusherServiceName:         pusherServiceName,
		PusherServiceProject:      pusherServiceProject,
		OperationBatchSize:        operationBatchSize,
		RegionName:                regionName,
		EnableVolumeMetrics:       enableVolumeMetrics,
		PushBatchSize:             pushBatchSize,
		Environment:               environment,
		MaxGoogleBillingPushRetry: maxGoogleBillingPushRetry,
	}
}

func LoadMetricsConfigFromBytes() *MetricsConfig {
	var config MetricsConfig
	if err := yaml.Unmarshal(metricListYAML, &config); err != nil {
		log.Fatalf("Failed to unmarshal metrics config: %v", err)
	}

	return &config
}
