package common

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"

type TelemetryConfig struct {
	// Server configuration
	OperationBatchSize   int64
	PusherServiceName    string
	PusherServiceProject string
	RootUrl              string
	RegionName           string
}

func LoadConfig() *TelemetryConfig {
	rootUrl := env.GetString("ROOT_URL", "https://servicecontrol.googleapis.com")
	operationBatchSize := env.GetInt64("OPERATION_BATCH_SIZE", 200)
	pusherServiceName := env.GetString("PUSHER_SERVICE_NAME", "autopush-netapp.sandbox.googleapis.com")
	pusherServiceProject := env.GetString("PUSHER_SERVICE_PROJECT", "netapp-au-se1-autopush-sde-tst")
	regionName := env.GetString("REGION", "")

	return &TelemetryConfig{
		RootUrl:              rootUrl,
		PusherServiceName:    pusherServiceName,
		PusherServiceProject: pusherServiceProject,
		OperationBatchSize:   operationBatchSize,
		RegionName:           regionName,
	}
}
