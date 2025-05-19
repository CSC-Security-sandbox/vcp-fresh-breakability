package workflow_engine

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	CustomerTaskQueue   = "customer-workflows"
	BackgroundTaskQueue = "background-workflows"
)

type TemporalWorkflowEngine struct {
	temporalClient client.Client
}

func (t *TemporalWorkflowEngine) LoadConfig() workflow_engine.ClientConfig {
	return LoadTemporalConfig()
}

func (t *TemporalWorkflowEngine) InitializeClient(cfg workflow_engine.ClientConfig, logger log.Logger) error {
	// Initialize the temporal server client
	clientOptions, err := createClientOptionsFromEnv(cfg, logger)
	if err != nil {
		logger.Error("failed to create temporal client options: %w", "error", err.Error())
		os.Exit(1)
	}

	if cfg.ShouldEnableDataEncryption() && cfg.GetEncryptionID() != "" {
		logger.Infof("Enabling encrypting Data Converter: %s", cfg.GetEncryptionID())
		defaultDataConverter := converter.GetDefaultDataConverter()
		// TODO: Store the Data Encryption Key in a Secret Management Service and add logic to retrieve the secret at runtime
		clientOptions.DataConverter = util.NewEncryptionDataConverter(defaultDataConverter, cfg.GetEncryptionID())
	}

	var temporalClient client.Client
	for {
		temporalClient, err = client.Dial(clientOptions)
		if err == nil {
			break
		}
		logger.Error("Failed to connect to the temporal, retrying...", "error", err.Error())
		time.Sleep(2 * time.Second) // Add a delay between retries to avoid overwhelming the temporal server
	}
	t.temporalClient = temporalClient
	return err
}

func (t *TemporalWorkflowEngine) RunWorker(ctx context.Context, client client.Client, dbcon database.Storage) error {
	w := worker.New(client, CustomerTaskQueue, worker.Options{
		MaxConcurrentWorkflowTaskPollers: 10,
		MaxConcurrentActivityTaskPollers: 10,
	})

	// Register the workflows and activities with the worker. Any new workflows and activities need to be registered below.
	registerWorkflowsAndActivities(w, dbcon)

	err := w.Run(worker.InterruptCh())
	return err
}

func (t *TemporalWorkflowEngine) CloseClient(client client.Client) {
	client.Close()
}

// GetTemporalClient returns the temporal client instance.
func (t *TemporalWorkflowEngine) GetTemporalClient() client.Client {
	return t.temporalClient
}

func registerWorkflowsAndActivities(worker worker.Worker, dbcon database.Storage) {
	worker.RegisterWorkflow(workflows.CreatePoolWorkflow)
	worker.RegisterWorkflow(workflows.DeletePoolWorkflow)
	worker.RegisterWorkflow(workflows.CreateVolumeWorkflow)
	worker.RegisterWorkflow(workflows.DeleteVolumeWorkflow)

	worker.RegisterActivity(&activities.CommonActivities{SE: dbcon})
	worker.RegisterActivity(&activities.PoolActivity{SE: &dbcon})
	worker.RegisterActivity(&activities.VolumeCreateActivity{SE: dbcon})
	worker.RegisterActivity(&activities.VolumeDeleteActivity{SE: dbcon})
}

// CreateClientOptionsFromEnv creates a client.Options instance, configures
// it based on environment variables, and returns that instance. It
// supports the following environment variables:
//
//	TEMPORAL_ADDRESS: Host and port (formatted as host:port) of the Temporal Frontend Service
//	TEMPORAL_NAMESPACE: Namespace to be used by the Client
//	TEMPORAL_TLS_CERT: Path to the x509 certificate
//	TEMPORAL_TLS_KEY: Path to the private certificate key
//
// If these environment variables are not set, the client.Options
// instance returned will be based on the SDK's default configuration.
func createClientOptionsFromEnv(cfg workflow_engine.ClientConfig, logger log.Logger) (client.Options, error) {
	tracingInterceptor, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
		Tracer: otel.GetTracerProvider().Tracer("Temporal-Worker"),
	})
	if err != nil {
		logger.Error("Unable to create interceptor", "error", err)
		tracingInterceptor = nil
	}
	clientOpts := client.Options{
		HostPort:  cfg.GetHostPort(),
		Namespace: cfg.GetNamespace(),
		Interceptors: func() []interceptor.ClientInterceptor {
			if tracingInterceptor != nil {
				return []interceptor.ClientInterceptor{tracingInterceptor}
			}
			return nil
		}(),
		MetricsHandler: opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{
			Meter: otel.GetMeterProvider().Meter("Temporal-Worker"),
		}),
	}

	if cfg.GetTLSCertPath() != "" && cfg.GetTLSKeyPath() != "" {
		cert, err := tls.LoadX509KeyPair(cfg.GetTLSCertPath(), cfg.GetTLSKeyPath())
		if err != nil {
			return clientOpts, fmt.Errorf("failed loading tls key pair for temporal: %w", err)
		}

		clientOpts.ConnectionOptions.TLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	clientOpts.ContextPropagators = []workflow.ContextPropagator{
		util.NewContextMapPropagator(),
	}

	return clientOpts, nil
}
