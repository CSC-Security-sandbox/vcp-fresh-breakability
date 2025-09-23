package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/api/option"
	cloudrun "google.golang.org/api/run/v2"
)

type DeploymentConfig struct {
	ServiceName  string
	Image        string
	ProjectID    string
	Region       string
	MinInstances int64
	EnvVars      map[string]string
}

func main() {
	// Command line flags
	var (
		serviceName  = flag.String("service", "vsa-harvest-otel", "Cloud Run service name")
		image        = flag.String("image", "", "Container image URL")
		projectID    = flag.String("project", "", "GCP project ID")
		region       = flag.String("region", "australia-southeast1", "GCP region")
		minInstances = flag.Int64("min-instances", 0, "Minimum number of instances")
		envVarsFlag  = flag.String("env-vars", "", "Environment variables in format KEY1=VALUE1,KEY2=VALUE2")
	)
	flag.Parse()

	// Use default values from the gcloud command if not provided
	if *image == "" {
		*image = "us-docker.pkg.dev/gcnv-artifact-registry-nonprod/vcp-container-images-us/telemetry:25092.0.0-DEV.15"
	}
	if *projectID == "" {
		*projectID = "netapp-au-se1-autopush-sde-tst"
	}

	// Parse environment variables
	envVars := parseEnvVars(*envVarsFlag)

	// Add default environment variables from the gcloud command
	if len(envVars) == 0 {
		envVars = getDefaultEnvVars()
	}

	config := &DeploymentConfig{
		ServiceName:  *serviceName,
		Image:        *image,
		ProjectID:    *projectID,
		Region:       *region,
		MinInstances: *minInstances,
		EnvVars:      envVars,
	}

	if err := deployCloudRunService(context.Background(), config); err != nil {
		log.Fatalf("Failed to deploy Cloud Run service: %v", err)
	}

	log.Printf("Successfully deployed %s to %s in project %s\n", config.ServiceName, config.Region, config.ProjectID)
}

func deployCloudRunService(ctx context.Context, config *DeploymentConfig) error {
	// Initialize Cloud Run client
	client, err := cloudrun.NewService(ctx, option.WithScopes("https://www.googleapis.com/auth/cloud-platform"))
	if err != nil {
		return fmt.Errorf("failed to create Cloud Run client: %w", err)
	}

	// Convert environment variables to Cloud Run format
	envVars := make([]*cloudrun.GoogleCloudRunV2EnvVar, 0, len(config.EnvVars))
	for key, value := range config.EnvVars {
		// Skip commented out variables (starting with ^#^)
		if strings.HasPrefix(key, "^#^") {
			continue
		}
		envVars = append(envVars, &cloudrun.GoogleCloudRunV2EnvVar{
			Name:  key,
			Value: value,
		})
	}

	// Create container configuration
	container := &cloudrun.GoogleCloudRunV2Container{
		Image: config.Image,
		Env:   envVars,
		Ports: []*cloudrun.GoogleCloudRunV2ContainerPort{
			{
				ContainerPort: 8080,
			},
		},
	}

	// Create service template with scaling
	template := &cloudrun.GoogleCloudRunV2RevisionTemplate{
		Containers: []*cloudrun.GoogleCloudRunV2Container{container},
		Scaling: &cloudrun.GoogleCloudRunV2RevisionScaling{
			MinInstanceCount: config.MinInstances,
		},
		VpcAccess: &cloudrun.GoogleCloudRunV2VpcAccess{
			NetworkInterfaces: []*cloudrun.GoogleCloudRunV2NetworkInterface{
				{
					Network:    "cv-tst-au-se1-k8s-vpc",
					Subnetwork: "cloud-run",
				},
			},
		},
		ServiceAccount: "svc-sde-metrics-producer@netapp-au-se1-autopush-sde-tst.iam.gserviceaccount.com",
	}

	// Create the service
	service := &cloudrun.GoogleCloudRunV2Service{
		Template: template,
		Labels: map[string]string{
			"managed-by": "telemetry-deployer",
		},
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", config.ProjectID, config.Region)

	// Check if service already exists
	existingService, err := client.Projects.Locations.Services.Get(
		fmt.Sprintf("%s/services/%s", parent, config.ServiceName),
	).Do()

	if err != nil {
		// Service doesn't exist, create it
		log.Printf("Creating new Cloud Run service: %s\n", config.ServiceName)
		operation, err := client.Projects.Locations.Services.Create(parent, service).
			ServiceId(config.ServiceName).Do()
		if err != nil {
			return fmt.Errorf("failed to create Cloud Run service: %w", err)
		}
		log.Printf("Service creation operation: %s\n", operation.Name)

		// Wait for the creation operation to complete
		if err := waitForOperation(ctx, client, operation.Name); err != nil {
			return fmt.Errorf("failed to wait for service creation: %w", err)
		}
		log.Printf("Service creation completed successfully\n")
	} else {
		// Service exists, update it
		log.Printf("Updating existing Cloud Run service: %s\n", config.ServiceName)

		// Update the existing service template
		existingService.Template = template

		operation, err := client.Projects.Locations.Services.Patch(
			fmt.Sprintf("%s/services/%s", parent, config.ServiceName),
			existingService,
		).Do()
		if err != nil {
			return fmt.Errorf("failed to update Cloud Run service: %w", err)
		}
		log.Printf("Service update operation: %s\n", operation.Name)

		// Wait for the update operation to complete
		if err := waitForOperation(ctx, client, operation.Name); err != nil {
			return fmt.Errorf("failed to wait for service update: %w", err)
		}
		log.Printf("Service update completed successfully\n")
	}

	// Update traffic to route 100% to latest revision
	log.Printf("Updating traffic to latest revision\n")
	trafficUpdate := &cloudrun.GoogleCloudRunV2Service{
		Traffic: []*cloudrun.GoogleCloudRunV2TrafficTarget{
			{
				Type:    "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST",
				Percent: 100,
			},
		},
	}

	_, err = client.Projects.Locations.Services.Patch(
		fmt.Sprintf("%s/services/%s", parent, config.ServiceName),
		trafficUpdate,
	).UpdateMask("traffic").Do()
	if err != nil {
		return fmt.Errorf("failed to update traffic: %w", err)
	}

	return nil
}

func parseEnvVars(envVarsStr string) map[string]string {
	envVars := make(map[string]string)
	if envVarsStr == "" {
		return envVars
	}

	pairs := strings.Split(envVarsStr, ",")
	for _, pair := range pairs {
		if kv := strings.SplitN(pair, "=", 2); len(kv) == 2 {
			envVars[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return envVars
}

// waitForOperation waits for a Cloud Run operation to complete
func waitForOperation(ctx context.Context, client *cloudrun.Service, operationName string) error {
	log.Printf("Waiting for operation to complete: %s\n", operationName)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Check operation status
			op, err := client.Projects.Locations.Operations.Get(operationName).Do()
			if err != nil {
				return fmt.Errorf("failed to get operation status: %w", err)
			}

			if op.Done {
				if op.Error != nil {
					return fmt.Errorf("operation failed: %s", op.Error.Message)
				}
				log.Printf("Operation completed successfully\n")
				return nil
			}

			// Wait before checking again
			time.Sleep(5 * time.Second)
		}
	}
}

func getDefaultEnvVars() map[string]string {
	getEnvOrDefault := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	return map[string]string{
		"ENV":                           getEnvOrDefault("ENV", "local"),
		"GCP_PROXY_PORT":                getEnvOrDefault("GCP_PROXY_PORT", "8090"),
		"DB_TYPE":                       getEnvOrDefault("DB_TYPE", "postgres"),
		"DB_HOST":                       getEnvOrDefault("DB_HOST", "postgres.default.svc.cluster.local"),
		"DB_PORT":                       getEnvOrDefault("DB_PORT", "5432"),
		"DB_USER":                       getEnvOrDefault("DB_USER", "postgres"),
		"DB_PASSWORD":                   getEnvOrDefault("DB_PASSWORD", "testpass"),
		"DB_NAME":                       getEnvOrDefault("DB_NAME", "vcp"),
		"DB_SSL_MODE":                   getEnvOrDefault("DB_SSL_MODE", "disable"),
		"DB_TIMEZONE":                   getEnvOrDefault("DB_TIMEZONE", "UTC"),
		"DB_MAX_OPEN_CONNS":             getEnvOrDefault("DB_MAX_OPEN_CONNS", "25"),
		"DB_MAX_IDLE_CONNS":             getEnvOrDefault("DB_MAX_IDLE_CONNS", "25"),
		"DB_CONN_MAX_LIFETIME":          getEnvOrDefault("DB_CONN_MAX_LIFETIME", "1h"),
		"MIGRATION_PATH":                getEnvOrDefault("MIGRATION_PATH", "migrations/core"),
		"DB_ADMIN_USER":                 getEnvOrDefault("DB_ADMIN_USER", "postgres"),
		"DB_ADMIN_PASSWORD":             getEnvOrDefault("DB_ADMIN_PASSWORD", "testpass"),
		"METRICS_DB_TYPE":               getEnvOrDefault("METRICS_DB_TYPE", "postgres"),
		"METRICS_DB_HOST":               getEnvOrDefault("METRICS_DB_HOST", "postgres.default.svc.cluster.local"),
		"METRICS_DB_PORT":               getEnvOrDefault("METRICS_DB_PORT", "5432"),
		"METRICS_DB_USER":               getEnvOrDefault("METRICS_DB_USER", "postgres"),
		"METRICS_DB_PASSWORD":           getEnvOrDefault("METRICS_DB_PASSWORD", "testpass"),
		"METRICS_DB_NAME":               getEnvOrDefault("METRICS_DB_NAME", "metrics"),
		"METRICS_DB_SSL_MODE":           getEnvOrDefault("METRICS_DB_SSL_MODE", "disable"),
		"METRICS_DB_TIMEZONE":           getEnvOrDefault("METRICS_DB_TIMEZONE", "UTC"),
		"METRICS_DB_MAX_OPEN_CONNS":     getEnvOrDefault("METRICS_DB_MAX_OPEN_CONNS", "25"),
		"METRICS_DB_MAX_IDLE_CONNS":     getEnvOrDefault("METRICS_DB_MAX_IDLE_CONNS", "25"),
		"METRICS_DB_CONN_MAX_LIFETIME":  getEnvOrDefault("METRICS_DB_CONN_MAX_LIFETIME", "1h"),
		"RUN_MIGRATION_ON_START":        getEnvOrDefault("RUN_MIGRATION_ON_START", "true"),
		"GCE_METADATA_HOST":             getEnvOrDefault("GCE_METADATA_HOST", "34.151.70.197:9090"),
		"ROOT_URL":                      getEnvOrDefault("ROOT_URL", "https://servicecontrol.googleapis.com"),
		"OPERATION_BATCH_SIZE":          getEnvOrDefault("OPERATION_BATCH_SIZE", "200"),
		"PUSHER_SERVICE_NAME":           getEnvOrDefault("PUSHER_SERVICE_NAME", "autopush-netapp.sandbox.googleapis.com"),
		"PUSHER_SERVICE_PROJECT":        getEnvOrDefault("PUSHER_SERVICE_PROJECT", "netapp-au-se1-autopush-sde-tst"),
		"LOCAL_REGION":                  getEnvOrDefault("LOCAL_REGION", "us-west2"),
		"ENABLE_VOLUME_METRICS":         getEnvOrDefault("ENABLE_VOLUME_METRICS", "false"),
		"PUSH_BATCH_SIZE":               getEnvOrDefault("PUSH_BATCH_SIZE", "1000"),
		"MAX_GOOGLE_BILLING_PUSH_RETRY": getEnvOrDefault("MAX_GOOGLE_BILLING_PUSH_RETRY", "5"),
		"PAGE_SIZE":                     getEnvOrDefault("PAGE_SIZE", "1000"),
	}
}
