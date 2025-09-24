package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cloudscheduler "google.golang.org/api/cloudscheduler/v1"
	"google.golang.org/api/option"
	cloudrun "google.golang.org/api/run/v2"
)

type DeploymentConfig struct {
	ServiceName        string
	Image              string
	ProjectID          string
	Region             string
	MinInstances       int64
	EnvVars            map[string]string
	EnableScheduler    bool
	SchedulerCron      string
	ServiceURL         string
	ServiceAccountName string
	Network            string
	Subnetwork         string
}

func main() {
	// Command line flags
	var (
		serviceName        = flag.String("service", "vsa-harvest-otel", "Cloud Run service name")
		image              = flag.String("image", "", "Container image URL")
		projectID          = flag.String("project", "", "GCP project ID")
		region             = flag.String("region", "australia-southeast1", "GCP region")
		network            = flag.String("network", "cv-tst-au-se1-k8s-vpc", "Network name")
		subnet             = flag.String("subnet", "cloud-run", "Subnetwork name")
		minInstances       = flag.Int64("min-instances", 0, "Minimum number of instances")
		envVarsFlag        = flag.String("env-vars", "", "Environment variables in format KEY1=VALUE1,KEY2=VALUE2")
		enableScheduler    = flag.Bool("enable-scheduler", true, "Enable Cloud Scheduler to invoke the service")
		serviceAccountName = flag.String("service-account-name", "svc-sde-metrics-producer@netapp-au-se1-autopush-sde-tst.iam.gserviceaccount.com", "Cloud Run service account name")
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
		ServiceName:        *serviceName,
		Image:              *image,
		ProjectID:          *projectID,
		Region:             *region,
		MinInstances:       *minInstances,
		EnvVars:            envVars,
		EnableScheduler:    *enableScheduler,
		SchedulerCron:      "*/5 * * * *",
		ServiceAccountName: *serviceAccountName,
		Network:            *network,
		Subnetwork:         *subnet,
	}

	if err := deployCloudRunService(context.Background(), config); err != nil {
		log.Fatalf("Failed to deploy Cloud Run service: %v", err)
	}

	log.Printf("Successfully deployed %s to %s in project %s\n", config.ServiceName, config.Region, config.ProjectID)

	// Deploy Cloud Scheduler if enabled
	if config.EnableScheduler {
		// Get the service URL after deployment

		log.Println("Deploying 5-minutely Cloud Scheduler")
		serviceURL, err := getCloudRunServiceURL(context.Background(), config)
		if err != nil {
			log.Panicf("Failed to get Cloud Run service URL: %v", err)
		}
		config.ServiceURL = serviceURL + "/v1/performance"
		config.ServiceName = "performance"
		config.SchedulerCron = "*/5 * * * *" // Every 5 minutes

		if err := deployCloudScheduler(context.Background(), config); err != nil {
			log.Panicf("Failed to deploy Cloud Scheduler: %v", err)
		}

		log.Printf("Successfully deployed Cloud Scheduler for service %s with cron: %s\n", config.ServiceName, config.SchedulerCron)

		log.Println("Deploying hourly Cloud Scheduler")
		config.SchedulerCron = "15 * * * *"
		config.ServiceName = "usage"
		config.ServiceURL = serviceURL + "/v1/usage"

		if err := deployCloudScheduler(context.Background(), config); err != nil {
			log.Panicf("Failed to deploy Cloud Scheduler: %v", err)
		}

		log.Printf("Successfully deployed Cloud Scheduler for service %s with cron: %s\n", config.ServiceName, config.SchedulerCron)
	}
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
					Network:    config.Network,
					Subnetwork: config.Subnetwork,
				},
			},
		},
		ServiceAccount: config.ServiceAccountName,
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

	printCloudRunServiceInfo(existingService)

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

// getCloudRunServiceURL retrieves the URL of the deployed Cloud Run service
func getCloudRunServiceURL(ctx context.Context, config *DeploymentConfig) (string, error) {
	client, err := cloudrun.NewService(ctx, option.WithScopes("https://www.googleapis.com/auth/cloud-platform"))
	if err != nil {
		return "", fmt.Errorf("failed to create Cloud Run client: %w", err)
	}

	serviceName := fmt.Sprintf("projects/%s/locations/%s/services/%s", config.ProjectID, config.Region, config.ServiceName)
	service, err := client.Projects.Locations.Services.Get(serviceName).Do()
	if err != nil {
		return "", fmt.Errorf("failed to get service: %w", err)
	}

	if service.Uri == "" {
		return "", fmt.Errorf("service URL is empty")
	}

	return service.Uri, nil
}

// printCloudRunServiceInfo prints detailed information about a Cloud Run service
func printCloudRunServiceInfo(service *cloudrun.GoogleCloudRunV2Service) {
	log.Printf("=== Cloud Run Service Details ===\n")
	log.Printf("Service Name: %s\n", service.Name)
	log.Printf("Service URL:  %s\n", service.Uri)

	if service.Template != nil {
		if len(service.Template.Containers) > 0 {
			container := service.Template.Containers[0]
			log.Printf("Image:        %s\n", container.Image)

			if container.Resources != nil {
				log.Printf("CPU Idle:     %t\n", container.Resources.CpuIdle)
				if container.Resources.Limits != nil {
					for k, v := range container.Resources.Limits {
						log.Printf("Resource %s:  %s\n", k, v)
					}
				}
			}
		}

		if service.Template.Scaling != nil {
			if service.Template.Scaling.MinInstanceCount > 0 {
				log.Printf("Min Instances: %d\n", service.Template.Scaling.MinInstanceCount)
			}
			if service.Template.Scaling.MaxInstanceCount > 0 {
				log.Printf("Max Instances: %d\n", service.Template.Scaling.MaxInstanceCount)
			}
		}

		if service.Template.ServiceAccount != "" {
			log.Printf("Service Account: %s\n", service.Template.ServiceAccount)
		}
	}

	if len(service.Traffic) > 0 {
		log.Printf("Traffic Allocation:\n")
		for _, traffic := range service.Traffic {
			log.Printf("  - %d%% -> %s\n", traffic.Percent, traffic.Type)
		}
	}

	if len(service.Labels) > 0 {
		log.Printf("Labels:\n")
		for k, v := range service.Labels {
			log.Printf("  %s: %s\n", k, v)
		}
	}

	log.Printf("Creation Time: %s\n", service.CreateTime)
	log.Printf("Update Time:   %s\n", service.UpdateTime)
	log.Printf("================================\n")
}

// deployCloudScheduler creates or updates a Cloud Scheduler job to invoke the Cloud Run service
func deployCloudScheduler(ctx context.Context, config *DeploymentConfig) error {
	client, err := cloudscheduler.NewService(ctx, option.WithScopes("https://www.googleapis.com/auth/cloud-platform"))
	if err != nil {
		return fmt.Errorf("failed to create Cloud Scheduler client: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/%s", config.ProjectID, config.Region)
	jobName := fmt.Sprintf("%s/jobs/%s-trigger", parent, config.ServiceName)

	// Create the job configuration
	job := &cloudscheduler.Job{
		Name:        jobName,
		Description: fmt.Sprintf("Scheduled trigger for %s Cloud Run service", config.ServiceName),
		Schedule:    config.SchedulerCron,
		TimeZone:    "UTC",
		HttpTarget: &cloudscheduler.HttpTarget{
			Uri:        config.ServiceURL,
			HttpMethod: "POST",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			OidcToken: &cloudscheduler.OidcToken{
				ServiceAccountEmail: config.ServiceAccountName,
				Audience:            config.ServiceURL,
			},
		},
		RetryConfig: &cloudscheduler.RetryConfig{
			RetryCount:         3,
			MaxRetryDuration:   "300s",
			MinBackoffDuration: "5s",
			MaxBackoffDuration: "60s",
			MaxDoublings:       3,
		},
	}

	// Check if job already exists
	existingJob, err := client.Projects.Locations.Jobs.Get(jobName).Do()
	if err != nil {
		// Job doesn't exist, create it
		log.Printf("Creating new Cloud Scheduler job: %s\n", config.ServiceName+"-trigger")
		_, err := client.Projects.Locations.Jobs.Create(parent, job).Do()
		if err != nil {
			return fmt.Errorf("failed to create Cloud Scheduler job: %w", err)
		}
		log.Printf("Cloud Scheduler job created successfully\n")
	} else {
		// Job exists, update it
		log.Printf("Updating existing Cloud Scheduler job: %s\n", config.ServiceName+"-trigger")

		// Update the existing job
		existingJob.Schedule = job.Schedule
		existingJob.Description = job.Description
		existingJob.HttpTarget = job.HttpTarget
		existingJob.RetryConfig = job.RetryConfig
		existingJob.TimeZone = job.TimeZone

		_, err := client.Projects.Locations.Jobs.Patch(jobName, existingJob).Do()
		if err != nil {
			return fmt.Errorf("failed to update Cloud Scheduler job: %w", err)
		}
		log.Printf("Cloud Scheduler job updated successfully\n")
	}

	return nil
}
