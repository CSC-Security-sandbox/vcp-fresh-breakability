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
	MaxInstances       int64
	EnvVars            map[string]string
	EnableScheduler    bool
	SchedulerCron      string
	ServiceURL         string
	ServiceAccountName string
	Network            string
	Subnetwork         string
	VPCConnector       string
	CloudSQLInstances  []string // List of Cloud SQL instance connection names
	CloudSQLImage      string
}

func main() {
	// Command line flags
	var (
		serviceName        = flag.String("service", "telemetry-service", "Cloud Run service name")
		image              = flag.String("image", "us-docker.pkg.dev/gcnv-artifact-registry-nonprod/vcp-container-images-us/telemetry:latest", "Container image URL")
		projectID          = flag.String("project", "", "GCP project ID")
		region             = flag.String("region", "", "GCP region")
		network            = flag.String("network", "", "Network name")
		subnet             = flag.String("subnet", "", "Subnetwork name")
		vpcConnector       = flag.String("vpc-connector", "projects/netapp-au-se1-autopush-sde-tst/locations/australia-southeast1/connectors/db-connector", "VPC Access connector")
		minInstances       = flag.Int64("min-instances", 0, "Minimum number of instances")
		maxInstances       = flag.Int64("max-instances", 1, "Maximum number of instances (0 for no limit)")
		envVarsFlag        = flag.String("env-vars", "", "Environment variables in format KEY1=VALUE1,KEY2=VALUE2")
		cloudSQLInstances  = flag.String("cloud-sql-instances", "netapp-au-se1-autopush-sde-tst:australia-southeast1:netapp-au-se1-autopush-sde-tst-db-postgres", "Comma-separated list of Cloud SQL instance connection names (project:region:instance)")
		enableScheduler    = flag.Bool("enable-scheduler", true, "Enable Cloud Scheduler to invoke the service")
		serviceAccountName = flag.String("service-account-name", "vcp-metrics-producer@netapp-au-se1-autopush-sde-tst.iam.gserviceaccount.com", "Cloud Run service account name")
		cloudSQLImage      = flag.String("cloud-sql-image", "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.15.1", "Cloud SQL image URL")
	)
	flag.Parse()

	// Parse environment variables
	envVars := parseEnvVars(*envVarsFlag)

	// Parse Cloud SQL instances
	var sqlInstances []string
	if *cloudSQLInstances != "" {
		sqlInstances = strings.Split(*cloudSQLInstances, ",")
		for i, instance := range sqlInstances {
			sqlInstances[i] = strings.TrimSpace(instance)
		}
	}

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
		MaxInstances:       *maxInstances,
		EnvVars:            envVars,
		EnableScheduler:    *enableScheduler,
		SchedulerCron:      "*/5 * * * *",
		ServiceAccountName: *serviceAccountName,
		Network:            *network,
		Subnetwork:         *subnet,
		VPCConnector:       *vpcConnector,
		CloudSQLInstances:  sqlInstances,
		CloudSQLImage:      *cloudSQLImage,
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

		log.Println("Deploying Daily BizOps Cloud Scheduler")
		config.SchedulerCron = "0 10 * * *"
		config.ServiceName = "bizops"
		config.ServiceURL = serviceURL + "/v1/generateReport"

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
	envVars := make([]*cloudrun.GoogleCloudRunV2EnvVar, 0, len(config.EnvVars)+2) // +2 for the secrets

	// Define secrets that should be fetched from Secret Manager
	secretKeys := map[string]string{
		"DB_PASSWORD":         "vcp-autopush-tst-au-se1-db-password-key",
		"METRICS_DB_PASSWORD": "vcp-autopush-tst-au-se1-metrics-db-password-key",
	}

	for key, value := range config.EnvVars {
		// Skip commented out variables (starting with ^#^)
		if strings.HasPrefix(key, "^#^") {
			continue
		}

		// Skip if this variable should be configured as a secret
		if _, isExists := secretKeys[key]; isExists {
			// Add as secret reference instead of plain value
			envVars = append(envVars, &cloudrun.GoogleCloudRunV2EnvVar{
				Name: key,
				ValueSource: &cloudrun.GoogleCloudRunV2EnvVarSource{
					SecretKeyRef: &cloudrun.GoogleCloudRunV2SecretKeySelector{
						Secret:  value,
						Version: "latest",
					},
				},
			})
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

	if len(config.CloudSQLInstances) == 0 {
		log.Panicf("Cloud SQL Instances are empty")
	}

	cloudSqlInstance := config.CloudSQLInstances[0]
	// SQL Proxy Container Configuration to connect to Cloud SQL instances
	sqlProxyContainer := &cloudrun.GoogleCloudRunV2Container{
		Name:  "vsa-cloud-sql-proxy",
		Image: config.CloudSQLImage,
		Args: []string{
			"--private-ip",
			"--structured-logs",
			"--port=5432",
			cloudSqlInstance,
		},
	}

	// Create service template with scaling
	template := &cloudrun.GoogleCloudRunV2RevisionTemplate{
		Containers: []*cloudrun.GoogleCloudRunV2Container{sqlProxyContainer, container},
		Scaling: &cloudrun.GoogleCloudRunV2RevisionScaling{
			MinInstanceCount: config.MinInstances,
			MaxInstanceCount: config.MaxInstances,
		},
		ServiceAccount: config.ServiceAccountName,
	}

	// Configure VPC access
	vpcAccess := &cloudrun.GoogleCloudRunV2VpcAccess{}
	if config.VPCConnector != "" {
		vpcAccess.Connector = config.VPCConnector
	} else if config.Subnetwork != "" {
		vpcAccess.NetworkInterfaces = append(vpcAccess.NetworkInterfaces,
			&cloudrun.GoogleCloudRunV2NetworkInterface{
				Network:    config.Network,
				Subnetwork: config.Subnetwork,
			})
	} else {
		log.Printf("No VPC connector or subnet specified in config")
		return fmt.Errorf("either Subnetwork or VPC Connector must be specified")
	}
	template.VpcAccess = vpcAccess

	// Create the service
	service := &cloudrun.GoogleCloudRunV2Service{
		Template: template,
		Ingress:  "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER",
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

	if existingService == nil {
		// Fetch the newly created service details
		existingService, err = client.Projects.Locations.Services.Get(
			fmt.Sprintf("%s/services/%s", parent, config.ServiceName),
		).Do()
		if err != nil {
			log.Printf("Warning: Failed to get service info after deployment: %v\n", err)
		}
	}
	if existingService != nil {
		printCloudRunServiceInfo(existingService)
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
		"ENV":                                getEnvOrDefault("ENV", "local"),
		"GCP_PROXY_PORT":                     getEnvOrDefault("GCP_PROXY_PORT", "8090"),
		"DB_TYPE":                            getEnvOrDefault("DB_TYPE", "postgres"),
		"DB_HOST":                            getEnvOrDefault("DB_HOST", "127.0.0.1"),
		"DB_PORT":                            getEnvOrDefault("DB_PORT", "5432"),
		"DB_USER":                            getEnvOrDefault("DB_USER", "postgres"),
		"DB_PASSWORD":                        getEnvOrDefault("DB_PASSWORD", ""),
		"DB_NAME":                            getEnvOrDefault("DB_NAME", "vcp"),
		"DB_SSL_MODE":                        getEnvOrDefault("DB_SSL_MODE", "disable"),
		"DB_TIMEZONE":                        getEnvOrDefault("DB_TIMEZONE", "UTC"),
		"DB_MAX_OPEN_CONNS":                  getEnvOrDefault("DB_MAX_OPEN_CONNS", "25"),
		"DB_MAX_IDLE_CONNS":                  getEnvOrDefault("DB_MAX_IDLE_CONNS", "25"),
		"DB_CONN_MAX_LIFETIME":               getEnvOrDefault("DB_CONN_MAX_LIFETIME", "1h"),
		"DB_ADMIN_USER":                      getEnvOrDefault("DB_ADMIN_USER", "postgres"),
		"METRICS_DB_TYPE":                    getEnvOrDefault("METRICS_DB_TYPE", "postgres"),
		"METRICS_DB_HOST":                    getEnvOrDefault("METRICS_DB_HOST", "127.0.0.1"),
		"METRICS_DB_PORT":                    getEnvOrDefault("METRICS_DB_PORT", "5432"),
		"METRICS_DB_USER":                    getEnvOrDefault("METRICS_DB_USER", "metrics"),
		"METRICS_DB_PASSWORD":                getEnvOrDefault("METRICS_DB_PASSWORD", ""),
		"METRICS_DB_NAME":                    getEnvOrDefault("METRICS_DB_NAME", "metrics"),
		"METRICS_DB_SSL_MODE":                getEnvOrDefault("METRICS_DB_SSL_MODE", "disable"),
		"METRICS_DB_TIMEZONE":                getEnvOrDefault("METRICS_DB_TIMEZONE", "UTC"),
		"METRICS_DB_MAX_OPEN_CONNS":          getEnvOrDefault("METRICS_DB_MAX_OPEN_CONNS", "25"),
		"METRICS_DB_MAX_IDLE_CONNS":          getEnvOrDefault("METRICS_DB_MAX_IDLE_CONNS", "25"),
		"METRICS_DB_CONN_MAX_LIFETIME":       getEnvOrDefault("METRICS_DB_CONN_MAX_LIFETIME", "1h"),
		"ROOT_URL":                           getEnvOrDefault("ROOT_URL", "https://servicecontrol.googleapis.com"),
		"OPERATION_BATCH_SIZE":               getEnvOrDefault("OPERATION_BATCH_SIZE", "200"),
		"PUSHER_SERVICE_NAME":                getEnvOrDefault("PUSHER_SERVICE_NAME", "autopush-netapp.sandbox.googleapis.com"),
		"PUSHER_SERVICE_PROJECT":             getEnvOrDefault("PUSHER_SERVICE_PROJECT", "netapp-au-se1-autopush-sde-tst"),
		"LOCAL_REGION":                       getEnvOrDefault("LOCAL_REGION", "us-west2"),
		"ENABLE_VOLUME_METRICS":              getEnvOrDefault("ENABLE_VOLUME_METRICS", "false"),
		"ENABLE_BACKUP_METRICS":              getEnvOrDefault("ENABLE_BACKUP_METRICS", "false"),
		"ENABLE_BACKUP_BILLING_METRICS":      getEnvOrDefault("ENABLE_BACKUP_BILLING_METRICS", "false"),
		"ENABLE_REPLICATION_BILLING_METRICS": getEnvOrDefault("ENABLE_REPLICATION_BILLING_METRICS", "false"),
		"PUSH_BATCH_SIZE":                    getEnvOrDefault("PUSH_BATCH_SIZE", "1000"),
		"MAX_GOOGLE_BILLING_PUSH_RETRY":      getEnvOrDefault("MAX_GOOGLE_BILLING_PUSH_RETRY", "5"),
		"PAGE_SIZE":                          getEnvOrDefault("PAGE_SIZE", "1000"),
		"GOOGLE_CONTINENTS":                  getEnvOrDefault("GOOGLE_CONTINENTS", ""),
		"BIZOPS_ACCOUNT_PAGINATION_LIMIT":    getEnvOrDefault("BIZOPS_ACCOUNT_PAGINATION_LIMIT", "1000"),
		"BIZOPS_REPORT_NAME":                 getEnvOrDefault("BIZOPS_REPORT_NAME", ""),
		"BIZOPS_BUCKET_NAME":                 getEnvOrDefault("BIZOPS_BUCKET_NAME", ""),
		"GOOGLE_REGION":                      getEnvOrDefault("GOOGLE_REGION", ""),
		"ENVIRONMENT":                        getEnvOrDefault("ENVIRONMENT", "gcp"),
		"NUM_WORKERS":                        getEnvOrDefault("NUM_WORKERS", "10"),
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
