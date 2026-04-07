// Package setup provides automatic environment setup for SafeSQL CLI.
// It handles port-forwarding, database password fetching, and directory creation
// based on environment variables.
package setup

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// AutoSetup performs automatic environment setup based on environment variables.
// It returns an error if critical setup fails.
//
// Auth mode is resolved in this order:
//  1. SAFESQL_USE_IAM=false explicitly set → password mode (no discovery)
//  2. Otherwise → auto-discover: find a core pod; if its sidecar has
//     --auto-iam-authn, derive DB_USER from Workload Identity and use IAM mode.
//  3. If discovery fails or sidecar lacks the flag → fall back to password mode.
func AutoSetup() error {
	if os.Getenv("SAFESQL_NO_AUTO_SETUP") == "true" {
		return nil
	}

	if err := createDirectories(); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	// Resolve auth mode unless the user has explicitly disabled IAM.
	if os.Getenv("SAFESQL_USE_IAM") == "false" {
		fmt.Fprintln(os.Stderr, "[INFO] SAFESQL_USE_IAM=false: using password authentication")
		if err := maybeAutoFetchPassword(); err != nil {
			fmt.Fprintf(os.Stderr, "[WARNING] Could not auto-fetch DB password: %v\n", err)
		}
	} else {
		// Auto-discover IAM from the cluster.
		namespace := getEnvOrDefault("DB_PORT_FORWARD_NAMESPACE", "vcp")
		podLabel := getEnvOrDefault("DB_PORT_FORWARD_POD_LABEL", "app=core")

		disc, err := discoverIAM(namespace, podLabel)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[INFO] IAM auto-discovery: %v — falling back to password auth\n", err)
		}

		if disc.enabled {
			os.Setenv("SAFESQL_USE_IAM", "true")
			if disc.dbUser != "" {
				existing := os.Getenv("DB_USER")
				// Override DB_USER if it is unset or is not an IAM principal (no "@").
				// This handles stale env vars like DB_USER=postgres from a previous session.
				if existing == "" || !strings.Contains(existing, "@") {
					os.Setenv("DB_USER", disc.dbUser)
					fmt.Fprintf(os.Stderr, "[INFO] IAM auto-discovered: DB_USER=%s\n", disc.dbUser)
				}
			}
			// Guard against DB_NAME accidentally set to an email address (e.g. from a
			// previous manual test where someone exported DB_NAME=$DB_USER by mistake).
			if dbName := os.Getenv("DB_NAME"); strings.Contains(dbName, "@") {
				fmt.Fprintf(os.Stderr, "[WARNING] DB_NAME=%q looks like an email — resetting to default 'vcp'\n", dbName)
				os.Setenv("DB_NAME", "vcp")
			}
		} else {
			os.Setenv("SAFESQL_USE_IAM", "false")
			fmt.Fprintln(os.Stderr, "[INFO] IAM not available on cluster — using password authentication")
			if err := maybeAutoFetchPassword(); err != nil {
				fmt.Fprintf(os.Stderr, "[WARNING] Could not auto-fetch DB password: %v\n", err)
			}
		}
	}

	// Setup port-forward to the right target (pod sidecar for IAM, service for password).
	if shouldSetupPortForward() {
		if err := setupPortForward(); err != nil {
			fmt.Fprintf(os.Stderr, "[WARNING] Could not setup port-forward: %v\n", err)
			fmt.Fprintln(os.Stderr, "[INFO] Ensure kubectl is configured or set DB_HOST to an accessible host")
		}
	}

	if os.Getenv("SAFESQL_OPERATOR") == "" {
		if username := getCurrentUsername(); username != "" {
			os.Setenv("SAFESQL_OPERATOR", username)
		}
	}

	return nil
}

// iamDiscovery holds the result of auto-detecting IAM auth from the cluster.
type iamDiscovery struct {
	enabled bool
	dbUser  string // Cloud SQL IAM username derived from Workload Identity SA
}

// discoverIAM finds a running pod matching podLabel in namespace, checks whether
// its Cloud SQL Proxy sidecar carries --auto-iam-authn, and if so derives the
// DB username from the pod's Workload Identity GCP service account.
func discoverIAM(namespace, podLabel string) (iamDiscovery, error) {
	podName, err := resolveIAMPod(namespace, podLabel)
	if err != nil {
		return iamDiscovery{}, fmt.Errorf("no running pod with label %q in ns %q: %w", podLabel, namespace, err)
	}

	if !podHasIAMAuthn(namespace, podName) {
		return iamDiscovery{}, fmt.Errorf("pod %s sidecar does not have --auto-iam-authn", podName)
	}

	dbUser, err := dbUserFromPod(namespace, podName)
	if err != nil {
		// IAM is available but we couldn't derive the user; caller must set DB_USER manually.
		fmt.Fprintf(os.Stderr, "[WARNING] Could not derive DB_USER from pod Workload Identity: %v\n", err)
		fmt.Fprintln(os.Stderr, "[INFO] Set DB_USER manually (IAM email, e.g. sa@project.iam.gserviceaccount.com)")
		return iamDiscovery{enabled: true}, nil
	}

	return iamDiscovery{enabled: true, dbUser: dbUser}, nil
}

// podHasIAMAuthn reports whether any container in the pod has --auto-iam-authn in its args.
// podName may be in "pod/name" form (as returned by kubectl -o name) or bare "name".
func podHasIAMAuthn(namespace, podName string) bool {
	out, err := exec.Command("kubectl", "get",
		"-n", namespace, podName,
		"-o", "jsonpath={.spec.containers[*].args}").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "--auto-iam-authn")
}

// dbUserFromPod derives the Cloud SQL IAM username by reading the pod's
// Kubernetes service account and its iam.gke.io/gcp-service-account annotation.
//
// GCP SA email:  vcp-core@project.iam.gserviceaccount.com
// DB username:   vcp-core@project.iam   (strip ".gserviceaccount.com")
func dbUserFromPod(namespace, podName string) (string, error) {
	ksaOut, err := exec.Command("kubectl", "get",
		"-n", namespace, podName,
		"-o", "jsonpath={.spec.serviceAccountName}").Output()
	if err != nil {
		return "", fmt.Errorf("get pod serviceAccountName: %w", err)
	}
	ksa := strings.TrimSpace(string(ksaOut))
	if ksa == "" {
		return "", fmt.Errorf("pod has no serviceAccountName")
	}

	gcpSAOut, err := exec.Command("kubectl", "get", "serviceaccount",
		"-n", namespace, ksa,
		"-o", `jsonpath={.metadata.annotations.iam\.gke\.io/gcp-service-account}`).Output()
	if err != nil {
		return "", fmt.Errorf("get serviceaccount annotation: %w", err)
	}
	gcpSA := strings.TrimSpace(string(gcpSAOut))
	if gcpSA == "" {
		return "", fmt.Errorf("no iam.gke.io/gcp-service-account annotation on serviceaccount %q", ksa)
	}

	// Cloud SQL IAM username = GCP SA email without the ".gserviceaccount.com" suffix.
	return strings.TrimSuffix(gcpSA, ".gserviceaccount.com"), nil
}

func maybeAutoFetchPassword() error {
	if os.Getenv("DB_PASSWORD") != "" || os.Getenv("SAFESQL_AUTO_FETCH_PASSWORD") == "false" {
		return nil
	}
	return fetchDBPasswordFromK8s()
}

func createDirectories() error {
	// Only create config directory for config.yaml
	// Plans and audit logs are now stored in GCS, not locally
	configDir := getEnvOrDefault("SAFESQL_CONFIG_DIR", fmt.Sprintf("%s/.safesql", os.Getenv("HOME")))

	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory %s: %w", configDir, err)
	}

	return nil
}

func fetchDBPasswordFromK8s() error {
	secretName := getEnvOrDefault("DB_SECRET_NAME", "postgres-credentials")
	secretNamespace := getEnvOrDefault("DB_SECRET_NAMESPACE", "sde")
	secretKey := getEnvOrDefault("DB_SECRET_KEY", "postgres-root-password")

	fmt.Fprintf(os.Stderr, "Fetching DB password from k8s secret %s/%s (key: %s)...\n", secretNamespace, secretName, secretKey)

	// Get base64-encoded password
	cmd := exec.Command("kubectl", "get", "secret",
		"-n", secretNamespace,
		secretName,
		"-o", fmt.Sprintf("jsonpath={.data.%s}", secretKey))

	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("kubectl get secret failed: %w", err)
	}

	if len(output) == 0 {
		// Try alternative key names
		alternativeKeys := []string{"password", "postgres-password", "POSTGRES_PASSWORD"}
		for _, altKey := range alternativeKeys {
			cmd = exec.Command("kubectl", "get", "secret",
				"-n", secretNamespace,
				secretName,
				"-o", fmt.Sprintf("jsonpath={.data.%s}", altKey))
			output, err = cmd.Output()
			if err == nil && len(output) > 0 {
				secretKey = altKey
				fmt.Fprintf(os.Stderr, "   Found password in key: %s\n", altKey)
				break
			}
		}
		if len(output) == 0 {
			return fmt.Errorf("password not found in secret (tried keys: %s, password, postgres-password)", secretKey)
		}
	}

	// Decode base64
	decodeCmd := exec.Command("base64", "-d")
	decodeCmd.Stdin = strings.NewReader(string(output))
	decoded, err := decodeCmd.Output()
	if err != nil {
		return fmt.Errorf("base64 decode failed: %w", err)
	}

	password := strings.TrimSpace(string(decoded))
	if password == "" {
		return fmt.Errorf("empty password after decode")
	}

	os.Setenv("DB_PASSWORD", password)
	fmt.Fprintln(os.Stderr, "[INFO] DB password fetched successfully")
	return nil
}

func shouldSetupPortForward() bool {
	// Only setup port-forward if:
	// 1. Auto port-forward is not disabled
	// 2. DB_HOST is localhost/127.0.0.1
	// 3. Port is not already accessible

	if os.Getenv("SAFESQL_AUTO_PORT_FORWARD") == "false" {
		return false
	}

	dbHost := getEnvOrDefault("DB_HOST", "127.0.0.1")
	if dbHost != "127.0.0.1" && dbHost != "localhost" {
		return false
	}

	dbPort := getEnvOrDefault("DB_PORT", "5432")
	return !isPortAccessible(dbHost, dbPort)
}

func setupPortForward() error {
	dbPort := getEnvOrDefault("DB_PORT", "5432")
	namespace := getEnvOrDefault("DB_PORT_FORWARD_NAMESPACE", "vcp")
	targetPort := getEnvOrDefault("DB_PORT_FORWARD_PORT", "5432")

	// Check if kubectl is available and configured
	if err := exec.Command("kubectl", "cluster-info").Run(); err != nil {
		return fmt.Errorf("kubectl not configured or not accessible")
	}

	// When IAM is enabled, port-forward to a running pod whose Cloud SQL Proxy
	// sidecar was started with --auto-iam-authn (e.g. core-api, worker).
	// The standalone cloud-sql-proxy service does NOT carry that flag.
	useIAM := os.Getenv("SAFESQL_USE_IAM") != "false"

	var target string
	if useIAM {
		podLabel := getEnvOrDefault("DB_PORT_FORWARD_POD_LABEL", "app=core")
		podName, err := resolveIAMPod(namespace, podLabel)
		if err != nil {
			return fmt.Errorf("no IAM-enabled pod found (label %s in ns %s): %w", podLabel, namespace, err)
		}
		target = podName
		fmt.Fprintf(os.Stderr, "[INFO] Port-forwarding to pod %s/%s (IAM proxy sidecar)\n", namespace, podName)
	} else {
		service := getEnvOrDefault("DB_PORT_FORWARD_SERVICE", "cloud-sql-proxy")
		target = fmt.Sprintf("svc/%s", service)
		fmt.Fprintf(os.Stderr, "[INFO] Port-forwarding to %s/%s\n", namespace, service)
	}

	cmd := exec.Command("kubectl", "port-forward",
		"-n", namespace,
		target,
		fmt.Sprintf("%s:%s", dbPort, targetPort))

	logFile := "/tmp/safesql-portforward.log"
	f, err := os.Create(logFile)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer f.Close()

	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start port-forward: %w", err)
	}

	pidFile := "/tmp/safesql-portforward.pid"
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "[WARNING] Could not save PID: %v\n", err)
	}

	dbHost := getEnvOrDefault("DB_HOST", "127.0.0.1")
	for i := 0; i < 20; i++ {
		if isPortAccessible(dbHost, dbPort) {
			fmt.Fprintf(os.Stderr, "[INFO] Port-forward established (PID: %d)\n", cmd.Process.Pid)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("port-forward failed to become ready within 10 seconds")
}

// resolveIAMPod returns the full pod resource path (e.g. "pod/core-abc-xyz") of
// any Running pod matched by labelSelector in namespace, ready to be passed directly
// to kubectl port-forward.
func resolveIAMPod(namespace, labelSelector string) (string, error) {
	cmd := exec.Command("kubectl", "get", "pod",
		"-n", namespace,
		"-l", labelSelector,
		"--field-selector=status.phase=Running",
		"-o", "name")

	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("kubectl get pod failed: %w", err)
	}

	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if name := strings.TrimSpace(line); name != "" {
			return name, nil
		}
	}

	return "", fmt.Errorf("no running pod with label %q found in namespace %q", labelSelector, namespace)
}

func isPortAccessible(host, port string) bool {
	cmd := exec.Command("nc", "-z", host, port)
	return cmd.Run() == nil
}

func getCurrentUsername() string {
	// Try multiple environment variables
	for _, env := range []string{"USER", "LOGNAME", "USERNAME"} {
		if username := os.Getenv(env); username != "" {
			return username
		}
	}

	// Try whoami command as fallback
	if cmd := exec.Command("whoami"); cmd != nil {
		if output, err := cmd.Output(); err == nil {
			return strings.TrimSpace(string(output))
		}
	}

	return "unknown"
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// ShowEnvironment displays current SafeSQL configuration from environment variables.
func ShowEnvironment() {
	fmt.Println("SafeSQL Environment Configuration:")
	fmt.Println()

	useIAMEnv := os.Getenv("SAFESQL_USE_IAM")
	iamExplicit := useIAMEnv != ""
	useIAM := useIAMEnv != "false"

	fmt.Println("Database:")
	fmt.Printf("  DB_HOST=%s\n", getEnvOrDefault("DB_HOST", "127.0.0.1"))
	fmt.Printf("  DB_PORT=%s\n", getEnvOrDefault("DB_PORT", "5432"))
	fmt.Printf("  DB_NAME=%s\n", getEnvOrDefault("DB_NAME", "vcp"))
	if useIAM {
		dbUser := os.Getenv("DB_USER")
		if dbUser == "" {
			fmt.Println("  DB_USER=(auto-discovered from cluster Workload Identity)")
		} else {
			fmt.Printf("  DB_USER=%s\n", dbUser)
		}
	} else {
		fmt.Printf("  DB_USER=%s\n", getEnvOrDefault("DB_USER", "postgres"))
	}
	if iamExplicit {
		fmt.Printf("  SAFESQL_USE_IAM=%s (explicit)\n", useIAMEnv)
	} else {
		fmt.Println("  SAFESQL_USE_IAM=(auto-detected from cluster)")
	}
	if useIAM {
		fmt.Println("  DB_PASSWORD=(ignored, IAM authentication)")
	} else if os.Getenv("DB_PASSWORD") != "" {
		fmt.Println("  DB_PASSWORD=***set***")
	} else {
		fmt.Println("  DB_PASSWORD=***not set***")
	}
	fmt.Printf("  DB_SSL_MODE=%s\n", getEnvOrDefault("DB_SSL_MODE", "disable"))
	fmt.Println()

	fmt.Println("GitHub:")
	if os.Getenv("GITHUB_TOKEN") != "" {
		fmt.Println("  GITHUB_TOKEN=***set***")
	} else {
		fmt.Println("  GITHUB_TOKEN=***not set***")
	}
	fmt.Printf("  SAFESQL_GITHUB_REPO=%s\n", getEnvOrDefault("SAFESQL_GITHUB_REPO", "VCP-VSA-control-Plane/vsa-control-plane"))
	fmt.Printf("  SAFESQL_GITHUB_BRANCH=%s\n", getEnvOrDefault("SAFESQL_GITHUB_BRANCH", "main"))
	fmt.Println()

	fmt.Println("Storage:")
	fmt.Printf("  SAFESQL_CONFIG_DIR=%s (local config only)\n", getEnvOrDefault("SAFESQL_CONFIG_DIR", fmt.Sprintf("%s/.safesql", os.Getenv("HOME"))))
	fmt.Printf("  SAFESQL_GCS_BUCKET=%s (plans & audit logs)\n", os.Getenv("SAFESQL_GCS_BUCKET"))
	fmt.Println()

	fmt.Println("Operator:")
	fmt.Printf("  SAFESQL_OPERATOR=%s\n", getEnvOrDefault("SAFESQL_OPERATOR", getCurrentUsername()))
	fmt.Println()

	fmt.Println("Auto-Setup:")
	fmt.Printf("  SAFESQL_NO_AUTO_SETUP=%s\n", getEnvOrDefault("SAFESQL_NO_AUTO_SETUP", "false"))
	fmt.Printf("  SAFESQL_AUTO_FETCH_PASSWORD=%s\n", getEnvOrDefault("SAFESQL_AUTO_FETCH_PASSWORD", "true"))
	fmt.Printf("  SAFESQL_AUTO_PORT_FORWARD=%s\n", getEnvOrDefault("SAFESQL_AUTO_PORT_FORWARD", "true"))
	fmt.Println()

	fmt.Println("Kubernetes (for auto-setup):")
	fmt.Printf("  DB_PORT_FORWARD_NAMESPACE=%s\n", getEnvOrDefault("DB_PORT_FORWARD_NAMESPACE", "vcp"))
	fmt.Printf("  DB_PORT_FORWARD_PORT=%s\n", getEnvOrDefault("DB_PORT_FORWARD_PORT", "5432"))
	fmt.Printf("  DB_PORT_FORWARD_POD_LABEL=%s  (IAM: pod label to find sidecar with --auto-iam-authn)\n",
		getEnvOrDefault("DB_PORT_FORWARD_POD_LABEL", "app=core"))
	fmt.Printf("  DB_PORT_FORWARD_SERVICE=%s  (password fallback: standalone proxy service)\n",
		getEnvOrDefault("DB_PORT_FORWARD_SERVICE", "cloud-sql-proxy"))
	fmt.Printf("  DB_SECRET_NAME=%s\n", getEnvOrDefault("DB_SECRET_NAME", "postgres-credentials"))
	fmt.Printf("  DB_SECRET_NAMESPACE=%s\n", getEnvOrDefault("DB_SECRET_NAMESPACE", "sde"))
}

// CheckDatabaseConnectivity tests database connection.
func CheckDatabaseConnectivity() error {
	dbHost := getEnvOrDefault("DB_HOST", "127.0.0.1")
	dbPort := getEnvOrDefault("DB_PORT", "5432")
	dbName := getEnvOrDefault("DB_NAME", "vcp")
	dbUser := getEnvOrDefault("DB_USER", "postgres")
	dbPassword := os.Getenv("DB_PASSWORD")

	fmt.Println("[INFO] Checking database connectivity...")
	fmt.Printf("  Host: %s\n", dbHost)
	fmt.Printf("  Port: %s\n", dbPort)
	fmt.Printf("  Database: %s\n", dbName)
	fmt.Printf("  User: %s\n", dbUser)
	fmt.Println()

	// Check port accessibility
	if isPortAccessible(dbHost, dbPort) {
		fmt.Println("[PASS] Database port is accessible")
	} else {
		fmt.Println("[FAIL] Cannot reach database port")
		fmt.Println("[INFO] Ensure port-forward is running or DB_HOST is set correctly")
		return fmt.Errorf("database port not accessible")
	}

	// Try actual connection if psql is available
	isIAM := os.Getenv("SAFESQL_USE_IAM") != "false"
	if _, err := exec.LookPath("psql"); err == nil && (dbPassword != "" || isIAM) {
		fmt.Println("[INFO] Testing database authentication...")

		cmd := exec.Command("psql",
			"-h", dbHost,
			"-p", dbPort,
			"-U", dbUser,
			"-d", dbName,
			"-c", "SELECT version();")
		if !isIAM {
			cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", dbPassword))
		}

		if output, err := cmd.CombinedOutput(); err == nil {
			fmt.Println("[PASS] Database connection successful")
			lines := strings.Split(string(output), "\n")
			if len(lines) > 0 {
				fmt.Printf("   %s\n", lines[0])
			}
		} else {
			fmt.Println("[FAIL] Database authentication failed")
			fmt.Printf("   Error: %s\n", string(output))
			return fmt.Errorf("database authentication failed")
		}
	} else {
		if !isIAM && dbPassword == "" {
			fmt.Println("[WARNING] DB_PASSWORD not set, skipping authentication test")
		}
	}

	return nil
}
