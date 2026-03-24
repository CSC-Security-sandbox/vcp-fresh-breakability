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
func AutoSetup() error {
	// Check if auto-setup is disabled
	if os.Getenv("SAFESQL_NO_AUTO_SETUP") == "true" {
		return nil
	}

	// 1. Create necessary directories
	if err := createDirectories(); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	// 2. Fetch DB password from Kubernetes if not set
	if os.Getenv("DB_PASSWORD") == "" && os.Getenv("SAFESQL_AUTO_FETCH_PASSWORD") != "false" {
		if err := fetchDBPasswordFromK8s(); err != nil {
			// Non-fatal: warn but continue
			fmt.Fprintf(os.Stderr, "[WARNING] Could not auto-fetch DB password: %v\n", err)
			fmt.Fprintln(os.Stderr, "[INFO] Set DB_PASSWORD manually or ensure kubectl access to secrets")
		}
	}

	// 3. Setup port-forward if needed
	if shouldSetupPortForward() {
		if err := setupPortForward(); err != nil {
			// Non-fatal for some commands
			fmt.Fprintf(os.Stderr, "[WARNING] Could not setup port-forward: %v\n", err)
			fmt.Fprintln(os.Stderr, "[INFO] Ensure kubectl is configured or set DB_HOST to accessible host")
		}
	}

	// 4. Set default operator if not set
	if os.Getenv("SAFESQL_OPERATOR") == "" {
		if username := getCurrentUsername(); username != "" {
			os.Setenv("SAFESQL_OPERATOR", username)
		}
	}

	return nil
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
	service := getEnvOrDefault("DB_PORT_FORWARD_SERVICE", "cloud-sql-proxy")
	namespace := getEnvOrDefault("DB_PORT_FORWARD_NAMESPACE", "sde")
	targetPort := getEnvOrDefault("DB_PORT_FORWARD_PORT", "5432")

	fmt.Fprintf(os.Stderr, "🔌 Setting up port-forward to %s/%s...\n", namespace, service)

	// Check if kubectl is available and configured
	if err := exec.Command("kubectl", "cluster-info").Run(); err != nil {
		return fmt.Errorf("kubectl not configured or not accessible")
	}

	// Start port-forward in background
	cmd := exec.Command("kubectl", "port-forward",
		"-n", namespace,
		fmt.Sprintf("svc/%s", service),
		fmt.Sprintf("%s:%s", dbPort, targetPort))

	// Redirect output to log file
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

	// Save PID for cleanup'
	pidFile := "/tmp/safesql-portforward.pid"
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "[WARNING] Could not save PID: %v\n", err)
	}

	// Wait for port to become accessible (max 10 seconds)
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
	fmt.Println("Database:")
	fmt.Printf("  DB_HOST=%s\n", getEnvOrDefault("DB_HOST", "127.0.0.1"))
	fmt.Printf("  DB_PORT=%s\n", getEnvOrDefault("DB_PORT", "5432"))
	fmt.Printf("  DB_NAME=%s\n", getEnvOrDefault("DB_NAME", "vcp"))
	fmt.Printf("  DB_USER=%s\n", getEnvOrDefault("DB_USER", "postgres"))
	if os.Getenv("DB_PASSWORD") != "" {
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
	fmt.Printf("  DB_SECRET_NAME=%s\n", getEnvOrDefault("DB_SECRET_NAME", "postgres-credentials"))
	fmt.Printf("  DB_SECRET_NAMESPACE=%s\n", getEnvOrDefault("DB_SECRET_NAMESPACE", "sde"))
	fmt.Printf("  DB_PORT_FORWARD_SERVICE=%s\n", getEnvOrDefault("DB_PORT_FORWARD_SERVICE", "cloud-sql-proxy"))
	fmt.Printf("  DB_PORT_FORWARD_NAMESPACE=%s\n", getEnvOrDefault("DB_PORT_FORWARD_NAMESPACE", "sde"))
	fmt.Printf("  DB_PORT_FORWARD_PORT=%s\n", getEnvOrDefault("DB_PORT_FORWARD_PORT", "5432"))
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
	if _, err := exec.LookPath("psql"); err == nil && dbPassword != "" {
		fmt.Println("[INFO] Testing database authentication...")

		cmd := exec.Command("psql",
			"-h", dbHost,
			"-p", dbPort,
			"-U", dbUser,
			"-d", dbName,
			"-c", "SELECT version();")
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", dbPassword))

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
		if dbPassword == "" {
			fmt.Println("[WARNING] DB_PASSWORD not set, skipping authentication test")
		}
	}

	return nil
}
