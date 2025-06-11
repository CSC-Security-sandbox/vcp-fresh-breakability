package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
)

type UpdateConfig struct {
	FilePath string   `json:"filePath"`
	YamlPath []string `json:"yamlPath"`
}

var digestYAMLPathToEnv = map[string]string{
	".images.core.digest":         "core-digest",
	".images.vcpDbMigrate.digest": "vcp-db-migrate-digest",
	".images.googleProxy.digest":  "google-proxy-digest",
	".images.vcpWorker.digest":    "vcp-worker-digest",
}

func UpdateKeys(file string, yamlPaths []string, version string) error {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		return fmt.Errorf("file %s not found", file)
	}

	log.Printf("Updating %s...\n", file)
	for _, yamlPath := range yamlPaths {
		// Prefix the path with a dot if not already present
		if !strings.HasPrefix(yamlPath, ".") {
			yamlPath = "." + yamlPath
		}

		// Handle array paths dynamically
		if strings.Contains(yamlPath, "[]") {
			// Extract the base path before the array indicator
			basePath := strings.Split(yamlPath, "[]")[0]

			// Use yq to iterate over array elements and update each one
			cmd := exec.Command("yq", "eval", "-i", fmt.Sprintf(`with(%s[]; .version = "%s")`, basePath, version), file)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to update array path %s in file %s: %v\nstderr: %s", yamlPath, file, err, stderr.String())
			}
		} else if strings.HasSuffix(yamlPath, "digest") {
			envVar, exists := digestYAMLPathToEnv[yamlPath]
			if !exists {
				return fmt.Errorf("unknown digest path %s in file %s", yamlPath, file)
			}
			envValue := os.Getenv(envVar)
			if envValue == "" {
				return fmt.Errorf("environment variable %s is not set", envVar)
			}
			cmd := exec.Command("yq", "eval", "-i", fmt.Sprintf(`%s = "%s"`, yamlPath, envValue), file)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to update digest key %s in file %s: %v\nstderr: %s", yamlPath, file, err, stderr.String())
			}
		} else {
			// Update simple paths
			updatedVersion := version
			if strings.HasSuffix(yamlPath, ".tag") {
				updatedVersion = "v" + updatedVersion
			}
			cmd := exec.Command("yq", "eval", "-i", fmt.Sprintf(`%s = "%s"`, yamlPath, updatedVersion), file)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to update key %s in file %s: %v\nstderr: %s", yamlPath, file, err, stderr.String())
			}
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 3 {
		log.Println("Usage: ./updateHelmVersion <version> <config.json>")
		os.Exit(1)
	}

	version := strings.TrimPrefix(os.Args[1], "v") // Remove 'v' prefix if present
	configFile := os.Args[2]

	// Read and parse the JSON file
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Printf("Error reading config file: %v\n", err)
		os.Exit(1)
	}

	var configs []UpdateConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		log.Printf("Error parsing config file: %v\n", err)
		os.Exit(1)
	}

	// Update YAML files based on the JSON configuration
	for _, config := range configs {
		if err := UpdateKeys(config.FilePath, config.YamlPath, version); err != nil {
			log.Printf("Error updating keys in file %s: %v\n", config.FilePath, err)
			os.Exit(1)
		}
	}

	log.Println("Helm Version update completed!")
}
