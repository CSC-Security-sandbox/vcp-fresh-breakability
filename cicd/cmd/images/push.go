package images

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/spf13/cobra"
)

var registry string

var pushCmd = &cobra.Command{
	Use:   "push",
	Short: "A command to handle all images push functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true

		if registry != "ghcr" && registry != "gcp" && registry != "" {
			return fmt.Errorf("invalid registry value: %s, must be 'ghcr', 'gcp' ", registry)
		}
		err := PushImages(registry)
		if err != nil {
			return err
		}
		return nil
	},
}

func PushImages(registry string) error {
	// Environment variables
	imagesConfig := GetImagesConfig()

	// List of images to process
	images := []string{"google-proxy", "core", "vcp-db-migrate", "vcp-worker", "telemetry"}
	log.Printf("Images to process: %v\n", images)

	// WaitGroup to manage concurrency
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errorsArr []error

	for _, image := range images {
		// Process GHCR if specified
		if registry == "ghcr" || registry == "" {
			wg.Add(1)
			go func(image string) {
				defer wg.Done()
				log.Printf("Processing image %s for GHCR\n", image)

				// Tagging the image for GHCR
				tagCmd := exec.Command("docker", "tag", fmt.Sprintf("%s:%s", image, imagesConfig.ImagesTag),
					fmt.Sprintf("%s/%s/%s:%s", imagesConfig.PrimaryRegistry, imagesConfig.PrimaryRepo, image, imagesConfig.ImagesTag))
				log.Printf("Running command: %s\n", tagCmd.String())
				if err := tagCmd.Run(); err != nil {
					mu.Lock()
					errorsArr = append(errorsArr, fmt.Errorf("failed to tag image %s for GHCR: %v", image, err))
					mu.Unlock()
					log.Printf("Error tagging image %s for GHCR: %v\n", image, err)
					return
				}
				log.Printf("Successfully tagged image %s for GHCR\n", image)

				// Pushing the image to GHCR
				pushCmd := exec.Command("docker", "push", fmt.Sprintf("%s/%s/%s:%s", imagesConfig.PrimaryRegistry, imagesConfig.PrimaryRepo, image, imagesConfig.ImagesTag))
				log.Printf("Running command: %s\n", pushCmd.String())
				pushCmd.Stderr = os.Stderr
				pushCmd.Stdout = os.Stdout
				if err := pushCmd.Run(); err != nil {
					mu.Lock()
					errorsArr = append(errorsArr, fmt.Errorf("failed to push image %s to GHCR: %v", image, err))
					mu.Unlock()
					log.Printf("Error pushing image %s to GHCR: %v\n", image, err)
					return
				}
				log.Printf("Successfully pushed image %s to GHCR\n", image)
			}(image)
		}

		// Process GCP if specified
		if registry == "gcp" || registry == "" {
			wg.Add(1)
			go func(image string) {
				defer wg.Done()
				log.Printf("Processing image %s for GCP\n", image)

				// Tagging the image for GCP
				tagCmd := exec.Command("docker", "tag", fmt.Sprintf("%s:%s", image, imagesConfig.ImagesTag),
					fmt.Sprintf("%s/%s/%s/%s:%s", imagesConfig.SecondaryRegistry, imagesConfig.GCPProject, imagesConfig.SecondaryRepo, image, imagesConfig.ImagesTag))
				log.Printf("Running command: %s\n", tagCmd.String())
				if err := tagCmd.Run(); err != nil {
					mu.Lock()
					errorsArr = append(errorsArr, fmt.Errorf("failed to tag image %s for GCP: %v", image, err))
					mu.Unlock()
					log.Printf("Error tagging image %s for GCP: %v\n", image, err)
					return
				}
				log.Printf("Successfully tagged image %s for GCP\n", image)

				// Pushing the image to GCP
				pushCmd := exec.Command("docker", "push", fmt.Sprintf("%s/%s/%s/%s:%s", imagesConfig.SecondaryRegistry, imagesConfig.GCPProject, imagesConfig.SecondaryRepo, image, imagesConfig.ImagesTag))
				log.Printf("Running command: %s\n", pushCmd.String())
				pushCmd.Stderr = os.Stderr
				pushCmd.Stdout = os.Stdout
				if err := pushCmd.Run(); err != nil {
					mu.Lock()
					errorsArr = append(errorsArr, fmt.Errorf("failed to push image %s to GCP: %v", image, err))
					mu.Unlock()
					log.Printf("Error pushing image %s to GCP: %v\n", image, err)
					return
				}
				log.Printf("Successfully pushed image %s to GCP\n", image)
			}(image)
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Handle errors
	if len(errorsArr) > 0 {
		log.Println("Errors occurred during image processing:")
		for _, err := range errorsArr {
			log.Println(err)
		}
		return errors.Join(errorsArr...)
	}
	log.Println("All images pushed successfully.")
	return nil
}

func init() {
	pushCmd.Flags().StringVarP(&registry, "registry", "r", "", "Specify the registry to push images to: 'ghcr' or 'gcp'")
}
