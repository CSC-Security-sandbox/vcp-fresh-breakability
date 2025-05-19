package images

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var (
	ghcrRepo   string
	gcpRepo    string
	gcpProject string
	imagesTag  string
	gcpRegion  string
)

const primaryRegistry = "ghcr.io"
const secondaryRegistry = "us-docker.pkg.dev"
const secondaryRegistryTemplate = "%s-docker.pkg.dev"
const defaultPrimaryRepo = "vcp-vsa-control-plane"
const defaultSecondaryRepo = "vcp-container-images-us"
const defaultGCPProject = "gcnv-artifact-registry-nonprod"
const gcpProjectVar = "GCP_PROJECT"
const ghcrRepoVar = "GHCR_REPO"
const gcpRepoVar = "GCP_REPO"
const imageTagVar = "IMAGES_TAG"
const gcpRegionVar = "GCP_REGION"

type ImagesConfig struct {
	PrimaryRegistry   string
	SecondaryRegistry string
	PrimaryRepo       string
	SecondaryRepo     string
	ImagesTag         string
	GCPProject        string
}

func GetImagesConfig() ImagesConfig {
	imagesConfig := ImagesConfig{
		PrimaryRegistry:   primaryRegistry,
		SecondaryRegistry: secondaryRegistry,
		PrimaryRepo:       defaultPrimaryRepo,
		SecondaryRepo:     defaultSecondaryRepo,
		GCPProject:        defaultGCPProject,

		ImagesTag: "latest",
	}
	if ghcrRepo != "" {
		imagesConfig.PrimaryRepo = ghcrRepo
	}
	if gcpRepo != "" {
		imagesConfig.SecondaryRepo = gcpRepo
	}
	if imagesTag != "" {
		imagesConfig.ImagesTag = imagesTag
	}
	if gcpProject != "" {
		imagesConfig.GCPProject = gcpProject
	}
	if gcpRegion != "" {
		imagesConfig.SecondaryRegistry = fmt.Sprintf(secondaryRegistryTemplate, strings.ToLower(gcpRegion))
	}
	return imagesConfig
}

var ImagesCmd = &cobra.Command{
	Use:   "images",
	Short: "A command to handle all images functionalities",
}

func init() {
	ghcrRepo = os.Getenv(ghcrRepoVar)
	gcpRepo = os.Getenv(gcpRepoVar)
	imagesTag = os.Getenv(imageTagVar)
	gcpProject = os.Getenv(gcpProjectVar)
	gcpRegion = os.Getenv(gcpRegionVar)

	ImagesCmd.AddCommand(pushCmd)
}
