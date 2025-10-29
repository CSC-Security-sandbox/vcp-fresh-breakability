package images

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	ghcrOrg    string
	imagePath  string
	gcpProject string
	imagesTag  string
	gcpRegion  string
)

const primaryRegistry = "ghcr.io"
const secondaryRegistry = "us-docker.pkg.dev"
const secondaryRegistryTemplate = "%s-docker.pkg.dev"
const defaultGhcrOrg = "vcp-vsa-control-plane"
const defaultImagePath = "vcp-container-images-us"
const defaultGCPProject = "gcnv-artifact-registry-nonprod"
const gcpProjectVar = "GCP_PROJECT"
const ghcrOrgVar = "GHCR_ORG"
const imagePathVar = "IMAGE_PATH"
const imageTagVar = "IMAGES_TAG"
const gcpRegionVar = "GCP_REGION"

type ImagesConfig struct {
	PrimaryRegistry   string
	SecondaryRegistry string
	GHCROrg           string
	Path              string
	ImagesTag         string
	GCPProject        string
}

func GetImagesConfig() ImagesConfig {
	imagesConfig := ImagesConfig{
		PrimaryRegistry:   primaryRegistry,
		SecondaryRegistry: secondaryRegistry,
		GHCROrg:           defaultGhcrOrg,
		Path:              defaultImagePath,
		GCPProject:        defaultGCPProject,

		ImagesTag: "latest",
	}
	if ghcrOrg != "" {
		imagesConfig.GHCROrg = ghcrOrg
	}
	if imagePath != "" {
		imagesConfig.Path = imagePath
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
	ghcrOrg = os.Getenv(ghcrOrgVar)
	imagePath = os.Getenv(imagePathVar)
	imagesTag = os.Getenv(imageTagVar)
	gcpProject = os.Getenv(gcpProjectVar)
	gcpRegion = os.Getenv(gcpRegionVar)

	ImagesCmd.AddCommand(pushCmd)
}
