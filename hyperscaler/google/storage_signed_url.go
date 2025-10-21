package google

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	"cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"cloud.google.com/go/storage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	vsaWorkerSAEmail = env.GetString("VSA_WORKER_SA_EMAIL", "")
)

// GenerateSignedURL generates a signed URL for a GCS object using the storage client
func (gcp *GcpServices) GenerateSignedURL(ctx context.Context, bucketName, objectName string, duration time.Duration) (string, error) {
	logger := util.GetLogger(ctx)

	if gcp.AdminGCPService == nil || gcp.AdminGCPService.storageService == nil {
		return "", fmt.Errorf("storage service not initialized")
	}

	// Determine which service account is active
	var saEmail string
	var err error
	if env.GetString("ENV", "") == "local" {
		saEmail = vsaWorkerSAEmail
	} else {
		saEmail, err = gcp.getActiveServiceAccountEmail(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to detect service account: %w", err)
		}
	}

	iamClient, err := credentials.NewIamCredentialsClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create IAM credentials client: %w", err)
	}
	defer func(iamClient *credentials.IamCredentialsClient) {
		err := iamClient.Close()
		if err != nil {
			logger.Error("failed to close IAM credentials client", "error", err)
		}
	}(iamClient)

	opts := &storage.SignedURLOptions{
		Method:         "GET",
		Expires:        time.Now().Add(duration),
		Scheme:         storage.SigningSchemeV4,
		GoogleAccessID: saEmail,
		SignBytes: func(b []byte) ([]byte, error) {
			resp, err := iamClient.SignBlob(ctx, &credentialspb.SignBlobRequest{
				Name:    fmt.Sprintf("projects/-/serviceAccounts/%s", saEmail),
				Payload: b,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to sign blob: %w", err)
			}
			return resp.SignedBlob, nil
		},
	}

	signedURL, err := storage.SignedURL(bucketName, objectName, opts)
	if err != nil {
		logger.Error("Failed to generate signed URL",
			"error", err,
			"bucketName", bucketName,
			"objectName", objectName,
			"duration", duration)
		return "", fmt.Errorf("failed to generate signed URL: %w", err)
	}

	logger.Info("Generated signed URL successfully", "bucketName", bucketName, "objectName", objectName, "duration", duration)
	return signedURL, nil
}

// GenerateVSASignedURL generates a signed URL for VSA image based on the image name
func (gcp *GcpServices) GenerateVSASignedURL(ctx context.Context, vsaImageName string) (string, error) {
	logger := util.GetLogger(ctx)

	// Get bucket name from environment variable
	bucketName := env.GetString("VSA_IMAGE_BUCKET", "vsa-compute-images-tst")

	// Construct object name - assuming the image name follows the pattern
	// For example: "r9.17.1PxN_250902_0747_promo_image.tgz"
	objectName := vsaImageName

	// Get signed URL duration from environment variable (default: 12 hours)
	durationHours := env.GetInt("VSA_SIGNED_URL_DURATION_HOURS", 12)
	duration := time.Duration(durationHours) * time.Hour

	// Validate duration is within reasonable bounds (1 hour to 24 hours)
	if durationHours < 1 || durationHours > 24 {
		logger.Warn("VSA_SIGNED_URL_DURATION_HOURS is out of bounds, using default", "configuredHours", durationHours, "defaultHours", 12)
		duration = 12 * time.Hour
	}

	signedURL, err := gcp.GenerateSignedURL(ctx, bucketName, objectName, duration)
	if err != nil {
		logger.Error("Failed to generate VSA signed URL", "error", err, "vsaImageName", vsaImageName, "duration", duration)
		return "", err
	}

	logger.Info("Generated VSA signed URL", "vsaImageName", vsaImageName, "bucketName", bucketName, "duration", duration)
	return signedURL, nil
}

// GenerateSignedURLWithCustomDuration generates a signed URL with custom duration
func (gcp *GcpServices) GenerateSignedURLWithCustomDuration(ctx context.Context, bucketName, objectName string, duration time.Duration) (string, error) {
	return gcp.GenerateSignedURL(ctx, bucketName, objectName, duration)
}

func (gcp *GcpServices) getActiveServiceAccountEmail(ctx context.Context) (string, error) {
	const metaURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
	req, _ := http.NewRequestWithContext(ctx, "GET", metaURL, nil)
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to query metadata server: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			util.GetLogger(ctx).Error("failed to close response body", "error", err)
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("metadata request failed: %s (%s)", resp.Status, string(body))
	}

	email, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}
	return string(email), nil
}
