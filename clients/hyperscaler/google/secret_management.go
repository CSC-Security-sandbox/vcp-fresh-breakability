package google

import (
	"encoding/base64"
	"fmt"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"google.golang.org/api/secretmanager/v1"
)

var (
	AddSecretVersion = _addSecretVersion
	GetSecretVersion = _getSecretVersion
)

// CreateSecret GetCertificate creates a secret
func (gcpService *GcpServices) CreateSecret(projectID, region, secretID, secretValue string) (*models.CustomSecret, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateSecret for project id : %s, secret id : %s", projectID, secretID))

	// Define the parent resource
	parent := fmt.Sprintf("projects/%s", projectID)

	// Create the secret
	// TODO : Add expiration time and rotation for the secret
	secret := &secretmanager.Secret{
		Replication: &secretmanager.Replication{
			UserManaged: &secretmanager.UserManaged{
				Replicas: []*secretmanager.Replica{{Location: region}},
			},
		},
	}

	secret, err := gcpService.AdminGCPService.secretManagerService.Projects.Secrets.Create(parent, secret).SecretId(secretID).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("CreateSecret failed for project : %s, secretID : %s, err : %s", projectID, secretID, err.Error())
		return nil, err
	}

	gcpService.Logger.Debugf("CreateSecret success with response :  %s", secret.Name)

	// Add secret version
	version, err := AddSecretVersion(gcpService, projectID, secretID, secretValue)
	if err != nil {
		return nil, err
	}

	customSecret, err := _convertSecretToCustomSecret(secret, version)
	if err != nil {
		return nil, err
	}

	return customSecret, nil
}

// GetSecretWithLatestVersion GetSecret retrieves a secret from the secret manager.
func (gcpService *GcpServices) GetSecretWithLatestVersion(projectID, secretID string) (*models.CustomSecret, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetSecretWithLatestVersion for project id : %s, secretID : %s", projectID, secretID))
	name := fmt.Sprintf("projects/%s/secrets/%s", projectID, secretID)

	secret, err := gcpService.AdminGCPService.secretManagerService.Projects.Secrets.Get(name).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("GetSecretWithLatestVersion failed for secret : %s, err : %s", name, err.Error())
		return nil, err
	}

	version, err := GetSecretVersion(gcpService, projectID, secretID, LatestVersion)
	if err != nil {
		return nil, err
	}
	gcpService.Logger.Debug(fmt.Sprintf("GetSecretWithLatestVersion success with response :  %s", name))
	customSecret, err := _convertSecretToCustomSecret(secret, version)
	if err != nil {
		return nil, err
	}
	return customSecret, nil
}

// DeleteSecret deletes a secret from the secret manager
func (gcpService *GcpServices) DeleteSecret(projectID, secretID string) error {
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetSecretWithLatestVersion for project id : %s, secretID : %s", projectID, secretID))
	name := fmt.Sprintf("projects/%s/secrets/%s", projectID, secretID)
	_, err := gcpService.AdminGCPService.secretManagerService.Projects.Secrets.Delete(name).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("GetSecretWithLatestVersion failed for secret : %s, err : %s", name, err.Error())
		return err
	}
	return nil
}

// AddSecretVersion creates a secret version and stores the private key in the secret manager.
func _addSecretVersion(gcpService *GcpServices, projectID, secretName, secretValue string) (*models.CustomSecretVersion, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateSecretVersion for project id : %s, secret id : %s", projectID, secretName))
	encodedData := base64.StdEncoding.EncodeToString([]byte(secretValue))
	parent := fmt.Sprintf("projects/%s/secrets/%s", projectID, secretName)
	req := &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{
			Data: encodedData,
		},
	}
	secretVersion, err := gcpService.AdminGCPService.secretManagerService.Projects.Secrets.AddVersion(parent, req).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("CreateSecretVersion failed for project : %s, secret : %s, err : %s", projectID, secretName, err.Error())
		return nil, err
	}

	customSecretVersion, err := _convertSecretVersionToCustomSecretVersion(secretVersion.Name, secretValue)
	if err != nil {
		return nil, err
	}
	return customSecretVersion, nil
}

// GetSecretVersion retrieves a secret version from the secret manager.
func _getSecretVersion(gcpService *GcpServices, projectID, secretName, versionID string) (*models.CustomSecretVersion, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetSecretVersion for project id : %s, secret id : %s, version id : %s", projectID, secretName, versionID))
	name := fmt.Sprintf("projects/%s/secrets/%s/versions/%s", projectID, secretName, versionID)

	secretVersion, err := gcpService.AdminGCPService.secretManagerService.Projects.Secrets.Versions.Access(name).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("GetSecretVersion failed for secret : %s, err : %s", name, err.Error())
		return nil, err
	}

	gcpService.Logger.Debug(fmt.Sprintf("GetSecretVersion success with response :  %s", name))
	if secretVersion.Payload == nil {
		return nil, fmt.Errorf("secret version name is empty")
	}
	secretValue, err := base64.StdEncoding.DecodeString(secretVersion.Payload.Data)
	if err != nil {
		gcpService.Logger.Errorf("unable to decode key-data for secret %s with error: %v", secretName, err)
		return nil, err
	}
	customSecretVersion, err := _convertSecretVersionToCustomSecretVersion(secretVersion.Name, string(secretValue))
	if err != nil {
		return nil, err
	}
	return customSecretVersion, nil
}
