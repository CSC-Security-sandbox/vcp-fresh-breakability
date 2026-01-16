package google

import (
	"errors"
	"fmt"
	"time"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/privateca/v1"
)

const (
	LatestVersion      = "latest"
	PrivilegeWithdrawn = "PRIVILEGE_WITHDRAWN"
)

// CreateCertificate creates a new certificate in the specified CA. Reference: https://cloud.google.com/certificate-authority-service/docs/reference/rest/v1/projects.locations.caPools.certificates/create
func (gcpService *GcpServices) CreateCertificate(cert *models.CustomCertificate) (*models.CustomCertificate, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateCertificate for project name : %s, region : %s, pool : %s, certificate id : %s", cert.CertOwningEntity, cert.Region, cert.CaGroupName, cert.CertificateID))

	caResourceName := fmt.Sprintf("projects/%s/locations/%s/caPools/%s/certificateAuthorities/%s", cert.CertOwningEntity, cert.Region, cert.CaGroupName, cert.CaName)
	parent := fmt.Sprintf("projects/%s/locations/%s/caPools/%s", cert.CertOwningEntity, cert.Region, cert.CaGroupName)

	certificate := &privateca.Certificate{
		PemCsr:                     cert.PemCsr,
		Lifetime:                   env.CertificateLifetime,
		IssuerCertificateAuthority: caResourceName,
		CreateTime:                 time.Now().UTC().Format(time.RFC3339),
	}

	certificate, err := gcpService.AdminGCPService.privateCaService.Projects.Locations.CaPools.Certificates.Create(parent, certificate).CertificateId(cert.CertificateID).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to create certificate: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}

	customCertificate, err := ValidateAndConvertPrivateCACertificateToCustomCertificate(cert.CertificateID, certificate)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrModelConversionError, err)
	}
	return customCertificate, nil
}

// RevokeCertificate revokes a certificate in the specified CA. Reference: https://cloud.google.com/certificate-authority-service/docs/reference/rest/v1/projects.locations.caPools.certificates/revoke
func (gcpService *GcpServices) RevokeCertificate(cert *models.CustomCertificate) (string, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateCertificate for project name : %s, region : %s, pool : %s, certificate id : %s", cert.CertOwningEntity, cert.Region, cert.CaGroupName, cert.CertificateID))

	resourceName := fmt.Sprintf("projects/%s/locations/%s/caPools/%s/certificates/%s", cert.CertOwningEntity, cert.Region, cert.CaGroupName, cert.CertificateID)
	revokeCertificateRequest := &privateca.RevokeCertificateRequest{
		Reason: PrivilegeWithdrawn,
	}

	_, err := gcpService.AdminGCPService.privateCaService.Projects.Locations.CaPools.Certificates.Revoke(resourceName, revokeCertificateRequest).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to revoke certificate: %v", err)
		return "", vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}

	return resourceName, nil
}

// GetCertificate retrieves a certificate in the specified CA. Reference: https://cloud.google.com/certificate-authority-service/docs/reference/rest/v1/projects.locations.caPools.certificates/get
func (gcpService *GcpServices) GetCertificate(projectID, region, poolName, certificateID string) (*models.CustomCertificate, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetCertificate for project name : %s, region : %s, pool : %s, certificate id : %s", projectID, region, poolName, certificateID))

	certificateName := fmt.Sprintf("projects/%s/locations/%s/caPools/%s/certificates/%s", projectID, region, poolName, certificateID)
	certificate, err := gcpService.AdminGCPService.privateCaService.Projects.Locations.CaPools.Certificates.Get(certificateName).Context(gcpService.Ctx).Do()

	if err != nil {
		gcpService.Logger.Errorf("GetCertificate failed for certificate : %s, err: %s", certificateName, err.Error())
		return nil, googleResourceNotFoundCheck(err)
	}

	// Check if the certificate has revocation details implies certificate is revoked
	if certificate.RevocationDetails != nil && certificate.RevocationDetails.RevocationState == PrivilegeWithdrawn {
		gcpService.Logger.Debug(fmt.Sprintf("Certificate :%s is in revoked state already", certificateName))
		return nil, fmt.Errorf("certificate %s is revoked and cannot be used", certificateID)
	}

	gcpService.Logger.Debug(fmt.Sprintf("GetCertificate success with response :  %s", certificateID))
	customCertificate, err := ValidateAndConvertPrivateCACertificateToCustomCertificate(certificateID, certificate)
	if err != nil {
		return nil, err
	}
	return customCertificate, nil
}

func googleResourceNotFoundCheck(err error) error {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) && gErr.Code == 404 {
		return nil
	}
	return err
}
