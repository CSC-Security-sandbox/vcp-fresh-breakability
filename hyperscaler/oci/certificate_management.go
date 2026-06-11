package oci

import (
	"fmt"
	"time"

	"github.com/oracle/oci-go-sdk/v65/certificates"
	"github.com/oracle/oci-go-sdk/v65/certificatesmanagement"
	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// ──────────────────────────────────────────────────────────────────────────────
//
// OCI Certificates Service docs:
//   - CreateCertificate:        https://docs.oracle.com/en-us/iaas/api/#/en/certificatesmanagement/20210224/Certificate/CreateCertificate
//   - GetCertificate:           https://docs.oracle.com/en-us/iaas/api/#/en/certificatesmanagement/20210224/Certificate/GetCertificate
//   - ScheduleCertificateDeletion: https://docs.oracle.com/en-us/iaas/api/#/en/certificatesmanagement/20210224/Certificate/ScheduleCertificateDeletion
//   - GetCertificateBundle:     https://docs.oracle.com/en-us/iaas/api/#/en/certificates/20210224/CertificateBundle/GetCertificateBundle
//   - Go SDK reference (mgmt):  https://docs.oracle.com/en-us/iaas/tools/go/latest/certificatesmanagement/index.html
//   - Go SDK reference (read):  https://docs.oracle.com/en-us/iaas/tools/go/latest/certificates/index.html
//
// Why this file exists:
//
// In GCP we generate the certificate ourselves: GenerateCSR (local RSA + CSR
// build) → CreatePrivateKeyInSecretManager → CreateCertificateInCAS. Three API
// calls across two services, with the private key managed by us.
//
// On OCI we use the *internally-managed* flow instead. OCI generates the key
// pair, builds the CSR, and signs the certificate against the issuer CA in a
// single CreateCertificate call. The private key is then retrievable via
// GetCertificateBundle with CertificateBundleType=WITH_PRIVATE_KEY — there is
// no separate Vault/Secret Manager fetch. This eliminates the local-keygen
// step entirely.
//
//	GCP step (3 ops, 2 services)                  | OCI internally-managed (1 op, 1 service)
//	----------------------------------------------|----------------------------------------------
//	1. GenerateCSR (local RSA + CSR)              | OCI generates the key pair internally
//	2. CreatePrivateKeyInSecretManager (GCP SM)   | OCI stores the private key internally
//	3. CreateCertificate (GCP CAS, signs CSR)     | OCI signs the cert with its internal CA
//
// ──────────────────────────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────────────────────────
// Test-seam variables (same pattern as the secret-management file).
// Tests can override these to isolate the public methods from the OCI SDK.
// ──────────────────────────────────────────────────────────────────────────────

var (
	GetCertificateBundleWithPrivateKey = _getCertificateBundleWithPrivateKey
)

// Polling defaults for WaitForCertificateActive. Tuned for OCI's typical
// CREATING → ACTIVE transition (a few seconds) with conservative headroom.
//
// These are declared as `var` (not `const`) so unit tests that exercise the
// activity-level cert helpers (which transitively call WaitForCertificateActive)
// can shrink the poll interval to keep the suite fast without forcing a
// pollInterval parameter through every public API. Production code must
// never mutate these — they are package-private and intended as a test seam
// only. See _createCertificateForVSAClusterOCI for the upstream caller.
var (
	defaultCertActivePollInterval = 3 * time.Second
	defaultCertActiveTimeout      = 90 * time.Second
)

// String constants mirroring certificatesmanagement.CertificateLifecycleStateEnum
// values. Exposed so callers outside this package (notably ontap_provider.go,
// which deliberately avoids importing the OCI SDK directly) can match on the
// OCICustomCertificate.LifecycleState field without pulling in
// github.com/oracle/oci-go-sdk/v65/certificatesmanagement.
const (
	CertLifecycleStateCreating           = "CREATING"
	CertLifecycleStateActive             = "ACTIVE"
	CertLifecycleStateUpdating           = "UPDATING"
	CertLifecycleStateFailed             = "FAILED"
	CertLifecycleStateDeleting           = "DELETING"
	CertLifecycleStateDeleted            = "DELETED"
	CertLifecycleStateSchedulingDeletion = "SCHEDULING_DELETION"
	CertLifecycleStatePendingDeletion    = "PENDING_DELETION"
)

// OCICustomCertificate is the OCI-side counterpart of hyperscaler/models.CustomCertificate.
//
// It includes PrivateKeyPem because OCI's internally-managed flow returns the
// private key inline via GetCertificateBundle(WITH_PRIVATE_KEY). On GCP the
// private key lives separately in Secret Manager and is joined back into a
// CustomCertificateResponse by the orchestrator; on OCI everything required
// for ONTAP mTLS comes from a single API call.
type OCICustomCertificate struct {
	// Ocid is the OCI-assigned certificate OCID (analogous to GCP
	// CustomCertificate.Name resource path).
	Ocid string

	// Name is the human-friendly certificate name supplied at creation time.
	Name string

	// Subject fields populated from the OCI Certificate.Subject.
	SubjectCommonName   string
	SubjectOrganization string
	SubjectAltNames     []string

	// PEM material. PemCertificate and PemCertificateChain are populated only
	// after the certificate transitions from CREATING to ACTIVE and the bundle
	// has been retrieved. PrivateKeyPem is populated only when the bundle is
	// fetched with CertificateBundleType=WITH_PRIVATE_KEY.
	PemCertificate      string
	PemCertificateChain string
	PrivateKeyPem       string

	// SerialNumber is the certificate serial in OCI's octet format
	// (e.g. "03 AC FC FA ..."). Required for ONTAP ModifySSL.
	SerialNumber string

	// VersionNumber is the certificate version returned by OCI; useful when
	// revoking a specific version (RevokeCertificateVersion).
	VersionNumber int64

	// Validity window — populated from the bundle's Validity object.
	NotBefore *time.Time
	NotAfter  *time.Time

	// CompartmentID and IssuerCAOCID identify where the cert lives and which
	// CA signed it. Mirror GCP's CertOwningEntity / IssuerCertificateAuthority.
	CompartmentID string
	IssuerCAOCID  string

	// LifecycleState is the current OCI lifecycle state (CREATING, ACTIVE,
	// DELETING, ...). Empty when populated only from a bundle retrieval.
	LifecycleState string

	// TimeCreated is the certificate's creation timestamp.
	TimeCreated *time.Time
}

// CreateCertificate creates an internally-managed certificate in OCI Certificates
// Service. OCI generates the RSA key pair, builds the CSR, and signs the cert
// with the issuer CA — all in this single API call.
//
// GCP equivalent: GenerateCSR + CreatePrivateKeyInSecretManager + CreateCertificate.
//
// Parameters:
//   - compartmentID:        OCID of the compartment that will own the certificate
//     (analogous to GCP CertOwningEntity / projectID).
//   - issuerCAOCID:         OCID of the OCI private CA that will sign the cert
//     (analogous to GCP CaName/CaPoolName resource path).
//   - certName:             Human-friendly cert name; must be unique in the
//     compartment (analogous to GCP CertificateID).
//   - commonName:           Certificate Subject CN (e.g. ONTAP admin username).
//   - organization:         Certificate Subject O (e.g. "Netapp").
//   - dnsDomains:           DNS SANs (e.g. "*.<cluster>.<dnsZone>").
//   - isServerAuthEnabled:  true → CertificateProfileType=TLS_SERVER_OR_CLIENT
//     (DigitalSignature + KeyEncipherment + clientAuth + serverAuth);
//     false → TLS_CLIENT (clientAuth only). Mirrors the
//     ASN.1 EKU OID handling in hyperscaler.GenerateCSR.
//   - validityDays:         Cert lifetime in days. Pass <=0 to use the
//     OCI_CERTIFICATE_VALIDITY_DAYS env default.
//
// Returns OCICustomCertificate populated with metadata only — the cert is in
// CREATING state on return; no PEM is present yet. Callers should poll via
// GetCertificate() (or wait for the lifecycle state to become ACTIVE) before
// calling GetCertificate to retrieve the signed cert + private key bundle.
func (ociService *OciServices) CreateCertificate(
	compartmentID string,
	issuerCAOCID string,
	certName string,
	commonName string,
	organization string,
	dnsDomains []string,
	isServerAuthEnabled bool,
	validityDays int,
) (*OCICustomCertificate, error) {
	ociService.Logger.Infof("Calling CreateCertificate — compartment: %s, certName: %s, commonName: %s",
		compartmentID, certName, commonName)

	if compartmentID == "" || issuerCAOCID == "" || certName == "" || commonName == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceProvisionError,
			fmt.Errorf("CreateCertificate: compartmentID, issuerCAOCID, certName and commonName are required"))
	}

	// Choose the certificate profile. OCI replaces GCP's manual EKU OID list
	// (clientAuth 1.3.6.1.5.5.7.3.2 / serverAuth 1.3.6.1.5.5.7.3.1) with a
	// single enum that also configures KeyUsage:
	//   TLS_SERVER_OR_CLIENT → DigitalSignature + KeyEncipherment + clientAuth + serverAuth
	//   TLS_CLIENT           → DigitalSignature + KeyEncipherment + clientAuth only
	profileType := certificatesmanagement.CertificateProfileTypeTlsClient
	if isServerAuthEnabled {
		profileType = certificatesmanagement.CertificateProfileTypeTlsServerOrClient
	}

	// Build the SAN list (DNS only — matches the GCP DNSNames in the CSR).
	sans := make([]certificatesmanagement.CertificateSubjectAlternativeName, 0, len(dnsDomains))
	for _, domain := range dnsDomains {
		if domain == "" {
			continue
		}
		sans = append(sans, certificatesmanagement.CertificateSubjectAlternativeName{
			Type:  certificatesmanagement.CertificateSubjectAlternativeNameTypeDns,
			Value: ocicommon.String(domain),
		})
	}

	// Validity window. GCP uses a duration string ("8760h"); OCI takes explicit
	// NotBefore/NotAfter timestamps.
	if validityDays <= 0 {
		validityDays = env.OCICertificateValidityDays
	}
	notAfter := time.Now().UTC().AddDate(0, 0, validityDays)

	req := certificatesmanagement.CreateCertificateRequest{
		CreateCertificateDetails: certificatesmanagement.CreateCertificateDetails{
			Name:          ocicommon.String(certName),
			CompartmentId: ocicommon.String(compartmentID),
			Description:   ocicommon.String("VCP managed internally-managed certificate: " + certName),
			CertificateConfig: certificatesmanagement.CreateCertificateIssuedByInternalCaConfigDetails{
				IssuerCertificateAuthorityId: ocicommon.String(issuerCAOCID),
				CertificateProfileType:       profileType,
				Subject: &certificatesmanagement.CertificateSubject{
					CommonName:   ocicommon.String(commonName),
					Organization: ocicommon.String(organization),
				},
				SubjectAlternativeNames: sans,
				KeyAlgorithm:            certificatesmanagement.KeyAlgorithmRsa4096,
				SignatureAlgorithm:      certificatesmanagement.SignatureAlgorithmSha256WithRsa,
				Validity: &certificatesmanagement.Validity{
					TimeOfValidityNotAfter: &ocicommon.SDKTime{Time: notAfter},
				},
			},
		},
	}

	resp, err := ociService.AdminOCIService.certManagementClient.CreateCertificate(ociService.Ctx, req)
	if err != nil {
		ociService.Logger.Errorf("CreateCertificate failed for compartment: %s, certName: %s, err: %s",
			compartmentID, certName, err.Error())
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceProvisionError, err)
	}

	cert := convertOCICertificateMetadata(&resp.Certificate)
	// OCI declares IssuerCertificateAuthorityId as mandatory:"false" on the
	// management Certificate object, so a regression or an unexpected ConfigType
	// could surface it as empty. We just signed the cert with issuerCAOCID, so
	// it is safe (and unambiguous) to backfill it here. WARN — never silent —
	// so the gap is detectable if OCI ever changes the contract.
	if cert.IssuerCAOCID == "" {
		ociService.Logger.Warnf(
			"CreateCertificate: OCI did not return IssuerCertificateAuthorityId for certName: %s — backfilling from request issuerCAOCID: %s",
			certName, issuerCAOCID,
		)
		cert.IssuerCAOCID = issuerCAOCID
	}
	ociService.Logger.Infof("CreateCertificate success — OCID: %s, lifecycleState: %s",
		cert.Ocid, cert.LifecycleState)
	return cert, nil
}

// GetCertificate retrieves the certificate bundle (signed cert + chain + private
// key) by OCID. The bundle is requested with CertificateBundleType=WITH_PRIVATE_KEY
// so that everything needed for ONTAP mTLS comes back in one API call
// Returns:
//   - (*OCICustomCertificate, nil) when the cert is found.
//   - (nil, nil) when the cert is not found (HTTP 404), so the caller can treat
//     it the same way the GCP path treats a missing cert.
//   - (nil, error) on any other failure.
func (ociService *OciServices) GetCertificate(certOCID string) (*OCICustomCertificate, error) {
	ociService.Logger.Infof("Calling GetCertificate — certOCID: %s", certOCID)

	if certOCID == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError,
			fmt.Errorf("GetCertificate: certOCID is required"))
	}

	// Step 1: read management metadata (lifecycle state, subject, issuer).
	// We do this first so we can short-circuit if the cert is in a deletion
	// state — the bundle fetch in Step 2 would either fail or hand back a
	// cert that's about to disappear.
	metaResp, err := ociService.AdminOCIService.certManagementClient.GetCertificate(
		ociService.Ctx,
		certificatesmanagement.GetCertificateRequest{CertificateId: ocicommon.String(certOCID)},
	)
	if err != nil {
		ociService.Logger.Errorf("GetCertificate (metadata) failed for certOCID: %s, err: %s",
			certOCID, err.Error())
		// Mirror google.googleResourceNotFoundCheck: 404 → (nil, nil).
		if ociResourceNotFoundCheck(err) == nil {
			return nil, nil
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError, err)
	}

	if isCertInDeletionState(metaResp.Certificate.LifecycleState) {
		ociService.Logger.Warnf("GetCertificate: certOCID %s is in %s state, treating as not found",
			certOCID, metaResp.Certificate.LifecycleState)
		return nil, nil
	}

	cert := convertOCICertificateMetadata(&metaResp.Certificate)
	// Read path: we don't know which CA signed this cert, so we never backfill
	// IssuerCAOCID from env here — that would silently lie in any future
	// multi-CA setup. Just surface the gap so it's detectable in logs.
	if cert.IssuerCAOCID == "" {
		ociService.Logger.Warnf("GetCertificate: OCI returned empty IssuerCertificateAuthorityId for certOCID: %s",
			certOCID)
	}

	// Step 2: pull the bundle WITH the private key. This call goes to a
	// different SDK client (certificates vs. certificatesmanagement) and is
	// the OCI equivalent of GCP's GetSecretWithLatestVersion for the key.
	bundle, err := GetCertificateBundleWithPrivateKey(ociService, certOCID)
	if err != nil {
		// 404 here means the cert exists in management but has no current
		// version yet (still CREATING). Treat as "not found" so callers can
		// retry — same shape they'd see for a missing cert on GCP.
		if ociResourceNotFoundCheck(err) == nil {
			ociService.Logger.Warnf("GetCertificate: bundle not yet available for certOCID: %s "+
				"(state=%s), treating as not found", certOCID, metaResp.Certificate.LifecycleState)
			return nil, nil
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError, err)
	}

	mergeCertificateBundleIntoCustomCertificate(cert, bundle)
	ociService.Logger.Infof("GetCertificate success — certOCID: %s, version: %d, serial: %s",
		cert.Ocid, cert.VersionNumber, cert.SerialNumber)
	return cert, nil
}

// DeleteCertificate schedules the certificate for deletion. once scheduled,
// the cert can no longer be used, and it is permanently purged after the retention
// window(controlled by env.OCICertificateDeletionRetentionDays).
//
// The call is idempotent: if the cert is already in a deletion lifecycle state
// (SCHEDULING_DELETION / PENDING_DELETION / DELETING / DELETED) we skip the
// ScheduleCertificateDeletion call to avoid the HTTP 409 Conflict that OCI
// returns for re-deletion attempts. A 404 on the pre-flight Get is treated as
// "already gone".
//
// GCP returned the cert resource path on success; we return only error since
// no caller in the OCI flow uses that string.
func (ociService *OciServices) DeleteCertificate(certOCID string) error {
	retentionDays := env.OCICertificateDeletionRetentionDays
	ociService.Logger.Infof("Calling DeleteCertificate (ScheduleCertificateDeletion) — certOCID: %s, retentionDays: %d",
		certOCID, retentionDays)

	if certOCID == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceDeprovisionError,
			fmt.Errorf("DeleteCertificate: certOCID is required"))
	}

	// Pre-flight lifecycle check — avoid the conflict error when the cert is
	// already scheduled for / undergoing deletion. Treat 404 as "already gone".
	getResp, err := ociService.AdminOCIService.certManagementClient.GetCertificate(
		ociService.Ctx,
		certificatesmanagement.GetCertificateRequest{CertificateId: ocicommon.String(certOCID)},
	)
	if err != nil {
		if ociResourceNotFoundCheck(err) == nil {
			ociService.Logger.Infof("DeleteCertificate: certOCID %s not found, treating as already deleted",
				certOCID)
			return nil
		}
		ociService.Logger.Errorf("DeleteCertificate: GetCertificate failed for certOCID: %s, err: %s",
			certOCID, err.Error())
		return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError, err)
	}
	if isCertInDeletionState(getResp.Certificate.LifecycleState) {
		ociService.Logger.Infof("DeleteCertificate: certOCID %s already in %s state, skipping ScheduleCertificateDeletion",
			certOCID, getResp.Certificate.LifecycleState)
		return nil
	}

	deletionTime := time.Now().UTC().AddDate(0, 0, retentionDays)
	_, err = ociService.AdminOCIService.certManagementClient.ScheduleCertificateDeletion(
		ociService.Ctx,
		certificatesmanagement.ScheduleCertificateDeletionRequest{
			CertificateId: ocicommon.String(certOCID),
			ScheduleCertificateDeletionDetails: certificatesmanagement.ScheduleCertificateDeletionDetails{
				TimeOfDeletion: &ocicommon.SDKTime{Time: deletionTime},
			},
		},
	)
	if err != nil {
		ociService.Logger.Errorf("DeleteCertificate (ScheduleCertificateDeletion) failed for certOCID: %s, err: %s",
			certOCID, err.Error())
		return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceDeprovisionError, err)
	}

	ociService.Logger.Infof("DeleteCertificate success — certOCID: %s scheduled for deletion in %d days",
		certOCID, retentionDays)
	return nil
}

// GetCertificateByName looks up a certificate by its human-friendly name inside
// the given compartment, returning metadata only (no PEM bundle). It is the
// idempotency primitive for create-or-get flows: at create time the caller
// holds only the cert name (e.g. poolCredentials.CertificateID), not the OCID
// that OCI assigns. Once this returns the metadata the caller can use
// GetCertificate(ocid) to pull the signed cert + private key bundle.
//
// OCI cert names are unique within a compartment, so a single result is
// expected. If multiple matches exist the first ACTIVE/CREATING one wins;
// entries already in any deletion lifecycle state are skipped (they would
// fail bundle retrieval anyway).
//
// Returns:
//   - (*OCICustomCertificate, nil) on the first usable match.
//   - (nil, nil) when no certificate with that name exists in the compartment,
//     so the caller can treat it as "create from scratch" — same shape used by
//     GetCertificate for 404s.
//   - (nil, error) on any other failure.
func (ociService *OciServices) GetCertificateByName(certName, compartmentOCID string) (*OCICustomCertificate, error) {
	ociService.Logger.Infof("Calling GetCertificateByName — certName: %s, compartmentOCID: %s",
		certName, compartmentOCID)

	if certName == "" || compartmentOCID == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError,
			fmt.Errorf("GetCertificateByName: certName and compartmentOCID are required"))
	}

	resp, err := ociService.AdminOCIService.certManagementClient.ListCertificates(
		ociService.Ctx,
		certificatesmanagement.ListCertificatesRequest{
			CompartmentId: ocicommon.String(compartmentOCID),
			Name:          ocicommon.String(certName),
		},
	)
	if err != nil {
		ociService.Logger.Errorf("GetCertificateByName ListCertificates failed for certName: %s, err: %s",
			certName, err.Error())
		// A 404 on ListCertificates would mean the compartment itself is missing
		// or the caller lacks permission; treat as fetch error rather than
		// "cert absent" so the create path doesn't silently mask config issues.
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError, err)
	}

	for i := range resp.Items {
		summary := resp.Items[i]
		if derefString(summary.Name) != certName {
			continue
		}
		// Skip entries already on the way out — they can't be reused and the
		// caller should create a fresh one.
		if isCertInDeletionState(certificatesmanagement.CertificateLifecycleStateEnum(summary.LifecycleState)) {
			ociService.Logger.Warnf("GetCertificateByName: skipping certName: %s OCID: %s in lifecycle state: %s",
				certName, derefString(summary.Id), summary.LifecycleState)
			continue
		}
		cert := convertOCICertificateSummary(&summary)
		// Read path: same rule as GetCertificate — never backfill from env.
		// Just flag the gap if OCI omitted the issuer CA on the summary.
		if cert.IssuerCAOCID == "" {
			ociService.Logger.Warnf("GetCertificateByName: OCI returned empty IssuerCertificateAuthorityId for certName: %s, OCID: %s",
				certName, cert.Ocid)
		}
		ociService.Logger.Infof("GetCertificateByName found — certName: %s, OCID: %s, lifecycleState: %s",
			certName, cert.Ocid, cert.LifecycleState)
		return cert, nil
	}

	ociService.Logger.Infof("GetCertificateByName: no certificate found for certName: %s in compartment: %s",
		certName, compartmentOCID)
	return nil, nil
}

// WaitForCertificateActive polls the certificate's lifecycle state until it
// reaches ACTIVE, or returns an error if it enters a terminal non-ACTIVE state
// (FAILED, any deletion state) or the timeout fires.
//
// Why this exists:
//
// OCI's CreateCertificate is asynchronous — the cert starts in CREATING and
// transitions to ACTIVE typically within a few seconds. GetCertificateBundle
// only returns PEM material once the cert is ACTIVE, so callers that chain
// CreateCertificate → GetCertificate must wait in between.
//
// State handling:
//   - ACTIVE                          → success.
//   - FAILED                          → terminal error.
//   - DELETING / DELETED /
//     SCHEDULING_DELETION /
//     PENDING_DELETION                → terminal error (someone else is tearing
//     the cert down concurrently).
//   - CREATING / UPDATING             → transient — keep polling. UPDATING is
//     reachable when CAS rotates or
//     renews the cert in the background;
//     both states resolve back to ACTIVE
//     under normal operation, so we
//     treat them identically.
//   - any unrecognised future state   → keep polling (logged at WARN) and let
//     the deadline guard end the loop.
//
// Pass timeout <= 0 to use the package default (90s, 3s poll interval).
// The 3s poll interval is overridable via the package-private
// defaultCertActivePollInterval var as a test seam (see comment on that var).
func (ociService *OciServices) WaitForCertificateActive(certOCID string, timeout time.Duration) error {
	if certOCID == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError,
			fmt.Errorf("WaitForCertificateActive: certOCID is required"))
	}
	if timeout <= 0 {
		timeout = defaultCertActiveTimeout
	}

	pollInterval := defaultCertActivePollInterval
	deadline := time.Now().Add(timeout)
	ociService.Logger.Infof("Calling WaitForCertificateActive — certOCID: %s, timeout: %s, pollInterval: %s",
		certOCID, timeout, pollInterval)

	for attempt := 1; ; attempt++ {
		resp, err := ociService.AdminOCIService.certManagementClient.GetCertificate(
			ociService.Ctx,
			certificatesmanagement.GetCertificateRequest{CertificateId: ocicommon.String(certOCID)},
		)
		if err != nil {
			ociService.Logger.Errorf("WaitForCertificateActive GetCertificate failed for certOCID: %s, err: %s",
				certOCID, err.Error())
			return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceFetchError, err)
		}

		state := resp.Certificate.LifecycleState
		// Per-poll visibility for the surrounding (up to 90s) activity wait.
		ociService.Logger.Debugf("WaitForCertificateActive: poll #%d certOCID: %s state: %s timeRemaining: %s",
			attempt, certOCID, state, time.Until(deadline).Round(time.Second))

		switch state {
		case certificatesmanagement.CertificateLifecycleStateActive:
			ociService.Logger.Infof("WaitForCertificateActive: certOCID: %s is ACTIVE after %d poll(s)", certOCID, attempt)
			return nil
		case certificatesmanagement.CertificateLifecycleStateFailed:
			return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceProvisionError,
				fmt.Errorf("WaitForCertificateActive: certOCID %s reached terminal state %s", certOCID, state))
		case certificatesmanagement.CertificateLifecycleStateDeleting,
			certificatesmanagement.CertificateLifecycleStateDeleted,
			certificatesmanagement.CertificateLifecycleStateSchedulingDeletion,
			certificatesmanagement.CertificateLifecycleStatePendingDeletion:
			return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceProvisionError,
				fmt.Errorf("WaitForCertificateActive: certOCID %s is being deleted (state=%s)", certOCID, state))
		case certificatesmanagement.CertificateLifecycleStateCreating,
			certificatesmanagement.CertificateLifecycleStateUpdating:
			// Transient — keep polling. Behaviour is identical to the previous
			// implicit fall-through; the explicit case exists so readers don't
			// have to infer that UPDATING is intentionally treated as benign.
		default:
			// Unknown future lifecycle state (e.g. one added by a later OCI
			// SDK upgrade). Log loudly but keep polling — the deadline guard
			// below will end the loop if we never converge to ACTIVE.
			ociService.Logger.Warnf("WaitForCertificateActive: unexpected lifecycle state %q for certOCID %s — continuing to poll until deadline",
				state, certOCID)
		}

		if time.Now().After(deadline) {
			return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceProvisionError,
				fmt.Errorf("WaitForCertificateActive: timeout after %s waiting for certOCID %s to become ACTIVE (last state: %s)",
					timeout, certOCID, state))
		}

		select {
		case <-ociService.Ctx.Done():
			return vsaerrors.NewVCPError(vsaerrors.ErrOCIResourceProvisionError,
				fmt.Errorf("WaitForCertificateActive: context cancelled while waiting for certOCID %s: %w",
					certOCID, ociService.Ctx.Err()))
		case <-time.After(pollInterval):
		}
	}
}

// _getCertificateBundleWithPrivateKey fetches the current bundle for the given
// certificate OCID, asking OCI to include the private key inline.
//
// Split out as a test seam so unit tests can stub bundle retrieval without
// hitting the SDK's polymorphic-JSON unmarshalling path.
func _getCertificateBundleWithPrivateKey(ociService *OciServices, certOCID string) (*certificates.CertificateBundleWithPrivateKey, error) {
	resp, err := ociService.AdminOCIService.certReadClient.GetCertificateBundle(
		ociService.Ctx,
		certificates.GetCertificateBundleRequest{
			CertificateId:         ocicommon.String(certOCID),
			Stage:                 certificates.GetCertificateBundleStageCurrent,
			CertificateBundleType: certificates.GetCertificateBundleCertificateBundleTypeWithPrivateKey,
		},
	)
	if err != nil {
		return nil, err
	}

	// When requested with WITH_PRIVATE_KEY the concrete type is
	// CertificateBundleWithPrivateKey (rather than CertificateBundlePublicOnly).
	// Use the two-value type assertion form per CODING_GUIDELINES.md.
	bundle, ok := resp.CertificateBundle.(certificates.CertificateBundleWithPrivateKey)
	if !ok {
		return nil, fmt.Errorf("unexpected certificate bundle type: %T (expected CertificateBundleWithPrivateKey)", resp.CertificateBundle)
	}
	return &bundle, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// convertOCICertificateMetadata copies non-PEM fields from an OCI Certificate
// (returned by certificatesmanagement) into our OCICustomCertificate. The PEM
// body and private key are filled later by mergeCertificateBundleIntoCustomCertificate
// once the bundle is retrieved.
//
// VersionNumber and SerialNumber are read from cert.CurrentVersion when present,
// so callers that take this metadata-only path (CreateCertificate, name-by-name
// lookups via GetCertificateByName) get a non-zero version/serial without an
// extra GetCertificateBundle round trip. The bundle path overwrites the same
// fields with the bundle's authoritative values, which is fine because they
// describe the same current version.
func convertOCICertificateMetadata(cert *certificatesmanagement.Certificate) *OCICustomCertificate {
	if cert == nil {
		return &OCICustomCertificate{}
	}

	out := &OCICustomCertificate{
		Ocid:           derefString(cert.Id),
		Name:           derefString(cert.Name),
		CompartmentID:  derefString(cert.CompartmentId),
		IssuerCAOCID:   derefString(cert.IssuerCertificateAuthorityId),
		LifecycleState: string(cert.LifecycleState),
	}

	if cert.TimeCreated != nil {
		t := cert.TimeCreated.Time
		out.TimeCreated = &t
	}
	if cert.Subject != nil {
		out.SubjectCommonName = derefString(cert.Subject.CommonName)
		out.SubjectOrganization = derefString(cert.Subject.Organization)
	}
	if cert.CurrentVersion != nil {
		out.VersionNumber = derefInt64(cert.CurrentVersion.VersionNumber)
		out.SerialNumber = derefString(cert.CurrentVersion.SerialNumber)
	}
	return out
}

// convertOCICertificateSummary copies non-PEM fields from a CertificateSummary
// (returned by ListCertificates) into our OCICustomCertificate. The summary
// shape mirrors the full Certificate object for the fields we care about:
// IssuerCertificateAuthorityId is present (declared mandatory:"false" by the
// SDK, so it may be empty on certain ConfigType variants), and the current
// version metadata lives under CurrentVersionSummary instead of CurrentVersion.
//
// VersionNumber and SerialNumber are read from summary.CurrentVersionSummary
// when present, matching convertOCICertificateMetadata, so name-based lookups
// via GetCertificateByName surface a non-zero version without a follow-up
// bundle fetch.
func convertOCICertificateSummary(summary *certificatesmanagement.CertificateSummary) *OCICustomCertificate {
	if summary == nil {
		return &OCICustomCertificate{}
	}

	out := &OCICustomCertificate{
		Ocid:           derefString(summary.Id),
		Name:           derefString(summary.Name),
		CompartmentID:  derefString(summary.CompartmentId),
		IssuerCAOCID:   derefString(summary.IssuerCertificateAuthorityId),
		LifecycleState: string(summary.LifecycleState),
	}

	if summary.TimeCreated != nil {
		t := summary.TimeCreated.Time
		out.TimeCreated = &t
	}
	if summary.Subject != nil {
		out.SubjectCommonName = derefString(summary.Subject.CommonName)
		out.SubjectOrganization = derefString(summary.Subject.Organization)
	}
	if summary.CurrentVersionSummary != nil {
		out.VersionNumber = derefInt64(summary.CurrentVersionSummary.VersionNumber)
		out.SerialNumber = derefString(summary.CurrentVersionSummary.SerialNumber)
	}
	return out
}

// mergeCertificateBundleIntoCustomCertificate fills in the PEM material,
// serial number, validity window, and version number on cert from the bundle.
// Safe to call with a nil bundle (no-op).
func mergeCertificateBundleIntoCustomCertificate(cert *OCICustomCertificate, bundle *certificates.CertificateBundleWithPrivateKey) {
	if cert == nil || bundle == nil {
		return
	}

	cert.PemCertificate = derefString(bundle.CertificatePem)
	cert.PemCertificateChain = derefString(bundle.CertChainPem)
	cert.PrivateKeyPem = derefString(bundle.PrivateKeyPem)
	cert.SerialNumber = derefString(bundle.SerialNumber)
	cert.VersionNumber = derefInt64(bundle.VersionNumber)

	if bundle.Validity != nil {
		if bundle.Validity.TimeOfValidityNotBefore != nil {
			t := bundle.Validity.TimeOfValidityNotBefore.Time
			cert.NotBefore = &t
		}
		if bundle.Validity.TimeOfValidityNotAfter != nil {
			t := bundle.Validity.TimeOfValidityNotAfter.Time
			cert.NotAfter = &t
		}
	}

	// Bundle TimeCreated is the version-creation time. Only adopt it when the
	// metadata leg didn't already populate the certificate-creation timestamp.
	if cert.TimeCreated == nil && bundle.TimeCreated != nil {
		t := bundle.TimeCreated.Time
		cert.TimeCreated = &t
	}
}

// isCertInDeletionState returns true when the certificate's lifecycle state
// indicates it has been deleted or is in the process of being deleted. OCI
// uses a two-phase deletion model (ScheduleCertificateDeletion → PENDING_DELETION
// → permanent removal), unlike GCP which revokes immediately. During these
// states the cert can no longer be used and bundle retrieval may fail.
func isCertInDeletionState(state certificatesmanagement.CertificateLifecycleStateEnum) bool {
	switch state {
	case certificatesmanagement.CertificateLifecycleStatePendingDeletion,
		certificatesmanagement.CertificateLifecycleStateSchedulingDeletion,
		certificatesmanagement.CertificateLifecycleStateDeleting,
		certificatesmanagement.CertificateLifecycleStateDeleted:
		return true
	default:
		return false
	}
}
