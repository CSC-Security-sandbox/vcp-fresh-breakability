package active_directory_activities

import (
	"context"
	"fmt"
	"strings"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/activity"
)

var getOntapRestProvider = _getOntapRestProvider

func _getOntapRestProvider(ctx context.Context, node *models.Node) (*vsa.OntapRestProvider, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	ontapProvider, ok := provider.(*vsa.OntapRestProvider)
	if !ok {
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("provider is not OntapRestProvider"))
	}
	return ontapProvider, nil
}

// mapCreateCIFSServerError maps ONTAP CIFS/AD join errors to VCP tracking IDs.
// Order matters: more specific (e.g. credential) checks first. Returns (trackingID, true) if matched.
func mapCreateCIFSServerError(err error) (trackingID int, ok bool) {
	if err == nil {
		return 0, false
	}
	s := err.Error()
	if strings.Contains(s, "Invalid Credentials") || strings.Contains(s, "KRB5KDC_ERR_PREAUTH_FAILED") {
		return vsaerrors.ErrADInvalidCredentials, true
	}
	if strings.Contains(s, "does not match password stored in Active Directory") {
		return vsaerrors.ErrADPasswordNotInSync, true
	}
	if strings.Contains(s, "Invalid credentials were given") || strings.Contains(s, "Username format not supported") ||
		strings.Contains(s, "Reason: Invalid credentials.") {
		return vsaerrors.ErrADIncorrectUsername, true
	}
	if strings.Contains(s, "Clients credentials have been revoked") || strings.Contains(s, "credentials have been revoked") {
		return vsaerrors.ErrADUserDisabled, true
	}
	if strings.Contains(s, "KDC has no support for encryption type") ||
		(strings.Contains(s, "msDS-SupportedEncryptionTypes") && strings.Contains(s, "Insufficient access")) {
		return vsaerrors.ErrADAESEncryptionSettingsInvalid, true
	}
	if strings.Contains(s, "Failed to bind service principal name on LIF") || strings.Contains(s, "KDC Unreachable Details") || strings.Contains(s, "KRB5_KDC_UNREACH") {
		return vsaerrors.ErrADKDCUnreachable, true
	}
	if strings.Contains(s, "Cannot find any domain controllers") || (strings.Contains(s, "no server available") && strings.Contains(s, "SecD")) {
		return vsaerrors.ErrADDomainControllersUnreachable, true
	}
	if strings.Contains(s, "RESULT_ERROR_LDAPSERVER_SERVER_DOWN") && strings.Contains(s, "Can't contact LDAP server") {
		return vsaerrors.ErrADLDAPUnreachable, true
	}
	if strings.Contains(s, "ou not found") || strings.Contains(s, "Lookup of organizational_unit failed") {
		return vsaerrors.ErrADInvalidOU, true
	}
	if strings.Contains(s, "insufficient access rights") || strings.Contains(s, "insufficient privilege") ||
		strings.Contains(s, "LDAP constraint") {
		return vsaerrors.ErrADInsufficientPermission, true
	}
	if strings.Contains(s, "cannot find the indicated default site") {
		return vsaerrors.ErrADDefaultSiteInvalid, true
	}
	if strings.Contains(s, "Unable to connect to NetLogon") || strings.Contains(s, "RESULT_ERROR_SPINCLIENT") {
		return vsaerrors.ErrADNetLogonError, true
	}
	if strings.Contains(s, "Operation timed out") && strings.Contains(s, "domain controllers") {
		return vsaerrors.ErrADLDAPNetworkIssue, true
	}
	if strings.Contains(s, "Unable to connect to any") && strings.Contains(s, "domain controllers") {
		return vsaerrors.ErrADLDAPNetworkIssue, true
	}
	return 0, false
}

// GetOrCreateCifsServiceResult contains the result of GetOrCreateCifsService
type GetOrCreateCifsServiceResult struct {
	FQDN            string // FQDN if service was created, empty if service existed
	NeedsDDNS       bool   // Whether DDNS needs to be enabled (only set if service existed)
	CifsServiceName string // Name of the CIFS service (for building FQDN)
	AdDomain        string // AD domain (for building FQDN)
}

// CreateOrModifyADDNS creates or modifies Active Directory DNS configuration
func (a ActiveDirectoryActivity) CreateOrModifyADDNS(ctx context.Context, node *models.Node, ad *vsa.ActiveDirectory, svmName, externalSVMUUID string) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting CreateOrModifyADDNS activity")

	ontapProvider, err := getOntapRestProvider(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	client, err := ontapProvider.CreateRESTClient()
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get ONTAP client: %w", err))
	}

	if err := ontapProvider.EnsureCifsServerNamePostFix(client, ad, svmName); err != nil {
		logger.Error("failed to ensure CIFS server name postfix", "error", err.Error())
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	dnsServersSlice := strings.Split(strings.ReplaceAll(ad.DNS, " ", ""), ",")
	domainsSlice := []string{ad.Domain}

	logger.Info("creating or modifying DNS", "domains", domainsSlice, "dnsServers", dnsServersSlice, "svmUUID", externalSVMUUID)
	dns, err := client.NameServices().DNSGet(&ontapRest.DNSGetParams{
		BaseParams: ontapRest.BaseParams{Fields: []string{"servers", "domains"}},
		SvmUUID:    externalSVMUUID,
	})
	if err != nil && !utilerrors.IsNotFoundErr(err) {
		logger.Error("failed to get DNS which is not found error", "error", err.Error(), "domains", domainsSlice, "dnsServers", dnsServersSlice)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if dns == nil {
		_, err = client.NameServices().DnsCreate(&ontapRest.DNSCreateParams{
			SvmUUID:    externalSVMUUID,
			Domains:    domainsSlice,
			DNSServers: dnsServersSlice,
		})
		if err != nil {
			logger.Error("failed to create DNS", "error", err.Error(), "domains", domainsSlice, "dnsServers", dnsServersSlice, "svmUUID", externalSVMUUID)
			if strings.Contains(err.Error(), "cannot be reached") {
				return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDNSServerUnreachable, err))
			}
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
		return nil
	}

	if utils.ComparePointerStringSlices(dns.Servers, dnsServersSlice) && utils.ComparePointerStringSlices(dns.Domains, domainsSlice) {
		return nil
	}

	if err := client.NameServices().DNSModify(&ontapRest.DNSModifyParams{
		SvmUUID:     externalSVMUUID,
		Domains:     domainsSlice,
		NameServers: dnsServersSlice,
	}); err != nil {
		logger.Error("failed to modify DNS", "error", err.Error(), "domains", domainsSlice, "dnsServers", dnsServersSlice)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished CreateOrModifyADDNS activity")
	return nil
}

func (a ActiveDirectoryActivity) GetCifsService(ctx context.Context, node *models.Node, svmName, externalSVMUUID string) (*ontapRest.CifsService, error) {
	ontapProvider, err := getOntapRestProvider(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	cifs, err := ontapProvider.GetCIFSService(svmName, externalSVMUUID)
	if err != nil {
		return nil, err
	}
	return cifs, nil
}

// GetOrCreateCifsService gets an existing CIFS service or creates one if it doesn't exist
// Returns information about the service state
func (a ActiveDirectoryActivity) GetOrCreateCifsService(ctx context.Context, node *models.Node, ad *vsa.ActiveDirectory, svmName, externalSVMUUID string) (*GetOrCreateCifsServiceResult, error) {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting GetOrCreateCifsService activity")

	decryptedPassword, err := utils.DecryptPassword(ad.Password)
	if err != nil {
		logger.Error("failed to decrypt AD password", "error", err.Error())
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to decrypt AD password: %w", err))
	}
	ad.Password = log.Secret(*decryptedPassword)

	ontapProvider, err := getOntapRestProvider(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	client, err := ontapProvider.CreateRESTClient()
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get ONTAP client: %w", err))
	}

	if err := ontapProvider.EnsureCifsServerNamePostFix(client, ad, svmName); err != nil {
		logger.Error("failed to ensure CIFS server name postfix", "error", err.Error())
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	cifs, err := client.NAS().CifsServiceGet(&ontapRest.CifsServiceGetParams{
		SvmUUID: &externalSVMUUID,
		SvmName: &svmName,
		BaseParams: ontapRest.BaseParams{Fields: []string{
			"ad_domain",
			"name",
		}},
	})
	if err != nil {
		if !utilerrors.IsNotFoundErr(err) {
			logger.Error("failed to get CIFS service", "error", err.Error())
			return nil, vsaerrors.WrapAsTemporalApplicationError(err)
		}

		logger.Info("CIFS service not found, creating new CIFS service", "svm", externalSVMUUID)
		fqdn, createErr := ontapProvider.CreateAndSetupCIFSServer(client, ad, externalSVMUUID, svmName)
		if createErr != nil {
			logger.Error("failed to createAndSetupCIFSServer", "error", createErr.Error())
			if trackingID, ok := mapCreateCIFSServerError(createErr); ok {
				return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(trackingID, createErr))
			}
			return nil, vsaerrors.WrapAsTemporalApplicationError(createErr)
		}
		return &GetOrCreateCifsServiceResult{FQDN: fqdn, NeedsDDNS: false}, nil
	}

	result := &GetOrCreateCifsServiceResult{}
	if cifs.Name != nil {
		result.CifsServiceName = *cifs.Name
	}
	if cifs.AdDomain != nil && cifs.AdDomain.Fqdn != nil {
		result.AdDomain = *cifs.AdDomain.Fqdn
	}

	ddnsEnabled := ontapProvider.IsDDNSEnabled(client, externalSVMUUID)
	if !ddnsEnabled && cifs.Name != nil && cifs.AdDomain != nil && cifs.AdDomain.Fqdn != nil {
		result.NeedsDDNS = true
	}

	logger.Info("CIFS service already exists", "svm", externalSVMUUID, "name", cifs.Name, "needsDDNS", result.NeedsDDNS)
	activity.RecordHeartbeat(ctx, "Finished GetOrCreateCifsService activity")
	return result, nil
}

// DdnsModify enables DDNS (Dynamic DNS) for the CIFS service
func (a ActiveDirectoryActivity) DdnsModify(ctx context.Context, node *models.Node, externalSVMUUID, fqdn string) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting DDnsModify activity")
	ontapProvider, err := getOntapRestProvider(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	client, err := ontapProvider.CreateRESTClient()
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get ONTAP client: %w", err))
	}

	secureDDNS := true
	if err := client.NameServices().DNSModify(&ontapRest.DNSModifyParams{
		SvmUUID: externalSVMUUID,
		DDNSModifyParams: ontapRest.DDNSModifyParams{
			UseSecure: &secureDDNS,
			Fqdn:      &fqdn,
			Enabled:   nillable.ToPointer(true),
		},
	}); err != nil {
		logger.Error("failed to update DDNS", "error", err.Error(), "fqdn", fqdn)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	activity.RecordHeartbeat(ctx, "Finished DDnsModify activity")
	logger.Info("Successfully enabled DDNS", "svm", externalSVMUUID, "fqdn", fqdn)
	return nil
}

// CreateJunctionPathForCifsShare creates a CIFS share for the junction path
func (a ActiveDirectoryActivity) CreateJunctionPathForCifsShare(ctx context.Context, node *models.Node, svmName, junctionPath string, smbshareProperties []string) error {
	logger := util.GetLogger(ctx)
	activity.RecordHeartbeat(ctx, "Starting CreateJunctionPathForCifsShare activity")

	ontapProvider, err := getOntapRestProvider(ctx, node)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	client, err := ontapProvider.CreateRESTClient()
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get ONTAP client: %w", err))
	}

	logger.Info("Creating CIFS share", "svm", svmName, "junctionPath", junctionPath, "shareProperties", smbshareProperties)
	if err := client.NAS().CifsShareCreate(&ontapRest.CifsShareCreateParams{
		SvmName:         &svmName,
		Path:            junctionPath,
		Name:            junctionPath[1:],
		ShareProperties: smbshareProperties,
	}); err != nil {
		if utilerrors.IsConflictErr(err) || strings.Contains(err.Error(), "duplicate entry") {
			logger.Infof("CIFS share already exists for SVM  %s", svmName)
		} else {
			logger.Error("failed to create junction path for CIFS share", "error", err.Error())
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}
	activity.RecordHeartbeat(ctx, "Finished CreateJunctionPathForCifsShare activity")
	logger.Info("Successfully created CIFS share", "svm", svmName, "junctionPath", junctionPath)
	return nil
}
