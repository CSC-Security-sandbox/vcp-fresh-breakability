package errors

import "strings"

// ErrorDomain scopes ONTAP error classification so the same rule table can be used
// across AD, Kerberos, DNS, SMB, LDAP, and general ONTAP flows with domain-specific fallbacks.
type ErrorDomain string

const (
	DomainAD       ErrorDomain = "active_directory"
	DomainKerberos ErrorDomain = "kerberos"
	DomainDNS      ErrorDomain = "dns"
	DomainSMB      ErrorDomain = "smb"
	DomainLDAP     ErrorDomain = "ldap"
	DomainONTAP    ErrorDomain = "ontap_general"
	DomainKMS      ErrorDomain = "kms"
)

// ErrorRule defines a single classification rule. Order in the rule table matters: more specific rules first.
// - Substrings: ALL must be present in the error message (AND logic).
// - AnyOf: at least ONE must be present (OR logic); checked only when Substrings is empty.
// - Domains: which domains this rule applies to; nil means all domains.
type ErrorRule struct {
	Substrings []string      // ALL must match (AND logic)
	AnyOf      []string      // at least ONE must match (OR logic); used only if Substrings is empty
	TrackingID int           // VCP tracking ID to assign
	Domains    []ErrorDomain // which domains this rule applies to; nil = all
}

// ontapErrorRules is the ordered rule table. More specific rules must appear first.
var ontapErrorRules = []ErrorRule{
	// --- Original mapCreateCIFSServerError patterns (same order and IDs) ---
	{AnyOf: []string{"Invalid Credentials", "KRB5KDC_ERR_PREAUTH_FAILED"}, TrackingID: ErrADInvalidCredentials, Domains: []ErrorDomain{DomainAD, DomainKerberos, DomainSMB}},
	{Substrings: []string{"does not match password stored in Active Directory"}, TrackingID: ErrADPasswordNotInSync, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{AnyOf: []string{"Invalid credentials were given", "Username format not supported", "Reason: Invalid credentials."}, TrackingID: ErrADIncorrectUsername, Domains: []ErrorDomain{DomainAD, DomainKerberos, DomainSMB}},
	{AnyOf: []string{"Clients credentials have been revoked", "credentials have been revoked"}, TrackingID: ErrADUserDisabled, Domains: []ErrorDomain{DomainAD, DomainKerberos, DomainSMB}},
	{AnyOf: []string{"KDC has no support for encryption type"}, TrackingID: ErrADAESEncryptionSettingsInvalid, Domains: []ErrorDomain{DomainAD, DomainKerberos, DomainSMB}},
	{Substrings: []string{"msDS-SupportedEncryptionTypes", "Insufficient access"}, TrackingID: ErrADAESEncryptionSettingsInvalid, Domains: []ErrorDomain{DomainAD, DomainKerberos, DomainSMB}},
	{AnyOf: []string{"Failed to bind service principal name on LIF", "KDC Unreachable Details", "KRB5_KDC_UNREACH"}, TrackingID: ErrADKDCUnreachable, Domains: []ErrorDomain{DomainAD, DomainKerberos, DomainSMB}},
	{AnyOf: []string{"Cannot find any domain controllers"}, TrackingID: ErrADDomainControllersUnreachable, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{Substrings: []string{"SecD Error: no server available", "Password update failed"}, TrackingID: ErrADPasswordUpdateFailed, Domains: []ErrorDomain{DomainAD}},
	{Substrings: []string{"no server available", "SecD"}, TrackingID: ErrADDomainControllersUnreachable, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{Substrings: []string{"RESULT_ERROR_LDAPSERVER_SERVER_DOWN", "Can't contact LDAP server"}, TrackingID: ErrADLDAPUnreachable, Domains: []ErrorDomain{DomainAD, DomainLDAP, DomainSMB}},
	{AnyOf: []string{"ou not found", "Lookup of organizational_unit failed"}, TrackingID: ErrADInvalidOU, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{AnyOf: []string{"insufficient access rights", "insufficient privilege", "LDAP constraint"}, TrackingID: ErrADInsufficientPermission, Domains: []ErrorDomain{DomainAD, DomainLDAP, DomainSMB}},
	{AnyOf: []string{"cannot find the indicated default site"}, TrackingID: ErrADDefaultSiteInvalid, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{Substrings: []string{"Unable to connect to LSA service", "RESULT_ERROR_SPINCLIENT"}, TrackingID: ErrADLSAServiceUnreachable, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{AnyOf: []string{"Unable to connect to NetLogon", "RESULT_ERROR_SPINCLIENT"}, TrackingID: ErrADNetLogonError, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{Substrings: []string{"Operation timed out", "domain controllers"}, TrackingID: ErrADLDAPNetworkIssue, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{Substrings: []string{"Unable to connect to any", "domain controllers"}, TrackingID: ErrADDCUnreachable, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	// --- DNS (inline "cannot be reached" pattern) ---
	{AnyOf: []string{"cannot be reached"}, TrackingID: ErrDNSServerUnreachable, Domains: []ErrorDomain{DomainDNS}},
	// --- New patterns (SDE gap) ---
	{Substrings: []string{"Failed to determine if site is a valid default site"}, TrackingID: ErrADDefaultSiteValidationFailed, Domains: []ErrorDomain{DomainAD}},
	{Substrings: []string{"No servers available for MS_LDAP_AD"}, TrackingID: ErrADLDAPServerNotIdentified, Domains: []ErrorDomain{DomainAD, DomainLDAP}},
	{Substrings: []string{"Unable to start TLS: Server is unavailable"}, TrackingID: ErrADUnableToStartTLS, Domains: []ErrorDomain{DomainAD, DomainLDAP}},
	{Substrings: []string{"vserver cifs users-and-groups remove-stale-records"}, TrackingID: ErrADStaleCacheCleanup, Domains: []ErrorDomain{DomainAD}},
	{Substrings: []string{"CIFS Server Modify Job is RUNNING"}, TrackingID: ErrADUpdateInProgress, Domains: []ErrorDomain{DomainAD}},
	{Substrings: []string{"Failed to resolve name"}, TrackingID: ErrADUserResolutionFailed, Domains: []ErrorDomain{DomainAD}},
	{Substrings: []string{"LDAP Error: The search was timed out"}, TrackingID: ErrADLDAPSearchTimeout, Domains: []ErrorDomain{DomainAD, DomainLDAP}},
	{Substrings: []string{"SASL bind to LDAP", "GSSAPI"}, TrackingID: ErrADLDAPBindFailed, Domains: []ErrorDomain{DomainAD}},
	{AnyOf: []string{"RESULT_ERROR_LDAPSERVER_LOCAL_ERROR"}, TrackingID: ErrADLDAPBindFailed, Domains: []ErrorDomain{DomainAD}},
	// --- AD additional SDE patterns ---
	{AnyOf: []string{"machine account does not exist"}, TrackingID: ErrADMachineAccountNotFound, Domains: []ErrorDomain{DomainAD, DomainSMB}},
	{Substrings: []string{"Invalid value specified for", "kdc-ip"}, TrackingID: ErrADInvalidKdcIP, Domains: []ErrorDomain{DomainAD, DomainKerberos}},
	{AnyOf: []string{"Failed to lookup SRV record"}, TrackingID: ErrADSRVRecordLookupFailed, Domains: []ErrorDomain{DomainAD, DomainDNS}},
	// --- DNS additional SDE patterns ---
	{AnyOf: []string{"DNS resolution failed for all the specified servers"}, TrackingID: ErrDNSResolutionFailed, Domains: []ErrorDomain{DomainDNS}},
	{AnyOf: []string{"Unable to contact DNS to discover domain"}, TrackingID: ErrDNSContactFailed, Domains: []ErrorDomain{DomainDNS}},
	// --- LDAP SDE patterns (specific before generic) ---
	{Substrings: []string{"Validate the Ldap configuration procedure failed", "Certificate verification failed"}, TrackingID: ErrLDAPCertificateError, Domains: []ErrorDomain{DomainLDAP}},
	{Substrings: []string{"Validate the Ldap configuration procedure failed", "Can't contact LDAP server"}, TrackingID: ErrADLDAPUnreachable, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"bind password or bind DN is invalid"}, TrackingID: ErrLDAPInvalidBindCredentials, Domains: []ErrorDomain{DomainLDAP}},
	{Substrings: []string{"Validate the Ldap configuration procedure failed", "does not match CN"}, TrackingID: ErrLDAPConfigValidationFailed, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"Validate the Ldap configuration procedure failed"}, TrackingID: ErrLDAPConfigValidationFailed, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"User DN specified in the LDAP client configuration is not available"}, TrackingID: ErrLDAPUserDNNotAvailable, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"Group DN specified in the LDAP client configuration is not available"}, TrackingID: ErrLDAPGroupDNNotAvailable, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"User DN specified in the LDAP client configuration failed"}, TrackingID: ErrLDAPUserDNInvalid, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"Group DN specified in the LDAP client configuration failed"}, TrackingID: ErrLDAPGroupDNInvalid, Domains: []ErrorDomain{DomainLDAP}},
	{Substrings: []string{"LDAP client configuration", "invalid"}, TrackingID: ErrLDAPInvalidConfiguration, Domains: []ErrorDomain{DomainLDAP}},
	{Substrings: []string{"LDAP configuration", "invalid"}, TrackingID: ErrLDAPInvalidConfiguration, Domains: []ErrorDomain{DomainLDAP}},
	{AnyOf: []string{"ssl3_get_server_certificate"}, TrackingID: ErrLDAPCertificateError, Domains: []ErrorDomain{DomainLDAP}},
	// --- SMB additional SDE patterns ---
	{AnyOf: []string{"The CIFS server name must be different from the NetBIOS name"}, TrackingID: ErrSMBNetBIOSNameConflict, Domains: []ErrorDomain{DomainSMB, DomainAD}},
	{AnyOf: []string{"Only one CIFS server is supported per SVM"}, TrackingID: ErrSMBCIFSServerAlreadyExists, Domains: []ErrorDomain{DomainSMB}},
	// --- ONTAP general SDE patterns ---
	{AnyOf: []string{"Operation is disallowed on an aggregate which is not home"}, TrackingID: ErrONTAPAggregateNotHome, Domains: []ErrorDomain{DomainONTAP}},
	{AnyOf: []string{"This operation is not allowed when giveback is in progress"}, TrackingID: ErrONTAPGivebackInProgress, Domains: []ErrorDomain{DomainONTAP}},
	{Substrings: []string{"Node", "on ring", "is offline"}, TrackingID: ErrONTAPNodeOffline, Domains: []ErrorDomain{DomainONTAP}},
	// --- KMS / CMEK ONTAP patterns ---
	{AnyOf: []string{"a key manager has already been configured for this SVM"}, TrackingID: ErrKMSAlreadyExistsEKM, Domains: []ErrorDomain{DomainKMS}},
}

// domainDefaults provides the fallback tracking ID for unclassified errors per domain.
var domainDefaults = map[ErrorDomain]int{
	DomainAD:       ErrADUnclassified,
	DomainKerberos: ErrKerberosUnclassified,
	DomainDNS:      ErrDNSUnclassified,
	DomainSMB:      ErrSMBUnclassified,
	DomainLDAP:     ErrLDAPUnclassified,
	DomainONTAP:    ErrInternalServerError,
	DomainKMS:      ErrKMSConfigureEKM,
}

func ruleAppliesToDomain(rule ErrorRule, domain ErrorDomain) bool {
	if len(rule.Domains) == 0 {
		return true
	}
	for _, d := range rule.Domains {
		if d == domain {
			return true
		}
	}
	return false
}

func matchesRule(s string, rule ErrorRule) bool {
	if len(rule.Substrings) > 0 {
		for _, sub := range rule.Substrings {
			if !strings.Contains(s, sub) {
				return false
			}
		}
		return true
	}
	if len(rule.AnyOf) > 0 {
		for _, sub := range rule.AnyOf {
			if strings.Contains(s, sub) {
				return true
			}
		}
		return false
	}
	return false
}

// ClassifyOntapError maps an ONTAP error to a VCP CustomError using the rule table and domain fallback.
// The original error is preserved in CustomError.OriginalErr. Returns nil if err is nil.
func ClassifyOntapError(err error, domain ErrorDomain) *CustomError {
	if err == nil {
		return nil
	}
	s := err.Error()
	for _, rule := range ontapErrorRules {
		if !ruleAppliesToDomain(rule, domain) {
			continue
		}
		if matchesRule(s, rule) {
			return NewVCPError(rule.TrackingID, err)
		}
	}
	if defaultTID, ok := domainDefaults[domain]; ok {
		return NewVCPError(defaultTID, err)
	}
	return NewVCPError(ErrInternalServerError, err)
}

// WrapOntapError classifies the ONTAP error for the given domain and wraps it for Temporal.
// Convenience for activities: ClassifyOntapError + WrapAsTemporalApplicationError in one call.
func WrapOntapError(err error, domain ErrorDomain) error {
	if err == nil {
		return nil
	}
	return WrapAsTemporalApplicationError(ClassifyOntapError(err, domain))
}
