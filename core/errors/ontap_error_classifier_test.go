package errors

import (
	"encoding/json"
	"errors"
	"testing"

	"go.temporal.io/sdk/temporal"
)

// ensureErrorMapLoaded reloads errorMap from the embedded JSON (other tests in the
// package may overwrite it with small inline maps). It saves the current map and
// restores it via t.Cleanup so it doesn't affect other tests.
func ensureErrorMapLoaded(t *testing.T) {
	t.Helper()
	savedMap := errorMap
	t.Cleanup(func() { errorMap = savedMap })

	var fresh map[int]ErrorMessage
	if err := json.Unmarshal(embeddedErrorsJSON, &fresh); err != nil {
		t.Fatalf("failed to reload errorMap from embedded JSON: %v", err)
	}
	errorMap = fresh
}

func TestClassifyOntapError_Nil(t *testing.T) {
	got := ClassifyOntapError(nil, DomainAD)
	if got != nil {
		t.Errorf("ClassifyOntapError(nil, DomainAD) = %v; want nil", got)
	}
}

func TestClassifyOntapError_Rules(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		name           string
		msg            string
		domain         ErrorDomain
		wantTrackingID int
	}{
		// Original CIFS/AD rules (same order as rule table)
		{"Invalid Credentials", "Invalid Credentials", DomainAD, ErrADInvalidCredentials},
		{"KRB5KDC_ERR_PREAUTH_FAILED", "KRB5KDC_ERR_PREAUTH_FAILED", DomainKerberos, ErrADInvalidCredentials},
		{"password not in sync", "does not match password stored in Active Directory", DomainAD, ErrADPasswordNotInSync},
		{"Invalid credentials were given", "Invalid credentials were given", DomainSMB, ErrADIncorrectUsername},
		{"credentials have been revoked", "credentials have been revoked", DomainAD, ErrADUserDisabled},
		{"KDC encryption type", "KDC has no support for encryption type", DomainKerberos, ErrADAESEncryptionSettingsInvalid},
		{"msDS and Insufficient access", "msDS-SupportedEncryptionTypes and Insufficient access", DomainAD, ErrADAESEncryptionSettingsInvalid},
		{"KDC Unreachable", "KDC Unreachable Details", DomainAD, ErrADKDCUnreachable},
		{"domain controllers", "Cannot find any domain controllers", DomainSMB, ErrADDomainControllersUnreachable},
		{"no server SecD", "no server available for SecD", DomainAD, ErrADDomainControllersUnreachable},
		{"LDAP server down", "RESULT_ERROR_LDAPSERVER_SERVER_DOWN and Can't contact LDAP server", DomainAD, ErrADLDAPUnreachable},
		{"ou not found", "ou not found", DomainAD, ErrADInvalidOU},
		{"insufficient privilege", "insufficient privilege", DomainSMB, ErrADInsufficientPermission},
		{"default site", "cannot find the indicated default site", DomainAD, ErrADDefaultSiteInvalid},
		{"NetLogon", "Unable to connect to NetLogon", DomainAD, ErrADNetLogonError},
		{"Operation timed out domain", "Operation timed out for domain controllers", DomainAD, ErrADLDAPNetworkIssue},
		{"Unable to connect to any domain", "Unable to connect to any domain controllers", DomainSMB, ErrADDCUnreachable},
		// DNS
		{"cannot be reached", "DNS server cannot be reached", DomainDNS, ErrDNSServerUnreachable},
		// New patterns
		{"default site validation", "Failed to determine if site is a valid default site", DomainAD, ErrADDefaultSiteValidationFailed},
		{"LDAP server not identified", "No servers available for MS_LDAP_AD", DomainAD, ErrADLDAPServerNotIdentified},
		{"Unable to start TLS", "Unable to start TLS: Server is unavailable", DomainAD, ErrADUnableToStartTLS},
		{"stale cache", "vserver cifs users-and-groups remove-stale-records failed", DomainAD, ErrADStaleCacheCleanup},
		{"CIFS job RUNNING", "CIFS Server Modify Job is RUNNING", DomainAD, ErrADUpdateInProgress},
		{"Failed to resolve name", "Failed to resolve name", DomainAD, ErrADUserResolutionFailed},
		{"LDAP search timeout", "LDAP Error: The search was timed out", DomainAD, ErrADLDAPSearchTimeout},
		{"SASL bind GSSAPI", "SASL bind to LDAP with GSSAPI failed", DomainAD, ErrADLDAPBindFailed},
		{"RESULT_ERROR_LDAPSERVER_LOCAL_ERROR", "RESULT_ERROR_LDAPSERVER_LOCAL_ERROR", DomainAD, ErrADLDAPBindFailed},
		// AD additional SDE patterns
		{"AD machine account not found", "SecD Error: machine account does not exist", DomainAD, ErrADMachineAccountNotFound},
		{"AD machine account not found SMB", "machine account does not exist for SVM", DomainSMB, ErrADMachineAccountNotFound},
		{"AD invalid kdc-ip", "Invalid value specified for element kdc-ip", DomainAD, ErrADInvalidKdcIP},
		{"AD invalid kdc-ip kerberos", "Invalid value specified for kdc-ip address", DomainKerberos, ErrADInvalidKdcIP},
		{"AD SRV record lookup", "Failed to lookup SRV record for domain", DomainAD, ErrADSRVRecordLookupFailed},
		{"AD SRV record lookup DNS", "Failed to lookup SRV record", DomainDNS, ErrADSRVRecordLookupFailed},
		{"AD password update failed", "SecD Error: no server available. Password update failed for SVM", DomainAD, ErrADPasswordUpdateFailed},
		{"AD LSA unreachable", "Unable to connect to LSA service on dc.example.com Error: RESULT_ERROR_SPINCLIENT_UNABLE_TO_RESOLVE_SERVER", DomainAD, ErrADLSAServiceUnreachable},
		// DNS additional SDE patterns
		{"DNS resolution failed", "DNS resolution failed for all the specified servers", DomainDNS, ErrDNSResolutionFailed},
		{"DNS contact failed", "Unable to contact DNS to discover domain example.com", DomainDNS, ErrDNSContactFailed},
		// LDAP SDE patterns
		{"LDAP validate cert failed", "Validate the Ldap configuration procedure failed. Certificate verification failed for server", DomainLDAP, ErrLDAPCertificateError},
		{"LDAP validate cant contact", "Validate the Ldap configuration procedure failed. Can't contact LDAP server at ldap.example.com", DomainLDAP, ErrADLDAPUnreachable},
		{"LDAP validate CN mismatch", "Validate the Ldap configuration procedure failed. Subject does not match CN for server", DomainLDAP, ErrLDAPConfigValidationFailed},
		{"LDAP validate generic", "Validate the Ldap configuration procedure failed", DomainLDAP, ErrLDAPConfigValidationFailed},
		{"LDAP invalid bind credentials", "The specified bind password or bind DN is invalid. Unable to connect to LDAP.", DomainLDAP, ErrLDAPInvalidBindCredentials},
		{"LDAP User DN not available", "User DN specified in the LDAP client configuration is not available", DomainLDAP, ErrLDAPUserDNNotAvailable},
		{"LDAP Group DN not available", "Group DN specified in the LDAP client configuration is not available", DomainLDAP, ErrLDAPGroupDNNotAvailable},
		{"LDAP User DN invalid", "User DN specified in the LDAP client configuration failed validation", DomainLDAP, ErrLDAPUserDNInvalid},
		{"LDAP Group DN invalid", "Group DN specified in the LDAP client configuration failed validation", DomainLDAP, ErrLDAPGroupDNInvalid},
		{"LDAP config invalid", "LDAP configuration contains invalid settings", DomainLDAP, ErrLDAPInvalidConfiguration},
		{"LDAP ssl cert error", "ssl3_get_server_certificate: certificate verify failed", DomainLDAP, ErrLDAPCertificateError},
		// SMB additional SDE patterns
		{"SMB NetBIOS conflict", "The CIFS server name must be different from the NetBIOS name of the home domain.", DomainSMB, ErrSMBNetBIOSNameConflict},
		{"SMB CIFS already exists", "Only one CIFS server is supported per SVM", DomainSMB, ErrSMBCIFSServerAlreadyExists},
		// ONTAP general SDE patterns
		{"ONTAP aggregate not home", "Operation is disallowed on an aggregate which is not home", DomainONTAP, ErrONTAPAggregateNotHome},
		{"ONTAP giveback in progress", "Reason: This operation is not allowed when giveback is in progress", DomainONTAP, ErrONTAPGivebackInProgress},
		{"ONTAP node offline", "Node node1 on ring nblade is offline", DomainONTAP, ErrONTAPNodeOffline},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.msg)
			ce := ClassifyOntapError(err, tt.domain)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("TrackingID = %d; want %d", ce.TrackingID, tt.wantTrackingID)
			}
			if ce.OriginalErr != err {
				t.Errorf("OriginalErr not preserved")
			}
		})
	}
}

func TestClassifyOntapError_DomainScoping(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		domain         ErrorDomain
		wantTrackingID int
	}{
		{DomainDNS, ErrDNSServerUnreachable},
		{DomainAD, ErrADUnclassified},
		{DomainKerberos, ErrKerberosUnclassified},
		{DomainSMB, ErrSMBUnclassified},
	}
	msg := "server cannot be reached"
	for _, tt := range tests {
		t.Run(string(tt.domain), func(t *testing.T) {
			err := errors.New(msg)
			ce := ClassifyOntapError(err, tt.domain)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("domain %s: TrackingID = %d; want %d", tt.domain, ce.TrackingID, tt.wantTrackingID)
			}
		})
	}
}

func TestClassifyOntapError_DomainFallback(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		domain         ErrorDomain
		wantTrackingID int
	}{
		{DomainAD, ErrADUnclassified},
		{DomainKerberos, ErrKerberosUnclassified},
		{DomainDNS, ErrDNSUnclassified},
		{DomainSMB, ErrSMBUnclassified},
		{DomainLDAP, ErrLDAPUnclassified},
		{DomainONTAP, ErrInternalServerError},
	}
	msg := "some unknown ontap error xyz"
	for _, tt := range tests {
		t.Run(string(tt.domain), func(t *testing.T) {
			err := errors.New(msg)
			ce := ClassifyOntapError(err, tt.domain)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("domain %s: TrackingID = %d; want %d", tt.domain, ce.TrackingID, tt.wantTrackingID)
			}
		})
	}
}

func TestWrapOntapError_Nil(t *testing.T) {
	got := WrapOntapError(nil, DomainAD)
	if got != nil {
		t.Errorf("WrapOntapError(nil, DomainAD) = %v; want nil", got)
	}
}

func TestWrapOntapError_ReturnsTemporalApplicationError(t *testing.T) {
	ensureErrorMapLoaded(t)
	err := errors.New("Invalid Credentials")
	wrapped := WrapOntapError(err, DomainAD)
	if wrapped == nil {
		t.Fatal("WrapOntapError returned nil")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(wrapped, &appErr) {
		t.Errorf("WrapOntapError did not return ApplicationError: %T", wrapped)
	}
	if appErr.Type() != CustomErrorType {
		t.Errorf("ApplicationError type = %q; want %q", appErr.Type(), CustomErrorType)
	}
}

// TestRuleAppliesToDomain_NilDomains verifies that a rule with nil Domains applies to every domain.
func TestRuleAppliesToDomain_NilDomains(t *testing.T) {
	rule := ErrorRule{AnyOf: []string{"test"}, TrackingID: 1, Domains: nil}
	for _, d := range []ErrorDomain{DomainAD, DomainKerberos, DomainDNS, DomainSMB, DomainLDAP, DomainONTAP} {
		if !ruleAppliesToDomain(rule, d) {
			t.Errorf("nil Domains should match all; got false for %s", d)
		}
	}
}

// TestMatchesRule_EmptyRule verifies that a rule with no Substrings and no AnyOf never matches.
func TestMatchesRule_EmptyRule(t *testing.T) {
	rule := ErrorRule{TrackingID: 1}
	if matchesRule("anything", rule) {
		t.Error("empty rule (no Substrings, no AnyOf) should return false")
	}
}

// TestClassifyOntapError_RuleOrder verifies that the first matching rule wins when multiple rules could match.
func TestClassifyOntapError_RuleOrder(t *testing.T) {
	ensureErrorMapLoaded(t)
	msg := "Invalid Credentials and ou not found"
	err := errors.New(msg)
	ce := ClassifyOntapError(err, DomainAD)
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID != ErrADInvalidCredentials {
		t.Errorf("first match should win: TrackingID = %d; want %d (ErrADInvalidCredentials)", ce.TrackingID, ErrADInvalidCredentials)
	}
}

// TestClassifyOntapError_SubstringsAND verifies that Substrings rules require ALL substrings (no partial match).
func TestClassifyOntapError_SubstringsAND(t *testing.T) {
	ensureErrorMapLoaded(t)
	msg := "msDS-SupportedEncryptionTypes mentioned"
	err := errors.New(msg)
	ce := ClassifyOntapError(err, DomainAD)
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID == ErrADAESEncryptionSettingsInvalid {
		t.Error("partial Substrings match should not classify as ErrADAESEncryptionSettingsInvalid")
	}
	if ce.TrackingID != ErrADUnclassified {
		t.Errorf("unmatched message should use domain fallback: TrackingID = %d; want %d (ErrADUnclassified)", ce.TrackingID, ErrADUnclassified)
	}
}

// TestClassifyOntapError_AnyOfAlternatives covers remaining AnyOf alternatives not in the main rules table.
func TestClassifyOntapError_AnyOfAlternatives(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		name           string
		msg            string
		domain         ErrorDomain
		wantTrackingID int
	}{
		{"Username format not supported", "Username format not supported", DomainAD, ErrADIncorrectUsername},
		{"Lookup of organizational_unit failed", "Lookup of organizational_unit failed", DomainSMB, ErrADInvalidOU},
		{"Reason Invalid credentials", "Reason: Invalid credentials.", DomainKerberos, ErrADIncorrectUsername},
		{"Clients credentials revoked", "Clients credentials have been revoked", DomainAD, ErrADUserDisabled},
		{"Failed to bind SPN", "Failed to bind service principal name on LIF", DomainKerberos, ErrADKDCUnreachable},
		{"KRB5_KDC_UNREACH", "KRB5_KDC_UNREACH", DomainSMB, ErrADKDCUnreachable},
		{"RESULT_ERROR_SPINCLIENT", "RESULT_ERROR_SPINCLIENT", DomainAD, ErrADNetLogonError},
		{"LDAP constraint", "LDAP constraint violation", DomainLDAP, ErrADInsufficientPermission},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.msg)
			ce := ClassifyOntapError(err, tt.domain)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("TrackingID = %d; want %d", ce.TrackingID, tt.wantTrackingID)
			}
		})
	}
}

// TestClassifyOntapError_UnknownDomain verifies that an unlisted domain falls back to ErrInternalServerError.
func TestClassifyOntapError_UnknownDomain(t *testing.T) {
	ensureErrorMapLoaded(t)
	err := errors.New("any error")
	ce := ClassifyOntapError(err, ErrorDomain("unknown_domain"))
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID != ErrInternalServerError {
		t.Errorf("unknown domain should fall back to internal: TrackingID = %d; want %d", ce.TrackingID, ErrInternalServerError)
	}
}

// TestClassifyOntapError_EmptyMessage verifies empty error message gets domain fallback.
func TestClassifyOntapError_EmptyMessage(t *testing.T) {
	ensureErrorMapLoaded(t)
	err := errors.New("")
	ce := ClassifyOntapError(err, DomainAD)
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID != ErrADUnclassified {
		t.Errorf("empty message should use domain fallback: TrackingID = %d; want %d", ce.TrackingID, ErrADUnclassified)
	}
	if ce.OriginalErr != err {
		t.Error("OriginalErr not preserved")
	}
}

// TestWrapOntapError_ExtractCustomError verifies that WrapOntapError produces an error that ExtractCustomError can unwrap with correct TrackingID.
func TestWrapOntapError_ExtractCustomError(t *testing.T) {
	ensureErrorMapLoaded(t)
	origMsg := "Invalid Credentials"
	origErr := errors.New(origMsg)
	wrapped := WrapOntapError(origErr, DomainAD)
	if wrapped == nil {
		t.Fatal("WrapOntapError returned nil")
	}
	ce := ExtractCustomError(wrapped)
	if ce == nil {
		t.Fatal("ExtractCustomError returned nil")
	}
	if ce.TrackingID != ErrADInvalidCredentials {
		t.Errorf("ExtractCustomError TrackingID = %d; want %d", ce.TrackingID, ErrADInvalidCredentials)
	}
	if ce.OriginalErr == nil {
		t.Fatal("OriginalErr is nil")
	}
	if ce.OriginalErr.Error() != origMsg {
		t.Errorf("OriginalErr message = %q; want %q", ce.OriginalErr.Error(), origMsg)
	}
}

// TestClassifyOntapError_LDAPDomain verifies rules that include DomainLDAP apply when domain is LDAP.
func TestClassifyOntapError_LDAPDomain(t *testing.T) {
	ensureErrorMapLoaded(t)
	err := errors.New("RESULT_ERROR_LDAPSERVER_SERVER_DOWN and Can't contact LDAP server")
	ce := ClassifyOntapError(err, DomainLDAP)
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID != ErrADLDAPUnreachable {
		t.Errorf("DomainLDAP should match LDAP rule: TrackingID = %d; want %d", ce.TrackingID, ErrADLDAPUnreachable)
	}
}

// TestClassifyOntapError_PasswordUpdateVsDCUnreachable verifies that the password-update-specific rule
// matches before the generic DC-unreachable rule when both "no server available" and "SecD" are present.
func TestClassifyOntapError_PasswordUpdateVsDCUnreachable(t *testing.T) {
	ensureErrorMapLoaded(t)
	msg := "Failed to modify CIFS server. Reason: SecD Error: no server available. Password update failed for SVM test-svm"
	err := errors.New(msg)
	ce := ClassifyOntapError(err, DomainAD)
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID != ErrADPasswordUpdateFailed {
		t.Errorf("password update rule should win over DC unreachable: TrackingID = %d; want %d", ce.TrackingID, ErrADPasswordUpdateFailed)
	}
}

// TestClassifyOntapError_LSAvsNetLogon verifies that the LSA rule (more specific)
// matches before the NetLogon rule when both contain RESULT_ERROR_SPINCLIENT.
func TestClassifyOntapError_LSAvsNetLogon(t *testing.T) {
	ensureErrorMapLoaded(t)
	msg := "Unable to connect to LSA service on dc.example.com. Error: RESULT_ERROR_SPINCLIENT_UNABLE_TO_RESOLVE_SERVER"
	err := errors.New(msg)
	ce := ClassifyOntapError(err, DomainAD)
	if ce == nil {
		t.Fatal("ClassifyOntapError returned nil")
	}
	if ce.TrackingID != ErrADLSAServiceUnreachable {
		t.Errorf("LSA rule should win: TrackingID = %d; want %d", ce.TrackingID, ErrADLSAServiceUnreachable)
	}
}

// TestClassifyOntapError_LDAPValidationSpecificity verifies that specific LDAP validation rules
// match before the generic "Validate the Ldap configuration procedure failed" catch-all.
func TestClassifyOntapError_LDAPValidationSpecificity(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		name           string
		msg            string
		wantTrackingID int
	}{
		{
			"cert failure beats generic",
			"Validate the Ldap configuration procedure failed. Certificate verification failed for server ldap.example.com",
			ErrLDAPCertificateError,
		},
		{
			"cant contact beats generic",
			"Validate the Ldap configuration procedure failed. Can't contact LDAP server at 10.0.0.1",
			ErrADLDAPUnreachable,
		},
		{
			"CN mismatch beats generic",
			"Validate the Ldap configuration procedure failed. Subject does not match CN for server",
			ErrLDAPConfigValidationFailed,
		},
		{
			"generic fallback",
			"Validate the Ldap configuration procedure failed. Unknown reason.",
			ErrLDAPConfigValidationFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.msg)
			ce := ClassifyOntapError(err, DomainLDAP)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("TrackingID = %d; want %d", ce.TrackingID, tt.wantTrackingID)
			}
		})
	}
}

// TestClassifyOntapError_NewDomainScoping verifies that new rules respect domain boundaries.
func TestClassifyOntapError_NewDomainScoping(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		name           string
		msg            string
		domain         ErrorDomain
		wantTrackingID int
	}{
		{"machine account AD", "machine account does not exist", DomainAD, ErrADMachineAccountNotFound},
		{"machine account DNS falls through", "machine account does not exist", DomainDNS, ErrDNSUnclassified},
		{"DNS resolution in AD falls through", "DNS resolution failed for all the specified servers", DomainAD, ErrADUnclassified},
		{"LDAP User DN in AD falls through", "User DN specified in the LDAP client configuration is not available", DomainAD, ErrADUnclassified},
		{"ONTAP aggregate not home in AD falls through", "Operation is disallowed on an aggregate which is not home", DomainAD, ErrADUnclassified},
		{"ONTAP aggregate not home matches ONTAP", "Operation is disallowed on an aggregate which is not home", DomainONTAP, ErrONTAPAggregateNotHome},
		{"SMB CIFS limit in ONTAP falls through", "Only one CIFS server is supported per SVM", DomainONTAP, ErrInternalServerError},
		{"NetBIOS conflict matches AD", "The CIFS server name must be different from the NetBIOS name", DomainAD, ErrSMBNetBIOSNameConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.msg)
			ce := ClassifyOntapError(err, tt.domain)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("TrackingID = %d; want %d", ce.TrackingID, tt.wantTrackingID)
			}
		})
	}
}

// TestClassifyOntapError_RealisticONTAPMessages tests with realistic full ONTAP error messages
// similar to what SDE receives, not just isolated substrings.
func TestClassifyOntapError_RealisticONTAPMessages(t *testing.T) {
	ensureErrorMapLoaded(t)
	tests := []struct {
		name           string
		msg            string
		domain         ErrorDomain
		wantTrackingID int
	}{
		{
			"full AD machine account error",
			`Failed to modify the CIFS server "NETAPP". Reason: SecD Error: machine account does not exist.`,
			DomainAD,
			ErrADMachineAccountNotFound,
		},
		{
			"full LSA firewall error",
			`Unable to connect to LSA service on dc1.example.com. Error: RESULT_ERROR_SPINCLIENT_UNABLE_TO_RESOLVE_SERVER. Could not find Windows SID. Retry requested, but the retry window has expired; giving up`,
			DomainAD,
			ErrADLSAServiceUnreachable,
		},
		{
			"full password update error",
			`Failed to modify CIFS server "NETAPP". Reason: SecD Error: no server available. Machine account creation procedure failed. Password update failed for SVM svm-test`,
			DomainAD,
			ErrADPasswordUpdateFailed,
		},
		{
			"full LDAP bind credentials error",
			`The specified bind password or bind DN is invalid. Validate the Ldap configuration procedure failed. Unable to connect to LDAP server. Error: Invalid credentials`,
			DomainLDAP,
			ErrLDAPInvalidBindCredentials,
		},
		{
			"full LDAP cert verify error",
			`Validate the Ldap configuration procedure failed. Certificate verification failed for server ldaps://10.0.0.1:636`,
			DomainLDAP,
			ErrLDAPCertificateError,
		},
		{
			"full DNS discovery error",
			`FAILURE: Unable to contact DNS to discover domain example.com`,
			DomainDNS,
			ErrDNSContactFailed,
		},
		{
			"full node offline error",
			`Node node-02 on ring nblade is offline. Some operations may be impacted.`,
			DomainONTAP,
			ErrONTAPNodeOffline,
		},
		{
			"full aggregate not home",
			`Operation is disallowed on an aggregate which is not home. The aggregate may have been relocated.`,
			DomainONTAP,
			ErrONTAPAggregateNotHome,
		},
		{
			"full giveback in progress",
			`Reason: This operation is not allowed when giveback is in progress on node node-01`,
			DomainONTAP,
			ErrONTAPGivebackInProgress,
		},
		{
			"full LDAP config invalid",
			`The LDAP configuration contains invalid parameters. Please review settings.`,
			DomainLDAP,
			ErrLDAPInvalidConfiguration,
		},
		{
			"full ssl3 cert error",
			`error:14090086:SSL routines:ssl3_get_server_certificate:certificate verify failed`,
			DomainLDAP,
			ErrLDAPCertificateError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.msg)
			ce := ClassifyOntapError(err, tt.domain)
			if ce == nil {
				t.Fatal("ClassifyOntapError returned nil")
			}
			if ce.TrackingID != tt.wantTrackingID {
				t.Errorf("TrackingID = %d; want %d", ce.TrackingID, tt.wantTrackingID)
			}
			if ce.OriginalErr != err {
				t.Error("OriginalErr not preserved")
			}
		})
	}
}
