package vsa

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// Type Definitions and Constructor

type activeDirectoryUpdater struct {
	provider *OntapRestProvider
	params   UpdateActiveDirectoryCredentialsParams
	api      ontapRest.RESTClient
	svm      *ontapRest.Svm
	svmName  string
	svmUUID  string
}

// Function variables for testing
var (
	newActiveDirectoryUpdater         = _newActiveDirectoryUpdater
	updateServerCertificate           = _updateServerCertificate
	removeDomainFromUsers             = _removeDomainFromUsers
	removeDomainFromUser              = _removeDomainFromUser
	updateSiteONTAP                   = _updateSiteONTAP
	getCifsGroups                     = _getCifsGroups
	removeUsersFromGroup              = _removeUsersFromGroup
	addUsersToGroup                   = _addUsersToGroup
	getSecurityPrivilegedUsers        = _getSecurityPrivilegedUsers
	removeSecurityPrivilegesFromUsers = _removeSecurityPrivilegesFromUsers
	addSecurityPrivilegesToUsers      = _addSecurityPrivilegesToUsers
	modifyADLdapSigning               = _modifyADLdapSigning
	decryptPassword                   = utils.DecryptPassword
)

const (
	serverDiscoveryModeField = "server_discovery_mode"
)

func _newActiveDirectoryUpdater(params UpdateActiveDirectoryCredentialsParams, provider *OntapRestProvider, api ontapRest.RESTClient, svmName, svmUUID string) *activeDirectoryUpdater {
	return &activeDirectoryUpdater{
		provider: provider,
		params:   params,
		api:      api,
		svmName:  svmName,
		svmUUID:  svmUUID,
	}
}

// Main Update Entry Point

// UpdateActiveDirectoryCredentials updates the Active Directory credentials in ONTAP
func (provider *OntapRestProvider) UpdateActiveDirectoryCredentials(params UpdateActiveDirectoryCredentialsParams, cifs ontapRest.CifsService, svmName, svmExternalUUID string) error {
	if params.NewCredentials != nil && svmExternalUUID == "" {
		return errors.New("Error determining server for update")
	}
	forceUpdate := strings.Contains(params.OldCredentials.Status, datamodel.LifeCycleStateError)
	serverForUpdate := svmName

	api, err := provider.CreateRESTClient()
	if err != nil {
		return err
	}
	adu := newActiveDirectoryUpdater(params, provider, api, svmName, svmExternalUUID)

	err = adu.LoadSVM(serverForUpdate)
	if err != nil {
		return err
	}

	dnsState := isDDNSEnabled(provider.Logger, api, svmExternalUUID)

	// DNS modification
	if forceUpdate || params.NewCredentials.DNS != params.OldCredentials.DNS {
		err = adu.ModifyDNS()
		if err != nil {
			return err
		}
	}

	// Password handling
	decryptedPassword, err := decryptPassword(params.NewCredentials.Password)
	if err != nil {
		return errors.New("Password could not be decrypted.")
	}
	params.NewCredentials.Password = log.Secret(*decryptedPassword)

	// Determine what needs updating

	updateSite := forceUpdate || params.NewCredentials.Site != nil && *params.NewCredentials.Site != nillable.GetString(params.OldCredentials.Site, "")
	updateNetBIOS := params.NewCredentials.NetBIOS != params.OldCredentials.NetBIOS
	updateUsers := forceUpdate || params.NewCredentials.Users != nil && !reflect.DeepEqual(params.OldCredentials.Users, params.NewCredentials.Users)
	updateAesEncryption := forceUpdate || params.NewCredentials.AesEncryption != nil && (params.OldCredentials.AesEncryption == nil || *params.NewCredentials.AesEncryption != *params.OldCredentials.AesEncryption)
	updateLdapSigning := forceUpdate || params.NewCredentials.LdapSigning != nil && (params.OldCredentials.LdapSigning == nil || *params.NewCredentials.LdapSigning != *params.OldCredentials.LdapSigning)
	updateServerCaCertificate := forceUpdate || params.NewCredentials.ServerRootCaCertificate != nil && *params.NewCredentials.ServerRootCaCertificate != nillable.GetString(params.OldCredentials.ServerRootCaCertificate, "")
	updateAllowLocalNFSUsersWithLdap := forceUpdate || params.NewCredentials.AllowLocalNFSUsersWithLdap != nil && (params.OldCredentials.AllowLocalNFSUsersWithLdap == nil || *params.NewCredentials.AllowLocalNFSUsersWithLdap != *params.OldCredentials.AllowLocalNFSUsersWithLdap)
	updateLdapOverTLS := forceUpdate || params.NewCredentials.LdapOverTLS != nil && *params.NewCredentials.LdapOverTLS != *params.OldCredentials.LdapOverTLS
	encryptDCConnections := forceUpdate || params.NewCredentials.EncryptDCConnections != nil && *params.NewCredentials.EncryptDCConnections != *params.OldCredentials.EncryptDCConnections
	updateKdcIP := forceUpdate || params.NewCredentials.KdcIP != params.OldCredentials.KdcIP
	updateADName := forceUpdate || params.NewCredentials.AdName != params.OldCredentials.AdName
	updateName := forceUpdate || params.NewCredentials.Name != nil && *params.NewCredentials.Name != nillable.GetString(params.OldCredentials.Name, "")
	updateDNAndFilter := forceUpdate || params.NewCredentials.UserDN != nil && *params.NewCredentials.UserDN != nillable.GetString(params.OldCredentials.UserDN, "") || params.NewCredentials.GroupDN != nil && *params.NewCredentials.GroupDN != nillable.GetString(params.OldCredentials.GroupDN, "") || params.NewCredentials.GroupMembershipFilter != nil && *params.NewCredentials.GroupMembershipFilter != nillable.GetString(params.OldCredentials.GroupMembershipFilter, "")
	updatePreferredServersForLdapClient := forceUpdate || params.NewCredentials.PreferredServersForLdapClient != nil && *params.NewCredentials.PreferredServersForLdapClient != nillable.GetString(params.OldCredentials.PreferredServersForLdapClient, "")

	if !updateNetBIOS && !updateSite && !updateUsers && !updateAesEncryption && !updateServerCaCertificate && !updateLdapSigning && !updateAllowLocalNFSUsersWithLdap && !updateLdapOverTLS && !updateADName && !updateKdcIP && !updateDNAndFilter && !updatePreferredServersForLdapClient && !updateName {
		return nil
	}

	cifsServerName, domain, _, err := adu.LoadCIFSServer(&cifs)
	if err != nil {
		return err
	}

	// Update server CA root certificate
	if updateServerCaCertificate && params.NewCredentials.ServerRootCaCertificate != nil {
		err = adu.UpdateServerCACertificate()
		if err != nil {
			return err
		}
	}

	// update LDAP over TLS on CIFS server and on LDAP client
	if updateLdapOverTLS && params.NewCredentials.LdapOverTLS != nil {
		err = adu.UpdateLDAPOverTLS()
		if err != nil {
			return err
		}
	}

	if updateAllowLocalNFSUsersWithLdap && params.NewCredentials.AllowLocalNFSUsersWithLdap != nil {
		err = adu.UpdateAllowLocalNFSUsersWithLdap()
		if err != nil {
			return err
		}
	}

	// Core CIFS updates requiring credentials
	if updateNetBIOS || updateSite || updateAesEncryption || updateLdapSigning || updateADName || updateKdcIP {
		username := nillable.GetString(&params.NewCredentials.Username, params.OldCredentials.Username)
		var password log.Secret
		if params.NewCredentials.Password == "" {
			password = params.OldCredentials.Password
		} else {
			password = params.NewCredentials.Password
		}

		site := nillable.GetString(params.NewCredentials.Site, nillable.GetString(params.OldCredentials.Site, ""))
		aesEncryption := nillable.GetBool(params.NewCredentials.AesEncryption, nillable.GetBool(params.OldCredentials.AesEncryption, false))

		netBIOS := params.OldCredentials.NetBIOS
		// Normalize netBIOS to uppercase
		if len(netBIOS) > 10 {
			netBIOS = strings.ToUpper(netBIOS[0:10])
		} else {
			netBIOS = strings.ToUpper(netBIOS)
		}

		// Extract postfix from current CIFS server name
		servernamePostFix := strings.Replace(*cifs.Name, netBIOS, "", 1)

		netBIOS = nillable.GetString(&params.NewCredentials.NetBIOS, params.OldCredentials.NetBIOS)
		newNetBIOS := ""
		// Construct new NetBIOS name with postfix if it exists
		if servernamePostFix == "" {
			newNetBIOS = netBIOS
		} else {
			newNetBIOS = netBIOS + servernamePostFix
		}

		if forceUpdate && !updateNetBIOS && !strings.EqualFold(*cifsServerName, newNetBIOS) {
			updateNetBIOS = true
		}

		if updateNetBIOS {
			defaultSite := ""
			if params.OldCredentials.Site != nil {
				defaultSite = *params.OldCredentials.Site
			}

			err = adu.UpdateNetBios(newNetBIOS, defaultSite, username, password)

			if err != nil {
				return err
			}
		}

		if updateSite {
			err = adu.UpdateSite(site, false)
			if err != nil {
				return err
			}
		}

		if updateAesEncryption {
			err = adu.UpdateAesEncryption(aesEncryption, username, password)
			if err != nil {
				return err
			}
		}

		if !dnsState {
			return errors.New("DNS is not enabled")
		}

		fqdn := newNetBIOS + "." + params.OldCredentials.Domain
		err = adu.UpdateDDNS(fqdn)
		if err != nil {
			return err
		}

		// TODO: TO be revisited when kerberos is supported
		//
		//	err = adu.UpdateNFSService(updateNetBIOS, updateADName, updateKdcIP, fqdn, domain, username, organizationalUnit, password)
		//	if err != nil {
		//		return nil, err
		// }
	}

	if updateUsers {
		err = adu.UpdateUsers(*domain)
		if err != nil {
			return err
		}
	}

	if updatePreferredServersForLdapClient || updateDNAndFilter {
		err = adu.UpdatePreferredDCOrDNAndFilter(updatePreferredServersForLdapClient, updateDNAndFilter)
		if err != nil {
			return err
		}
	}

	if encryptDCConnections && params.NewCredentials.EncryptDCConnections != nil {
		err = adu.UpdateEncryptDCConnections()
		if err != nil {
			return err
		}
	}

	return nil
}

// Resource Loading

// LoadSVM caches the ontap SVM of the target CIFS server to be used for the rest of the flow
func (adu *activeDirectoryUpdater) LoadSVM(svmName string) error {
	getParams := GetSvmParams{
		Name: svmName,
	}
	svm, err := adu.provider.GetSVM(getParams)
	if err != nil {
		return err
	}
	adu.svm = svm
	return nil
}

// LoadCIFSServer caches the ontap CIFS server of the target svm to be used for the rest of the flow
func (adu *activeDirectoryUpdater) LoadCIFSServer(cifs *ontapRest.CifsService) (*string, *string, *string, error) {
	if cifs == nil {
		return nil, nil, nil, errors.New("Could not retrieve CIFS server")
	}

	return cifs.Name, cifs.AdDomain.Fqdn, cifs.AdDomain.OrganizationalUnit, nil
}

// CIFS Server Operations

// UpdateNetBios updates the netbios on the CIFS server
func (adu *activeDirectoryUpdater) UpdateNetBios(newNetBIOS, site, username string, password log.Secret) (err error) {
	tnas := adu.api.NAS()

	if err = tnas.CifsServiceModify(&ontapRest.CifsServiceModifyParams{
		Enabled: nillable.ToPointer(false),
		SvmUUID: &adu.svmUUID,
	}); err != nil {
		return err
	}
	defer func() {
		if err2 := tnas.CifsServiceModify(&ontapRest.CifsServiceModifyParams{
			Enabled: nillable.ToPointer(true),
			SvmUUID: &adu.svmUUID,
		}); err2 != nil {
			if err == nil {
				err = err2
			} else {
				adu.provider.Logger.Errorf("Error starting CIFS server. err: %s", err2.Error())
			}
		}
	}()

	err3 := tnas.CifsServiceModify(&ontapRest.CifsServiceModifyParams{
		Name:     &newNetBIOS,
		Username: &username,
		Password: nillable.ToPointer(string(password)),
		SvmUUID:  &adu.svmUUID,
		Site:     &site,
	})
	return err3
}

// UpdateAesEncryption updates the CIFS server AES Encryption settings
func (adu *activeDirectoryUpdater) UpdateAesEncryption(aesEncryption bool, username string, password log.Secret) error {
	return adu.api.NAS().CifsServiceModify(&ontapRest.CifsServiceModifyParams{
		AesEncryptionEnabled: &aesEncryption,
		Username:             &username,
		Password:             nillable.ToPointer(string(password)),
		SvmUUID:              &adu.svmUUID,
	})
}

// UpdateEncryptDCConnections updates the encryption flag on the CIFS server
func (adu *activeDirectoryUpdater) UpdateEncryptDCConnections() error {
	modifyEncryptedDCConnectionsParams := &ontapRest.CifsServiceModifyParams{
		EncryptDCConnections: adu.params.NewCredentials.EncryptDCConnections,
		SvmUUID:              &adu.svmUUID,
	}
	return adu.api.NAS().CifsServiceModify(modifyEncryptedDCConnectionsParams)
}

// Site Management

// UpdateSite sets or clears the default site name and domain discovery mode
func (adu *activeDirectoryUpdater) UpdateSite(site string, preferredDCsForCIFSDomainSet bool) error {
	// Fetch preferred DCs for the given site
	var preferredDCsForCifsDomain []string
	var err error
	if site != "" {
		lookupType := "ipv4"
		fetchDomainControllersUsingSrvLookupParams := &ontapRest.SrvLookupParams{
			SVMName:      adu.svmName,
			LookupString: "_ldap._tcp." + site + "._sites." + adu.params.NewCredentials.Domain,
			LookupType:   &lookupType,
		}
		preferredDCsForCifsDomain, err = adu.api.NAS().DomainControllersSrvLookupGet(fetchDomainControllersUsingSrvLookupParams)

		if err != nil {
			if strings.Contains(err.Error(), "Reason: dns not found") {
				return vsaerror.NewVCPError(vsaerror.ErrBadRequest, err)
			}
			return err
		}
	}

	// Remove preferred DCs after the site change
	var preferredDCsSetOnONTAP []string
	defer func() {
		if len(preferredDCsSetOnONTAP) > 0 {
			for _, preferredDCForCifsDomain := range preferredDCsSetOnONTAP {
				params := &ontapRest.CifsDomainPreferredDCDeleteParams{
					SvmUUID:  adu.svmUUID,
					ServerIP: &preferredDCForCifsDomain,
					Fqdn:     &adu.params.NewCredentials.Domain,
				}
				errRemovingPreferredDC := adu.api.NAS().CifsDomainPreferredDCDelete(params)
				if errRemovingPreferredDC != nil {
					adu.provider.Logger.Errorf("Error removing preferred DC for serverIP %s, fqdn %s", preferredDCForCifsDomain, adu.params.NewCredentials.Domain)
				}
			}
		}
	}()

	// Setting preferred domain before site change so that only reachable DC's are tried for site change
	if len(preferredDCsForCifsDomain) > 0 {
		for _, preferredDCForCifsDomain := range preferredDCsForCifsDomain {
			skipConfigValidation := true
			params := &ontapRest.CifsDomainPreferredDCCreateParams{
				SvmUUID:              adu.svmUUID,
				SkipConfigValidation: &skipConfigValidation,
				CifsDomainPreferredDC: &ontapRest.CifsDomainPreferredDC{
					ServerIP: &preferredDCForCifsDomain,
					Fqdn:     &adu.params.NewCredentials.Domain,
				},
			}
			errAddingPreferredDC := adu.api.NAS().CifsDomainPreferredDCCreate(params)
			if errAddingPreferredDC != nil {
				adu.provider.Logger.Errorf("Error adding preferred DC for serverIP %s, fqdn %s", preferredDCForCifsDomain, adu.params.NewCredentials.Domain)
			} else {
				preferredDCsSetOnONTAP = append(preferredDCsSetOnONTAP, preferredDCForCifsDomain)
				preferredDCsForCIFSDomainSet = true
			}
		}
	}
	return updateSiteONTAP(&site, preferredDCsForCIFSDomainSet, adu)
}

func _updateSiteONTAP(site *string, usePreferredDC bool, adu *activeDirectoryUpdater) (err error) {
	if site == nil {
		return nil
	}

	if adu == nil {
		return errors.New("activeDirectoryUpdater is nil")
	}

	originalMode := string(DiscoveryModeAll)
	defer func() {
		if usePreferredDC && err != nil {
			// If changing site fails then we change back from none to previously set mode
			modifyParams := &ontapRest.CifsDomainModifyParams{
				DiscoveryMode: &originalMode,
				SvmUUID:       adu.svmUUID,
			}
			if rollbackErr := adu.api.NAS().CifsDomainModify(modifyParams); rollbackErr != nil {
				adu.provider.Logger.Errorf("Error restoring domain discovery mode: %v", rollbackErr)
			}
		}
	}()

	if *site != "" {
		if usePreferredDC {
			// Get current server discovery mode before changing to 'none'
			fields := []string{serverDiscoveryModeField}
			params := &ontapRest.CifsDomainGetParams{
				BaseParams: ontapRest.BaseParams{Fields: fields},
				SvmUUID:    adu.svmUUID,
			}
			var response *ontapRest.CifsDomain
			response, err = adu.api.NAS().CifsDomainGet(params)
			if err != nil {
				return vsaerror.New(fmt.Sprintf("Error fetching domain discovery mode: %v", err))
			}

			if response != nil && response.ServerDiscoveryMode != nil {
				originalMode = *response.ServerDiscoveryMode
			}

			// Set discovery mode to 'none' before site change
			mode := string(DiscoveryModeNone)
			modifyParams := &ontapRest.CifsDomainModifyParams{
				DiscoveryMode: &mode,
				SvmUUID:       adu.svmUUID,
			}
			if err = adu.api.NAS().CifsDomainModify(modifyParams); err != nil {
				return vsaerror.New(fmt.Sprintf("Error setting domain discovery mode to none: %v", err))
			}
		}

		// Modify CIFS server to set the site
		cifsModifyParams := &ontapRest.CifsServiceModifyParams{
			Site:    site,
			SvmUUID: &adu.svmUUID,
		}
		if err = adu.api.NAS().CifsServiceModify(cifsModifyParams); err != nil {
			return vsaerror.New(fmt.Sprintf("Error updating CIFS service with Site: %v, err: %v", site, err))
		}

		siteMode := string(DiscoveryModeSite)
		siteModifyParams := &ontapRest.CifsDomainModifyParams{
			DiscoveryMode: &siteMode,
			SvmUUID:       adu.svmUUID,
		}
		if err = adu.api.NAS().CifsDomainModify(siteModifyParams); err != nil {
			return vsaerror.New(fmt.Sprintf("Error setting domain discovery mode to site: %v", err))
		}
		return nil
	}

	allMode := string(DiscoveryModeAll)
	allModifyParams := &ontapRest.CifsDomainModifyParams{
		DiscoveryMode: &allMode,
		SvmUUID:       adu.svmUUID,
	}
	if err = adu.api.NAS().CifsDomainModify(allModifyParams); err != nil {
		return vsaerror.New(fmt.Sprintf("Error setting domain discovery mode to all: %v", err))
	}

	// Modify CIFS server to set the site
	cifsModifyParams := &ontapRest.CifsServiceModifyParams{
		Site:    site,
		SvmUUID: &adu.svmUUID,
	}
	if err = adu.api.NAS().CifsServiceModify(cifsModifyParams); err != nil {
		return vsaerror.New(fmt.Sprintf("Error updating CIFS service with Site: %v, err: %v", site, err))
	}
	return nil
}

// DNS Operations

// ModifyDNS modifies the DNS
func (adu *activeDirectoryUpdater) ModifyDNS() error {
	return adu.api.NameServices().DNSModify(&ontapRest.DNSModifyParams{
		SvmUUID:     adu.svmUUID,
		Domains:     []string{adu.params.NewCredentials.Domain},
		NameServers: strings.Split(strings.Replace(adu.params.NewCredentials.DNS, " ", "", -1), ","),
	})
}

// UpdateDDNS updates a DDNS configuration for the SVM
func (adu *activeDirectoryUpdater) UpdateDDNS(fqdn string) error {
	return adu.api.NameServices().DNSModify(&ontapRest.DNSModifyParams{
		SvmUUID: adu.svmUUID,
		DDNSModifyParams: ontapRest.DDNSModifyParams{
			UseSecure: &secureDDNS,
			Fqdn:      &fqdn,
			Enabled:   nillable.ToPointer(true),
		},
	})
}

// User Management

// UpdateUsers updates the backup, seSecurity and builtin/Administrators users on the CIFS server
func (adu *activeDirectoryUpdater) UpdateUsers(domainWorkgroup string) error {
	groups := getActiveDirectoryUserMapKeys(adu.params.OldCredentials.Users, adu.params.NewCredentials.Users)

	domain := strings.ToUpper(nillable.GetString(&adu.params.NewCredentials.Domain, adu.params.OldCredentials.Domain))

	if adu.params.NewCredentials.Users == nil {
		adu.params.NewCredentials.Users = adu.params.OldCredentials.Users
	}

	tnas := adu.api.NAS()
	svmUUID := adu.svmUUID
	logger := adu.provider.Logger
	var ontapGroups map[string]*ontapRest.CifsGroup

	for _, group := range groups {
		if adu.params.NewCredentials.Users[group] == nil {
			adu.params.NewCredentials.Users[group] = adu.params.OldCredentials.Users[group]
		}
		newCredentialUsers := adu.params.NewCredentials.Users[group]
		newDomainUsers := prependDomainToUsers(newCredentialUsers, domain)

		if group != utils.ActiveDirectorySeSecurityPrivilege {
			if ontapGroups == nil {
				var err error
				ontapGroups, err = getCifsGroups(tnas, svmUUID)
				if err != nil {
					return err
				}
			}

			ontapGroup, ok := ontapGroups[group]
			if !ok {
				return errors.Errorf("Local group '%s' not found", group)
			}

			newUsers, removedUsers := getActiveDirectoryUserChanges(prependDomainToUsers(ontapGroup.Members, domain), newDomainUsers)

			if len(removedUsers) > 0 {
				if err := removeUsersFromGroup(logger, tnas, svmUUID, domainWorkgroup, ontapGroup, removedUsers); err != nil {
					return err
				}
			}

			if len(newUsers) > 0 {
				if err := addUsersToGroup(logger, tnas, svmUUID, domainWorkgroup, ontapGroup, newUsers); err != nil {
					return err
				}
			}
		} else {
			ontapSecurityMembers, err := getSecurityPrivilegedUsers(tnas, svmUUID)
			if err != nil {
				return err
			}

			newUsers, removedUsers := getActiveDirectoryUserChanges(prependDomainToUsers(ontapSecurityMembers, domain), newDomainUsers)

			if len(removedUsers) > 0 {
				if err = removeSecurityPrivilegesFromUsers(logger, tnas, svmUUID, domainWorkgroup, removedUsers); err != nil {
					return err
				}
			}

			if len(newUsers) > 0 {
				if err = addSecurityPrivilegesToUsers(logger, tnas, svmUUID, domainWorkgroup, newUsers); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Certificate Management

// UpdateServerCACertificate deletes old certificate from the SVM and installs new certificate
func (adu *activeDirectoryUpdater) UpdateServerCACertificate() error {
	err := updateServerCertificate(adu.provider.Logger, adu.api, adu.svmName, adu.params.NewCredentials.ServerRootCaCertificate, adu.params.OldCredentials.ServerRootCaCertificate)
	return err
}

func _updateServerCertificate(logger log.Logger, trc ontapRest.RESTClient, svmName string, tlsCert, oldCert *string) error {
	serverRootCAGetParams := &ontapRest.ServerRootCAGetParams{
		SvmName:         &svmName,
		CertificateType: nillable.ToPointer("server-ca"),
	}
	cert, err := trc.Security().ServerRootCACertificateGet(serverRootCAGetParams)
	if err != nil && !errors.IsNotFoundErr(err) {
		return err
	} else if err == nil {
		serverRootCADeleteParams := &ontapRest.ServerRootCADeleteParams{
			SvmName:              &svmName,
			SerialNumber:         cert.SerialNumber,
			CommonName:           cert.CommonName,
			CertificateAuthority: cert.Ca,
		}

		delCertErr := trc.Security().ServerRootCACertificateDelete(serverRootCADeleteParams)
		if delCertErr != nil {
			return delCertErr
		}

		serverRootCAInstallParams := &ontapRest.ServerRootCAInstallParams{
			SvmName:         &svmName,
			Certificate:     tlsCert,
			CertificateType: nillable.ToPointer("server-ca"),
		}

		_, installCertErr := trc.Security().ServerRootCACertificateInstall(serverRootCAInstallParams)
		if installCertErr != nil {
			serverRootCAInstallParams := &ontapRest.ServerRootCAInstallParams{
				SvmName:         &svmName,
				Certificate:     oldCert,
				CertificateType: nillable.ToPointer("server-ca"),
			}
			_, oldCertInstallErr := trc.Security().ServerRootCACertificateInstall(serverRootCAInstallParams)
			if oldCertInstallErr != nil {
				logger.Errorf("old certificate installation attempt failed. oldCert: %s, err: %s", *oldCert, oldCertInstallErr.Error())
			}
			return installCertErr
		}
	}

	return nil
}

// LDAP Operations

// UpdateLDAPSigning updates a CIFS server AD LDAP signing settings
func (adu *activeDirectoryUpdater) UpdateLDAPSigning(sign bool) error {
	return modifyADLdapSigning(adu.api, sign, &adu.svmUUID)
}

func _modifyADLdapSigning(tc ontapRest.RESTClient, ldapSigning bool, svmUUID *string) error {
	modifyLdapSigningParams := &ontapRest.CifsServiceModifyParams{
		SessionSecurityForAdLdap: getSessionSecurity(ldapSigning),
		SvmUUID:                  svmUUID,
	}
	return tc.NAS().CifsServiceModify(modifyLdapSigningParams)
}

// UpdateAllowLocalNFSUsersWithLdap modifies the "-auth-sys-extended-groups"
func (adu *activeDirectoryUpdater) UpdateAllowLocalNFSUsersWithLdap() error {
	allowLocalUsers := !*adu.params.NewCredentials.AllowLocalNFSUsersWithLdap
	allowLocalNFSUsersWithLdapParams := &ontapRest.NfsModifyParams{
		SvmUUID:                    adu.svmUUID,
		AllowLocalNFSUsersWithLdap: &allowLocalUsers,
	}
	return adu.api.NAS().NfsModify(allowLocalNFSUsersWithLdapParams)
}

// UpdateLDAPOverTLS updates LDAP over TLS on CIFS server and on LDAP client
func (adu *activeDirectoryUpdater) UpdateLDAPOverTLS() error {
	certificates, err := adu.api.Security().ServerRootCACertificateCollectionGet(&ontapRest.ServerRootCAGetCollectionParams{
		SvmName:         &adu.params.NewCredentials.CIFSServers[0].SVMName,
		CertificateType: nillable.ToPointer("server-ca"),
	})
	if err != nil {
		return err
	}

	// Certificate must be installed on SVM if LDAP Over TLS is being enabled
	if *adu.params.NewCredentials.LdapOverTLS && len(certificates) == 0 {
		var cert string
		if adu.params.NewCredentials.ServerRootCaCertificate != nil {
			cert = *adu.params.NewCredentials.ServerRootCaCertificate
		} else {
			cert = *adu.params.OldCredentials.ServerRootCaCertificate
		}
		serverCACertificateInstallParams := &ontapRest.ServerRootCAInstallParams{
			Certificate:     &cert,
			CertificateType: nillable.ToPointer("server-ca"),
			SvmName:         &adu.params.NewCredentials.CIFSServers[0].SVMName,
		}
		_, err = adu.api.Security().ServerRootCACertificateInstall(serverCACertificateInstallParams)
		if err != nil {
			return err
		}
	}

	// Enable/disable "-use-start-tls-for-ad-ldap" on CIFS server
	modifyCIFSSecurityTLS := &ontapRest.CifsServiceModifyParams{
		TLSEnabled: adu.params.NewCredentials.LdapOverTLS,
		SvmUUID:    &adu.params.NewCredentials.CIFSServers[0].SVMUUID,
	}
	err = adu.api.NAS().CifsServiceModify(modifyCIFSSecurityTLS)
	if err != nil {
		return err
	}

	// Enable "-use-start-tls" on LDAP client too
	getLdapClientParams := &ontapRest.LdapGetParams{SvmUUID: adu.params.NewCredentials.CIFSServers[0].SVMUUID}
	ldapClient, err := adu.api.NameServices().LdapGet(getLdapClientParams)
	if err != nil && !errors.IsNotFoundErr(err) {
		return err
	}

	if ldapClient != nil {
		ldapClientTLSModifyParams := &ontapRest.LdapModifyParams{
			TLSEnabled: adu.params.NewCredentials.LdapOverTLS,
			SvmUUID:    adu.params.NewCredentials.CIFSServers[0].SVMUUID,
		}
		err = adu.api.NameServices().LdapModify(ldapClientTLSModifyParams)
		if err != nil {
			return err
		}
	}

	// Remove certificate from ONTAP after disabling LDAP over TLS
	if !*adu.params.NewCredentials.LdapOverTLS && len(certificates) > 0 {
		return adu.api.Security().ServerRootCACertificateDelete(&ontapRest.ServerRootCADeleteParams{UUID: certificates[0].UUID})
	}
	return nil
}

// UpdatePreferredDCOrDNAndFilter updates the preferred DC or modifies the userDN and groupDN on a ldap client
func (adu *activeDirectoryUpdater) UpdatePreferredDCOrDNAndFilter(updatePreferredServersForLdapClient, updateDNAndFilter bool) error {
	getLdapClientParams := &ontapRest.LdapGetParams{SvmUUID: adu.params.NewCredentials.CIFSServers[0].SVMUUID}
	ldapGetResp, err := adu.api.NameServices().LdapGet(getLdapClientParams)
	if err != nil && !errors.IsNotFoundErr(err) {
		return err
	}

	if ldapGetResp != nil {
		if updatePreferredServersForLdapClient && adu.params.NewCredentials.PreferredServersForLdapClient != nil {
			preferredServers := make([]*string, 0)

			// Empty "" value for preferredServers indicates the value needs to be removed on ONTAP
			if nillable.FromPointer(adu.params.NewCredentials.PreferredServersForLdapClient) != "" {
				preferredServersForLdapClient := strings.Split(strings.Replace(*adu.params.NewCredentials.PreferredServersForLdapClient, " ", "", -1), ",")
				for i := range preferredServersForLdapClient {
					preferredServers = append(preferredServers, &preferredServersForLdapClient[i])
				}
			}
			preferredADServersModifyParams := &ontapRest.LdapModifyParams{
				PreferredServersForLdapClient: preferredServers,
				SvmUUID:                       adu.params.NewCredentials.CIFSServers[0].SVMUUID,
			}

			err = adu.api.NameServices().LdapModifyPreferredAdServers(preferredADServersModifyParams)
			if err != nil {
				return err
			}
		}

		if updateDNAndFilter && (adu.params.NewCredentials.UserDN != nil || adu.params.NewCredentials.GroupDN != nil || adu.params.NewCredentials.GroupMembershipFilter != nil) {
			modifyLdapClientDNAndFilterParams := &ontapRest.LdapModifyParams{
				UserDn:                adu.params.NewCredentials.UserDN,
				GroupDn:               adu.params.NewCredentials.GroupDN,
				GroupMembershipFilter: adu.params.NewCredentials.GroupMembershipFilter,
				SvmUUID:               adu.params.NewCredentials.CIFSServers[0].SVMUUID,
			}
			err = adu.api.NameServices().LdapModify(modifyLdapClientDNAndFilterParams)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Utility Functions - User Management

func getActiveDirectoryUserMapKeys(oldADUsers, newAdUsers map[string][]string) []string {
	var keys []string
	for k := range oldADUsers {
		keys = append(keys, k)
	}
	for k := range newAdUsers {
		if !slices.Contains(keys, k) {
			keys = append(keys, k)
		}
	}
	return keys
}

func getActiveDirectoryUserChanges(currentADUsers, newADUsers []string) ([]string, []string) {
	var addUsers []string
	var removeUsers []string
	for _, user := range newADUsers {
		if !slices.Contains(currentADUsers, user) {
			addUsers = append(addUsers, user)
		}
	}
	for _, user := range currentADUsers {
		if !slices.Contains(newADUsers, user) {
			removeUsers = append(removeUsers, user)
		}
	}
	return addUsers, removeUsers
}

func _removeDomainFromUsers(users []string) []string {
	var domainUsers []string
	for _, user := range users {
		domainUsers = append(domainUsers, removeDomainFromUser(user))
	}
	return domainUsers
}

func _removeDomainFromUser(user string) string {
	splitUser := strings.Split(user, `\`)
	return splitUser[len(splitUser)-1]
}

// Utility Functions - CIFS Groups

func _getCifsGroups(nas ontapRest.NASClient, svmUUID string) (map[string]*ontapRest.CifsGroup, error) {
	ontapGroups := make(map[string]*ontapRest.CifsGroup)
	if err := nas.CifsServiceCollectionGetGroups(&ontapRest.CifsServiceCollectionGetGroupsParams{
		BaseParams: ontapRest.BaseParams{Fields: []string{"name", "members"}},
		SvmUUID:    svmUUID,
	}, func(cifsGroups []*ontapRest.CifsGroup) error {
		for _, cifsGroup := range cifsGroups {
			ontapGroups[cifsGroup.Name] = cifsGroup
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return ontapGroups, nil
}

func _removeUsersFromGroup(logger log.Logger, nas ontapRest.NASClient, svmUUID, domainWorkgroup string, group *ontapRest.CifsGroup, removedUsers []string) error {
	if err := nas.CifsServiceRemoveMembers(&ontapRest.CifsServiceModifyGroupMembersParams{
		Sid:     group.Sid,
		Members: removedUsers,
		SvmUUID: svmUUID,
	}); err != nil {
		if !strings.Contains(err.Error(), "Unable to resolve user name") {
			return err
		}

		removedUsers = removeDomainFromUsers(removedUsers)
		removedUsers = prependDomainToUsers(removedUsers, domainWorkgroup)
		if err2 := nas.CifsServiceRemoveMembers(&ontapRest.CifsServiceModifyGroupMembersParams{
			Sid:     group.Sid,
			Members: removedUsers,
			SvmUUID: svmUUID,
		}); err2 != nil {
			logger.Errorf("failed to remove a user from AD during update. err: %s, err2: %s", err.Error(), err2.Error())
			return err
		}
	}

	return nil
}

func _addUsersToGroup(logger log.Logger, nas ontapRest.NASClient, svmUUID, domainWorkgroup string, group *ontapRest.CifsGroup, newUsers []string) error {
	if err := nas.CifsServiceAddMembers(&ontapRest.CifsServiceModifyGroupMembersParams{
		Sid:     group.Sid,
		Members: newUsers,
		SvmUUID: svmUUID,
	}); err != nil {
		if !strings.Contains(err.Error(), "Unable to resolve user name") {
			return err
		}

		newUsers = removeDomainFromUsers(newUsers)
		newUsers = prependDomainToUsers(newUsers, domainWorkgroup)
		if err2 := nas.CifsServiceAddMembers(&ontapRest.CifsServiceModifyGroupMembersParams{
			Sid:     group.Sid,
			Members: newUsers,
			SvmUUID: svmUUID,
		}); err2 != nil {
			logger.Error("failed to add a user to AD during update. err: %s, err2: %s", err.Error(), err2.Error())
			return err
		}
	}

	return nil
}

// Utility Functions - Security Privileges

func _getSecurityPrivilegedUsers(nas ontapRest.NASClient, svmUUID string) ([]string, error) {
	var ontapSecurityMembers []string
	if err := nas.CifsServiceCollectionGetPrivilegedMembers(&ontapRest.CifsServiceCollectionGetPrivilegedMembersParams{
		SvmUUID: svmUUID,
	}, func(securityMembers []string) error {
		ontapSecurityMembers = append(ontapSecurityMembers, securityMembers...)
		return nil
	}); err != nil {
		return nil, err
	}

	return ontapSecurityMembers, nil
}

func _removeSecurityPrivilegesFromUsers(logger log.Logger, nas ontapRest.NASClient, svmUUID, domainWorkgroup string, users []string) error {
	for _, user := range users {
		if err := nas.CifsServiceRemoveSecurityPrivilege(&ontapRest.CifsServiceModifySecurityPrivilegeParams{
			Member:  user,
			SvmUUID: svmUUID,
		}); err != nil {
			if !strings.Contains(err.Error(), "Unable to resolve user name") {
				return err
			}
			user = removeDomainFromUser(user)
			user = prependDomainToUser(user, domainWorkgroup)
			if err2 := nas.CifsServiceRemoveSecurityPrivilege(&ontapRest.CifsServiceModifySecurityPrivilegeParams{
				Member:  user,
				SvmUUID: svmUUID,
			}); err2 != nil {
				logger.Errorf("failed to remove a user from AD during update. err: %s, err2: %s", err.Error(), err2.Error())
				return err
			}
		}
	}
	return nil
}

func _addSecurityPrivilegesToUsers(logger log.Logger, nas ontapRest.NASClient, svmUUID, domainWorkgroup string, users []string) error {
	for _, user := range users {
		if err := nas.CifsServiceAddSecurityPrivilege(&ontapRest.CifsServiceModifySecurityPrivilegeParams{
			Member:  user,
			SvmUUID: svmUUID,
		}); err != nil {
			if !strings.Contains(err.Error(), "Unable to resolve user name") {
				return err
			}
			user = removeDomainFromUser(user)
			user = prependDomainToUser(user, domainWorkgroup)
			if err2 := nas.CifsServiceAddSecurityPrivilege(&ontapRest.CifsServiceModifySecurityPrivilegeParams{
				Member:  user,
				SvmUUID: svmUUID,
			}); err2 != nil {
				logger.Errorf("failed to add a user to AD during update. err: %s, err2: %s", err.Error(), err2.Error())
				return err
			}
		}
	}

	return nil
}

// TODO: To be modified/enabled as necessary when kerberos is supported
// Kerberos/NFS Operations (Disabled)

//  // UpdateNFSService updates the NFS service including kerberos realm update and recreating machine account
//  func (adu *activeDirectoryUpdater) UpdateNFSService(updateNetBIOS, updateADName, updateKdcIP bool, fqdn, domain, username, organizationalUnit string, password log.Secret) error {
//	nfsUpdate := false
//	kerberosRealmExists := false
//	var err error
//
//	if updateNetBIOS || updateADName || updateKdcIP {
//		// NFS41-Kerberos: disable kerberos on data-lif and enable again to re-create the machine account on AD
//		nfsUpdate, err = doesKerberosExportExist(adu.api, adu.params.NewCredentials.CIFSServers[0].SVMName)
//		if err != nil {
//			return err
//		}
//	}
//
//	if updateADName || updateKdcIP {
//		kerberosRealmExists, err = doesKerberosRealmExist(adu.api, strings.ToUpper(adu.params.OldCredentials.Domain), adu.params.NewCredentials.CIFSServers[0].SVMUUID)
//		if err != nil {
//			return err
//		}
//
//		if kerberosRealmExists {
//			var kdcIP string
//			var adServerName string
//			if updateKdcIP {
//				kdcIP = adu.params.NewCredentials.KdcIP
//			} else {
//				kdcIP = adu.params.OldCredentials.KdcIP
//			}
//			adminServerIP, passwordServerIP, adServerIP := kdcIP, kdcIP, kdcIP
//			if updateADName {
//				adServerName = adu.params.NewCredentials.AdName
//			} else {
//				adServerName = adu.params.OldCredentials.AdName
//			}
//
//			kerberosRealmModifyParams := &ontapRest.KerberosRealmModifyParams{
//				Realm:            nillable.ToPointer(strings.ToUpper(adu.params.OldCredentials.Domain)),
//				KdcIP:            &kdcIP,
//				AdminServerIP:    &adminServerIP,
//				PasswordServerIP: &passwordServerIP,
//				ADServerIP:       &adServerIP,
//				ADServerName:     &adServerName,
//				SvmUUID:          adu.params.NewCredentials.CIFSServers[0].SVMUUID,
//			}
//			err = adu.api.NAS().KerberosRealmModify(kerberosRealmModifyParams)
//			if err != nil {
//				return err
//			}
//		}
//	}
//
//	if nfsUpdate {
//		// Disable kerberos interface
//		kerberosInterfaces, err := adu.api.NAS().KerberosInterfaceCollectionGet(&ontapRest.KerberosInterfaceCollectionGetParams{
//			BaseParams: ontapRest.BaseParams{Fields: []string{"*"}},
//			SvmName:    adu.params.OldCredentials.CIFSServers[0].SVMName,
//		})
//		if err != nil {
//			return err
//		}
//
//		spn := "nfs/" + fqdn + "@" + strings.ToUpper(domain)
//		machineAccount := getUniqueMachineAccount(fqdn, domain)
//
//		// Check if kerberos interface exists and kerberos is enabled
//		if len(kerberosInterfaces) > 0 && nillable.FromPointer(kerberosInterfaces[0].Enabled) {
//			// Disable interface to delete account from active directory
//			kerberosConfigModifyParams := &ontapRest.KerberosConfigModifyParams{
//				AdminPassword:     string(password),
//				AdminUsername:     &username,
//				InterfaceUUID:     nillable.FromPointer(kerberosInterfaces[0].Interface.UUID),
//				IsKerberosEnabled: nillable.ToPointer(false),
//				OU:                &organizationalUnit,
//				MachineAccount:    machineAccount,
//				Force:             nillable.ToPointer(true),
//			}
//			err = adu.api.NAS().KerberosConfigModify(kerberosConfigModifyParams)
//			if err != nil {
//				return err
//			}
//
//			// Enable again to create machine account on active directory
//			kerberosConfigModifyParams.IsKerberosEnabled = nillable.ToPointer(true)
//			kerberosConfigModifyParams.Spn = spn
//			err = adu.api.NAS().KerberosConfigModify(kerberosConfigModifyParams)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
// }
