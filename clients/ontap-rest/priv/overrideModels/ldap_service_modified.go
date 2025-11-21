package overridemodels

import (
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
)

// LdapServiceModified struct for setting the preferred ad servers field as omit-empty false
// This struct is specifically used for preferredServersForLdapClient update that call LdapModify on ONTAP.
type LdapServiceModified struct {
	// ldap service inline preferred ad servers
	LdapServiceInlinePreferredAdServers []*string `json:"preferred_ad_servers"`

	// Indicates whether the validation for the specified LDAP configuration is disabled.
	SkipConfigValidation *bool `json:"skip_config_validation,omitempty"`
}

// SetClientRequestWriterForLdapPreferredAdServer sets the preferred ad servers for the ldap service with omit-empty false
func (lsm *LdapServiceModified) SetClientRequestWriterForLdapPreferredAdServer(ldapModifyParams *name_services.LdapModifyParams) (name_services.ClientOption, error) {
	return func(operation *runtime.ClientOperation) {
		operation.Params = runtime.ClientRequestWriterFunc(func(req runtime.ClientRequest, _ strfmt.Registry) error {
			lsm.LdapServiceInlinePreferredAdServers = ldapModifyParams.Info.LdapServiceInlinePreferredAdServers
			lsm.SkipConfigValidation = ldapModifyParams.Info.SkipConfigValidation
			err := req.SetBodyParam(lsm)
			if err != nil {
				return err
			}
			err = req.SetPathParam("svm.uuid", ldapModifyParams.SvmUUID)
			return err
		})
	}, nil
}
