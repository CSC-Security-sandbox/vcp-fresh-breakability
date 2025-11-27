package ontap_rest

import (
	"context"
	"strings"

	nas "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/n_a_s"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	priv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// NASClient describes a NAS client
type NASClient interface { // generate:mock
	ExportPolicyCreate(params *ExportPolicyCreateParams) (string, error)
	ExportPolicyGet(params *ExportPolicyGetParams) (*ExportPolicy, error)
	ExportPoliciesGet(params *ExportPolicyGetParams) ([]*ExportPolicy, error)
	ExportPolicyModify(params *ExportPolicyModifyParams) error
	ExportPolicyDelete(params *ExportPolicyDeleteParams) error
	NfsServiceGet(params *NfsServiceGetParams) (*NfsService, error)
	NfsServiceCreate(params *NfsServiceCreateParams) error
	NfsServiceModify(params *NfsServiceModifyParams) error
	NfsParamsModify(ctx context.Context, params *NfsModifyParams) error
	CifsServiceGet(params *CifsServiceGetParams) (*CifsService, error)
	CifsServiceList(params *CifsServiceGetParams) ([]*CifsService, error)
	CifsServiceCreate(params *CifsServiceCreateParams) (bool, *JobAccepted, error)
	CifsServiceModify(params *CifsServiceModifyParams) error
	CifsDomainModify(params *CifsDomainModifyParams) error
	CifsShareACLDelete(params *CifsShareACLDeleteParams) error
	CifsServiceAddMembers(params *CifsServiceModifyGroupMembersParams) error
	CifsServiceDelete(params *CifsServiceDeleteParams) error
	CifsServiceAddSecurityPrivilege(params *CifsServiceModifySecurityPrivilegeParams) error
	CifsShareCreate(params *CifsShareCreateParams) error
	CifsShareModify(params *CifsShareModifyParams) error
	CifsShareCollectionGet(params *CifsShareCollectionGetParams) (*CifsShareGetResponse, error)
	DomainControllersSrvLookupGet(params *SrvLookupParams) ([]string, error)
	CifsDomainPreferredDCDelete(params *CifsDomainPreferredDCDeleteParams) error
	CifsDomainPreferredDCCreate(params *CifsDomainPreferredDCCreateParams) error
	CifsServiceCollectionGetGroups(params *CifsServiceCollectionGetGroupsParams, ucbf UserCallbackFunc[[]*CifsGroup]) error
	CifsServiceRemoveMembers(params *CifsServiceModifyGroupMembersParams) error
	CifsServiceCollectionGetPrivilegedMembers(params *CifsServiceCollectionGetPrivilegedMembersParams, ucbf UserCallbackFunc[[]string]) error
	CifsServiceRemoveSecurityPrivilege(params *CifsServiceModifySecurityPrivilegeParams) error
	NfsModify(params *NfsModifyParams) error
}

var (
	paginateExportPolicyCollectionGet = _paginate[[]*ExportPolicy]
	cifsUserSeSecurityPrivilege       = nillable.ToPointer(utils.ActiveDirectorySeSecurityPrivilege)
	convertCifsShareFromREST          = _convertCifsShareFromREST
)

type nasClient struct {
	api     nas.ClientService
	apiPriv *priv.ClientService
	poller  Poller
}

// ExportPolicyCreate invokes clients/ontap-rest/client/n_a_s/Client.ExportPolicyCreate to create an export policy
func (t *nasClient) ExportPolicyCreate(params *ExportPolicyCreateParams) (string, error) {
	response, err := t.api.ExportPolicyCreate(exportPolicyCreateParamsToONTAP(params), nil)
	if err != nil {
		return "", err
	}

	// Extract the policy name from the response
	if response.Payload != nil && response.Payload.ExportPolicyResponseInlineRecords != nil &&
		len(response.Payload.ExportPolicyResponseInlineRecords) > 0 &&
		response.Payload.ExportPolicyResponseInlineRecords[0].Name != nil {
		return *response.Payload.ExportPolicyResponseInlineRecords[0].Name, nil
	}

	return "", errors.New("failed to get export policy name from response")
}

// ExportPolicyGet invokes clients/ontap-rest/client/n_a_s/Client.ExportPolicyGet to get a specific export policy
func (t *nasClient) ExportPolicyGet(params *ExportPolicyGetParams) (*ExportPolicy, error) {
	if params.Name == nil {
		return nil, errors.New("missing required parameter 'name' when getting a specific export policy")
	}

	response, err := t.ExportPoliciesGet(params)
	if err != nil {
		return nil, err
	}

	if len(response) == 0 {
		return nil, errors.NewNotFoundErr("export policy", nil)
	}

	if len(response) > 1 {
		return nil, errors.New("unexpected response when querying export policy")
	}

	return response[0], nil
}

// ExportPoliciesGet invokes clients/ontap-rest/client/n_a_s/Client.ExportPolicyCollectionGet to get export policies
func (t *nasClient) ExportPoliciesGet(params *ExportPolicyGetParams) ([]*ExportPolicy, error) {
	otParams := exportPolicyGetParamsToONTAP(params)
	var exportPolicies []*ExportPolicy
	if err := paginateExportPolicyCollectionGet(func(next string) ([]*ExportPolicy, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := t.api.ExportPolicyCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*ExportPolicy, len(rsp.Payload.ExportPolicyResponseInlineRecords))
		for i, policy := range rsp.Payload.ExportPolicyResponseInlineRecords {
			resp[i] = &ExportPolicy{ExportPolicy: *policy}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, func(ep []*ExportPolicy) error {
		exportPolicies = append(exportPolicies, ep...)
		return nil
	}); err != nil {
		return nil, err
	}

	return exportPolicies, nil
}

// ExportPolicyModify invokes clients/ontap-rest/client/n_a_s/Client.ExportPolicyModify to modify an export policy
func (t *nasClient) ExportPolicyModify(params *ExportPolicyModifyParams) error {
	_, err := t.api.ExportPolicyModify(exportPolicyModifyParamsToONTAP(params), nil)
	return err
}

// ExportPolicyDelete invokes clients/ontap-rest/client/n_a_s/Client.ExportPolicyDeleteCollection to delete an export policy
func (t *nasClient) ExportPolicyDelete(params *ExportPolicyDeleteParams) error {
	_, err := t.api.ExportPolicyDeleteCollection(exportPolicyDeleteParamsToONTAP(params), nil)
	return err
}

// NfsServiceGet invokes clients/ontap-rest/client/n_a_s/Client.NfsGet to get NFS service configuration
func (t *nasClient) NfsServiceGet(params *NfsServiceGetParams) (*NfsService, error) {
	response, err := t.api.NfsGet(nfsServiceGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	return &NfsService{NfsService: *response.Payload}, nil
}

// NfsServiceCreate invokes clients/ontap-rest/client/n_a_s/Client.NfsCreate to create NFS service
func (t *nasClient) NfsServiceCreate(params *NfsServiceCreateParams) error {
	_, err := t.api.NfsCreate(nfsServiceCreateParamsToONTAP(params), nil)
	return err
}

// NfsServiceModify invokes clients/ontap-rest/client/n_a_s/Client.NfsModify to modify NFS service
// This is the original method for modifying NFS service with basic parameters (without context).
// For quota rule operations that require context, use NfsModify instead.
func (t *nasClient) NfsServiceModify(params *NfsServiceModifyParams) error {
	_, err := t.api.NfsModify(nfsServiceModifyParamsToONTAP(params), nil)
	return err
}

// NfsParamsModify invokes clients/ontap-rest/client/n_a_s/Client.NfsModify to modify NFS service
// This method includes context support and is used for quota rule operations (e.g., RquotaEnabled).
func (t *nasClient) NfsParamsModify(ctx context.Context, params *NfsModifyParams) error {
	_, err := t.api.NfsModify(nfsParamsModifyToONTAP(ctx, params), nil)
	return err
}

// CifsServiceGet invokes clients/ontap-rest/client/n_a_s/Client.CifsServiceCollectionGet to get CIFS service configuration
func (t *nasClient) CifsServiceGet(params *CifsServiceGetParams) (*CifsService, error) {
	response, err := t.api.CifsServiceCollectionGet(cifsServiceGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if len(response.Payload.CifsServiceResponseInlineRecords) == 0 {
		return nil, errors.NewNotFoundErr("cifs service", nil)
	}

	return &CifsService{CifsService: *response.Payload.CifsServiceResponseInlineRecords[0]}, nil
}

// CifsServiceList invokes clients/ontap-rest/client/n_a_s/Client.CifsServiceCollectionGet to get CIFS service configuration
func (t *nasClient) CifsServiceList(params *CifsServiceGetParams) ([]*CifsService, error) {
	response, err := t.api.CifsServiceCollectionGet(cifsServiceGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	resp := make([]*CifsService, nillable.FromPointer(response.Payload.NumRecords))
	for i, c := range response.Payload.CifsServiceResponseInlineRecords {
		resp[i] = &CifsService{*c}
	}
	return resp, nil
}

// CifsServiceCreate creates the cifs service for the specified svm
func (tnc *nasClient) CifsServiceCreate(params *CifsServiceCreateParams) (bool, *JobAccepted, error) {
	done, response, err := tnc.api.CifsServiceCreate(cifsServiceCreateParamsToONTAP(params), nil)
	if err != nil {
		return false, nil, err
	}

	if done != nil {
		return true, nil, nil
	}

	return false, &JobAccepted{JobUUID: string(nillable.FromPointer(response.Payload.Job.UUID))}, nil
}

// CifsServiceModify invokes clients/ontap-rest/client/n_a_s/Client.CifsServiceModify to modify CIFS service
func (tnc *nasClient) CifsServiceModify(params *CifsServiceModifyParams) error {
	_, _, err := tnc.api.CifsServiceModify(cifsServiceModifyParamsToONTAP(params), nil)
	return err
}

// CifsShareACLDelete deletes the specified ONTAP API CIFS share
func (tnc *nasClient) CifsShareACLDelete(params *CifsShareACLDeleteParams) error {
	_, err := tnc.api.CifsShareACLDelete(cifsShareACLDeleteParamsToONTAP(params), nil)
	return err
}

// CifsServiceAddMembers adds new CIFS users to groups
func (tnc *nasClient) CifsServiceAddMembers(params *CifsServiceModifyGroupMembersParams) error {
	lcgp := make([]*models.LocalCifsGroupMembersInlineRecordsInlineArrayItem, len(params.Members))
	for i, member := range params.Members {
		lcgp[i] = &models.LocalCifsGroupMembersInlineRecordsInlineArrayItem{Name: nillable.ToPointer(member)}
	}

	_, err := tnc.api.LocalCifsGroupMembersCreate(nas.NewLocalCifsGroupMembersCreateParams().WithSvmUUID(params.SvmUUID).WithLocalCifsGroupSid(params.Sid).WithInfo(
		&models.LocalCifsGroupMembers{
			LocalCifsGroupMembersInlineRecords: lcgp,
		}), nil)
	return err
}

// CifsServiceDelete deletes the cifs service for the specified svm
func (tnc *nasClient) CifsServiceDelete(params *CifsServiceDeleteParams) error {
	_, _, err := tnc.api.CifsServiceDelete(cifsServiceDeleteParamsToONTAP(params), nil)
	return err
}

// CifsServiceAddSecurityPrivilege adds a security privilege to a CIFS user
func (tnc *nasClient) CifsServiceAddSecurityPrivilege(params *CifsServiceModifySecurityPrivilegeParams) error {
	_, err := tnc.api.UserGroupPrivilegesCreate(nas.NewUserGroupPrivilegesCreateParams().WithInfo(&models.UserGroupPrivileges{
		Name:       &params.Member,
		Privileges: []*string{cifsUserSeSecurityPrivilege},
		Svm: &models.UserGroupPrivilegesInlineSvm{
			UUID: &params.SvmUUID,
		},
	}), nil)
	return err
}

// CifsDomainModify invokes pkg/ontap-rest/client/nas/Client.CifsDomainModify
func (tnc *nasClient) CifsDomainModify(params *CifsDomainModifyParams) error {
	_, err := tnc.api.CifsDomainModify(cifsDomainModifyParamsToONTAP(params), nil)
	return err
}

// CifsShareCreate creates a CIFS share for the ONTAP API SVM
func (tnc *nasClient) CifsShareCreate(params *CifsShareCreateParams) error {
	_, err := tnc.api.CifsShareCreate(cifsShareCreateParamsToONTAP(params), nil)
	if err != nil {
		return err
	}
	return nil
}

// CifsShareCollectionGet retrieves a CIFS share
func (tnc *nasClient) CifsShareCollectionGet(params *CifsShareCollectionGetParams) (*CifsShareGetResponse, error) {
	response, err := tnc.api.CifsShareCollectionGet(cifsShareCollectionGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}
	if (response.Payload != nil) && len(response.Payload.CifsShareResponseInlineRecords) < 1 {
		return nil, errors.NewNotFoundErr("Share", &params.ShareName)
	}
	return convertCifsShareFromREST(response.Payload.CifsShareResponseInlineRecords[0]), nil
}

func _convertCifsShareFromREST(resp *models.CifsShare) *CifsShareGetResponse {
	var shareProperties []string
	cs := &CifsShareGetResponse{}

	if resp.Browsable != nil && *resp.Browsable {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyBrowsable)
	}
	if resp.ContinuouslyAvailable != nil && *resp.ContinuouslyAvailable {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyCA)
	}
	if resp.ChangeNotify != nil && *resp.ChangeNotify {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyChangenotify)
	}
	if resp.Oplocks != nil && *resp.Oplocks {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyOplocks)
	}
	if resp.Encryption != nil && *resp.Encryption {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyEncryptData)
	}
	if resp.ShowPreviousVersions != nil && *resp.ShowPreviousVersions {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyShowPreviousVersions)
	}
	if resp.ShowSnapshot != nil && *resp.ShowSnapshot {
		shareProperties = append(shareProperties, utils.CIFSSharePropertyShowsnapshot)
	}
	if resp.AccessBasedEnumeration != nil && *resp.AccessBasedEnumeration {
		shareProperties = append(shareProperties, utils.CIFSAccessBasedEnumeration)
	}
	cs.ShareProperties = shareProperties

	return cs
}

// CifsShareModify Modifies a CIFS share for the ONTAP API SVM
func (nc *nasClient) CifsShareModify(params *CifsShareModifyParams) error {
	_, err := nc.api.CifsShareModify(cifsShareModifyParamsToONTAP(params), nil)
	return err
}

// DomainControllersSrvLookupGet invokes pkg/ontap-rest/diag/secd/dns/srv-lookup
func (nc *nasClient) DomainControllersSrvLookupGet(params *SrvLookupParams) ([]string, error) {
	response, err := (*nc.apiPriv).SrvLookup(srvLookupParamsToONTAP(params))
	if err != nil {
		return nil, err
	}
	// Sample output: "Got 2 Ip Addresses\n10.193.224.112\n10.193.215.176\n"
	cliOutput := nillable.FromPointer(&response.Payload.CliOutput)
	var domainControllerIPList []string
	if cliOutput != "" {
		domainControllerIPList = strings.Split(cliOutput, "\n")
		domainControllerIPList = domainControllerIPList[1 : len(domainControllerIPList)-1]
	}
	return domainControllerIPList, err
}

func srvLookupParamsToONTAP(params *SrvLookupParams) *priv.SrvLookupParams {
	otParams := priv.NewSrvLookupParams()
	if params == nil {
		return otParams
	}

	srvLookupRequestBody := &privmodels.SrvLookup{
		LookupString: params.LookupString,
		LookupType:   params.LookupType,
		Vserver:      params.SVMName,
	}

	otParams.SetBody(srvLookupRequestBody)
	return otParams
}

// CifsDomainPreferredDCDelete invokes pkg/ontap-rest/client/nas/Client.CifsDomainPreferredDcDelete
func (nc *nasClient) CifsDomainPreferredDCDelete(params *CifsDomainPreferredDCDeleteParams) error {
	_, err := nc.api.CifsDomainPreferredDcDelete(cifsDomainPreferredDCDeleteParamsToONTAP(params), nil)
	if err != nil {
		return err
	}
	return nil
}

func cifsDomainPreferredDCDeleteParamsToONTAP(params *CifsDomainPreferredDCDeleteParams) *nas.CifsDomainPreferredDcDeleteParams {
	otParams := nas.NewCifsDomainPreferredDcDeleteParams()
	if params == nil {
		return otParams
	}

	otParams.SetSvmUUID(params.SvmUUID)
	otParams.SetServerIP(nillable.FromPointer(params.ServerIP))
	otParams.SetFqdn(nillable.FromPointer(params.Fqdn))
	return otParams
}

// CifsDomainPreferredDCCreate invokes pkg/ontap-rest/client/nas/Client.CifsDomainPreferredDcCreate
func (nc *nasClient) CifsDomainPreferredDCCreate(params *CifsDomainPreferredDCCreateParams) error {
	_, err := nc.api.CifsDomainPreferredDcCreate(cifsDomainPreferredDCCreateParamsToONTAP(params), nil)
	if err != nil {
		return err
	}

	return nil
}

func cifsDomainPreferredDCCreateParamsToONTAP(params *CifsDomainPreferredDCCreateParams) *nas.CifsDomainPreferredDcCreateParams {
	otParams := nas.NewCifsDomainPreferredDcCreateParams()
	if params == nil {
		return otParams
	}
	info := models.CifsDomainPreferredDc{
		Fqdn:     nillable.FromPointer(params.CifsDomainPreferredDC).Fqdn,
		ServerIP: nillable.FromPointer(params.CifsDomainPreferredDC).ServerIP,
	}

	otParams.SetReturnRecords(nillable.ToStringPtr(params.ReturnRecords))
	otParams.SetSvmUUID(params.SvmUUID)
	otParams.SkipConfigValidation = nillable.ToStringPtr(params.SkipConfigValidation)
	otParams.SetInfo(&info)
	return otParams
}

var paginateCifsServiceCollectionGetGroups = _paginate[[]*CifsGroup]

// CifsServiceCollectionGetGroups paginates all CIFS groups (for this svm)
func (nc *nasClient) CifsServiceCollectionGetGroups(params *CifsServiceCollectionGetGroupsParams, ucbf UserCallbackFunc[[]*CifsGroup]) error {
	otParams := nas.NewLocalCifsGroupCollectionGetParams().WithFields(params.Fields).WithSvmUUID(&params.SvmUUID).WithSid(params.Sid).WithMaxRecords(getConstrainedMaxRecords(params.MaxRecords))

	return paginateCifsServiceCollectionGetGroups(func(next string) ([]*CifsGroup, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := nc.api.LocalCifsGroupCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*CifsGroup, len(rsp.Payload.LocalCifsGroupResponseInlineRecords))
		for i, group := range rsp.Payload.LocalCifsGroupResponseInlineRecords {
			resp[i] = &CifsGroup{Name: *group.Name, Sid: *group.Sid, Members: make([]string, len(group.LocalCifsGroupInlineMembers))}
			for j, member := range group.LocalCifsGroupInlineMembers {
				resp[i].Members[j] = strings.Split(*member.Name, `\`)[1]
			}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// CifsServiceRemoveMembers removes CIFS users from a group
func (nc *nasClient) CifsServiceRemoveMembers(params *CifsServiceModifyGroupMembersParams) error {
	lcgp := make([]*models.LocalCifsGroupMembersInlineRecordsInlineArrayItem, len(params.Members))
	for i, member := range params.Members {
		lcgp[i] = &models.LocalCifsGroupMembersInlineRecordsInlineArrayItem{Name: nillable.ToPointer(member)}
	}

	_, err := nc.api.LocalCifsGroupMembersBulkDelete(nas.NewLocalCifsGroupMembersBulkDeleteParams().WithSvmUUID(params.SvmUUID).WithLocalCifsGroupSid(params.Sid).WithInfo(
		&models.LocalCifsGroupMembers{
			LocalCifsGroupMembersInlineRecords: lcgp,
		}), nil)
	return err
}

var (
	paginateCifsServiceCollectionGetMembers = _paginate[[]string]
)

// CifsServiceCollectionGetPrivilegedMembers fetches all privileged CIFS users
func (nc *nasClient) CifsServiceCollectionGetPrivilegedMembers(params *CifsServiceCollectionGetPrivilegedMembersParams, ucbf UserCallbackFunc[[]string]) error {
	otParams := nas.NewUserGroupPrivilegesCollectionGetParams().WithFields(params.Fields).WithPrivileges(cifsUserSeSecurityPrivilege).WithMaxRecords(getConstrainedMaxRecords(params.MaxRecords))

	return paginateCifsServiceCollectionGetMembers(func(next string) ([]string, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := nc.api.UserGroupPrivilegesCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]string, len(rsp.Payload.UserGroupPrivilegesResponseInlineRecords))
		for i, member := range rsp.Payload.UserGroupPrivilegesResponseInlineRecords {
			splitUser := strings.Split(*member.Name, `\`)
			// MD: we have not inserted properly, if this statement is false
			if len(splitUser) == 2 {
				resp[i] = splitUser[1]
			}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// CifsServiceRemoveSecurityPrivilege removes privileges from a CIFS user
func (tnc *nasClient) CifsServiceRemoveSecurityPrivilege(params *CifsServiceModifySecurityPrivilegeParams) error {
	_, err := tnc.api.UserGroupPrivilegesModify(nas.NewUserGroupPrivilegesModifyParams().WithSvmUUID(params.SvmUUID).WithName(params.Member).WithInfo(&models.UserGroupPrivileges{
		Privileges: []*string{},
	}), nil)
	return err
}

// NfsModify invokes pkg/ontap-rest/client/nas/Client.NfsModify
func (nc *nasClient) NfsModify(params *NfsModifyParams) error {
	_, err := nc.api.NfsModify(nfsModifyParamsToONTAP(params), nil)
	return err
}

func nfsModifyParamsToONTAP(params *NfsModifyParams) *nas.NfsModifyParams {
	otParams := nas.NewNfsModifyParams()
	if params == nil {
		return otParams
	}
	info := &models.NfsService{
		ShowmountEnabled: params.ShowmountEnabled,
		Protocol:         &models.NfsServiceInlineProtocol{},
		Enabled:          params.Enabled,
	}
	if params.V4IDDomain != nil {
		info.Protocol.V4IDDomain = params.V4IDDomain
	}
	if params.AllowLocalNFSUsersWithLdap != nil {
		info.AuthSysExtendedGroupsEnabled = params.AllowLocalNFSUsersWithLdap
	}
	if params.ExtendedGroupsLimit != nil {
		info.ExtendedGroupsLimit = params.ExtendedGroupsLimit
	}
	if params.V3Enabled != nil {
		info.Protocol.V3Enabled = params.V3Enabled
	}
	if params.V40Enabled != nil {
		info.Protocol.V40Enabled = params.V40Enabled
	}
	if params.V41Enabled != nil {
		info.Protocol.V41Enabled = params.V41Enabled
	}
	if params.RquotaEnabled != nil {
		info.RquotaEnabled = params.RquotaEnabled
	}
	if params.VstorageEnabled != nil {
		info.VstorageEnabled = params.VstorageEnabled
	}
	if params.FileSessionIoGroupingCount != nil {
		info.FileSessionIoGroupingCount = params.FileSessionIoGroupingCount
	}

	otParams.SetInfo(info)
	otParams.SetSvmUUID(params.SvmUUID)

	return otParams
}
