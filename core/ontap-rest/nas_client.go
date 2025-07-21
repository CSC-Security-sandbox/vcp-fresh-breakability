package ontap_rest

import (
	nas "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/n_a_s"
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
	CifsServiceGet(params *CifsServiceGetParams) (*CifsService, error)
	CifsServiceCreate(params *CifsServiceCreateParams) error
	CifsServiceModify(params *CifsServiceModifyParams) error
}

var (
	paginateExportPolicyCollectionGet = _paginate[[]*ExportPolicy]
)

type nasClient struct {
	api    nas.ClientService
	poller Poller
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
func (t *nasClient) NfsServiceModify(params *NfsServiceModifyParams) error {
	_, err := t.api.NfsModify(nfsServiceModifyParamsToONTAP(params), nil)
	return err
}

// CifsServiceGet invokes clients/ontap-rest/client/n_a_s/Client.CifsServiceCollectionGet to get CIFS service configuration
func (t *nasClient) CifsServiceGet(params *CifsServiceGetParams) (*CifsService, error) {
	// MD: FIXME: add pagination support in case the service is slow
	response, err := t.api.CifsServiceCollectionGet(cifsServiceGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if len(response.Payload.CifsServiceResponseInlineRecords) == 0 {
		return nil, errors.NewNotFoundErr("cifs service", nil)
	}

	if len(response.Payload.CifsServiceResponseInlineRecords) > 1 {
		return nil, errors.New("unexpected response when querying for cifs service")
	}

	return &CifsService{CifsService: *response.Payload.CifsServiceResponseInlineRecords[0]}, nil
}

// CifsServiceCreate invokes clients/ontap-rest/client/n_a_s/Client.CifsServiceCreate to create CIFS service
func (t *nasClient) CifsServiceCreate(params *CifsServiceCreateParams) error {
	_, _, err := t.api.CifsServiceCreate(cifsServiceCreateParamsToONTAP(params), nil)
	return err
}

// CifsServiceModify invokes clients/ontap-rest/client/n_a_s/Client.CifsServiceModify to modify CIFS service
func (t *nasClient) CifsServiceModify(params *CifsServiceModifyParams) error {
	_, _, err := t.api.CifsServiceModify(cifsServiceModifyParamsToONTAP(params), nil)
	return err
}
