package ontap_rest

import (
	san "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/s_a_n"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// SANClient describes an SAN client
type SANClient interface { // generate:mock
	IscsiServiceGet(params *IscsiGetParams) (*Iscsi, error)
	IscsiServiceCreate(params *IscsiCreateParams) error
	IGroupCreate(params *IgroupCreateParams) (string, error)
	IGroupsGet(params *IgroupGetParams) ([]*Igroup, error)
	IGroupGet(params *IgroupGetParams) (*Igroup, error)
	LunCreate(params *LunCreateParams) (*Lun, error)
	LunMapCreate(params *LunMapCreateParams) error
}

var (
	paginateIgroupCollectionGet = _paginate[[]*Igroup]
)

type sanClient struct {
	api    san.ClientService
	poller Poller
}

func (t *sanClient) IscsiServiceGet(params *IscsiGetParams) (*Iscsi, error) {
	// MD: FIXME: add pagination support in case the service is slow
	response, err := t.api.IscsiServiceCollectionGet(iscsiServiceGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if len(response.Payload.IscsiServiceResponseInlineRecords) == 0 {
		return nil, errors.NewNotFoundErr("iscsi service", nil)
	}

	if len(response.Payload.IscsiServiceResponseInlineRecords) > 1 {
		return nil, errors.New("unexpected response when querying for iscsi service")
	}

	return &Iscsi{IscsiService: *response.Payload.IscsiServiceResponseInlineRecords[0]}, nil
}

func (t *sanClient) IscsiServiceCreate(params *IscsiCreateParams) error {
	_, err := t.api.IscsiServiceCreate(iscsiServiceCreateParamsToONTAP(params), nil)
	return err
}

// IGroupCreate invokes clients/ontap-rest/client/s_a_n/Client.IGroupCreate to create an initiator group
func (t *sanClient) IGroupCreate(params *IgroupCreateParams) (string, error) {
	response, err := t.api.IgroupCreate(igroupCreateParamsToONTAP(params), nil)
	if err != nil {
		return "", err
	}

	return *response.Payload.IgroupResponseInlineRecords[0].Name, nil
}

// LunCreate invokes clients/ontap-rest/client/s_a_n/Client.LunCreate to create a LUN
func (t *sanClient) LunCreate(params *LunCreateParams) (*Lun, error) {
	created, accepted, err := t.api.LunCreate(lunCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if accepted != nil {
		if err = t.poller.Poll(accepted.Payload.Job.UUID.String()); err != nil {
			return nil, err
		}

		return &Lun{Lun: *accepted.Payload.Records[0]}, nil
	}

	return &Lun{Lun: *created.Payload.LunResponseInlineRecords[0]}, nil
}

// LunMapCreate invokes clients/ontap-rest/client/s_a_n/Client.LunMapCreate to create a LUN mapping
func (t *sanClient) LunMapCreate(params *LunMapCreateParams) error {
	_, err := t.api.LunMapCreate(lunMapCreateParamsToONTAP(params), nil)
	return err
}

// IGroupGet invokes clients/ontap-rest/client/s_a_n/Client.IGroupCreate to create an initiator group
func (t *sanClient) IGroupGet(params *IgroupGetParams) (*Igroup, error) {
	if params.Name == nil {
		return nil, errors.New("missing required parameter 'name' when getting a specific Igroup")
	}

	response, err := t.IGroupsGet(params)
	if err != nil {
		return nil, err
	}

	if len(response) == 0 {
		return nil, errors.NewNotFoundErr("igroup", nil)
	}

	if len(response) > 1 {
		return nil, errors.New("unexpected response when querying igroup")
	}

	return response[0], nil
}

// IGroupsGet invokes clients/ontap-rest/client/s_a_n/Client.IGroupCreate to create an initiator group
func (t *sanClient) IGroupsGet(params *IgroupGetParams) ([]*Igroup, error) {
	otParams := igroupGetParamsToONTAP(params)
	var igroups []*Igroup
	if err := paginateIgroupCollectionGet(func(next string) ([]*Igroup, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := t.api.IgroupCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*Igroup, len(rsp.Payload.IgroupResponseInlineRecords))
		for i, igroup := range rsp.Payload.IgroupResponseInlineRecords {
			resp[i] = &Igroup{Igroup: *igroup}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, func(ig []*Igroup) error {
		igroups = append(igroups, ig...)
		return nil
	}); err != nil {
		return nil, err
	}

	return igroups, nil
}
