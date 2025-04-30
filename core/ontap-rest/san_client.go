package ontap_rest

import (
	san "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/s_a_n"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// SANClient describes an SAN client
type SANClient interface { // generate:mock
	IscsiServiceGet(params *IscsiGetParams) (*Iscsi, error)
	IscsiServiceCreate(params *IscsiCreateParams) error
	IGroupCreate(params *IgroupCreateParams) (string, error)
	LunCreate(params *LunCreateParams) (*Lun, error)
	LunMapCreate(params *LunMapCreateParams) error
}

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
