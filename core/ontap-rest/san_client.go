package ontap_rest

import (
	san "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/s_a_n"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// SANClient describes an SAN client
type SANClient interface { // generate:mock
	IscsiServiceGet(params *IscsiGetParams) (*Iscsi, error)
	IscsiServiceCreate(params *IscsiCreateParams) error
}

type sanClient struct {
	api san.ClientService
	//poller Poller
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
