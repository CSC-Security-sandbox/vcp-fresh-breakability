package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/security"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// SecurityClient describes a security client
type SecurityClient interface { // generate:mock
	GcpKmsCreate(params *GcpKmsCreateParams) ([]*GcpKms, error)
	GcpKmsGet(params *GcpKmsGetParams) (*GcpKms, error)
}

type securityClient struct {
	api *security.ClientService
}

// GcpKmsCreate invokes pkg/ontap-rest/client/security/Client.GcpKmsCreate
func (sc *securityClient) GcpKmsCreate(params *GcpKmsCreateParams) ([]*GcpKms, error) {
	response, _, err := (*sc.api).GcpKmsCreate(gcpKmsCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from GcpKmsCreate")
	}

	resp := make([]*GcpKms, nillable.FromPointer(response.Payload.NumRecords))
	for i, gcp := range response.Payload.GcpKmsResponseInlineRecords {
		resp[i] = &GcpKms{GcpKms: *gcp}
	}
	return resp, err
}

// GcpKmsGet invokes pkg/ontap-rest/client/security/Client.GcpKmsGet
func (sc *securityClient) GcpKmsGet(params *GcpKmsGetParams) (*GcpKms, error) {
	response, err := (*sc.api).GcpKmsGet(gcpKmsGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}
	resp := &GcpKms{GcpKms: *response.Payload}
	return resp, err
}
