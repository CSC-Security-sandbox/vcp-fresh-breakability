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
	GcpKmsDelete(params *GcpKmsDeleteParams) error
	SecurityLogForwardingCreate(params *SecurityLogForwardingCreateParams) ([]*SecurityAuditLogForward, error)
	SecurityLogForwardingGet(params *SecurityLogForwardingGetParams) (*SecurityAuditLogForward, error)
	SecurityAuditUpdate(params *SecurityAuditUpdateParams) (*SecurityAudit, error)
	SecurityAuditGet() (*SecurityAudit, error)
	GcpKmsModify(params *GcpKmsModifyParams) (*GcpKms, *JobAccepted, error)
	EnableAutoVolOfflineCronForGCPKMS() error
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

func (sc *securityClient) SecurityLogForwardingCreate(params *SecurityLogForwardingCreateParams) ([]*SecurityAuditLogForward, error) {
	response, err := (*sc.api).SecurityLogForwardingCreate(securityLogForwardingCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from SecurityLogForwardingCreate")
	}

	resp := make([]*SecurityAuditLogForward, nillable.FromPointer(response.Payload.NumRecords))
	for i, hyperscaler := range response.Payload.SecurityAuditLogForwardResponseInlineRecords {
		resp[i] = &SecurityAuditLogForward{SecurityAuditLogForward: *hyperscaler}
	}

	return resp, err
}

func (sc *securityClient) SecurityLogForwardingGet(params *SecurityLogForwardingGetParams) (*SecurityAuditLogForward, error) {
	response, err := (*sc.api).SecurityLogForwardingGet(securityLogForwardingGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	resp := &SecurityAuditLogForward{SecurityAuditLogForward: *response.Payload}

	return resp, nil
}

func (sc *securityClient) SecurityAuditGet() (*SecurityAudit, error) {
	params := security.SecurityAuditGetParams{}
	response, err := (*sc.api).SecurityAuditGet(&params, nil)
	if err != nil {
		return nil, err
	}

	resp := &SecurityAudit{SecurityAudit: *response.Payload}

	return resp, nil
}

func (sc *securityClient) SecurityAuditUpdate(params *SecurityAuditUpdateParams) (*SecurityAudit, error) {
	response, err := (*sc.api).SecurityAuditModify(securityAuditModifyParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from SecurityAuditUpdate")
	}

	resp := &SecurityAudit{SecurityAudit: *response.Payload}

	return resp, err
}

// GcpKmsModify invokes pkg/ontap-rest/client/security/Client.GcpKmsModify
func (sc *securityClient) GcpKmsModify(params *GcpKmsModifyParams) (*GcpKms, *JobAccepted, error) {
	responseOK, _, err := (*sc.api).GcpKmsModify(gcpKmsModifyParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}
	if responseOK != nil {
		// TODO garpur 2023-03-01 Missing return object in the swagger
		return &GcpKms{}, nil, nil
	}
	return nil, nil, nil
}

// GcpKmsDelete invokes pkg/ontap-rest/client/security/Client.GcpKmsDelete
func (sc *securityClient) GcpKmsDelete(params *GcpKmsDeleteParams) error {
	response, _, err := (*sc.api).GcpKmsDelete(gcpKmsDeleteParamsToOntap(params), nil)
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("ontap-rest response for GcpKmsDelete is nil")
	}
	return nil
}

// EnableAutoVolOfflineCronForGCPKMS invokes pkg/ontap-rest/client/security/Client.KeyManagerConfigModify
func (sc *securityClient) EnableAutoVolOfflineCronForGCPKMS() error {
	response, err := (*sc.api).KeyManagerConfigModify(getGCPKeyManagerConfigModifyParamsToOntap(), nil)
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("ontap-rest response for EnableAutoVolOfflineCronForGCPKMS is nil")
	}
	if !response.IsSuccess() {
		return errors.New("ontap-rest response for EnableAutoVolOfflineCronForGCPKMS is unsuccessful")
	}
	return nil
}
