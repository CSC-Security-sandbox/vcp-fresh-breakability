package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (rc *OntapRestProvider) UpdateSecurityAudit(params UpdateSecurityAuditParams) (*SecurityAudit, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	securityAuditResponse, err := client.Security().SecurityAuditUpdate(&ontapRest.SecurityAuditUpdateParams{
		Cli:    params.Cli,
		HTTP:   params.HTTP,
		Ontapi: params.Ontapi,
	})

	if err != nil {
		return nil, err
	}

	if securityAuditResponse != nil {
		securityAudit := securityAuditResponse.SecurityAudit
		response := &SecurityAudit{
			Cli:    nillable.GetBool(securityAudit.Cli, false),
			HTTP:   nillable.GetBool(securityAudit.HTTP, false),
			Ontapi: nillable.GetBool(securityAudit.Ontapi, false),
		}
		return response, nil
	}
	return nil, err
}

func (rc *OntapRestProvider) GetSecurityAudit() (*SecurityAudit, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	securityAudit, err := client.Security().SecurityAuditGet()

	if err != nil {
		return nil, err
	}

	if securityAudit != nil {
		response := &SecurityAudit{
			Cli:    *securityAudit.Cli,
			HTTP:   *securityAudit.HTTP,
			Ontapi: *securityAudit.Ontapi,
		}
		return response, nil
	}
	return nil, err
}
