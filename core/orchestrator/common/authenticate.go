package common

import (
	"github.com/go-openapi/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

const (
	USERNAME_PWD         = 0 // Username/Password authentication
	USERNAME_PWD_SEC_MGR = 1 // Username/Password authentication with secret manager
	USER_CERTIFICATE     = 2 // Certificate authentication
)

var (
	AuthType = env.GetInt("VSA_AUTH_TYPE", USERNAME_PWD) // 0 for username/password, 1 for username/password in secret manager and 2 for certificate authentication

	CaName                  = env.GetString("CA_NAME", "")
	CaPoolName              = env.GetString("CA_POOL_NAME", "")
	CaPoolDeployedProjectID = env.GetString("CA_POOL_DEPLOYED_PROJECT_ID", "")
	VsaDeployedDnsName      = env.GetString("VSA_DEPLOYED_DNS_NAME", "")
)

func ValidateEnvironmentVariables() error {
	switch AuthType {
	case USERNAME_PWD_SEC_MGR:
		if CaPoolDeployedProjectID == "" {
			return errors.New(500, "CA_POOL_DEPLOYED_PROJECT_ID must be set when using username/password authentication with secret manager")
		}
	case USER_CERTIFICATE:
		if CaName == "" {
			return errors.New(500, "CA_NAME must be set when using certificate authentication")
		}
		if CaPoolName == "" {
			return errors.New(500, "CA_POOL_NAME must be set when using certificate authentication")
		}
		if CaPoolDeployedProjectID == "" {
			return errors.New(500, "CA_POOL_DEPLOYED_PROJECT_ID must be set when using certificate authentication")
		}
		if VsaDeployedDnsName == "" {
			return errors.New(500, "VSA_DEPLOYED_DNS_NAME must be set when using certificate authentication")
		}
	default:
		return nil
	}
	return nil
}
