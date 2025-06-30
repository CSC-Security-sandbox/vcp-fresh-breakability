package common

// Unit tests for ValidateEnvironmentVariables
import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateEnvironmentVariables_UsernamePwdSecMgr(t *testing.T) {
	originalAuthtype := AuthType
	originalCaDeployedProjectID := CaPoolDeployedProjectID
	AuthType = USERNAME_PWD_SEC_MGR // Set AuthType to USERNAME_PWD_SEC_MGR for this test
	CaPoolDeployedProjectID = ""    // Reset CaPoolDeployedProjectID for this test
	defer func() {
		AuthType = originalAuthtype                           // Restore original AuthType after test
		CaPoolDeployedProjectID = originalCaDeployedProjectID // Restore original CaPoolDeployedProjectID
	}()

	err := ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_DEPLOYED_PROJECT_ID must be set when using username/password authentication with secret manager")

	CaPoolDeployedProjectID = "projectID" // Reset CaPoolDeployedProjectID for this test
	err = ValidateEnvironmentVariables()
	assert.NoError(t, err)
}

func TestValidateEnvironmentVariables_UserCertificate(t *testing.T) {
	originalAuthtype := AuthType
	originalCaDeployedProjectID := CaPoolDeployedProjectID
	originalCaPoolName := CaPoolName
	originalCaName := CaName
	originalVsaDeployedDnsName := VsaDeployedDnsName

	AuthType = USER_CERTIFICATE  // Set AuthType to USER_CERTIFICATE for this test
	CaPoolDeployedProjectID = "" // Reset CaPoolDeployedProjectID for this test
	CaName = ""                  // Reset CaName for this test
	CaPoolName = ""              // Reset CaPoolName for this test
	VsaDeployedDnsName = ""      // Reset VsaDeployedDnsName for this test

	defer func() {
		AuthType = originalAuthtype                           // Restore original AuthType after test
		CaPoolDeployedProjectID = originalCaDeployedProjectID // Restore original CaPoolDeployedProjectID
		CaPoolName = originalCaPoolName
		VsaDeployedDnsName = originalVsaDeployedDnsName
		CaName = originalCaName // Restore original CaName
	}()

	err := ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_NAME must be set when using certificate authentication")

	CaName = "ca-name"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_NAME must be set when using certificate authentication")

	CaPoolName = "ca-pool-name"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_DEPLOYED_PROJECT_ID must be set when using certificate authentication")

	CaPoolDeployedProjectID = "ca-pool-deployed-project-id"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_DEPLOYED_DNS_NAME must be set when using certificate authentication")

	VsaDeployedDnsName = "vsa-deployed-dns-name"
	err = ValidateEnvironmentVariables()
	assert.NoError(t, err)
}

func TestValidateEnvironmentVariables_Default(t *testing.T) {
	originalAuthtype := AuthType
	AuthType = USERNAME_PWD // Set AuthType to USERNAME_PWD for this test
	defer func() {
		AuthType = originalAuthtype // Restore original AuthType after test
	}()
	err := ValidateEnvironmentVariables()
	assert.NoError(t, err)
}
