package models

const (
	USERNAME_PWD         = 0
	USERNAME_PWD_SEC_MGR = 1
	USER_CERTIFICATE     = 2
)

type ContextKey string

const (
	AuthDataKey    ContextKey = "authDataKey"
	RuleContextKey ContextKey = "ruleContext"
)

type Certificate struct {
	SignedCertificate        string   `json:"signed_certificate"`
	PrivateKey               string   `json:"private_key"`
	InterMediateCertificates []string `json:"intermediate_certificate"`
	CommonName               string   `json:"common_name"`
	RootCaCertificate        string   `json:"root_ca_certificate"`
}

type AuthData struct {
	AuthType       int
	SecretID       string
	CertificateID  string
	Password       string
	Username       string
	PoolID         string
	AccountName    string
	UserName       string
	Certificate    *Certificate
	OntapEndpoints []OntapEndpoint
	// Format: ca_pool_deployed_project_id/ca_pool_name/ca_name
	CaURI string
}

type OntapEndpoint struct {
	IP  string `json:"ip"`
	DNS string `json:"dns"`
}

type PoolDetails struct {
	ProjectNumber string
	PoolID        string
	AccountName   string
	UserName      string
}
