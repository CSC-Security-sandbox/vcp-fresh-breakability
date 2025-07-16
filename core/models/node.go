package models

// Node represents a single Node resource
type Node struct {
	Name                           string
	EndpointAddress                string
	EndpointAddressesToHostNameMap map[string]string // for multiple host failover
	Username                       string
	Password                       string
	SecretID                       string
	CertificateID                  string
	InstanceType                   string
	ExternalUUID                   string
	Zone                           string
	State                          string
	DeploymentName                 string
	AuthType                       int
}
