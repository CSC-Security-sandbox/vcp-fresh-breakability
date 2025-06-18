package models

// Node represents a single Node resource
type Node struct {
	Name            string
	EndpointAddress string
	Username        string
	Password        string
	SecretID        string
	InstanceType    string
	ExternalUUID    string
	Zone            string
	State           string
}
