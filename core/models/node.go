package models

type Node struct {
	Name            string
	EndpointAddress string
	Username        string
	Password        string
	InstanceType    string
	ExternalUUID    string
	Zone            string
	State           string
}
