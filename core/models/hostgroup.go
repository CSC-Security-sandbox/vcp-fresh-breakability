package models

type HostGroup struct {
	BaseModel
	Name          string
	Description   string
	State         string
	StateDetails  string
	OSType        string
	Hosts         []string
	HostGroupType string
	AccountName   string
}
