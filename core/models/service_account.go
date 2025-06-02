package models

type ServiceAccount struct {
	BaseModel
	Name                           string
	Description                    string
	State                          string
	StateDetails                   string
	AccountID                      int64
	ServiceName                    string
	ServiceAccountEmail            string
	ServiceAccountPasswordLocation string
}
