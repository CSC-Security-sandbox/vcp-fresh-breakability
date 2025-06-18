package hyperscaler

import (
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CustomCertificate is a struct that represents a certificate independent of Hyperscaler
type CustomCertificate struct {
	Name                       string
	PemCertificate             string
	CreateTime                 *timestamppb.Timestamp
	LifeTime                   *timestamppb.Timestamp
	UpdateTime                 *timestamppb.Timestamp
	SubjectCommonName          string
	SubjectAltName             []string
	SerialNumber               string
	PemCertificateChain        []string
	PemCsr                     string
	IssuerCertificateAuthority string
	SubjectOrganization        string
	Region                     string
	AccountId                  string
	CaName                     string
	CaGroupName                string
	CertificateId              string
}

// CustomCertificateParam is a struct that represents the parameters needed to create a certificate
type CustomCertificateParam struct {
	Region        string
	AccountId     string
	CaName        string
	CaPoolName    string
	CertificateId string
	Domains       []string
	CommonName    string
}

// CustomSecret is a struct that represents a secret
type CustomSecret struct {
	Name          string
	AccountId     string
	Region        string
	CreateTime    *timestamppb.Timestamp
	LifeTime      *timestamppb.Timestamp
	SecretVersion *CustomSecretVersion
}

// CustomSecretVersion is a struct that represents a version of a secret
type CustomSecretVersion struct {
	Name      string
	Value     string
	AccountId string
	Region    string
}
