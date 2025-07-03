package hyperscaler

import (
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CustomCertificate is a struct that represents a certificate independent of Hyperscaler
type CustomCertificate struct {
	Name                       string
	PemCertificate             string
	CreateTime                 *timestamppb.Timestamp
	LifeTime                   string
	UpdateTime                 *timestamppb.Timestamp
	SubjectCommonName          string
	SubjectAltName             []string
	SerialNumber               string
	PemCertificateChain        []string
	PemCsr                     string
	IssuerCertificateAuthority string
	SubjectOrganization        string
	Region                     string
	CertOwningEntity           string
	CaName                     string
	CaGroupName                string
	CertificateID              string
}

// CustomCertificateParam is a struct that represents the parameters needed to create a certificate
type CustomCertificateParam struct {
	Region           string
	CertOwningEntity string
	CaName           string
	CaPoolName       string
	CertificateID    string
	Domains          []string
	CommonName       string
}

// CustomSecret is a struct that represents a secret
type CustomSecret struct {
	Name               string
	SecretOwningEntity string
	Region             string
	CreateTime         *timestamppb.Timestamp
	LifeTime           *timestamppb.Timestamp
	SecretVersion      *CustomSecretVersion
}

// CustomSecretVersion is a struct that represents a version of a secret
type CustomSecretVersion struct {
	Name               string
	Value              string
	SecretOwningEntity string
	Region             string
}

type CustomCloudDNSRecord struct {
	RecordName  string
	Type        string
	TTL         int64
	Data        string // IP address
	ManagedZone string
}
