package vsa

import "github.com/go-openapi/strfmt"

type CreateKmsConfigParams struct {
	KeyName           string
	KeyRingLocation   string
	KeyRingName       string
	ProjectID         string
	Credentials       *strfmt.Password
	SvmName           string
	PrivilegedAccount string // Cloud KMS account to impersonate.
}

type DeleteKmsConfigParams struct {
	ExternalKmsConfigID string
}

type CreateKmsConfigResponse struct {
	ProviderResponse
}

type DeleteKmsConfigResponse struct {
	ProviderResponse
}

type GetKmsConfigParams struct {
	ExternalKmsConfigID string
}
