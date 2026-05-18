package models

// ExternalCredRef is a lookup-key reference to an external secret-store entry
// (currently always an OCI Vault secret on the OCI ONTAP path, but the shape
// is the persistence-layer reference type, not an OCI-only marker — see
// models.Node.ExternalSecret / ExternalCertificate). It is shared by Node
// (in-memory representation) and datamodel.PoolCredentials (persistence
// representation), and lives in the leaf models package so that core/datamodel
// can depend on core/models without creating an import cycle.
//
// ExternalIdentifier holds the secret-store-native identifier (an OCID for
// OCI Vault). Name carries the human-readable secret name, and Version is the
// numeric secret version; together they form the lookup tuple.
type ExternalCredRef struct {
	ExternalIdentifier string `json:"external_identifier"`
	Name               string `json:"name"`
	Version            int64  `json:"version"`
}
