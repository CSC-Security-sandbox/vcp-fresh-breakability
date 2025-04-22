package transport

import (
	"bytes"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

var (
	idempotentTransportEnabled = env.GetBool("ONTAP_REST_IDEMPOTENT_TRANSPORT_ENABLED", true)
)

// IdempotentTransport is a wrapper for ClientTransport that resolves errors when a connection disruption happens
type IdempotentTransport struct {
	transport        runtime.ClientTransport
	conflictResolver func(operation *runtime.ClientOperation) (interface{}, error)
}

// NewIdempotentTransport creates a new NewIdempotentTransport
func NewIdempotentTransport(transport runtime.ClientTransport, conflictResolver func(operation *runtime.ClientOperation) (interface{}, error)) runtime.ClientTransport {
	return &IdempotentTransport{
		transport:        transport,
		conflictResolver: conflictResolver,
	}
}

// Submit submits the transport operation
func (t *IdempotentTransport) Submit(operation *runtime.ClientOperation) (interface{}, error) {
	rsp, err := t.transport.Submit(operation)
	if err != nil {
		if idempotentTransportEnabled && errors.IsNotFoundErr(err) && operation.Method == "DELETE" && operationAllowedForIdempotentDelete(operation.ID) {
			return operation.Reader.ReadResponse(&successEmptyResponse{code: 200}, &successEmptyConsumer{})
		}

		if idempotentTransportEnabled && errors.IsConflictErr(err) && operation.Method == "POST" && errors.GetTrackingID(err) == 0 {
			result, cerr := t.conflictResolver(operation)
			if cerr != nil {
				if errors.IsNotImplementedYetErr(cerr) {
					return nil, err
				}
				return nil, cerr
			}

			if result == nil {
				// MD: returning records not required - empty response is sufficient
				return operation.Reader.ReadResponse(&successEmptyResponse{code: 201}, &successEmptyConsumer{})
			}

			return result, nil
		}

		return nil, err
	}

	return rsp, nil
}

var operationAllowedForIdempotentDelete = _operationAllowedForIdempotentDelete

func _operationAllowedForIdempotentDelete(operationID string) bool {
	return operationID != "cluster_peer_delete" &&
		operationID != "local_host_delete" &&
		operationID != "ldap_delete" &&
		operationID != "name_mapping_delete" &&
		operationID != "audit_log_redirect_delete" &&
		operationID != "kerberos_realm_delete" &&
		operationID != "azure_key_vault_delete" &&
		operationID != "security_keystore_delete" &&
		operationID != "gcp_kms_delete" &&
		operationID != "ipsec_policy_delete" &&
		operationID != "snapmirror_relationship_delete" &&
		operationID != "network_ip_interface_delete" &&
		operationID != "quota_rule_delete" &&
		operationID != "file_delete" &&
		operationID != "svm_migration_delete" &&
		operationID != "audit_delete" &&
		operationID != "security_certificate_delete"
}

type successEmptyResponse struct {
	code int
}

func (nf *successEmptyResponse) Code() int {
	return nf.code
}

func (nf *successEmptyResponse) Message() string {
	return ""
}

func (nf *successEmptyResponse) GetHeader(_ string) string {
	return ""
}

func (nf *successEmptyResponse) GetHeaders(_ string) []string {
	return []string{}
}

func (nf *successEmptyResponse) Body() io.ReadCloser {
	return io.NopCloser(nopCloser{bytes.NewBufferString("")})
}

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

type successEmptyConsumer struct{}

func (c *successEmptyConsumer) Consume(_ io.Reader, _ interface{}) error {
	return nil
}
