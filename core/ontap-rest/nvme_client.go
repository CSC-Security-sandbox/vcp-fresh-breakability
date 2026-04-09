package ontap_rest

import (
	nvme "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/n_v_m_e"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// NVMeClient describes an NVMe client
type NVMeClient interface { // generate:mock
	NamespaceGet(params *NvmeNamespaceGetParams) ([]*NvmeNamespace, error)
}

type nvmeClient struct {
	api nvme.ClientService
}

// NamespaceGet invokes clients/ontap-rest/client/n_v_m_e/Client.NvmeNamespaceCollectionGet to list NVMe namespaces.
func (c *nvmeClient) NamespaceGet(params *NvmeNamespaceGetParams) ([]*NvmeNamespace, error) {
	response, err := c.api.NvmeNamespaceCollectionGet(nvmeNamespaceGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if len(response.Payload.NvmeNamespaceResponseInlineRecords) == 0 {
		return nil, errors.NewNotFoundErr("nvme namespace", nil)
	}

	namespaces := make([]*NvmeNamespace, len(response.Payload.NvmeNamespaceResponseInlineRecords))
	for i, ns := range response.Payload.NvmeNamespaceResponseInlineRecords {
		namespaces[i] = &NvmeNamespace{NvmeNamespace: *ns}
	}

	return namespaces, nil
}
