package ontap_rest

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	nvme "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/n_v_m_e"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
)

func TestNamespaceGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		nvmeAPI := nvme.New(transport, nil)
		client := &nvmeClient{api: nvmeAPI}
		result, err := client.NamespaceGet(&NvmeNamespaceGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, result)
	})

	t.Run("WhenNoRecordsReturned_ThenReturnNotFoundError", func(tt *testing.T) {
		transport := &mockTransport{response: &nvme.NvmeNamespaceCollectionGetOK{
			Payload: &models.NvmeNamespaceResponse{
				NvmeNamespaceResponseInlineRecords: []*models.NvmeNamespace{},
			},
		}}
		nvmeAPI := nvme.New(transport, nil)
		client := &nvmeClient{api: nvmeAPI}
		result, err := client.NamespaceGet(&NvmeNamespaceGetParams{})
		assert.EqualError(tt, err, "nvme namespace not found")
		assert.Nil(tt, result)
	})

	t.Run("WhenOneRecordReturned_ThenReturnSingleNamespace", func(tt *testing.T) {
		ns := &models.NvmeNamespace{}
		transport := &mockTransport{response: &nvme.NvmeNamespaceCollectionGetOK{
			Payload: &models.NvmeNamespaceResponse{
				NvmeNamespaceResponseInlineRecords: []*models.NvmeNamespace{ns},
			},
		}}
		nvmeAPI := nvme.New(transport, nil)
		client := &nvmeClient{api: nvmeAPI}
		result, err := client.NamespaceGet(&NvmeNamespaceGetParams{})
		assert.NoError(tt, err)
		assert.Len(tt, result, 1)
		assert.Equal(tt, ns, &result[0].NvmeNamespace)
	})

	t.Run("WhenMultipleRecordsReturned_ThenReturnAllNamespaces", func(tt *testing.T) {
		ns1 := &models.NvmeNamespace{}
		ns2 := &models.NvmeNamespace{}
		transport := &mockTransport{response: &nvme.NvmeNamespaceCollectionGetOK{
			Payload: &models.NvmeNamespaceResponse{
				NvmeNamespaceResponseInlineRecords: []*models.NvmeNamespace{ns1, ns2},
			},
		}}
		nvmeAPI := nvme.New(transport, nil)
		client := &nvmeClient{api: nvmeAPI}
		result, err := client.NamespaceGet(&NvmeNamespaceGetParams{})
		assert.NoError(tt, err)
		assert.Len(tt, result, 2)
		assert.Equal(tt, ns1, &result[0].NvmeNamespace)
		assert.Equal(tt, ns2, &result[1].NvmeNamespace)
	})
}
