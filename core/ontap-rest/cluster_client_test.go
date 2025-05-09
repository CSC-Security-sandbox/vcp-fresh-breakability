package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	securitypriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

type mockTransport struct {
	response interface{}
	err      error
}

func (mock *mockTransport) Submit(*runtime.ClientOperation) (interface{}, error) {
	return mock.response, mock.err
}

func TestClusterPeerList(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		clust := cluster.New(transport, nil)
		client := &clusterClient{api: clust}
		response, err := client.ClusterPeersList()
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		links := models.ClusterPeerResponseInlineLinks{
			Self: nil,
		}
		name := "name"
		transport := &mockTransport{response: &cluster.ClusterPeerCollectionGetOK{
			Payload: &models.ClusterPeerResponse{
				Links: &links,
				ClusterPeerResponseInlineRecords: []*models.ClusterPeer{
					{
						Authentication: &models.ClusterPeerInlineAuthentication{State: &name},
						Name:           nil,
						Remote:         &models.ClusterPeerInlineRemote{Name: &name},
						Status:         &models.ClusterPeerInlineStatus{State: &name},
						UUID:           &name,
					},
				},
				NumRecords: nillable.ToPointer(int64(1)),
			},
		}}
		clust := cluster.New(transport, nil)
		client := &clusterClient{api: clust}
		response, err := client.ClusterPeersList()
		assert.NoError(tt, err)
		assert.NotEmpty(tt, response)
	})
}

func TestClusterPeerDelete(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		clust := cluster.New(transport, nil)
		client := &clusterClient{api: clust}
		err := client.ClusterPeerDelete("someUUID")
		assert.EqualError(tt, err, transport.err.Error())
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &cluster.ClusterPeerDeleteOK{}}
		clust := cluster.New(transport, nil)
		client := &clusterClient{api: clust}
		err := client.ClusterPeerDelete("someUUID")
		assert.NoError(tt, err)
	})
}

func TestClusterPeerCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		clust := securitypriv.New(transport, nil)
		client := &clusterClient{apiPriv: clust}
		response, err := client.ClusterPeerCreate(ClusterPeerCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		links := privmodels.ClusterPeerSetupResponseInlineLinks{
			Self: nil,
		}
		passphrase := "test"
		ipAddresses := []string{"1.2.3.4"}
		transport := &mockTransport{response: &securitypriv.ClusterPeerCreateCreated{
			Payload: &privmodels.ClusterPeerSetupResponse{
				NumRecords: nillable.ToPointer(int64(1)),
				ClusterPeerResponseInlineRecords: []*privmodels.ClusterPeerSetupRecord{
					{
						Links: &links,
						Authentication: &privmodels.ClusterPeerSetupResponseInlineAuthentication{
							ExpiryTime: nil,
							Passphrase: &passphrase,
						},
						IPAddress: nil,
						Name:      nil,
					},
				},
			},
		}}
		clust := securitypriv.New(transport, nil)
		client := &clusterClient{apiPriv: clust}
		response, err := client.ClusterPeerCreate(ClusterPeerCreateParams{
			Name:               "cluster",
			IPAddresses:        ipAddresses,
			GeneratePassphrase: true,
		})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
	})
}

func TestClusterPeerGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		clust := cluster.New(transport, nil)
		client := &clusterClient{api: clust}
		response, err := client.ClusterPeerGet("test")
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		name := "name"
		transport := &mockTransport{response: &cluster.ClusterPeerGetOK{
			Payload: &models.ClusterPeer{
				Authentication: &models.ClusterPeerInlineAuthentication{State: &name},
				Name:           nil,
				Remote:         &models.ClusterPeerInlineRemote{Name: &name},
				Status:         &models.ClusterPeerInlineStatus{State: &name},
				UUID:           &name,
			},
		}}
		clust := cluster.New(transport, nil)
		client := &clusterClient{api: clust}
		response, err := client.ClusterPeerGet("test")
		assert.NoError(tt, err)
		assert.NotEmpty(tt, response)
	})
}
