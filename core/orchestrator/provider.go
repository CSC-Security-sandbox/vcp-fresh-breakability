package orchestrator

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"golang.org/x/net/context"
)

func getProviderByNode(ctx context.Context, node *models.Node) vsa.Provider {
	// as we don't have any other provider, we can directly return the ontap_rest provider
	return vsa.NewProvider(ctx, vsa.ProviderDetails{
		IPAddress: node.EndpointAddress,
		UserName:  node.Username,
		Password:  node.Password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	})
}
