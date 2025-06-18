package orchestrator

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"golang.org/x/net/context"
)

func GetProviderByNode(ctx context.Context, node *models.Node) vsa.Provider {
	var password string
	if node.SecretID != "" {
		password = activities.GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
	} else {
		password = node.Password
	}
	return vsa.NewProvider(vsa.ProviderDetails{
		IPAddress: node.EndpointAddress,
		UserName:  node.Username,
		Password:  password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	})
}
