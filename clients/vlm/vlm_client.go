package vlm

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	vlmClient "netapp.com/vsa/lifecycle-manager/pkg/vlmclient"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

type ClientFactory interface {
	VSAClusterDeployCreate(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error
	VSASVMCreate(ctx context.Context, svmConfig *vlmconfig.SVMConfigParams) error
	VSAClusterDeploymentDelete(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error
	VSAClusterDeployGet(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error)
}

type Client struct {
	vlmClient *vlmClient.VLMClient
	traceLog  log.Logger
}

func NewClient(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) *Client {
	newVLMClient, err := vlmClient.NewVLMClient(ctx, vlmConfig)
	if err != nil {
		return nil
	}
	return &Client{
		vlmClient: newVLMClient,
		traceLog:  logger,
	}
}

func (c *Client) VSAClusterDeployCreate(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return nil
	}
	err := c.vlmClient.CreateVSAClusterDeployment(ctx, vlmConfig)
	if err != nil {
		c.traceLog.Errorf("Error creating VSA cluster deployment: %v", err)
		return err
	}
	return nil
}
func (c *Client) VSAClusterDeployGet(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) (*vlmconfig.VLMConfig, error) {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return nil, nil
	}
	vlmConfig, err := c.vlmClient.GetVSAClusterDeployment(ctx, vlmConfig)
	if err != nil {
		c.traceLog.Errorf("Error getting VSA cluster deployment: %v", err)
		return nil, err
	}
	return vlmConfig, nil
}

func (c *Client) VSAClusterDeploymentDelete(ctx context.Context, vlmConfig *vlmconfig.VLMConfig) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return nil
	}
	err := c.vlmClient.DeleteVSAClusterDeployment(ctx, vlmConfig)
	if err != nil {
		c.traceLog.Errorf("Error deleting VSA cluster deployment: %v", err)
		return err
	}
	return nil
}

func (c *Client) VSASVMCreate(ctx context.Context, svmConfig *vlmconfig.SVMConfigParams) error {
	if c.vlmClient == nil {
		c.traceLog.Errorf("VLM client is nil")
		return nil
	}
	err := c.vlmClient.CreateVSASVM(ctx, *svmConfig)
	if err != nil {
		c.traceLog.Errorf("Error creating VSA SVM: %v", err)
		return err
	}
	return nil
}
