package vsa

import (
	"context"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

const (
	expectedNodeCount  = 2
	ipSpaceName        = "Default"
	defaultNetmask     = "255.255.255.255"
	iscsiServicePolicy = "default-data-iscsi"
)

type Provider interface {
	GetONTAPVersion() (string, error)
	AreAllNodeUpAndRunning() (bool, error)
	IsAggregateOnline(aggregateName string) (bool, error)
	GetNodes() ([]*Node, error)
	GetNodeByName(name string) (*Node, error)
	CreateSVM(params CreateSvmParams) (*ProviderResponse, error)
	CreateDataLIF(params CreateLifParams) (*Lif, error)
	CreateNetworkIpRoute(params CreateNetworkIPRouteParams) error
	CreateVolume(params CreateVolumeParams) (*ProviderResponse, error)
	DeleteVolume(volumeUUID, volumeName string) error
	IgroupCreate(params IgroupCreateParams) (string, error)
	LunCreate(params LunCreateParams) (*ProviderResponse, error)
	LunMapCreate(params LunMapCreateParams) error
}

type OntapRestProvider struct {
	provider           ProviderDetails
	client             ontapRest.RESTClient
	InsecureSkipVerify bool
	Logger             log.Logger
}

func NewProvider(ctx context.Context, provider ProviderDetails) *OntapRestProvider {
	return &OntapRestProvider{
		provider: provider,
		client: ontapRest.NewOntapRestClient(ontapRest.RESTClientParams{
			Host:               provider.IPAddress,
			Username:           provider.UserName,
			Password:           log.Secret(provider.Password),
			InsecureSkipVerify: provider.InsecureSkipVerify,
			Trace:              ctx.Value(middleware.ContextSLoggerKey).(log.Logger),
		}),
		Logger: ctx.Value(middleware.ContextSLoggerKey).(log.Logger),
	}
}
