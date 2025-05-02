package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

const (
	expectedNodeCount  = 2
	ipSpaceName        = "Default"
	defaultNetmask     = "255.255.255.255"
	iscsiServicePolicy = "default-data-iscsi"
)

type Provider interface {
	GetONTAPVersion() (*string, error)
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
	Provider           ProviderDetails            `json:"Provider"`
	ClientParams       ontapRest.RESTClientParams `json:"ClientParams"`
	InsecureSkipVerify bool                       `json:"insecureSkipVerify"`
	Logger             *log.Slogger               `json:"-"`
}

func NewProvider(provider ProviderDetails) *OntapRestProvider {
	return &OntapRestProvider{
		Provider: provider,
		ClientParams: ontapRest.RESTClientParams{
			Host:               provider.IPAddress,
			Username:           provider.UserName,
			Password:           log.Secret(provider.Password),
			InsecureSkipVerify: provider.InsecureSkipVerify,
			Trace:              log.NewLogger().(*log.Slogger),
		},
		Logger: log.NewLogger().(*log.Slogger),
	}
}
